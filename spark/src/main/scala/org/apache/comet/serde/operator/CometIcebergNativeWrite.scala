/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.serde.operator

import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.comet.{CometIcebergWriteExec, CometNativeExec, IcebergWriteExec}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.exprToProto

/**
 * Detection + serde for Comet's **native** Iceberg V2 write path. Converts an
 * [[IcebergWriteExec]] (the JVM-path writer emitted by `IcebergWriteStrategy`) into a
 * [[CometIcebergWriteExec]] that drives iceberg-rust via Comet's native execution pipeline.
 *
 * Gated by [[CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED]] (off by default) and by a long list
 * of fall-back triggers (table format, encryption, custom location providers, bloom filters, ...)
 * -- every trigger is pinned by `CometIcebergWriteDetectionSuite`. `requiresNativeChildren =
 * true` so the conversion only fires when the upstream plan is fully Comet-native and Arrow
 * batches flow without an intermediate Spark-to-Arrow boundary.
 *
 * Spark 4.x's `ReplaceData` row stream needs a column-strip projection -- see
 * `dropNonDataColumns` below.
 */
object CometIcebergNativeWrite extends CometOperatorSerde[IcebergWriteExec] with Logging {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED)

  override def requiresNativeChildren: Boolean = true

  /**
   * Iceberg `TableProperties` keys looked up reflectively the first time the gate runs.
   * Centralising them here keeps the property strings identical to Iceberg's own canonical names
   * (rather than duplicating literals that could drift across versions).
   */
  object PropertyKeys {
    lazy val DefaultFileFormat: String =
      IcebergReflection.tablePropertyConstant("DEFAULT_FILE_FORMAT")
    lazy val DefaultFileFormatValue: String =
      IcebergReflection.tablePropertyConstant("DEFAULT_FILE_FORMAT_DEFAULT")
    lazy val ObjectStoreEnabled: String =
      IcebergReflection.tablePropertyConstant("OBJECT_STORE_ENABLED")
    lazy val WriteLocationProviderImpl: String =
      IcebergReflection.tablePropertyConstant("WRITE_LOCATION_PROVIDER_IMPL")
    lazy val DefaultWriteMetricsMode: String =
      IcebergReflection.tablePropertyConstant("DEFAULT_WRITE_METRICS_MODE")
    lazy val MetricsModeColumnPrefix: String =
      IcebergReflection.tablePropertyConstant("METRICS_MODE_COLUMN_CONF_PREFIX")
    lazy val ParquetBloomFilterMaxBytes: String =
      IcebergReflection.tablePropertyConstant("PARQUET_BLOOM_FILTER_MAX_BYTES")
    lazy val BloomFilterColumnEnabledPrefix: String =
      IcebergReflection.tablePropertyConstant("PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX")
    lazy val MetricsMaxInferredColumnDefaults: String =
      IcebergReflection.tablePropertyConstant("METRICS_MAX_INFERRED_COLUMN_DEFAULTS")
    lazy val MetricsMaxInferredColumnDefaultsValue: Int =
      IcebergReflection.tablePropertyIntConstant("METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT")
    lazy val ParquetRowGroupCheckMinRecordCount: String =
      IcebergReflection.tablePropertyConstant("PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT")
    lazy val ParquetRowGroupCheckMinRecordCountDefault: Int =
      IcebergReflection.tablePropertyIntConstant(
        "PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT_DEFAULT")
    lazy val ParquetRowGroupCheckMaxRecordCount: String =
      IcebergReflection.tablePropertyConstant("PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT")
    lazy val ParquetRowGroupCheckMaxRecordCountDefault: Int =
      IcebergReflection.tablePropertyIntConstant(
        "PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT_DEFAULT")
    // Properties referenced as literals -- owned by parquet-mr / a catalog layer outside Iceberg's
    // `TableProperties` enum, so there is no constant to quote.
    val ParquetEnableDictionary: String = "parquet.enable.dictionary"
    val FileIOImpl: String = "io-impl"
  }

  private val EncryptionPropertyPrefix = "encryption."
  private val CountsMetricsMode = "counts"
  private val NoneMetricsMode = "none"
  // Storage backends the native iceberg_common::storage_factory_for resolves. Anything else
  // (hdfs, abfs, abfss, wasb, wasbs, ...) is supported in iceberg-java but would throw at
  // runtime in native, so we gate on the URI scheme here.
  private val SupportedStorageSchemes: Set[String] =
    Set("file", "memory", "s3", "s3a", "gs", "oss")
  // V3 introduces row lineage / variant types / DVs / etc. that iceberg-rust does not emit.
  private val MinUnsupportedFormatVersion = 3

  override def getSupportLevel(op: IcebergWriteExec): SupportLevel = {
    val triggers = checkTriggers(op)
    triggers match {
      case Some(reason) => Unsupported(Some(reason))
      case None => Compatible(None)
    }
  }

  /**
   * Sequence of fall-back gates. Returns the first reason a write should fall back to the JVM
   * path, or `None` when all gates pass. Each gate mirrors a behaviour where iceberg-rust
   * diverges from iceberg-java; the goal is to write byte-identical Parquet files only when we
   * can.
   */
  private def checkTriggers(op: IcebergWriteExec): Option[String] = {
    val batchWrite = op.batchWrite
    if (!IcebergReflection.isIcebergBatchWrite(batchWrite)) {
      // Defensive: the strategy only emits IcebergWriteExec for Iceberg writes, but the
      // BatchWrite is opaque past `toBatch` -- a future Iceberg version could expose a different
      // class hierarchy here.
      return Some(
        s"BatchWrite is not an Iceberg `SparkWrite` (got ${batchWrite.getClass.getName})")
    }

    // `batchWrite` is an inner class of `SparkWrite` (e.g. `SparkWrite$BatchAppend`); the
    // `Table` reference lives on the outer instance.
    val sparkWrite = IcebergReflection
      .getOuterSparkWrite(batchWrite)
      .getOrElse(return Some("Could not unwrap outer `SparkWrite` from `BatchWrite`"))
    val table = IcebergReflection
      .getTableFromSparkWrite(sparkWrite)
      .getOrElse(return Some("Iceberg `SparkWrite.table` reflection returned null"))

    val properties = IcebergReflection
      .getTableProperties(table)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])

    val context = TriggerContext(table, properties, sparkWrite)
    triggers.iterator.flatMap(rule => rule(context)).find(_.nonEmpty)
  }

  // -- Trigger implementations -------------------------------------------------------------------

  /**
   * Inputs available to every trigger. `sparkWrite` is needed for cases where Iceberg's effective
   * value diverges from the raw table property (e.g. the resolved data file format comes from
   * `SparkWriteConf.dataFileFormat()`, not just `write.format.default`).
   */
  private case class TriggerContext(table: Any, properties: Map[String, String], sparkWrite: Any)

  private type TriggerRule = TriggerContext => Option[String]

  private lazy val triggers: Seq[TriggerRule] = Seq(
    requireFormatParquet,
    requirePropertyAbsentOrNotTrue(
      PropertyKeys.ObjectStoreEnabled,
      "iceberg-rust has no hashed-prefix object-storage location generator"),
    requirePropertyAbsent(
      PropertyKeys.WriteLocationProviderImpl,
      "custom location providers are not supported"),
    requireFormatVersionAtMostTwo,
    requireNoEncryptionPrefix,
    requireMetricsModeIsNotCounts,
    requireMetricsModeIsNotNone,
    requireNoPerColumnCountsMetricsMode,
    requireNoPerColumnNoneMetricsMode,
    requirePropertyAbsent(
      PropertyKeys.ParquetBloomFilterMaxBytes,
      "parquet-rs has no global bloom-filter byte cap; explicit value would diverge"),
    requireNoBloomFilterColumnsEnabled,
    requireSchemaWithinMaxInferredColumnDefaults,
    requireRowGroupCheckMinRecordCountAtDefault,
    requireRowGroupCheckMaxRecordCountAtDefault,
    requirePropertyAbsent(
      PropertyKeys.ParquetEnableDictionary,
      "parquet-rs follows parquet-mr defaults; an explicit override would diverge"),
    requireNoPerColumnStatsEnabled,
    requirePropertyAbsent(
      PropertyKeys.FileIOImpl,
      "custom FileIO is not honoured by the native writer's URI-scheme-based storage selection"),
    requireSupportedStorageScheme)

  /**
   * Iceberg's `SparkWriteConf.dataFileFormat()` checks the per-write `write-format` option BEFORE
   * falling back to the `write.format.default` table property. Gating on the raw table property
   * alone produces a false-pass (table default parquet, write option `orc` -> Java writes ORC,
   * native would write parquet) and a symmetric false-fall-back; both diverge from Java. Prefer
   * the resolved `SparkWrite.format` when available; fall back to the table property only when
   * reflection misses (e.g. an Iceberg version we have not vetted).
   */
  private val requireFormatParquet: TriggerRule = ctx => {
    val resolved = IcebergReflection.getFormatFromSparkWrite(ctx.sparkWrite)
    val effective = resolved.getOrElse {
      val key = PropertyKeys.DefaultFileFormat
      val default = PropertyKeys.DefaultFileFormatValue
      ctx.properties.getOrElse(key, default).toLowerCase(Locale.ROOT)
    }
    if (effective == "parquet") None
    else Some(s"resolved write format=$effective (only parquet is supported)")
  }

  private def requirePropertyAbsentOrNotTrue(key: String, reason: String): TriggerRule =
    ctx => {
      if (ctx.properties.get(key).exists(_.equalsIgnoreCase("true"))) {
        Some(s"$key=true ($reason)")
      } else {
        None
      }
    }

  private def requirePropertyAbsent(key: String, reason: String): TriggerRule =
    ctx => {
      if (ctx.properties.contains(key)) Some(s"$key is set ($reason)") else None
    }

  private val requireFormatVersionAtMostTwo: TriggerRule = ctx =>
    IcebergReflection.getFormatVersion(ctx.table) match {
      case Some(v) if v >= MinUnsupportedFormatVersion =>
        Some(
          s"format-version=$v introduces features the Rust writer cannot emit " +
            "(row lineage, variant types, deletion vectors, revised manifest rules)")
      case _ => None
    }

  private val requireNoEncryptionPrefix: TriggerRule = ctx =>
    ctx.properties.keys
      .find(_.startsWith(EncryptionPropertyPrefix))
      .map(k => s"$k is set (Parquet modular encryption not supported)")

  private val requireMetricsModeIsNotCounts: TriggerRule = ctx => {
    val key = PropertyKeys.DefaultWriteMetricsMode
    ctx.properties
      .get(key)
      .filter(_.toLowerCase(Locale.ROOT).contains(CountsMetricsMode))
      .map(v =>
        s"$key=$v mentions '$CountsMetricsMode' (Parquet has no counts-without-bounds mode)")
  }

  /**
   * iceberg-java with `metrics.default=none` emits a `DataFile` with no per-column metrics.
   * iceberg-rust always populates `column_sizes` / `value_counts` / `null_value_counts` / bounds
   * from the parquet footer regardless of metrics mode (`parquet_writer.rs:363-397`), so a native
   * write would produce manifests strictly richer than Java's. Fall back rather than diverge.
   */
  private val requireMetricsModeIsNotNone: TriggerRule = ctx => {
    val key = PropertyKeys.DefaultWriteMetricsMode
    ctx.properties
      .get(key)
      .filter(_.trim.toLowerCase(Locale.ROOT) == NoneMetricsMode)
      .map(_ => s"$key=$NoneMetricsMode (iceberg-rust always populates per-column metrics)")
  }

  private val requireNoPerColumnCountsMetricsMode: TriggerRule = ctx => {
    val prefix = PropertyKeys.MetricsModeColumnPrefix
    ctx.properties
      .find { case (k, v) =>
        k.startsWith(prefix) && v.toLowerCase(Locale.ROOT) == CountsMetricsMode
      }
      .map { case (k, _) => s"$k=$CountsMetricsMode (counts-only mode not supported)" }
  }

  /** Same rationale as the default-mode `none` gate, applied per column. */
  private val requireNoPerColumnNoneMetricsMode: TriggerRule = ctx => {
    val prefix = PropertyKeys.MetricsModeColumnPrefix
    ctx.properties
      .find { case (k, v) =>
        k.startsWith(prefix) && v.trim.toLowerCase(Locale.ROOT) == NoneMetricsMode
      }
      .map { case (k, _) =>
        s"$k=$NoneMetricsMode (iceberg-rust always populates per-column metrics)"
      }
  }

  private val requireNoBloomFilterColumnsEnabled: TriggerRule = ctx => {
    val prefix = PropertyKeys.BloomFilterColumnEnabledPrefix
    ctx.properties
      .find { case (k, v) => k.startsWith(prefix) && v.equalsIgnoreCase("true") }
      .map { case (k, _) =>
        s"$k=true (iceberg-rust cannot enforce iceberg-java's bloom-filter byte cap)"
      }
  }

  /**
   * Per-column parquet stats-enabled overrides. `write.parquet.stats-enabled.column.<col>`
   * (`TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX`) was only added in Iceberg 1.10.0; on
   * 1.5.2 / 1.8.1 the key is silently ignored by Iceberg-Java, so a native write matches Java
   * without falling back. Gate only on the versions where the property has an effect -- detected
   * by the presence of the constant rather than hard-coding a version.
   */
  private val requireNoPerColumnStatsEnabled: TriggerRule = ctx =>
    IcebergReflection.tablePropertyConstantOpt("PARQUET_COLUMN_STATS_ENABLED_PREFIX").flatMap {
      prefix =>
        ctx.properties
          .find { case (k, _) => k.startsWith(prefix) }
          .map { case (k, v) =>
            s"$k=$v (per-column parquet stats overrides not honoured by the native writer)"
          }
    }

  /**
   * Native iceberg_common::storage_factory_for resolves only a fixed set of URI schemes; tables
   * whose data location uses any other scheme (hdfs, abfs, wasb, ...) would plan natively then
   * crash at write time. Gate on the URI scheme of `Table.locationProvider().newDataLocation()`.
   */
  private val requireSupportedStorageScheme: TriggerRule = ctx => {
    val location = IcebergReflection.getDataLocation(ctx.table).getOrElse("")
    val scheme = if (location.contains("://")) {
      location.substring(0, location.indexOf("://")).toLowerCase(Locale.ROOT)
    } else {
      "file"
    }
    if (SupportedStorageSchemes.contains(scheme)) {
      None
    } else {
      Some(
        s"data location scheme '$scheme' is not supported by the native writer " +
          s"(supported: ${SupportedStorageSchemes.toSeq.sorted.mkString(", ")})")
    }
  }

  private val requireSchemaWithinMaxInferredColumnDefaults: TriggerRule = ctx => {
    // Iceberg only applies the inferred-column truncation (first N projected IDs -> truncate(16),
    // every column beyond -> MetricsModes.None) when NO explicit `write.metadata.metrics.default`
    // is configured: a user-set default is applied to all columns regardless of count
    // (`MetricsConfig.from`). So this gate is irrelevant once a default mode is present -- the
    // metrics-mode gates above already cover whether that mode itself is supported.
    if (ctx.properties.contains(PropertyKeys.DefaultWriteMetricsMode)) {
      None
    } else {
      val key = PropertyKeys.MetricsMaxInferredColumnDefaults
      val limit =
        parseIntProperty(ctx.properties, key, PropertyKeys.MetricsMaxInferredColumnDefaultsValue)
      (for {
        schema <- IcebergReflection.getSchema(ctx.table)
        count <- IcebergReflection.getProjectedFieldIdCount(schema)
        reason <-
          if (count > limit) {
            Some(
              s"schema has $count projected field IDs which exceeds $key=$limit " +
                "(iceberg-java applies MetricsModes.None to columns beyond this index)")
          } else {
            None
          }
      } yield reason)
    }
  }

  private def parseIntProperty(props: Map[String, String], key: String, default: Int): Int =
    props.get(key).flatMap(s => scala.util.Try(s.trim.toInt).toOption).getOrElse(default)

  private val requireRowGroupCheckMinRecordCountAtDefault: TriggerRule =
    requireIntPropertyAtDefault(
      PropertyKeys.ParquetRowGroupCheckMinRecordCount,
      PropertyKeys.ParquetRowGroupCheckMinRecordCountDefault,
      "parquet-rs uses a different row-group sizing cadence; non-default value would diverge")

  private val requireRowGroupCheckMaxRecordCountAtDefault: TriggerRule =
    requireIntPropertyAtDefault(
      PropertyKeys.ParquetRowGroupCheckMaxRecordCount,
      PropertyKeys.ParquetRowGroupCheckMaxRecordCountDefault,
      "parquet-rs uses a different row-group sizing cadence; non-default value would diverge")

  private def requireIntPropertyAtDefault(
      key: String,
      default: Int,
      reason: String): TriggerRule = ctx =>
    ctx.properties.get(key).flatMap { raw =>
      scala.util.Try(raw.trim.toInt).toOption match {
        case Some(v) if v != default => Some(s"$key=$v (default=$default; $reason)")
        case Some(_) => None
        case None => Some(s"$key=$raw is not an int ($reason)")
      }
    }

  // -- Conversion --------------------------------------------------------------------------------

  override def convert(
      op: IcebergWriteExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    val _ = (builder, childOp) // unused: we synthesise our own FFI scan child below
    try {
      for {
        icebergWrite <- buildIcebergWriteProto(op)
        ffiScan <- buildFfiScan(op)
        writeChild <- dropNonDataColumns(op, ffiScan)
      } yield OperatorOuterClass.Operator
        .newBuilder()
        .setPlanId(op.id)
        .addChildren(writeChild)
        .setIcebergWrite(icebergWrite)
        .build()
    } catch {
      case e: Exception =>
        withFallbackReason(op, s"Failed to convert Iceberg native write: ${e.getMessage}")
        None
    }
  }

  /**
   * Spark 4.x rewrites CoW DML (`ReplaceData`) into a row stream with extra columns -- column 0
   * carries the per-row operation code (`__row_operation`: 5=WRITE, 6=WRITE_WITH_METADATA) and
   * the tail of the row carries file/partition metadata (`_file`, `_spec_id`, `_partition`). The
   * JVM-side `IcebergWriteExec.runReplaceDataWriter` handles this row-by-row by applying
   * `dispatch.rowProjection` before invoking the writer.
   *
   * The native path forwards Arrow batches as-is to the iceberg-rust writer, which expects
   * exactly the Iceberg table's data columns. Without an explicit projection step we end up
   * giving it the wider row (e.g. 6 columns when the schema has 3) and
   * `decorate_batch_with_field_ids` rejects the batch.
   *
   * For Spark 3.4 / 3.5 the strategy shim returns `None` for `replaceDataDispatch` and the
   * upstream plan already projects to the data columns -- no extra projection needed. For 4.x we
   * splice a `Projection` proto between our `IcebergWrite` op and the FFI `Scan`, selecting the
   * upstream attributes whose names match the Iceberg schema's columns. The JVM-side child stays
   * at the original wide output, so its `executeColumnar()` still emits the wide batches the FFI
   * scan declares; the projection then strips them inside the native runtime before the writer
   * sees the data.
   */
  private def dropNonDataColumns(
      op: IcebergWriteExec,
      scan: OperatorOuterClass.Operator): Option[OperatorOuterClass.Operator] = {
    if (op.replaceDataDispatch.isEmpty) return Some(scan)

    val sparkWrite = IcebergReflection.getOuterSparkWrite(op.batchWrite).getOrElse {
      withFallbackReason(op, "Could not unwrap outer SparkWrite for ReplaceData projection")
      return None
    }
    val writeSchema = IcebergReflection.getWriteSchemaFromSparkWrite(sparkWrite).getOrElse {
      withFallbackReason(
        op,
        "SparkWrite.writeSchema reflection failed for ReplaceData projection")
      return None
    }
    val dataFieldNames = IcebergReflection.getSchemaFieldNames(writeSchema).getOrElse {
      withFallbackReason(
        op,
        "Could not extract Iceberg schema column names for ReplaceData projection")
      return None
    }
    val upstreamOutput = op.child.output
    val missing = dataFieldNames.filterNot(name => upstreamOutput.exists(_.name == name))
    if (missing.nonEmpty) {
      withFallbackReason(
        op,
        s"ReplaceData projection: columns ${missing.mkString("[", ", ", "]")} not in upstream " +
          s"output ${upstreamOutput.map(_.name).mkString("[", ", ", "]")}")
      return None
    }
    val projectList = dataFieldNames.map(name => upstreamOutput.find(_.name == name).get)
    val protoExprs = projectList.map(attr => exprToProto(attr, upstreamOutput))
    if (!protoExprs.forall(_.isDefined)) {
      withFallbackReason(op, "Could not serialise ReplaceData projection attributes to proto")
      return None
    }
    val projection = OperatorOuterClass.Projection
      .newBuilder()
      .addAllProjectList(protoExprs.map(_.get).asJava)
      .build()
    Some(
      OperatorOuterClass.Operator
        .newBuilder()
        .setPlanId(op.id)
        .addChildren(scan)
        .setProjection(projection)
        .build())
  }

  private def buildFfiScan(op: IcebergWriteExec): Option[OperatorOuterClass.Operator] = {
    val scan =
      NativeWriteUtils.buildFfiScan(op.child, op.id, NativeWriteUtils.isUpstreamFfiSafe(op.child))
    if (scan.isEmpty) {
      withFallbackReason(
        op,
        "Cannot serialize upstream data types for Iceberg native write FFI scan")
    }
    scan
  }

  override def createExec(nativeOp: Operator, op: IcebergWriteExec): CometNativeExec = {
    val sparkWrite = IcebergReflection
      .getOuterSparkWrite(op.batchWrite)
      .getOrElse(
        throw new IllegalStateException(
          "Native Iceberg write conversion: could not unwrap outer SparkWrite from BatchWrite"))
    val table = IcebergReflection
      .getTableFromSparkWrite(sparkWrite)
      .getOrElse(
        throw new IllegalStateException(
          "Native Iceberg write conversion: SparkWrite.table reflection failed"))
    val outputSpecId = IcebergReflection
      .getOutputSpecIdFromSparkWrite(sparkWrite)
      .getOrElse(
        throw new IllegalStateException(
          "Native Iceberg write conversion: SparkWrite.outputSpecId reflection failed"))
    CometIcebergWriteExec(
      nativeOp,
      op.child,
      op.batchWrite,
      table.asInstanceOf[AnyRef],
      outputSpecId)
  }

  /**
   * Assemble the per-write `IcebergWrite` protobuf. All reflection calls are localised here so a
   * missing accessor surfaces as a `withFallbackReason` fall-back rather than a planning-time
   * crash.
   *
   * Note: even though `getSupportLevel` is currently returning `Unsupported`, this method stays
   * fully implemented so the moment the Avro decoder lands the native path engages without
   * additional plumbing work.
   */
  private def buildIcebergWriteProto(
      op: IcebergWriteExec): Option[OperatorOuterClass.IcebergWrite] = {
    val sparkWrite = IcebergReflection.getOuterSparkWrite(op.batchWrite).getOrElse {
      withFallbackReason(op, "Could not unwrap outer SparkWrite from BatchWrite")
      return None
    }
    val table = IcebergReflection.getTableFromSparkWrite(sparkWrite).getOrElse {
      withFallbackReason(op, "Could not extract Iceberg Table from SparkWrite")
      return None
    }

    val properties = IcebergReflection
      .getTableProperties(table)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])
    val catalogProperties =
      IcebergReflection.getFileIOProperties(table).getOrElse(Map.empty[String, String])

    val metadataLocation = IcebergReflection.getMetadataLocation(table).getOrElse {
      withFallbackReason(op, "Iceberg Table has no metadata location (metadata-table scan?)")
      return None
    }
    val outputSpecId = IcebergReflection.getOutputSpecIdFromSparkWrite(sparkWrite).getOrElse {
      withFallbackReason(op, "SparkWrite.outputSpecId reflection failed")
      return None
    }
    val partitionSpec = IcebergReflection.getPartitionSpecById(table, outputSpecId).getOrElse {
      withFallbackReason(op, s"No partition spec found for id=$outputSpecId")
      return None
    }
    val partitionSpecJson = IcebergReflection.partitionSpecToJson(partitionSpec).getOrElse {
      withFallbackReason(op, "PartitionSpecParser.toJson failed")
      return None
    }
    val writeSchema = IcebergReflection.getWriteSchemaFromSparkWrite(sparkWrite).getOrElse {
      withFallbackReason(op, "SparkWrite.writeSchema reflection failed")
      return None
    }
    val icebergSchemaJson = IcebergReflection.schemaToJson(writeSchema).getOrElse {
      withFallbackReason(op, "SchemaParser.toJson failed")
      return None
    }
    // Iceberg's Spark `SparkWrite$WriterFactory` does NOT wire the table sort order into the
    // per-file writer factory for batch appends in any Iceberg version Comet targets (1.5.2 /
    // 1.8.1 / 1.10.0): it builds `SparkFileWriterFactory` without `.dataSortOrder(...)`, so every
    // committed data file is stamped `sort_order_id = 0` (unsorted) even when the table itself has
    // a non-default sort order. We match that exactly. The
    // `SparkWriteConf.outputSortOrderId(writeRequirements)` resolver (explicit option / table order
    // when an ordering is required / unsorted) and the matching `.dataSortOrder(...)` wiring only
    // exist in Iceberg 1.11+; reflect the resolver when it is present so this stays correct if the
    // pinned runtime is bumped, otherwise default to 0.
    val sortOrderId =
      IcebergReflection.getOutputSortOrderIdFromSparkWrite(sparkWrite).getOrElse(0)
    val dataLocation = IcebergReflection.getDataLocation(table).getOrElse {
      withFallbackReason(op, "Table.locationProvider().newDataLocation reflection failed")
      return None
    }
    val operationId = IcebergReflection.getOperationIdFromSparkWrite(sparkWrite).getOrElse {
      withFallbackReason(op, "SparkWrite.queryId reflection failed")
      return None
    }
    val targetFileSize =
      IcebergReflection.getTargetFileSizeFromSparkWrite(sparkWrite).getOrElse {
        withFallbackReason(op, "SparkWrite.targetFileSize reflection failed")
        return None
      }
    val useFanoutWriter =
      IcebergReflection.getUseFanoutWriterFromSparkWrite(sparkWrite).getOrElse {
        withFallbackReason(op, "SparkWrite.useFanoutWriter reflection failed")
        return None
      }
    val specIsUnpartitioned = isUnpartitionedSpec(partitionSpec)
    val writerMode = IcebergWriteProtoTranslation.resolveWriterMode(
      specIsUnpartitioned = specIsUnpartitioned,
      useFanoutWriter = useFanoutWriter)

    val createdBy = s"Apache Iceberg ${IcebergReflection.icebergVersion()} (Comet)"
    // Iceberg's `RegistryBasedFileWriterFactory` merges resolved write properties (codec, level,
    // and other effective settings carried on `SparkWrite`) over the table's properties when
    // building the per-file writer. Mirror that merge here so per-write options (e.g.
    // `option("write-parquet-compression-codec", "gzip")`) survive into the native writer.
    val resolvedWriteProperties =
      IcebergReflection.getWritePropertiesFromSparkWrite(sparkWrite).getOrElse(Map.empty)
    val effectiveProperties = properties ++ resolvedWriteProperties
    val parquetSettings =
      IcebergWriteProtoTranslation.buildParquetSettings(effectiveProperties, createdBy)

    val common = IcebergWriteProtoTranslation.buildCommon(
      catalogProperties = catalogProperties,
      metadataLocation = metadataLocation,
      icebergSchemaJson = icebergSchemaJson,
      partitionSpecJson = partitionSpecJson,
      sortOrderId = sortOrderId,
      dataLocation = dataLocation,
      operationId = operationId,
      targetFileSizeBytes = targetFileSize,
      writerMode = writerMode,
      parquetSettings = parquetSettings,
      catalogName = IcebergReflection.deriveCatalogName(table))

    Some(OperatorOuterClass.IcebergWrite.newBuilder().setCommon(common).build())
  }

  /**
   * `PartitionSpec.isUnpartitioned()` -- accessed reflectively because Iceberg is `test`-scoped
   * on the main source classpath.
   */
  private def isUnpartitionedSpec(spec: Any): Boolean =
    try {
      spec.getClass.getMethod("isUnpartitioned").invoke(spec).asInstanceOf[Boolean]
    } catch {
      case _: Exception =>
        val fields = spec.getClass
          .getMethod("fields")
          .invoke(spec)
          .asInstanceOf[java.util.List[_]]
        fields.isEmpty
    }
}
