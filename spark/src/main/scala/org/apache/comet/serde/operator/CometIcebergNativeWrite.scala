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
import org.apache.spark.sql.comet.{CometNativeExec, IcebergWriteExec}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Fall-back-detection serde for Comet's native Iceberg V2 write path. Inspects an
 * [[IcebergWriteExec]] (the JVM-path file writer exec emitted by `IcebergWriteStrategy`) and
 * reports a [[SupportLevel]] capturing whether the table's properties and write shape allow
 * delegating the per-task Parquet write to iceberg-rust.
 *
 * Gated by [[CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED]] (off by default). Every documented
 * fall-back trigger is pinned by `CometIcebergWriteDetectionSuite`. The `convert` and
 * `createExec` overrides are stubs at this stage -- the native exec lands in a follow-up commit;
 * for now writes always go through the JVM two-op path regardless of the gate.
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
    // Properties referenced as literals -- either not promoted to `TableProperties` constants in
    // all supported Iceberg versions (1.5.2 / 1.8.1) or owned by parquet-mr / a catalog layer
    // outside Iceberg's `TableProperties` enum.
    val ParquetPageVersion: String = "write.parquet.page-version"
    val ParquetPageVersionDefault: String = "v1"
    val ParquetColumnStatsEnabledPrefix: String = "write.parquet.stats-enabled.column."
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
      return Some(
        s"BatchWrite is not an Iceberg `SparkWrite` (got ${batchWrite.getClass.getName})")
    }

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
    requireParquetPageVersionDefault,
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
   * Java's `write.parquet.page-version=v2` switches Parquet to DataPageV2 (with RLE_DICTIONARY
   * and per-page null counts) -- a format-level divergence parquet-rs's default writer does not
   * mirror.
   */
  private val requireParquetPageVersionDefault: TriggerRule = ctx => {
    val key = PropertyKeys.ParquetPageVersion
    val default = PropertyKeys.ParquetPageVersionDefault
    ctx.properties.get(key).filter(_.trim.toLowerCase(Locale.ROOT) != default).map { v =>
      s"$key=$v (parquet-rs writer does not implement DataPageV2)"
    }
  }

  /** Per-column parquet stats-enabled overrides not represented in the native settings. */
  private val requireNoPerColumnStatsEnabled: TriggerRule = ctx => {
    val prefix = PropertyKeys.ParquetColumnStatsEnabledPrefix
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
    val key = PropertyKeys.MetricsMaxInferredColumnDefaults
    val limit =
      parseIntProperty(ctx.properties, key, PropertyKeys.MetricsMaxInferredColumnDefaultsValue)
    for {
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
    } yield reason
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

  override def convert(
      op: IcebergWriteExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    val _ = (op, builder, childOp)
    None
  }

  override def createExec(nativeOp: Operator, op: IcebergWriteExec): CometNativeExec =
    throw new UnsupportedOperationException(
      "Native Iceberg write exec is not yet implemented; use the JVM two-op path")
}
