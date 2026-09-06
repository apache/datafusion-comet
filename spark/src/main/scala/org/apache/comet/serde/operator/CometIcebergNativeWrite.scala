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
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.comet.{CometIcebergWriteExec, CometNativeExec, IcebergWriteExec}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.exprToProto

object CometIcebergNativeWrite extends CometOperatorSerde[IcebergWriteExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED)

  override def requiresNativeChildren: Boolean = true

  object PropertyKeys {
    lazy val ObjectStoreEnabled: String =
      IcebergReflection.tablePropertyConstant("OBJECT_STORE_ENABLED")
    lazy val WriteLocationProviderImpl: String =
      IcebergReflection.tablePropertyConstant("WRITE_LOCATION_PROVIDER_IMPL")
    lazy val BloomFilterColumnEnabledPrefix: String =
      IcebergReflection.tablePropertyConstant("PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX")
    lazy val ParquetBloomFilterMaxBytes: String =
      IcebergReflection.tablePropertyConstant("PARQUET_BLOOM_FILTER_MAX_BYTES")
    val ParquetBloomFilterColumnFppPrefix = "write.parquet.bloom-filter-fpp.column."
    val ParquetBloomFilterColumnNdvPrefix = "write.parquet.bloom-filter-ndv.column."
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
    lazy val ParquetCompressionCodec: String =
      IcebergReflection.tablePropertyConstant("PARQUET_COMPRESSION")
    lazy val ParquetCompressionLevel: String =
      IcebergReflection.tablePropertyConstant("PARQUET_COMPRESSION_LEVEL")
    lazy val ParquetRowGroupSizeBytes: String =
      IcebergReflection.tablePropertyConstant("PARQUET_ROW_GROUP_SIZE_BYTES")
    lazy val ParquetPageSizeBytes: String =
      IcebergReflection.tablePropertyConstant("PARQUET_PAGE_SIZE_BYTES")
    lazy val ParquetPageRowLimit: String =
      IcebergReflection.tablePropertyConstant("PARQUET_PAGE_ROW_LIMIT")
    lazy val ParquetDictSizeBytes: String =
      IcebergReflection.tablePropertyConstant("PARQUET_DICT_SIZE_BYTES")
    val ParquetPageVersion: String = "write.parquet.page-version"
    val ParquetPageVersionDefault: String = "v1"
    val ParquetShredVariants: String = "write.parquet.shred-variants"
    val ParquetVariantBufferSize: String = "write.parquet.variant-inference-buffer-size"
    val ParquetEnableDictionary: String = "parquet.enable.dictionary"
    val FileIOImpl: String = "io-impl"
  }

  private val EncryptionPropertyPrefix = "encryption."
  private val UnsupportedWriteTypeIds: Set[String] = Set("UUID")
  // `oss` is deliberately absent: iceberg-rust has an OSS backend, but Comet does not forward
  // `oss.*` catalog properties to it and no functional test covers the path, so an OSS write
  // could silently drop endpoint/credential configuration. Fail closed until it is covered.
  private val SupportedStorageSchemes: Set[String] =
    Set("file", "memory", "s3", "s3a", "gs")
  private val MinUnsupportedFormatVersion = 3
  private val ParquetWritePropertyPrefix = "write.parquet."
  private val ParquetMrPropertyPrefix = "parquet."

  // Hadoop-side `parquet.*` keys that iceberg-java's writer never consumes, so seeing them
  // in the session Hadoop configuration does not indicate the native writer would diverge.
  // `parquet.hadoop.vectored.io.enabled` is a reader-side vectored-IO knob declared by
  // parquet-hadoop as `ParquetInputFormat.HADOOP_VECTORED_IO_ENABLED` (default `true` in
  // parquet-hadoop 1.16+) and only consulted by parquet-mr's Hadoop reader path. Keep it
  // out of the writer-compatibility gate so that environments which seed it into the
  // session Hadoop configuration do not silently disable native Iceberg writes.
  private val IgnoredHadoopParquetConfKeys: Set[String] = Set(
    "parquet.hadoop.vectored.io.enabled")

  private lazy val vettedParquetWriteKeys: Set[String] = Set(
    PropertyKeys.ParquetCompressionCodec,
    PropertyKeys.ParquetCompressionLevel,
    PropertyKeys.ParquetRowGroupSizeBytes,
    PropertyKeys.ParquetPageSizeBytes,
    PropertyKeys.ParquetPageRowLimit,
    PropertyKeys.ParquetDictSizeBytes,
    PropertyKeys.ParquetRowGroupCheckMinRecordCount,
    PropertyKeys.ParquetRowGroupCheckMaxRecordCount,
    PropertyKeys.ParquetPageVersion,
    PropertyKeys.ParquetShredVariants,
    PropertyKeys.ParquetVariantBufferSize,
    PropertyKeys.ParquetBloomFilterMaxBytes)

  private lazy val vettedParquetWritePrefixes: Seq[String] =
    Seq(
      PropertyKeys.BloomFilterColumnEnabledPrefix,
      PropertyKeys.ParquetBloomFilterColumnFppPrefix,
      PropertyKeys.ParquetBloomFilterColumnNdvPrefix)

  override def getSupportLevel(op: IcebergWriteExec): SupportLevel =
    try {
      checkTriggers(op) match {
        case Some(reason) => Unsupported(Some(reason))
        case None => Compatible(None)
      }
    } catch {
      case NonFatal(e) =>
        Unsupported(Some(s"Iceberg native write detection failed: ${e.getMessage}"))
    }

  private def checkTriggers(op: IcebergWriteExec): Option[String] = {
    val batchWrite = op.batchWrite
    if (!IcebergReflection.isIcebergBatchWrite(batchWrite)) {
      return Some(s"not an Iceberg SparkWrite: ${batchWrite.getClass.getName}")
    }

    val sparkWrite = IcebergReflection
      .getOuterSparkWrite(batchWrite)
      .getOrElse(return Some("could not unwrap SparkWrite"))
    val table = IcebergReflection
      .getTableFromSparkWrite(sparkWrite)
      .getOrElse(return Some("SparkWrite.table is null"))

    val tableProperties = IcebergReflection
      .getTableProperties(table)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])
    val writeProperties = IcebergReflection
      .getWritePropertiesFromSparkWrite(sparkWrite)
      .getOrElse(return Some("could not read SparkWrite.writeProperties"))

    val context = TriggerContext(
      table,
      tableProperties ++ writeProperties,
      sparkWrite,
      op.session.sessionState.newHadoopConf())
    triggers.iterator.map(rule => rule(context)).collectFirst { case Some(reason) => reason }
  }

  private case class TriggerContext(
      table: Any,
      properties: Map[String, String],
      sparkWrite: Any,
      hadoopConf: Configuration)

  private type TriggerRule = TriggerContext => Option[String]

  private lazy val triggers: Seq[TriggerRule] = Seq(
    requireFormatParquet,
    requirePropertyAbsentOrNotTrue(
      PropertyKeys.ObjectStoreEnabled,
      "object-storage layout unsupported"),
    requirePropertyAbsent(
      PropertyKeys.WriteLocationProviderImpl,
      "custom location provider unsupported"),
    requireFormatVersionAtMostTwo,
    requireNoUuidColumns,
    requireNoEncryptionPrefix,
    requireRowGroupCheckMinRecordCountAtDefault,
    requireRowGroupCheckMaxRecordCountAtDefault,
    requireParquetPageVersionDefault,
    requireShredVariantsDisabled,
    requireNativeSupportedCompressionLevel,
    requireNativeSupportedBloomFilterProperties,
    requireOnlyVettedParquetWriteProperties,
    requirePropertyAbsent(
      PropertyKeys.ParquetEnableDictionary,
      "dictionary override unsupported"),
    requireNoUnvettedParquetMrProperties,
    requirePropertyAbsent(PropertyKeys.FileIOImpl, "custom FileIO unsupported"),
    requireRecognizedTableFileIO,
    requirePlaintextEncryptionManager,
    requirePositiveIntParquetSizes,
    requireNoParquetHadoopConfOverrides,
    requireSupportedStorageScheme,
    requireExecutorReflectionResolvable)

  private val requireFormatParquet: TriggerRule = ctx =>
    IcebergReflection.getFormatFromSparkWrite(ctx.sparkWrite) match {
      case None => Some("could not resolve the effective write format from SparkWrite")
      case Some("parquet") => None
      case Some(other) => Some(s"resolved write format=$other (only parquet is supported)")
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
      case Some(v) if v >= MinUnsupportedFormatVersion => Some(s"format-version=$v unsupported")
      case Some(_) => None
      case None => Some("could not determine the table format-version")
    }

  // Iceberg maps `uuid` to Spark's StringType, so the native writer would receive a Utf8 column
  // while iceberg-rust's target Arrow schema demands FixedSizeBinary(16) -- no Arrow cast bridges
  // the two, so the write would pass detection and then fail the task. Decline it up front. This
  // is the only Spark-writable Iceberg type with such a mismatch: `fixed(N)` arrives as Binary
  // and casts to FixedSizeBinary(N), and the V3-only types are excluded by the format-version
  // gate.
  private val requireNoUuidColumns: TriggerRule = ctx =>
    IcebergReflection
      .getWriteSchemaFromSparkWrite(ctx.sparkWrite)
      .orElse(IcebergReflection.getSchema(ctx.table)) match {
      case None => Some("could not resolve the write schema for column type checking")
      case Some(schema) =>
        IcebergReflection
          .findFieldWithTypeIds(schema, UnsupportedWriteTypeIds)
          .map { case (name, typeId) =>
            s"column $name has Iceberg type ${typeId.toLowerCase(Locale.ROOT)}, " +
              "which the native writer cannot reproduce"
          }
    }

  private val requireNoEncryptionPrefix: TriggerRule = ctx =>
    ctx.properties.keys
      .find(_.startsWith(EncryptionPropertyPrefix))
      .map(k => s"$k set: encryption unsupported")

  // No metrics-mode gate: manifest `DataFile` metrics are re-derived on the JVM from the
  // written parquet footers with Iceberg's own `MetricsConfig` logic before commit (see
  // `CometIcebergWriteExec`), so every `write.metadata.metrics.*` value behaves exactly as it
  // does on the iceberg-java path.

  private val requireParquetPageVersionDefault: TriggerRule = ctx => {
    val key = PropertyKeys.ParquetPageVersion
    ctx.properties
      .get(key)
      .filter(_.trim.toLowerCase(Locale.ROOT) != PropertyKeys.ParquetPageVersionDefault)
      .map(v => s"$key=$v unsupported")
  }

  private val requireShredVariantsDisabled: TriggerRule = ctx => {
    val key = PropertyKeys.ParquetShredVariants
    ctx.properties
      .get(key)
      .filter(_.equalsIgnoreCase("true"))
      .map(_ => s"$key=true (variant shredding changes the parquet schema)")
  }

  // iceberg-java never validates the level -- the raw string flows into a codec-specific
  // parquet-mr writer property -- while parquet-rs enforces per-codec ranges when the native
  // writer is built. A level the JVM writer accepts (zstd 0, a negative zstd fast level) must
  // not become a mid-task native failure, and a non-integer must fail on the stock path; both
  // fall back. Range logic lives beside the codec resolution in IcebergWriteProtoTranslation.
  private val requireNativeSupportedCompressionLevel: TriggerRule = ctx =>
    IcebergWriteProtoTranslation.compressionLevelRejection(ctx.properties)

  // These are Apache Parquet Java BlockSplitBloomFilter implementation bounds, not Iceberg
  // TableProperties constants, so they cannot be obtained through IcebergReflection:
  // scalastyle:off line.size.limit
  // https://github.com/apache/parquet-java/blob/78a8d3230eb4769db93de5f2f2e18363c04cae81/parquet-column/src/main/java/org/apache/parquet/column/values/bloomfilter/BlockSplitBloomFilter.java#L40-L50
  // scalastyle:on line.size.limit
  private val MinBloomFilterBytes = 32
  private val MaxBloomFilterBytes = 128 * 1024 * 1024

  /**
   * parquet-rs 58.x represents Bloom filters as a power-of-two number of bytes. parquet-mr
   * accepts arbitrary caps and, when one binds, serializes that exact length. Keep those writes
   * on the classic path instead of silently changing the number of usable Bloom blocks.
   */
  private val requireNativeSupportedBloomFilterProperties: TriggerRule = ctx => {
    // The FPP constant is absent from Iceberg 1.5.2, and the NDV constant is absent through
    // 1.10. Use the literal prefixes to detect the properties, then optional reflection to check
    // capability: an older runtime ignores an explicit property, so that write must fall back.
    val unavailableRuntimeProperty = Seq(
      PropertyKeys.ParquetBloomFilterColumnFppPrefix ->
        "PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX",
      PropertyKeys.ParquetBloomFilterColumnNdvPrefix ->
        "PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX").collectFirst {
      case (prefix, constant)
          if ctx.properties.keys.exists(_.startsWith(prefix)) &&
            IcebergReflection.tablePropertyConstantOpt(constant).isEmpty =>
        s"$prefix* is not interpreted by the Iceberg version on the classpath"
    }
    val maxRejection =
      ctx.properties.get(PropertyKeys.ParquetBloomFilterMaxBytes).flatMap { raw =>
        scala.util.Try(java.lang.Integer.parseInt(raw)).toOption match {
          case None => Some(s"${PropertyKeys.ParquetBloomFilterMaxBytes}=$raw is not a Java int")
          case Some(value)
              if value < MinBloomFilterBytes || value > MaxBloomFilterBytes ||
                (value & (value - 1)) != 0 =>
            Some(
              s"${PropertyKeys.ParquetBloomFilterMaxBytes}=$value must be a power of two " +
                s"in [$MinBloomFilterBytes, $MaxBloomFilterBytes] for native writes")
          case Some(_) => None
        }
      }

    maxRejection.orElse(unavailableRuntimeProperty).orElse {
      val maxBytes = ctx.properties
        .get(PropertyKeys.ParquetBloomFilterMaxBytes)
        .flatMap(raw => scala.util.Try(java.lang.Integer.parseInt(raw)).toOption)
        .getOrElse(1024 * 1024)
      val enabled = ctx.properties.iterator.collect {
        case (key, value)
            if key.startsWith(PropertyKeys.BloomFilterColumnEnabledPrefix) &&
              value.equalsIgnoreCase("true") =>
          key.substring(PropertyKeys.BloomFilterColumnEnabledPrefix.length)
      }.toSeq
      enabled.iterator
        .flatMap { column =>
          val fppKey = PropertyKeys.ParquetBloomFilterColumnFppPrefix + column
          val ndvKey = PropertyKeys.ParquetBloomFilterColumnNdvPrefix + column
          val fppError = ctx.properties.get(fppKey).flatMap { raw =>
            scala.util.Try(java.lang.Double.parseDouble(raw)).toOption match {
              case Some(value)
                  if value > 0.0d && value < 1.0d && java.lang.Double.isFinite(value) &&
                    bloomFilterSizesRepresentable(maxBytes, value) =>
                None
              case Some(value)
                  if value > 0.0d && value < 1.0d && java.lang.Double.isFinite(value) =>
                Some(s"$fppKey=$raw cannot represent the configured native Bloom sizes")
              case _ => Some(s"$fppKey=$raw must be a finite double strictly between 0 and 1")
            }
          }
          val ndvError = ctx.properties.get(ndvKey).flatMap { raw =>
            scala.util.Try(java.lang.Long.parseLong(raw)).toOption match {
              case Some(value) if value > 0L => None
              case _ => Some(s"$ndvKey=$raw must be a positive Java long")
            }
          }
          Seq(fppError, ndvError).flatten
        }
        .toSeq
        .headOption
    }
  }

  // Planning-time counterpart of the native inverse-NDV check. A target B is safely encoded by
  // aiming at 3B/4, in the interior of parquet-rs's (B/2, B] round-up interval. Requiring every
  // power-of-two through the configured cap is conservative and keeps pathological-but-valid
  // floating-point FPPs on the JVM path rather than discovering them after task launch.
  private def bloomFilterSizesRepresentable(maxBytes: Int, fpp: Double): Boolean = {
    val denominator = -Math.log(1.0d - Math.pow(fpp, 1.0d / 8.0d))
    if (!java.lang.Double.isFinite(denominator) || denominator <= 0.0d) return false

    Iterator
      .iterate(MinBloomFilterBytes)(_ * 2)
      .takeWhile(_ <= maxBytes)
      .forall { target =>
        val ndv = Math.max(1L, Math.round(target.toDouble * 0.75d * denominator))
        val calculatedBits =
          (-8.0d * ndv.toDouble / Math.log(1.0d - Math.pow(fpp, 1.0d / 8.0d))).toLong
        val rawBytes = Math.max(
          MinBloomFilterBytes.toLong,
          Math.min(MaxBloomFilterBytes.toLong, calculatedBits / 8L))
        val allocated = java.lang.Long.highestOneBit(rawBytes - 1L) << 1
        allocated == target.toLong
      }
  }

  private val requireOnlyVettedParquetWriteProperties: TriggerRule = ctx =>
    ctx.properties
      .find { case (k, _) =>
        k.startsWith(ParquetWritePropertyPrefix) &&
        !vettedParquetWriteKeys.contains(k) &&
        !vettedParquetWritePrefixes.exists(k.startsWith)
      }
      .map { case (k, v) => s"$k=$v is not a vetted parquet write property" }

  private val requireNoUnvettedParquetMrProperties: TriggerRule = ctx =>
    ctx.properties.keys
      .find(k =>
        k.startsWith(ParquetMrPropertyPrefix) && k != PropertyKeys.ParquetEnableDictionary)
      .map(k => s"$k is set (parquet-mr properties are forwarded verbatim by iceberg-java)")

  private val requireNoParquetHadoopConfOverrides: TriggerRule = ctx =>
    ctx.hadoopConf.asScala
      .map(_.getKey)
      .filter(_.startsWith(ParquetMrPropertyPrefix))
      .find(k => !IgnoredHadoopParquetConfKeys.contains(k))
      .map(k => s"Hadoop configuration sets $k (reaches iceberg-java's writer but not native)")

  private val requireSupportedStorageScheme: TriggerRule = ctx =>
    IcebergReflection.getDataLocation(ctx.table) match {
      case None => Some("could not resolve the table data location")
      case Some(location) =>
        val scheme = if (location.contains("://")) {
          location.substring(0, location.indexOf("://")).toLowerCase(Locale.ROOT)
        } else {
          "file"
        }
        if (SupportedStorageSchemes.contains(scheme)) None
        else Some(s"unsupported storage scheme: $scheme")
    }

  // The commit-message assembly that runs on executors after iceberg-rust has already written
  // the task's data files is pure reflection over iceberg-java internals. Resolving the whole
  // surface up front turns an Iceberg release that moves any of it into a plan-time fallback
  // instead of a mid-write task failure.
  private val requireExecutorReflectionResolvable: TriggerRule = _ =>
    IcebergReflection.executorReflectionUnresolved

  // The `io-impl` property rule above only sees FileIO configured through table/write
  // properties; a catalog-level `io-impl` (or a catalog implementation installing its own
  // FileIO) leaves the properties clean while `table.io()` is still custom. Gate on the
  // instantiated FileIO's class hierarchy, mirroring the scan side's `isCompatibleFileIO` --
  // except that the EncryptingFileIO family, which the scan accepts (it reads ciphertext
  // through iceberg-rust's own storage layer), is rejected here: the native writer produces
  // plaintext data files.
  private val requireRecognizedTableFileIO: TriggerRule = ctx =>
    IcebergReflection.getFileIO(ctx.table) match {
      case None => Some("could not resolve table.io() for FileIO compatibility checking")
      case Some(io)
          if IcebergReflection.classNameInHierarchy(
            io.getClass,
            IcebergReflection.COMPATIBLE_FILE_IO_CLASSES) =>
        None
      case Some(io) =>
        Some(s"table.io() is ${io.getClass.getName}, which the native write path would bypass")
    }

  private val PlaintextEncryptionManagerClass =
    "org.apache.iceberg.encryption.PlaintextEncryptionManager"

  // The `encryption.*` property rule above infers plaintext from property absence, but the
  // output-file contract is `table.encryption()`, which a custom TableOperations can install
  // independently of table properties. The native writer writes plaintext data files, so
  // anything but Iceberg's PlaintextEncryptionManager fails closed.
  private val requirePlaintextEncryptionManager: TriggerRule = ctx =>
    IcebergReflection.getEncryptionManager(ctx.table) match {
      case None => Some("could not resolve table.encryption() for plaintext checking")
      case Some(mgr) if mgr.getClass.getName == PlaintextEncryptionManagerClass => None
      case Some(mgr) =>
        Some(
          s"table.encryption() is ${mgr.getClass.getName} " +
            "(the native writer writes plaintext data files)")
    }

  private lazy val positiveIntParquetSizeKeys: Seq[String] = Seq(
    PropertyKeys.ParquetRowGroupSizeBytes,
    PropertyKeys.ParquetPageSizeBytes,
    PropertyKeys.ParquetPageRowLimit,
    PropertyKeys.ParquetDictSizeBytes)

  // iceberg-java reads these through `PropertyUtil.propertyAsInt` -- `Integer.parseInt`
  // semantics, so no trimming and no values past Int.MaxValue -- and hands the result to
  // parquet-mr, which rejects non-positive sizes and limits at write time. A value the JVM
  // writer would fail on (or parse differently) must not be silently normalised by the native
  // translation, so anything but a positive Java int falls back and fails on the stock path.
  private val requirePositiveIntParquetSizes: TriggerRule = ctx =>
    positiveIntParquetSizeKeys.flatMap { key =>
      ctx.properties.get(key).flatMap { raw =>
        scala.util.Try(java.lang.Integer.parseInt(raw)).toOption match {
          case None => Some(s"$key=$raw is not a Java int (iceberg-java fails at write time)")
          case Some(v) if v <= 0 =>
            Some(s"$key=$v is not positive (parquet-mr rejects it at write time)")
          case Some(_) => None
        }
      }
    }.headOption

  private lazy val requireRowGroupCheckMinRecordCountAtDefault: TriggerRule =
    requireIntPropertyAtDefault(
      PropertyKeys.ParquetRowGroupCheckMinRecordCount,
      PropertyKeys.ParquetRowGroupCheckMinRecordCountDefault,
      "row-group record-count cadence unsupported")

  private lazy val requireRowGroupCheckMaxRecordCountAtDefault: TriggerRule =
    requireIntPropertyAtDefault(
      PropertyKeys.ParquetRowGroupCheckMaxRecordCount,
      PropertyKeys.ParquetRowGroupCheckMaxRecordCountDefault,
      "row-group record-count cadence unsupported")

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
    // Dropping the metadata columns is behaviour-identical to the JVM writer only while V3
    // tables are gated out: on format-version >= 3 Iceberg's writer reads row-lineage fields
    // from the metadata columns (`ExtractRowLineage`), which this projection discards. Revisit
    // together with `requireFormatVersionAtMostTwo`.
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
    val scan = NativeWriteUtils.buildFfiScan(op.child, op.id)
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
    val fileIOProperties =
      IcebergReflection.getFileIOProperties(table).getOrElse(Map.empty[String, String])

    // A brand-new table (CTAS/RTAS before its first commit) has no metadata file yet. The
    // native side only surfaces metadata_location in plan-debug output, so an empty string is
    // fine -- FileIO is initialised from data_location.
    val metadataLocation = IcebergReflection.getMetadataLocation(table).getOrElse("")
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

    // `FileIO.properties()` misses configuration a HadoopFileIO carries through the Hadoop
    // Configuration instead (fs.s3a.* credentials, custom endpoint, path-style access), which
    // the JVM writer would honour but iceberg-rust would never see. Mirror the scan side
    // (`CometScanRule`): extract the object-store options for the data location from the
    // session Hadoop configuration, translate them to the s3.* keys iceberg-rust consumes, and
    // let FileIO/vended properties win on conflict.
    val hadoopDerivedProperties = CometIcebergNativeScan.hadoopToIcebergS3Properties(
      NativeConfig.extractObjectStoreOptions(
        op.session.sessionState.newHadoopConf(),
        new java.net.URI(dataLocation)))
    val catalogProperties = hadoopDerivedProperties ++ fileIOProperties

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
