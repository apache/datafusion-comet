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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.comet.{CometNativeExec, IcebergWriteExec}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator

object CometIcebergNativeWrite extends CometOperatorSerde[IcebergWriteExec] with Logging {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED)

  override def requiresNativeChildren: Boolean = true

  object PropertyKeys {
    lazy val ObjectStoreEnabled: String =
      IcebergReflection.tablePropertyConstant("OBJECT_STORE_ENABLED")
    lazy val WriteLocationProviderImpl: String =
      IcebergReflection.tablePropertyConstant("WRITE_LOCATION_PROVIDER_IMPL")
    lazy val DefaultWriteMetricsMode: String =
      IcebergReflection.tablePropertyConstant("DEFAULT_WRITE_METRICS_MODE")
    lazy val MetricsModeColumnPrefix: String =
      IcebergReflection.tablePropertyConstant("METRICS_MODE_COLUMN_CONF_PREFIX")
    lazy val BloomFilterColumnEnabledPrefix: String =
      IcebergReflection.tablePropertyConstant("PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX")
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
  private val SupportedStorageSchemes: Set[String] =
    Set("file", "memory", "s3", "s3a", "gs", "oss")
  private val MinUnsupportedFormatVersion = 3
  private val ParquetWritePropertyPrefix = "write.parquet."
  private val ParquetMrPropertyPrefix = "parquet."

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
    PropertyKeys.ParquetVariantBufferSize)

  private lazy val vettedParquetWritePrefixes: Seq[String] =
    Seq(PropertyKeys.BloomFilterColumnEnabledPrefix)

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
    requireNoEncryptionPrefix,
    requireSupportedMetricsModes,
    requireNoBloomFilterColumnsEnabled,
    requireRowGroupCheckMinRecordCountAtDefault,
    requireRowGroupCheckMaxRecordCountAtDefault,
    requireParquetPageVersionDefault,
    requireShredVariantsDisabled,
    requireOnlyVettedParquetWriteProperties,
    requirePropertyAbsent(
      PropertyKeys.ParquetEnableDictionary,
      "dictionary override unsupported"),
    requireNoUnvettedParquetMrProperties,
    requirePropertyAbsent(PropertyKeys.FileIOImpl, "custom FileIO unsupported"),
    requireNoParquetHadoopConfOverrides,
    requireSupportedStorageScheme)

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

  private val requireNoEncryptionPrefix: TriggerRule = ctx =>
    ctx.properties.keys
      .find(_.startsWith(EncryptionPropertyPrefix))
      .map(k => s"$k set: encryption unsupported")

  private val TruncateModePattern = """truncate\((\d+)\)""".r

  private def isSupportedMetricsMode(value: String): Boolean =
    value.trim.toLowerCase(Locale.ROOT) match {
      case "full" => true
      case TruncateModePattern(n) => n.toInt > 0
      case _ => false
    }

  private val requireSupportedMetricsModes: TriggerRule = ctx => {
    val defaultKey = PropertyKeys.DefaultWriteMetricsMode
    val prefix = PropertyKeys.MetricsModeColumnPrefix
    ctx.properties
      .find { case (k, v) =>
        (k == defaultKey || k.startsWith(prefix)) && !isSupportedMetricsMode(v)
      }
      .map { case (k, v) => s"$k=$v (supported metrics modes: full, truncate(N))" }
  }

  private val requireNoBloomFilterColumnsEnabled: TriggerRule = ctx => {
    val prefix = PropertyKeys.BloomFilterColumnEnabledPrefix
    ctx.properties
      .find { case (k, v) => k.startsWith(prefix) && v.equalsIgnoreCase("true") }
      .map { case (k, _) => s"$k=true: bloom filters unsupported" }
  }

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
      .find(_.startsWith(ParquetMrPropertyPrefix))
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
    val _ = (builder, childOp)
    withFallbackReason(op, "native Iceberg write is not yet implemented")
    None
  }

  override def createExec(nativeOp: Operator, op: IcebergWriteExec): CometNativeExec =
    throw new UnsupportedOperationException("native Iceberg write not yet implemented")
}
