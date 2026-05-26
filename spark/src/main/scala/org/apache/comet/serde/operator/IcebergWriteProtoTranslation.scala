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
import scala.util.Try

import org.apache.spark.internal.Logging

import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.serde.OperatorOuterClass._

/**
 * Pure translation from the resolved per-write property map (Iceberg table properties merged with
 * any write options) and a small set of driver-side state values into the protobuf messages
 * shipped to the Rust writer.
 *
 * Kept free of any `SparkWrite` reference so each translation function can be unit-tested with
 * plain `Map[String, String]` inputs. The serde wires these into the protobuf during conversion;
 * the JVM exec wrapper fills in `partition_id` and `task_attempt_id` at task launch.
 */
object IcebergWriteProtoTranslation extends Logging {

  /**
   * Iceberg `TableProperties` constants the translation depends on. Resolved lazily through the
   * reflection bridge so we always quote Iceberg's canonical names rather than duplicating
   * literal strings.
   */
  object Keys {
    lazy val ParquetCompression: String =
      IcebergReflection.tablePropertyConstant("PARQUET_COMPRESSION")
    lazy val ParquetCompressionDefaultSince14: String =
      IcebergReflection.tablePropertyConstant("PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0")
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
    lazy val MetricsModeDefault: String =
      IcebergReflection.tablePropertyConstant("DEFAULT_WRITE_METRICS_MODE")
    lazy val MetricsModeDefaultValue: String =
      IcebergReflection.tablePropertyConstant("DEFAULT_WRITE_METRICS_MODE_DEFAULT")
    lazy val MetricsModeColumnPrefix: String =
      IcebergReflection.tablePropertyConstant("METRICS_MODE_COLUMN_CONF_PREFIX")
  }

  /** Iceberg's numeric defaults, pulled at runtime so they stay in lock-step with the runtime. */
  object Defaults {
    lazy val RowGroupSizeBytes: Long =
      IcebergReflection.tablePropertyIntConstant("PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT").toLong
    lazy val PageSizeBytes: Long =
      IcebergReflection.tablePropertyIntConstant("PARQUET_PAGE_SIZE_BYTES_DEFAULT").toLong
    lazy val DictSizeBytes: Long =
      IcebergReflection.tablePropertyIntConstant("PARQUET_DICT_SIZE_BYTES_DEFAULT").toLong
    lazy val PageRowLimit: Int =
      IcebergReflection.tablePropertyIntConstant("PARQUET_PAGE_ROW_LIMIT_DEFAULT")
  }

  /** Builds the parquet settings message. Pure: no SparkWrite or Iceberg `Table` access. */
  def buildParquetSettings(
      props: Map[String, String],
      createdBy: String): IcebergParquetWriteSettings = {
    val rowGroupSize =
      parseLong(props, Keys.ParquetRowGroupSizeBytes, Defaults.RowGroupSizeBytes)
    val pageSize = parseLong(props, Keys.ParquetPageSizeBytes, Defaults.PageSizeBytes)
    val dictSize = parseLong(props, Keys.ParquetDictSizeBytes, Defaults.DictSizeBytes)
    val pageRowLimit = parseInt(props, Keys.ParquetPageRowLimit, Defaults.PageRowLimit)
    val compression = resolveCompression(props)
    val builder = IcebergParquetWriteSettings
      .newBuilder()
      .setCompression(compression)
      .setRowGroupSizeBytes(rowGroupSize)
      .setPageSizeBytes(pageSize)
      .setDictSizeBytes(dictSize)
      .setPageRowLimit(pageRowLimit)
      .setCreatedBy(createdBy)

    resolveCompressionLevel(props, compression).foreach(builder.setCompressionLevel)

    val rawMetricsMode = props.getOrElse(Keys.MetricsModeDefault, Keys.MetricsModeDefaultValue)
    val defaultMetricsMode = parseMetricsMode(rawMetricsMode)
    builder.setDefaultStatisticsEnabled(defaultMetricsMode.enabled)
    defaultMetricsMode.truncateLength.foreach(builder.setStatisticsTruncateLength)

    collectColumnStatistics(props).foreach(builder.addColumnStatistics)

    builder.build()
  }

  /**
   * Driver-side resolution of the iceberg-rust writer flavor. The choice mirrors
   * `SparkWrite$WriterFactory`: unpartitioned tables get `UnpartitionedDataWriter`; partitioned
   * tables either fan out or cluster based on `SparkWriteConf.useFanoutWriter`.
   */
  def resolveWriterMode(
      specIsUnpartitioned: Boolean,
      useFanoutWriter: Boolean): IcebergWriterMode = {
    if (specIsUnpartitioned) IcebergWriterMode.ICEBERG_WRITER_UNPARTITIONED
    else if (useFanoutWriter) IcebergWriterMode.ICEBERG_WRITER_FANOUT
    else IcebergWriterMode.ICEBERG_WRITER_CLUSTERED
  }

  /** Builds the per-write broadcast message. */
  // scalastyle:off argcount
  def buildCommon(
      catalogProperties: Map[String, String],
      metadataLocation: String,
      icebergSchemaJson: String,
      partitionSpecJson: String,
      sortOrderId: Int,
      dataLocation: String,
      operationId: String,
      targetFileSizeBytes: Long,
      writerMode: IcebergWriterMode,
      parquetSettings: IcebergParquetWriteSettings,
      catalogName: Option[String]): IcebergWriteCommon = {
    val builder = IcebergWriteCommon
      .newBuilder()
      .setMetadataLocation(metadataLocation)
      .setIcebergSchemaJson(icebergSchemaJson)
      .setPartitionSpecJson(partitionSpecJson)
      .setSortOrderId(sortOrderId)
      .setDataLocation(dataLocation)
      .setOperationId(operationId)
      .setTargetFileSizeBytes(targetFileSizeBytes)
      .setWriterMode(writerMode)
      .setParquetSettings(parquetSettings)
    if (catalogProperties.nonEmpty) builder.putAllCatalogProperties(catalogProperties.asJava)
    catalogName.foreach(builder.setCatalogName)
    builder.build()
  }
  // scalastyle:on argcount

  // --- Internals ------------------------------------------------------------

  /**
   * Iceberg metrics-mode string -> (stats on/off, optional truncate length). Grammar is `none`,
   * `counts`, `truncate(N)`, or `full`. `counts` is filtered out by the detection rule earlier in
   * the pipeline; the parser still recognises it defensively so a stray value yields a clear
   * failure mode rather than silently mapping to "full".
   */
  private case class MetricsMode(enabled: Boolean, truncateLength: Option[Int])

  private val TruncatePattern = """truncate\((\d+)\)""".r

  private def parseMetricsMode(value: String): MetricsMode =
    value.trim.toLowerCase(Locale.ROOT) match {
      case "none" => MetricsMode(enabled = false, truncateLength = None)
      case "full" => MetricsMode(enabled = true, truncateLength = None)
      case TruncatePattern(n) =>
        MetricsMode(enabled = true, truncateLength = Some(n.toInt))
      case "counts" =>
        throw new IllegalArgumentException(
          "'counts' metrics mode must be filtered by the detection rule before translation")
      case other =>
        throw new IllegalArgumentException(s"Unrecognised metrics mode: $other")
    }

  /**
   * Iceberg accepts `uncompressed`, `none` (treated as uncompressed), `snappy`, `gzip`, `lz4`,
   * `zstd`, `brotli`. Defaults to `zstd` (since Iceberg 1.4).
   */
  private def resolveCompression(props: Map[String, String]): CompressionCodec = {
    val raw = props
      .get(Keys.ParquetCompression)
      .map(_.trim.toLowerCase(Locale.ROOT))
      .getOrElse(Keys.ParquetCompressionDefaultSince14)
    raw match {
      case "uncompressed" | "none" => CompressionCodec.None
      case "snappy" => CompressionCodec.Snappy
      case "gzip" => CompressionCodec.Gzip
      case "lz4" => CompressionCodec.Lz4
      case "zstd" => CompressionCodec.Zstd
      case "brotli" => CompressionCodec.Brotli
      case other =>
        throw new IllegalArgumentException(s"Unsupported parquet codec '$other'")
    }
  }

  /**
   * Iceberg leaves `write.parquet.compression-level` null by default and lets each parquet writer
   * pick its own per-codec default. The only codec where parquet-rs and parquet-mr diverge is
   * zstd (parquet-rs default 1, parquet-mr default 3). To produce files the same size as
   * iceberg-java would, substitute parquet-mr's 3 when zstd is in use and the user did not set an
   * explicit level. Other codecs match by accident: gzip 6 = 6, brotli 1 = 1; snappy and lz4 have
   * no level concept.
   */
  private def resolveCompressionLevel(
      props: Map[String, String],
      codec: CompressionCodec): Option[Int] = {
    val explicit =
      props.get(Keys.ParquetCompressionLevel).flatMap(s => Try(s.trim.toInt).toOption)
    explicit.orElse(parquetMrDefaultLevel(codec))
  }

  private def parquetMrDefaultLevel(codec: CompressionCodec): Option[Int] = codec match {
    case CompressionCodec.Zstd => Some(3)
    case _ => None
  }

  private def collectColumnStatistics(
      props: Map[String, String]): Seq[IcebergParquetColumnStatistics] = {
    val modeByColumn = props.collect {
      case (k, v) if k.startsWith(Keys.MetricsModeColumnPrefix) =>
        k.substring(Keys.MetricsModeColumnPrefix.length) -> parseMetricsMode(v)
    }

    modeByColumn.keys.toSeq.sorted.map { column =>
      val mode = modeByColumn(column)
      val builder = IcebergParquetColumnStatistics
        .newBuilder()
        .setColumn(column)
        .setEnabled(mode.enabled)
      mode.truncateLength.foreach(builder.setTruncateLength)
      builder.build()
    }
  }

  private def parseLong(props: Map[String, String], key: String, default: Long): Long =
    props.get(key).flatMap(s => Try(s.trim.toLong).toOption).getOrElse(default)

  private def parseInt(props: Map[String, String], key: String, default: Int): Int =
    props.get(key).flatMap(s => Try(s.trim.toInt).toOption).getOrElse(default)

}
