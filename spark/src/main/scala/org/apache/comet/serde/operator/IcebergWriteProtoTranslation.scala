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
object IcebergWriteProtoTranslation {

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
    lazy val ParquetBloomFilterColumnEnabledPrefix: String =
      IcebergReflection.tablePropertyConstant("PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX")
    lazy val ParquetBloomFilterMaxBytes: String =
      IcebergReflection.tablePropertyConstant("PARQUET_BLOOM_FILTER_MAX_BYTES")
    // These were added to Iceberg after bloom enablement/max-bytes. Literals let older runtimes
    // keep using the defaults; detection rejects an explicit property they would ignore.
    val ParquetBloomFilterColumnFppPrefix = "write.parquet.bloom-filter-fpp.column."
    val ParquetBloomFilterColumnNdvPrefix = "write.parquet.bloom-filter-ndv.column."
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
    lazy val BloomFilterMaxBytes: Int =
      IcebergReflection.tablePropertyIntConstant("PARQUET_BLOOM_FILTER_MAX_BYTES_DEFAULT")
    // Iceberg introduced the public constant together with the FPP property. Keep the literal
    // fallback for runtimes old enough not to expose it; 0.01 is also parquet-mr's default.
    lazy val BloomFilterFpp: Double =
      IcebergReflection
        .tablePropertyDoubleConstantOpt("PARQUET_BLOOM_FILTER_COLUMN_FPP_DEFAULT")
        .getOrElse(0.01d)
  }

  /**
   * Why the write's compression level (if set) cannot be honoured natively, or `None` when it
   * can. iceberg-java performs no validation at all -- `Parquet.WriteBuilder`'s context keeps
   * `compressionLevel` as a raw string and drops it into the codec-specific writer property
   * (`parquet.compression.codec.zstd.level`, `zlib.compress.level`, `compression.brotli.quality`)
   * -- while parquet-rs enforces per-codec ranges when the writer is built, so a level the JVM
   * writer happily writes with would fail the native task. The bounds mirror parquet-rs's
   * `CompressionLevel` impls (pinned 58.x: zstd 1..=22, gzip 0..=9, brotli 0..=11); codecs
   * without a native level concept (snappy, lz4, none) ignore the property on both sides.
   */
  def compressionLevelRejection(props: Map[String, String]): Option[String] =
    props.get(Keys.ParquetCompressionLevel).flatMap { raw =>
      scala.util.Try(java.lang.Integer.parseInt(raw)).toOption match {
        case None => Some(s"${Keys.ParquetCompressionLevel}=$raw is not a Java int")
        case Some(level) =>
          val nativeBounds = resolveCompression(props) match {
            case CompressionCodec.Zstd => Some((1, 22, "zstd"))
            case CompressionCodec.Gzip => Some((0, 9, "gzip"))
            case CompressionCodec.Brotli => Some((0, 11, "brotli"))
            case _ => None
          }
          nativeBounds.collect {
            case (min, max, codec) if level < min || level > max =>
              s"${Keys.ParquetCompressionLevel}=$level is outside the native writer's $codec " +
                s"range [$min, $max] (iceberg-java does not validate the level)"
          }
      }
    }

  /** Builds the parquet settings message. Pure: no SparkWrite or Iceberg `Table` access. */
  def buildParquetSettings(
      props: Map[String, String],
      createdBy: String): IcebergParquetWriteSettings = {
    val rowGroupSize =
      parseJavaInt(props, Keys.ParquetRowGroupSizeBytes, Defaults.RowGroupSizeBytes.toInt).toLong
    val pageSize =
      parseJavaInt(props, Keys.ParquetPageSizeBytes, Defaults.PageSizeBytes.toInt).toLong
    val dictSize =
      parseJavaInt(props, Keys.ParquetDictSizeBytes, Defaults.DictSizeBytes.toInt).toLong
    val pageRowLimit = parseJavaInt(props, Keys.ParquetPageRowLimit, Defaults.PageRowLimit)
    val compression = resolveCompression(props)
    val bloomFilterEnabledColumns = props.iterator
      .collect {
        case (key, value)
            if key.startsWith(Keys.ParquetBloomFilterColumnEnabledPrefix) &&
              value.equalsIgnoreCase("true") =>
          key.substring(Keys.ParquetBloomFilterColumnEnabledPrefix.length)
      }
      .toSeq
      .sorted
    val bloomFilterMaxBytes =
      parseJavaInt(props, Keys.ParquetBloomFilterMaxBytes, Defaults.BloomFilterMaxBytes).toLong
    val bloomFilterFppByColumn = bloomFilterEnabledColumns.map { column =>
      val value = props
        .get(Keys.ParquetBloomFilterColumnFppPrefix + column)
        .map(java.lang.Double.parseDouble)
        .getOrElse(Defaults.BloomFilterFpp)
      column -> value
    }.toMap
    val bloomFilterNdvByColumn = bloomFilterEnabledColumns.flatMap { column =>
      props
        .get(Keys.ParquetBloomFilterColumnNdvPrefix + column)
        .map(value => column -> java.lang.Long.parseLong(value))
    }.toMap
    val builder = IcebergParquetWriteSettings
      .newBuilder()
      .setCompression(compression)
      .setRowGroupSizeBytes(rowGroupSize)
      .setPageSizeBytes(pageSize)
      .setDictSizeBytes(dictSize)
      .setPageRowLimit(pageRowLimit)
      .setCreatedBy(createdBy)
      .addAllBloomFilterEnabledColumns(bloomFilterEnabledColumns.asJava)
      .setBloomFilterMaxBytes(bloomFilterMaxBytes)
      .putAllBloomFilterFppByColumn(bloomFilterFppByColumn.map { case (k, v) =>
        k -> Double.box(v)
      }.asJava)
      .putAllBloomFilterNdvByColumn(bloomFilterNdvByColumn.map { case (k, v) =>
        k -> Long.box(v)
      }.asJava)

    resolveCompressionLevel(props, compression).foreach(builder.setCompressionLevel)

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
   * pick its own per-codec default. The one known divergence between parquet-rs and parquet-mr is
   * zstd (parquet-rs default 1, parquet-mr default 3). To produce files the same size as
   * iceberg-java would, substitute parquet-mr's 3 when zstd is in use and the user did not set an
   * explicit level. gzip defaults to 6 on both sides; snappy and lz4 have no level concept;
   * compressed bytes are an accepted divergence regardless (see iceberg-writes.md).
   */
  private def resolveCompressionLevel(
      props: Map[String, String],
      codec: CompressionCodec): Option[Int] = {
    val explicit = props.get(Keys.ParquetCompressionLevel).map(java.lang.Integer.parseInt)
    explicit.orElse(parquetMrDefaultLevel(codec))
  }

  private def parquetMrDefaultLevel(codec: CompressionCodec): Option[Int] = codec match {
    case CompressionCodec.Zstd => Some(3)
    case _ => None
  }

  /**
   * `Integer.parseInt` on the raw value -- the exact semantics `PropertyUtil.propertyAsInt`
   * applies on the iceberg-java writer path (no trimming, values past Int.MaxValue throw). A
   * failure here means iceberg-java's own writer would have thrown at write time; the eligibility
   * gate already declines such values (`requirePositiveIntParquetSizes` /
   * `requireNativeSupportedCompressionLevel`), so this throwing is a translation-bug tripwire,
   * never silent normalisation.
   */
  private def parseJavaInt(props: Map[String, String], key: String, default: Int): Int =
    props.get(key).map(java.lang.Integer.parseInt).getOrElse(default)

}
