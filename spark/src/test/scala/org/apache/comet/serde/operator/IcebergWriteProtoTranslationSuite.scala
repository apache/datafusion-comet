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

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.serde.OperatorOuterClass.{CompressionCodec, IcebergWriterMode}

/**
 * Unit tests for the protobuf translation. Iceberg must be on the classpath because the property
 * keys / numeric defaults are looked up reflectively from `org.apache.iceberg.TableProperties`.
 */
class IcebergWriteProtoTranslationSuite extends AnyFunSuite {

  import IcebergWriteProtoTranslation._

  private val TestCreatedBy = "Apache DataFusion Comet (test)"

  // -- compression -----------------------------------------------------------

  test("compression defaults to zstd at parquet-mr's level 3 when no property is set") {
    val settings = buildParquetSettings(Map.empty, TestCreatedBy)
    assert(settings.getCompression == CompressionCodec.Zstd)
    // See the dedicated zstd-default test below for the rationale.
    assert(settings.getCompressionLevel == 3)
  }

  test("compression maps each Iceberg codec string to the matching enum") {
    val mapping = Seq(
      "uncompressed" -> CompressionCodec.None,
      "none" -> CompressionCodec.None,
      "snappy" -> CompressionCodec.Snappy,
      "gzip" -> CompressionCodec.Gzip,
      "lz4" -> CompressionCodec.Lz4,
      "zstd" -> CompressionCodec.Zstd,
      "brotli" -> CompressionCodec.Brotli)
    mapping.foreach { case (codec, expected) =>
      val settings =
        buildParquetSettings(Map(Keys.ParquetCompression -> codec), TestCreatedBy)
      assert(settings.getCompression == expected, s"codec=$codec")
    }
  }

  test("compression codec parsing is case-insensitive") {
    val settings =
      buildParquetSettings(Map(Keys.ParquetCompression -> "ZSTD"), TestCreatedBy)
    assert(settings.getCompression == CompressionCodec.Zstd)
  }

  test("unknown compression codec throws") {
    val ex = intercept[IllegalArgumentException] {
      buildParquetSettings(Map(Keys.ParquetCompression -> "lzo"), TestCreatedBy)
    }
    assert(ex.getMessage.contains("lzo"))
  }

  test("compression level is emitted when present and parseable") {
    val settings = buildParquetSettings(Map(Keys.ParquetCompressionLevel -> "9"), TestCreatedBy)
    assert(settings.hasCompressionLevel)
    assert(settings.getCompressionLevel == 9)
  }

  test("non-numeric compression level is silently dropped for non-zstd codecs") {
    val settings =
      buildParquetSettings(
        Map(Keys.ParquetCompression -> "snappy", Keys.ParquetCompressionLevel -> "fast"),
        TestCreatedBy)
    assert(!settings.hasCompressionLevel)
  }

  test("zstd without explicit level gets parquet-mr's default of 3") {
    // parquet-mr defaults zstd to level 3 but parquet-rs defaults to 1; we substitute 3 to keep
    // file sizes consistent with iceberg-java.
    val settings = buildParquetSettings(Map.empty, TestCreatedBy)
    assert(settings.getCompression == CompressionCodec.Zstd)
    assert(settings.hasCompressionLevel)
    assert(settings.getCompressionLevel == 3)
  }

  test("explicit zstd compression level wins over the parquet-mr default") {
    val settings = buildParquetSettings(
      Map(Keys.ParquetCompression -> "zstd", Keys.ParquetCompressionLevel -> "15"),
      TestCreatedBy)
    assert(settings.getCompressionLevel == 15)
  }

  test("non-zstd codecs without explicit level emit no level") {
    Seq("snappy", "gzip", "lz4", "brotli", "none").foreach { codec =>
      val settings =
        buildParquetSettings(Map(Keys.ParquetCompression -> codec), TestCreatedBy)
      assert(!settings.hasCompressionLevel, s"codec=$codec should leave compression level unset")
    }
  }

  // -- sizes / limits --------------------------------------------------------

  test("row-group/page/dict sizes fall back to Iceberg defaults when unset") {
    val settings = buildParquetSettings(Map.empty, TestCreatedBy)
    assert(settings.getRowGroupSizeBytes == Defaults.RowGroupSizeBytes)
    assert(settings.getPageSizeBytes == Defaults.PageSizeBytes)
    assert(settings.getDictSizeBytes == Defaults.DictSizeBytes)
    assert(settings.getPageRowLimit == Defaults.PageRowLimit)
  }

  test("sizes are read from properties when set") {
    val settings = buildParquetSettings(
      Map(
        Keys.ParquetRowGroupSizeBytes -> "67108864",
        Keys.ParquetPageSizeBytes -> "65536",
        Keys.ParquetDictSizeBytes -> "1048576",
        Keys.ParquetPageRowLimit -> "1000"),
      TestCreatedBy)
    assert(settings.getRowGroupSizeBytes == 67108864L)
    assert(settings.getPageSizeBytes == 65536L)
    assert(settings.getDictSizeBytes == 1048576L)
    assert(settings.getPageRowLimit == 1000)
  }

  // -- metrics mode (table-level) -------------------------------------------

  test("metrics default absent -> Iceberg's truncate(16) default applies") {
    val settings = buildParquetSettings(Map.empty, TestCreatedBy)
    assert(settings.getDefaultStatisticsEnabled)
    assert(settings.hasStatisticsTruncateLength)
    assert(settings.getStatisticsTruncateLength == 16)
  }

  test("metrics default 'full' -> stats enabled, no truncation") {
    val settings = buildParquetSettings(Map(Keys.MetricsModeDefault -> "full"), TestCreatedBy)
    assert(settings.getDefaultStatisticsEnabled)
    assert(!settings.hasStatisticsTruncateLength)
  }

  test("metrics default 'truncate(N)' -> stats enabled, truncate length set") {
    val settings =
      buildParquetSettings(Map(Keys.MetricsModeDefault -> "truncate(32)"), TestCreatedBy)
    assert(settings.getDefaultStatisticsEnabled)
    assert(settings.hasStatisticsTruncateLength)
    assert(settings.getStatisticsTruncateLength == 32)
  }

  test("metrics default 'none' -> stats disabled, no truncation") {
    val settings = buildParquetSettings(Map(Keys.MetricsModeDefault -> "none"), TestCreatedBy)
    assert(!settings.getDefaultStatisticsEnabled)
    assert(!settings.hasStatisticsTruncateLength)
  }

  test("metrics default 'counts' throws (must be filtered by detection rule)") {
    val ex = intercept[IllegalArgumentException] {
      buildParquetSettings(Map(Keys.MetricsModeDefault -> "counts"), TestCreatedBy)
    }
    assert(ex.getMessage.contains("counts"))
  }

  // -- per-column overrides -------------------------------------------------

  test("per-column metrics override emits enabled+truncate length") {
    val settings = buildParquetSettings(
      Map(
        s"${Keys.MetricsModeColumnPrefix}id" -> "full",
        s"${Keys.MetricsModeColumnPrefix}name" -> "truncate(8)",
        s"${Keys.MetricsModeColumnPrefix}payload" -> "none"),
      TestCreatedBy)
    val byColumn =
      settings.getColumnStatisticsList.asScala.map(s => s.getColumn -> s).toMap

    assert(byColumn("id").getEnabled)
    assert(!byColumn("id").hasTruncateLength)

    assert(byColumn("name").getEnabled)
    assert(byColumn("name").getTruncateLength == 8)

    assert(!byColumn("payload").getEnabled)
    assert(!byColumn("payload").hasTruncateLength)
  }

  // -- writer mode resolution -----------------------------------------------

  test("resolveWriterMode picks UNPARTITIONED for unpartitioned spec regardless of fanout") {
    assert(
      resolveWriterMode(specIsUnpartitioned = true, useFanoutWriter = false) ==
        IcebergWriterMode.ICEBERG_WRITER_UNPARTITIONED)
    assert(
      resolveWriterMode(specIsUnpartitioned = true, useFanoutWriter = true) ==
        IcebergWriterMode.ICEBERG_WRITER_UNPARTITIONED)
  }

  test("resolveWriterMode picks FANOUT for partitioned + fanout") {
    assert(
      resolveWriterMode(specIsUnpartitioned = false, useFanoutWriter = true) ==
        IcebergWriterMode.ICEBERG_WRITER_FANOUT)
  }

  test("resolveWriterMode picks CLUSTERED for partitioned + non-fanout") {
    assert(
      resolveWriterMode(specIsUnpartitioned = false, useFanoutWriter = false) ==
        IcebergWriterMode.ICEBERG_WRITER_CLUSTERED)
  }

  // -- common message --------------------------------------------------------

  test("buildCommon round-trips every field") {
    val settings = buildParquetSettings(Map.empty, TestCreatedBy)
    val common = buildCommon(
      catalogProperties = Map("s3.access-key-id" -> "AKIA", "s3.region" -> "us-east-1"),
      metadataLocation = "s3://bucket/warehouse/db/t/metadata/v3.metadata.json",
      icebergSchemaJson = """{"schema-id":0,"fields":[]}""",
      partitionSpecJson = """{"spec-id":0,"fields":[]}""",
      sortOrderId = 4,
      dataLocation = "s3://bucket/warehouse/db/t/data",
      operationId = "abc-123",
      targetFileSizeBytes = 512L * 1024 * 1024,
      writerMode = IcebergWriterMode.ICEBERG_WRITER_CLUSTERED,
      parquetSettings = settings,
      catalogName = Some("prod_glue"))

    assert(common.getMetadataLocation == "s3://bucket/warehouse/db/t/metadata/v3.metadata.json")
    assert(common.getIcebergSchemaJson == """{"schema-id":0,"fields":[]}""")
    assert(common.getPartitionSpecJson == """{"spec-id":0,"fields":[]}""")
    assert(common.getSortOrderId == 4)
    assert(common.getDataLocation == "s3://bucket/warehouse/db/t/data")
    assert(common.getOperationId == "abc-123")
    assert(common.getTargetFileSizeBytes == 512L * 1024 * 1024)
    assert(common.getWriterMode == IcebergWriterMode.ICEBERG_WRITER_CLUSTERED)
    assert(common.getParquetSettings == settings)
    assert(common.getCatalogName == "prod_glue")
    val cp = common.getCatalogPropertiesMap
    assert(cp.get("s3.access-key-id") == "AKIA")
    assert(cp.get("s3.region") == "us-east-1")
  }

  test("buildCommon omits the catalog map when no entries are provided") {
    val common = buildCommon(
      catalogProperties = Map.empty,
      metadataLocation = "file:/tmp/t/metadata/v1.metadata.json",
      icebergSchemaJson = "{}",
      partitionSpecJson = "{}",
      sortOrderId = 0,
      dataLocation = "file:/tmp/t/data",
      operationId = "op",
      targetFileSizeBytes = 1024L,
      writerMode = IcebergWriterMode.ICEBERG_WRITER_UNPARTITIONED,
      parquetSettings = buildParquetSettings(Map.empty, TestCreatedBy),
      catalogName = None)
    assert(common.getCatalogPropertiesMap.isEmpty)
    assert(common.getCatalogName.isEmpty)
  }

  // -- created_by passthrough ------------------------------------------------

  test("createdBy is written into the settings message verbatim") {
    val settings = buildParquetSettings(Map.empty, "Some Custom Identifier")
    assert(settings.getCreatedBy == "Some Custom Identifier")
  }
}
