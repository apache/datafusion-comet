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

  test("non-numeric compression level throws rather than being silently dropped") {
    // iceberg-java throws NumberFormatException for such a value at write time; the eligibility
    // gate (`requireNativeSupportedCompressionLevel`) declines it before translation ever runs, so a
    // throw here can only mean a gate/translation mismatch -- never a silent normalisation.
    intercept[NumberFormatException] {
      buildParquetSettings(
        Map(Keys.ParquetCompression -> "snappy", Keys.ParquetCompressionLevel -> "fast"),
        TestCreatedBy)
    }
  }

  test("compressionLevelRejection enforces parquet-rs per-codec level ranges") {
    // iceberg-java never validates the level (the raw string flows into codec-specific writer
    // properties), so anything parquet-rs would reject at writer construction must be declined
    // by the gate instead of failing the native task. Bounds: zstd 1..=22, gzip 0..=9,
    // brotli 0..=11.
    val rejected =
      Seq("zstd" -> "0", "zstd" -> "-3", "zstd" -> "23", "gzip" -> "-1", "brotli" -> "12")
    rejected.foreach { case (codec, level) =>
      val reason = compressionLevelRejection(
        Map(Keys.ParquetCompression -> codec, Keys.ParquetCompressionLevel -> level))
      assert(
        reason.exists(r => r.contains(codec) && r.contains("range")),
        s"$codec level $level should be rejected, got $reason")
    }

    val accepted = Seq(
      "zstd" -> "1",
      "zstd" -> "22",
      "gzip" -> "0",
      "gzip" -> "9",
      "brotli" -> "0",
      "brotli" -> "11",
      // Codecs with no native level concept ignore the property on both sides.
      "snappy" -> "999",
      "lz4" -> "-42",
      "none" -> "7")
    accepted.foreach { case (codec, level) =>
      val reason = compressionLevelRejection(
        Map(Keys.ParquetCompression -> codec, Keys.ParquetCompressionLevel -> level))
      assert(reason.isEmpty, s"$codec level $level should be accepted, got $reason")
    }

    // An unset codec resolves to the zstd default, so the zstd range applies.
    assert(compressionLevelRejection(Map(Keys.ParquetCompressionLevel -> "0")).isDefined)
    assert(
      compressionLevelRejection(Map(Keys.ParquetCompressionLevel -> "fast"))
        .exists(_.contains("not a Java int")))
    assert(compressionLevelRejection(Map.empty).isEmpty)
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

  test("size properties are parsed with Java Integer.parseInt semantics") {
    // No trimming and no values past Int.MaxValue -- exactly what iceberg-java's
    // PropertyUtil.propertyAsInt would do. The eligibility gate declines these values
    // (`requirePositiveIntParquetSizes`) before translation runs.
    Seq("garbage", " 1024", "2147483648").foreach { bad =>
      intercept[NumberFormatException] {
        buildParquetSettings(Map(Keys.ParquetRowGroupSizeBytes -> bad), TestCreatedBy)
      }
    }
  }

  // -- metrics modes are not translated --------------------------------------

  test("metrics-mode properties do not shape the parquet settings") {
    // Manifest metrics are re-derived on the JVM from the written footers with Iceberg's own
    // MetricsConfig logic before commit, so the native writer always emits full, untruncated
    // footer statistics and every write.metadata.metrics.* value is ignored here.
    val settings = buildParquetSettings(
      Map(
        "write.metadata.metrics.default" -> "none",
        "write.metadata.metrics.column.id" -> "counts"),
      TestCreatedBy)
    assert(settings == buildParquetSettings(Map.empty, TestCreatedBy))
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
