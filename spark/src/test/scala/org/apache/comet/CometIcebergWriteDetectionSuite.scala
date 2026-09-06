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

package org.apache.comet

import java.io.File

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.hadoop.{HadoopConfigurable, HadoopFileIO}
import org.apache.iceberg.io.{FileIO, InputFile, OutputFile}
import org.apache.iceberg.util.SerializableSupplier
import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.IcebergWriteExec

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.serde.{Compatible, SupportLevel, Unsupported}
import org.apache.comet.serde.operator.CometIcebergNativeWrite

class CometIcebergWriteDetectionSuite extends CometTestBase with CometIcebergTestBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key, "true")
      .set(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key, "true")
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      assume(icebergAvailable, "Iceberg not available in classpath")
      testFun
    }
  }

  test("clean parquet V2 table planned as AppendData yields Compatible") {
    withDetectionCatalog { dir =>
      createTable(dir, "ok", partitionSpec = "")
      assertSupportLevelIs[Compatible]("ok")
    }
  }

  test("registration tags a fall-back reason on the write exec") {
    withDetectionCatalog { dir =>
      createTable(dir, "tagged", partitionSpec = "")
      val writeExec = insertWriteExec("tagged")
      val reasons = writeExec.getTagValue(CometExplainInfo.FALLBACK_REASONS)
      assert(
        reasons.exists(_.nonEmpty),
        s"expected CometExecRule to record a fall-back reason on $writeExec")
    }
  }

  test("SparkWrite reflection helpers all resolve on the current Iceberg runtime") {
    withDetectionCatalog { dir =>
      createTable(dir, "refl_probe", partitionSpec = "")
      val sparkWrite = IcebergReflection
        .getOuterSparkWrite(insertWriteExec("refl_probe").batchWrite)
        .getOrElse(fail("could not unwrap outer SparkWrite from BatchWrite"))
      val table = IcebergReflection
        .getTableFromSparkWrite(sparkWrite)
        .getOrElse(fail("SparkWrite.table reflection returned None"))

      assert(IcebergReflection.getOperationIdFromSparkWrite(sparkWrite).isDefined, "queryId")
      assert(
        IcebergReflection.getTargetFileSizeFromSparkWrite(sparkWrite).isDefined,
        "targetFileSize")
      assert(
        IcebergReflection.getUseFanoutWriterFromSparkWrite(sparkWrite).isDefined,
        "useFanoutWriter")
      assert(
        IcebergReflection.getOutputSpecIdFromSparkWrite(sparkWrite).isDefined,
        "outputSpecId")
      assert(IcebergReflection.getWriteSchemaFromSparkWrite(sparkWrite).isDefined, "writeSchema")
      assert(IcebergReflection.getFormatFromSparkWrite(sparkWrite).isDefined, "format")
      assert(
        IcebergReflection.getWritePropertiesFromSparkWrite(sparkWrite).isDefined,
        "writeProperties")
      assert(IcebergReflection.getMetadataLocation(table).isDefined, "metadataLocation")
      assert(IcebergReflection.getDataLocation(table).isDefined, "dataLocation")
      assert(IcebergReflection.getTableProperties(table).isDefined, "tableProperties")
    }
  }

  test("Compatible when a session conf overrides the compression codec") {
    withDetectionCatalog { dir =>
      createTable(dir, "session_codec", partitionSpec = "")
      withSQLConf("spark.sql.iceberg.compression-codec" -> "gzip") {
        assertSupportLevelIs[Compatible]("session_codec")
      }
    }
  }

  test("fall-back: write.format.default=orc") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "fmt_orc",
        partitionSpec = "",
        properties = Some("'write.format.default'='orc'"))
      assertUnsupportedContains("fmt_orc", "format=orc", "only parquet")
    }
  }

  test("fall-back: per-write write-format option overrides parquet default") {
    withDetectionCatalog { dir =>
      createTable(dir, "fmt_orc_opt", partitionSpec = "")
      assertUnsupportedContains(
        dfWriteExec("fmt_orc_opt", "write-format" -> "orc"),
        "fmt_orc_opt",
        "format=orc",
        "only parquet")
    }
  }

  test("fall-back: write.object-storage.enabled=true") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "obj_store",
        partitionSpec = "",
        properties = Some("'write.object-storage.enabled'='true'"))
      assertUnsupportedContains("obj_store", "write.object-storage.enabled")
    }
  }

  test("fall-back: write.location-provider.impl set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "loc_provider",
        partitionSpec = "",
        properties = Some("'write.location-provider.impl'='com.example.MyProvider'"))
      assertUnsupportedContainsAllowingWriteFailure(
        "loc_provider",
        "write.location-provider.impl")
    }
  }

  test("fall-back: format-version=3") {
    assume(isSpark35Plus, "V3 tables require Iceberg 1.8.1+ (Spark 3.5 profile)")
    withDetectionCatalog { dir =>
      createTable(dir, "v3", partitionSpec = "", properties = Some("'format-version'='3'"))
      assertUnsupportedContains("v3", "format-version=3")
    }
  }

  test("fall-back: encryption.kms-client-impl set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "enc",
        partitionSpec = "",
        properties = Some("'encryption.kms-client-impl'='com.example.MyKms'"))
      assertUnsupportedContainsAllowingWriteFailure("enc", "encryption")
    }
  }

  // Metrics modes are not gated: manifest metrics are re-derived on the JVM with Iceberg's
  // own MetricsConfig logic before commit, so every mode behaves as it does on the java path.
  test("Compatible for every write.metadata.metrics mode") {
    withDetectionCatalog { dir =>
      Seq("counts", "none", "full", "truncate(32)").zipWithIndex.foreach { case (mode, i) =>
        createTable(
          dir,
          s"metrics_mode_$i",
          partitionSpec = "",
          properties = Some(s"'write.metadata.metrics.default'='$mode'"))
        assertSupportLevelIs[Compatible](s"metrics_mode_$i")
      }
    }
  }

  test("Compatible for per-column metrics modes") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "metrics_col_modes",
        partitionSpec = "",
        properties = Some(
          "'write.metadata.metrics.column.id'='counts', " +
            "'write.metadata.metrics.column.region'='none'"))
      assertSupportLevelIs[Compatible]("metrics_col_modes")
    }
  }

  // The JVM path fails such a write inside parquet-mr's codec setup, so allow the write failure
  // and pin only the fall-back reason.
  test("fall-back: non-integer write.parquet.compression-level") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "bad_level",
        partitionSpec = "",
        properties = Some("'write.parquet.compression-level'='fast'"))
      assertUnsupportedContainsAllowingWriteFailure(
        "bad_level",
        "write.parquet.compression-level",
        "not a Java int")
    }
  }

  test("fall-back: compression level outside the native writer's per-codec range") {
    // iceberg-java does not validate the level at all -- the raw string flows into
    // codec-specific parquet-mr writer properties -- so these values write fine on the stock
    // path but parquet-rs rejects them at writer construction (zstd 1..=22, gzip 0..=9,
    // brotli 0..=11). They must decline up front rather than fail the task.
    withDetectionCatalog { dir =>
      val cases =
        Seq("zstd" -> "0", "zstd" -> "-3", "zstd" -> "23", "gzip" -> "-1", "brotli" -> "12")
      cases.zipWithIndex.foreach { case ((codec, level), i) =>
        val table = s"bad_level_range_$i"
        createTable(
          dir,
          table,
          partitionSpec = "",
          properties = Some(
            s"'write.parquet.compression-codec'='$codec', " +
              s"'write.parquet.compression-level'='$level'"))
        assertUnsupportedContainsAllowingWriteFailure(
          table,
          "write.parquet.compression-level",
          codec)
      }
      // A boundary level parquet-rs accepts stays Compatible.
      createTable(
        dir,
        "good_level",
        partitionSpec = "",
        properties = Some(
          "'write.parquet.compression-codec'='zstd', 'write.parquet.compression-level'='22'"))
      assertSupportLevelIs[Compatible]("good_level")
    }
  }

  test("bloom-filter max-bytes accepts only representable power-of-two values") {
    withDetectionCatalog { dir =>
      Seq("32", "524288", "1048576", "134217728").zipWithIndex.foreach { case (value, index) =>
        val table = s"bloom_max_ok_$index"
        createTable(
          dir,
          table,
          partitionSpec = "",
          properties = Some(s"'write.parquet.bloom-filter-max-bytes'='$value'"))
        assertSupportLevelIs[Compatible](table)
      }

      Seq("31", "33", "100", "134217729", "0", "-1", " 32", "garbage", "2147483648").zipWithIndex
        .foreach { case (value, index) =>
          val table = s"bloom_max_bad_$index"
          createTable(
            dir,
            table,
            partitionSpec = "",
            properties = Some(s"'write.parquet.bloom-filter-max-bytes'='$value'"))
          assertUnsupportedContainsAllowingWriteFailure(
            table,
            "write.parquet.bloom-filter-max-bytes")
        }
    }
  }

  test("fall-back: invalid enabled-column FPP and NDV") {
    withDetectionCatalog { dir =>
      Seq(
        "'write.parquet.bloom-filter-fpp.column.id'='0'",
        "'write.parquet.bloom-filter-fpp.column.id'='1'",
        "'write.parquet.bloom-filter-fpp.column.id'='NaN'",
        // Positive and below one, but too small for any integer NDV to encode the requested
        // power-of-two allocation through parquet-rs's NDV/FPP API.
        "'write.parquet.bloom-filter-fpp.column.id'='4.9E-324'",
        "'write.parquet.bloom-filter-ndv.column.id'='0'",
        "'write.parquet.bloom-filter-ndv.column.id'='garbage'").zipWithIndex.foreach {
        case (property, index) =>
          val table = s"bloom_shape_bad_$index"
          createTable(
            dir,
            table,
            partitionSpec = "",
            properties =
              Some(s"'write.parquet.bloom-filter-enabled.column.id'='true', $property"))
          assertUnsupportedContainsAllowingWriteFailure(table, "write.parquet.bloom-filter")
      }
    }
  }

  test("Compatible when a per-column bloom filter is enabled") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "bloom_col",
        partitionSpec = "",
        properties = Some("'write.parquet.bloom-filter-enabled.column.id'='true'"))
      assertSupportLevelIs[Compatible]("bloom_col")
    }
  }

  test("Compatible when the schema exceeds max-inferred-column-defaults") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "too_many_cols",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.max-inferred-column-defaults'='2'"))
      assertSupportLevelIs[Compatible]("too_many_cols")
    }
  }

  test("fall-back: row-group-check-min-record-count non-default") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "rg_min",
        partitionSpec = "",
        properties = Some("'write.parquet.row-group-check-min-record-count'='500'"))
      assertUnsupportedContains("rg_min", "write.parquet.row-group-check-min-record-count=500")
    }
  }

  test("Compatible when row-group-check-min-record-count is at default (100)") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "rg_min_default",
        partitionSpec = "",
        properties = Some("'write.parquet.row-group-check-min-record-count'='100'"))
      assertSupportLevelIs[Compatible]("rg_min_default")
    }
  }

  test("fall-back: row-group-check-max-record-count non-default") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "rg_max",
        partitionSpec = "",
        properties = Some("'write.parquet.row-group-check-max-record-count'='50000'"))
      assertUnsupportedContains("rg_max", "write.parquet.row-group-check-max-record-count=50000")
    }
  }

  test("fall-back: write.parquet.page-version=v2") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "page_v2",
        partitionSpec = "",
        properties = Some("'write.parquet.page-version'='v2'"))
      assertUnsupportedContains("page_v2", "write.parquet.page-version", "v2")
    }
  }

  test("fall-back: parquet.enable.dictionary set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "enable_dict",
        partitionSpec = "",
        properties = Some("'parquet.enable.dictionary'='false'"))
      assertUnsupportedContains("enable_dict", "parquet.enable.dictionary")
    }
  }

  test("fall-back: per-column write.parquet.stats-enabled.<col> set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "col_stats",
        partitionSpec = "",
        properties = Some("'write.parquet.stats-enabled.column.region'='false'"))
      assertUnsupportedContains("col_stats", "write.parquet.stats-enabled.column.region")
    }
  }

  test("fall-back: unvetted write.parquet.* property") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "unvetted",
        partitionSpec = "",
        properties = Some("'write.parquet.bloom-filter-adaptive-enabled'='true'"))
      assertUnsupportedContains(
        "unvetted",
        "write.parquet.bloom-filter-adaptive-enabled",
        "not a vetted")
    }
  }

  test("fall-back: parquet.* table property other than enable.dictionary") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "pq_mr_prop",
        partitionSpec = "",
        properties = Some("'parquet.columnindex.truncate.length'='32'"))
      assertUnsupportedContains("pq_mr_prop", "parquet.columnindex.truncate.length")
    }
  }

  test("Compatible when a codec level side-channel property is set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "codec_level",
        partitionSpec = "",
        properties = Some("'zlib.compress.level'='9'"))
      assertSupportLevelIs[Compatible]("codec_level")
    }
  }

  test("fall-back: parquet.* key in the session Hadoop configuration") {
    withDetectionCatalog { dir =>
      createTable(dir, "hadoop_conf", partitionSpec = "")
      withSQLConf("parquet.block.size" -> "1048576") {
        assertUnsupportedContains("hadoop_conf", "parquet.block.size", "Hadoop configuration")
      }
    }
  }

  // parquet.hadoop.vectored.io.enabled is a reader-side vectored-IO knob declared by
  // parquet-hadoop (ParquetInputFormat.HADOOP_VECTORED_IO_ENABLED, default true in
  // parquet-hadoop 1.16+). iceberg-java's writer never consumes it, so it must not
  // disable native Iceberg writes when it happens to be present in the session
  // Hadoop configuration.
  test("Compatible when only parquet.hadoop.vectored.io.enabled is set in Hadoop configuration") {
    withDetectionCatalog { dir =>
      createTable(dir, "vectored_io_only", partitionSpec = "")
      withSQLConf("parquet.hadoop.vectored.io.enabled" -> "true") {
        assertSupportLevelIs[Compatible]("vectored_io_only")
      }
    }
  }

  test("fall-back: io-impl set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "io_impl",
        partitionSpec = "",
        properties = Some("'io-impl'='com.example.MyFileIO'"))
      assertUnsupportedContainsAllowingWriteFailure("io_impl", "io-impl")
    }
  }

  test("fall-back: data location URI scheme not supported by the native writer") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "bad_scheme",
        partitionSpec = "",
        properties = Some("'write.data.path'='hdfs://nonexistent.invalid/iceberg/db/bad_scheme'"))
      assertUnsupportedContainsAllowingWriteFailure("bad_scheme", "storage scheme", "hdfs")
    }
  }

  test("Compatible when the data location scheme is s3") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "s3_scheme",
        partitionSpec = "",
        properties = Some("'write.data.path'='s3://nonexistent-bucket/iceberg/db/s3_scheme'"))
      assertSupportLevelIs[Compatible]("s3_scheme", allowWriteFailure = true)
    }
  }

  test("Compatible for the remaining supported data location schemes") {
    withDetectionCatalog { dir =>
      Seq("gs", "memory").foreach { scheme =>
        val table = s"${scheme}_scheme"
        createTable(
          dir,
          table,
          partitionSpec = "",
          properties = Some(s"'write.data.path'='$scheme://nonexistent/iceberg/db/$table'"))
        assertSupportLevelIs[Compatible](table, allowWriteFailure = true)
      }
    }
  }

  test("fall-back: oss data location scheme (oss.* properties are not forwarded)") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "oss_scheme",
        partitionSpec = "",
        properties = Some("'write.data.path'='oss://nonexistent/iceberg/db/oss_scheme'"))
      assertUnsupportedContainsAllowingWriteFailure("oss_scheme", "storage scheme", "oss")
    }
  }

  test("fall-back: parquet size/limit properties that are not positive Java ints") {
    // iceberg-java parses these with Integer.parseInt (PropertyUtil.propertyAsInt): no trimming,
    // no values past Int.MaxValue, and parquet-mr rejects non-positive results at write time.
    // Each such value must fall back so the failure happens on the stock path, never be
    // silently normalised by the native translation.
    withDetectionCatalog { dir =>
      val keys = Seq(
        "write.parquet.row-group-size-bytes",
        "write.parquet.page-size-bytes",
        "write.parquet.page-row-limit",
        "write.parquet.dict-size-bytes")
      val badValues = Seq("garbage", "0", "-1", "2147483648", " 1024")
      keys.zipWithIndex.foreach { case (key, ki) =>
        badValues.zipWithIndex.foreach { case (value, vi) =>
          val table = s"bad_int_${ki}_$vi"
          createTable(dir, table, partitionSpec = "", properties = Some(s"'$key'='$value'"))
          assertUnsupportedContainsAllowingWriteFailure(table, key)
        }
      }
      // A positive Java int is Compatible, pinning that the gate is not over-broad.
      createTable(
        dir,
        "good_int",
        partitionSpec = "",
        properties = Some("'write.parquet.page-size-bytes'='1048576'"))
      assertSupportLevelIs[Compatible]("good_int")
    }
  }

  test("write proto forwards fs.s3a.* Hadoop configuration as s3.* FileIO properties") {
    // HadoopFileIO carries S3A credentials/endpoint/path-style through the Hadoop
    // Configuration, not FileIO.properties(); the JVM writer honours them, so the native
    // writer must receive them too (translated to the s3.* keys iceberg-rust consumes,
    // mirroring the scan side). SQLConf entries are copied verbatim into
    // sessionState.newHadoopConf(), so plain fs.s3a.* keys set here are what the gate and
    // proto assembly see. LocalTableScan conversion is enabled the same way the write action
    // suite does, so the VALUES insert converts and the built proto is inspectable.
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "s3a_props",
        partitionSpec = "",
        properties = Some("'write.data.path'='s3a://probe-bucket/iceberg/db/s3a_props'"))
      val conf = spark.sessionState.conf
      conf.setConfString(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key, "true")
      conf.setConfString("fs.s3a.endpoint", "http://localhost:9000")
      conf.setConfString("fs.s3a.access.key", "probe-access-key")
      conf.setConfString("fs.s3a.path.style.access", "true")
      try {
        val plan = captureWritePlan("s3a_props", allowWriteFailure = true) {
          spark.sql(s"INSERT INTO $catalog.$ns.s3a_props VALUES (1, 'us', 1.0)")
        }
        val cometWrite = findCometWriteExec(plan)
          .getOrElse(fail(s"expected CometIcebergWriteExec in:\n$plan"))
        val props = cometWrite.nativeOp.getIcebergWrite.getCommon.getCatalogPropertiesMap
        assert(props.get("s3.endpoint") == "http://localhost:9000", props)
        assert(props.get("s3.access-key-id") == "probe-access-key", props)
        assert(props.get("s3.path-style-access") == "true", props)
        // The exec's string rendering must never fall through to the protobuf's toString:
        // Spark's argString redaction only covers Scala Maps, so the property bag -- now
        // carrying credential-shaped values like the access key above -- would land verbatim
        // in explain(), the SQL UI, and the event log.
        val rendered = cometWrite.simpleString(Int.MaxValue)
        assert(!rendered.contains("probe-access-key"), rendered)
        assert(rendered.contains("s3a://probe-bucket"), rendered)
      } finally {
        conf.unsetConf("fs.s3a.path.style.access")
        conf.unsetConf("fs.s3a.access.key")
        conf.unsetConf("fs.s3a.endpoint")
        conf.unsetConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key)
      }
    }
  }

  test("fall-back: catalog-level custom FileIO that no property reveals") {
    // `io-impl` set at the CATALOG level never appears in table or write properties, so the
    // property rule cannot see it -- only inspecting the instantiated table.io() can. The test
    // FileIO delegates to HadoopFileIO by composition (inheritance would pass the hierarchy
    // check, by design), so the table itself works normally. A dedicated catalog name is
    // required: Spark caches catalog instances per session, so adding `io-impl` to the shared
    // detection catalog's conf would not reach an already-instantiated catalog.
    withTempIcebergDir { warehouseDir =>
      val ioCat = "io_probe_cat"
      withSQLConf(
        s"spark.sql.catalog.$ioCat" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$ioCat.type" -> "hadoop",
        s"spark.sql.catalog.$ioCat.warehouse" -> warehouseDir.getAbsolutePath,
        s"spark.sql.catalog.$ioCat.io-impl" -> classOf[DetectionDelegatingFileIO].getName) {
        spark.sql(s"""
          CREATE TABLE $ioCat.$ns.catalog_io (
            id INT,
            region STRING,
            amount DOUBLE
          ) USING iceberg
        """)
        val writeExec = captureWriteExec("catalog_io", allowWriteFailure = true) {
          spark.sql(s"INSERT INTO $ioCat.$ns.catalog_io VALUES (1, 'us', 1.0)")
        }
        assertUnsupportedContains(
          writeExec,
          "catalog_io",
          "table.io()",
          classOf[DetectionDelegatingFileIO].getName)
      }
    }
  }

  test("Compatible when the data location is an explicit file:// path") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "file_scheme",
        partitionSpec = "",
        properties = Some(s"'write.data.path'='file://${dir.getAbsolutePath}/file_scheme_data'"))
      assertSupportLevelIs[Compatible]("file_scheme")
    }
  }

  test("fall-back: write.parquet.shred-variants=true") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "shred",
        partitionSpec = "",
        properties = Some("'write.parquet.shred-variants'='true'"))
      assertUnsupportedContains("shred", "write.parquet.shred-variants")
    }
  }

  test("Compatible even for an unparseable write.metadata.metrics.default") {
    // Iceberg-Java's MetricsConfig is lenient: it warns and falls back to the default mode on
    // both paths, so the gate has nothing to protect; the JVM-side metrics assembly goes
    // through the same lenient parse.
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "metrics_typo",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.default'='truncat(16)'"))
      assertSupportLevelIs[Compatible]("metrics_typo")
    }
  }

  test("Compatible when write.spark.fanout.enabled=true") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "fanout",
        partitionSpec = "PARTITIONED BY (bucket(4, id))",
        properties = Some("'write.spark.fanout.enabled'='true'"))
      assertSupportLevelIs[Compatible]("fanout")
    }
  }

  test("Compatible when write.target-file-size-bytes is non-default") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "target_size",
        partitionSpec = "",
        properties = Some("'write.target-file-size-bytes'='1048576'"))
      assertSupportLevelIs[Compatible]("target_size")
    }
  }

  test("no fall-back reason is recorded when the iceberg write feature is disabled") {
    withDetectionCatalog { dir =>
      createTable(dir, "flag_off", partitionSpec = "")
      withSQLConf(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key -> "false") {
        val writeExec = insertWriteExec("flag_off")
        assert(
          writeExec.getTagValue(CometExplainInfo.FALLBACK_REASONS).isEmpty,
          "expected no fall-back reason on the write exec when the feature is disabled")
      }
    }
  }

  test("fall-back: BatchWrite that is not an Iceberg SparkWrite") {
    withDetectionCatalog { dir =>
      createTable(dir, "plain_write", partitionSpec = "")
      val stub = new org.apache.spark.sql.connector.write.BatchWrite {
        override def createBatchWriterFactory(
            info: org.apache.spark.sql.connector.write.PhysicalWriteInfo)
            : org.apache.spark.sql.connector.write.DataWriterFactory =
          throw new UnsupportedOperationException("stub")
        override def commit(
            messages: Array[org.apache.spark.sql.connector.write.WriterCommitMessage]): Unit =
          ()
        override def abort(
            messages: Array[org.apache.spark.sql.connector.write.WriterCommitMessage]): Unit =
          ()
      }
      val fake = insertWriteExec("plain_write").copy(batchWrite = stub)
      assertUnsupportedContains(fake, "plain_write", "not an Iceberg SparkWrite")
    }
  }

  test("Compatible when partitioned by a bucket transform") {
    withDetectionCatalog { dir =>
      createTable(dir, "part_bucket", partitionSpec = "PARTITIONED BY (bucket(4, id))")
      assertSupportLevelIs[Compatible]("part_bucket")
    }
  }

  test("Compatible when partitioned by identity on a string column") {
    withDetectionCatalog { dir =>
      createTable(dir, "part_string", partitionSpec = "PARTITIONED BY (region)")
      assertSupportLevelIs[Compatible]("part_string")
    }
  }

  test("fall-back: uuid column in the write schema") {
    withDetectionCatalog { dir =>
      // Spark DDL cannot declare `uuid`, so evolve the schema through the Iceberg API. Spark
      // plans the column as StringType, but the native writer's target Arrow schema demands
      // FixedSizeBinary(16) with no cast from Utf8 -- detection must decline before execution.
      createTable(dir, "uuid_col", partitionSpec = "")
      addIcebergColumn(loadIcebergTable(spark, catalog, ns, "uuid_col"), "u", icebergUuidType())
      spark.sql(s"REFRESH TABLE $catalog.$ns.uuid_col")
      val writeExec = captureWriteExec("uuid_col", allowWriteFailure = false) {
        spark.sql(
          s"INSERT INTO $catalog.$ns.uuid_col VALUES " +
            "(1, 'us', 1.0, 'f47ac10b-58cc-4372-a567-0e02b2c3d479')")
      }
      assertUnsupportedContains(writeExec, "uuid_col", "column u has Iceberg type uuid")
    }
  }

  private val catalog = "cat"
  private val ns = "db"

  private def withDetectionCatalog(f: File => Unit): Unit = withTempIcebergDir { warehouseDir =>
    withSQLConf(
      s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog.warehouse" -> warehouseDir.getAbsolutePath) {
      f(warehouseDir)
    }
  }

  private def createTable(
      warehouseDir: File,
      tableName: String,
      partitionSpec: String,
      properties: Option[String] = None): Unit = {
    val props = properties.map(s => s" TBLPROPERTIES ($s)").getOrElse("")
    spark.sql(s"""
      CREATE TABLE $catalog.$ns.$tableName (
        id INT,
        region STRING,
        amount DOUBLE
      ) USING iceberg
      $partitionSpec
      $props
    """)
  }

  private def insertWriteExec(
      tableName: String,
      allowWriteFailure: Boolean = false): IcebergWriteExec =
    captureWriteExec(tableName, allowWriteFailure) {
      spark.sql(s"INSERT INTO $catalog.$ns.$tableName VALUES (1, 'us', 1.0)")
    }

  private def dfWriteExec(tableName: String, options: (String, String)*): IcebergWriteExec =
    captureWriteExec(tableName, allowWriteFailure = false) {
      val df = spark
        .createDataFrame(Seq((1, "us", 1.0)))
        .toDF("id", "region", "amount")
      val writer = options.foldLeft(df.writeTo(s"$catalog.$ns.$tableName")) { case (w, (k, v)) =>
        w.option(k, v)
      }
      writer.append()
    }

  private def captureWriteExec(tableName: String, allowWriteFailure: Boolean)(
      trigger: => Unit): IcebergWriteExec =
    findWriteExecOrFail(captureWritePlan(tableName, allowWriteFailure)(trigger))

  private def captureWritePlan(tableName: String, allowWriteFailure: Boolean)(
      trigger: => Unit): org.apache.spark.sql.execution.SparkPlan = {
    val captured =
      new java.util.concurrent.atomic.AtomicReference[org.apache.spark.sql.execution.SparkPlan]()
    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(
          funcName: String,
          qe: org.apache.spark.sql.execution.QueryExecution,
          durationNs: Long): Unit =
        captured.compareAndSet(null, qe.executedPlan)
      override def onFailure(
          funcName: String,
          qe: org.apache.spark.sql.execution.QueryExecution,
          exception: Exception): Unit =
        captured.compareAndSet(null, qe.executedPlan)
    }
    var failure: Option[Throwable] = None
    try org.apache.spark.CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    catch { case _: java.util.concurrent.TimeoutException => () }
    spark.listenerManager.register(listener)
    try {
      try trigger
      catch { case scala.util.control.NonFatal(t) => failure = Some(t) }
      try org.apache.spark.CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      catch { case _: java.util.concurrent.TimeoutException => () }
    } finally {
      spark.listenerManager.unregister(listener)
    }
    if (!allowWriteFailure) {
      failure.foreach(t => fail(s"write to $tableName failed unexpectedly", t))
    }
    Option(captured.get())
      .getOrElse(fail(s"No QueryExecution captured for $tableName"))
  }

  private def findWriteExecOrFail(
      plan: org.apache.spark.sql.execution.SparkPlan): IcebergWriteExec =
    findWriteExec(plan).getOrElse(fail(s"no IcebergWriteExec found in:\n$plan"))

  private def findWriteExec(
      plan: org.apache.spark.sql.execution.SparkPlan): Option[IcebergWriteExec] =
    plan match {
      case e: IcebergWriteExec => Some(e)
      case other =>
        val descend = other.children.iterator ++ wrappedChildren(other).iterator
        descend.flatMap(findWriteExec).toSeq.headOption
    }

  private def wrappedChildren(plan: org.apache.spark.sql.execution.SparkPlan)
      : Iterable[org.apache.spark.sql.execution.SparkPlan] = {
    def viaAccessor(method: String): Option[org.apache.spark.sql.execution.SparkPlan] =
      scala.util
        .Try {
          plan.getClass
            .getMethod(method)
            .invoke(plan)
            .asInstanceOf[org.apache.spark.sql.execution.SparkPlan]
        }
        .toOption
        .filter(_ ne plan)
    Seq("commandPhysicalPlan", "executedPlan", "plan").flatMap(viaAccessor)
  }

  private def assertSupportLevelIs[T <: SupportLevel: scala.reflect.ClassTag](
      tableName: String,
      allowWriteFailure: Boolean = false): Unit = {
    val expected = scala.reflect.classTag[T].runtimeClass
    val plan = captureWritePlan(tableName, allowWriteFailure) {
      spark.sql(s"INSERT INTO $catalog.$ns.$tableName VALUES (1, 'us', 1.0)")
    }
    findWriteExec(plan) match {
      case Some(writeExec) =>
        val support = CometIcebergNativeWrite.getSupportLevel(writeExec)
        assert(
          expected.isInstance(support),
          s"expected ${expected.getSimpleName} for $tableName, got $support")
      case None =>
        // The write was converted, which is only possible when the serde returned Compatible
        // and the upstream plan was fully Comet-native.
        assert(
          containsCometWriteExec(plan),
          s"no IcebergWriteExec or CometIcebergWriteExec found in:\n$plan")
        assert(
          expected.isInstance(Compatible()),
          s"expected ${expected.getSimpleName} for $tableName, but the write was converted " +
            "to CometIcebergWriteExec (implying Compatible)")
    }
  }

  private def containsCometWriteExec(plan: org.apache.spark.sql.execution.SparkPlan): Boolean =
    plan.isInstanceOf[org.apache.spark.sql.comet.CometIcebergWriteExec] ||
      (plan.children.iterator ++ wrappedChildren(plan).iterator).exists(containsCometWriteExec)

  private def findCometWriteExec(plan: org.apache.spark.sql.execution.SparkPlan)
      : Option[org.apache.spark.sql.comet.CometIcebergWriteExec] =
    plan match {
      case c: org.apache.spark.sql.comet.CometIcebergWriteExec => Some(c)
      case other =>
        (other.children.iterator ++ wrappedChildren(other).iterator)
          .flatMap(findCometWriteExec)
          .toSeq
          .headOption
    }

  private def assertUnsupportedContains(tableName: String, fragments: String*): Unit =
    assertUnsupportedContains(insertWriteExec(tableName), tableName, fragments: _*)

  private def assertUnsupportedContainsAllowingWriteFailure(
      tableName: String,
      fragments: String*): Unit =
    assertUnsupportedContains(
      insertWriteExec(tableName, allowWriteFailure = true),
      tableName,
      fragments: _*)

  private def assertUnsupportedContains(
      writeExec: IcebergWriteExec,
      tableName: String,
      fragments: String*): Unit = {
    val support = CometIcebergNativeWrite.getSupportLevel(writeExec)
    support match {
      case Unsupported(Some(reason)) =>
        fragments.foreach(f =>
          assert(reason.contains(f), s"reason '$reason' missing fragment '$f' for $tableName"))
      case Unsupported(None) =>
        fail(s"Unsupported without a reason string for $tableName")
      case other =>
        fail(s"expected Unsupported for $tableName, got $other")
    }
  }
}

/**
 * A FileIO that works normally (delegating to HadoopFileIO) but whose class is not on Comet's
 * recognized-FileIO allowlist. Composition rather than inheritance is the point: a HadoopFileIO
 * SUBCLASS passes the hierarchy check by design, while this class must be declined. Instantiated
 * reflectively by Iceberg's `CatalogUtil.loadFileIO`, hence top-level with a no-arg constructor.
 */
class DetectionDelegatingFileIO extends FileIO with HadoopConfigurable {
  private val delegate = new HadoopFileIO()

  override def newInputFile(path: String): InputFile = delegate.newInputFile(path)
  override def newOutputFile(path: String): OutputFile = delegate.newOutputFile(path)
  override def deleteFile(path: String): Unit = delegate.deleteFile(path)
  override def initialize(properties: java.util.Map[String, String]): Unit =
    delegate.initialize(properties)
  override def setConf(conf: Configuration): Unit = delegate.setConf(conf)
  // No `override` modifier: `Configurable.getConf` exists on some supported Iceberg versions
  // (e.g. 1.8.1) and not others, and a plain def satisfies both shapes.
  def getConf: Configuration = delegate.getConf
  override def serializeConfWith(
      confSerializer: java.util.function.Function[
        Configuration,
        SerializableSupplier[Configuration]]): Unit =
    delegate.serializeConfWith(confSerializer)
}
