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
import org.apache.iceberg.{PartitionSpec, StructLike, TableMetadata, TableOperations}
import org.apache.iceberg.aws.s3.S3FileIO
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.encryption.EncryptionManager
import org.apache.iceberg.hadoop.{HadoopCatalog, HadoopConfigurable, HadoopFileIO}
import org.apache.iceberg.io.{FileIO, InputFile, LocationProvider, OutputFile, ResolvingFileIO}
import org.apache.iceberg.util.SerializableSupplier
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.comet.{CometIcebergWriteExec, CometSparkToColumnarExec, IcebergWriteExec}
import org.apache.spark.sql.execution.{ApplyColumnarRulesAndInsertTransitions, ColumnarToRowExec, CommandExecutionMode, LeafExecNode, SparkPlan}
import org.apache.spark.sql.types.{BinaryType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus}
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.rules.EliminateRedundantTransitions
import org.apache.comet.serde.{Compatible, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator
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

  test("plan-only mode leaves the write with Spark") {
    withDetectionCatalog { dir =>
      createTable(dir, "plan_only", partitionSpec = "")
      // IcebergWriteStrategy runs before CometRule, so it needs its own plan-only guard.
      // withSQLConf returns Unit on Spark 3.4/3.5, hence the var.
      var plan: SparkPlan = null
      withSQLConf(CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true") {
        plan = captureWritePlan("plan_only", allowWriteFailure = false) {
          spark.sql(s"INSERT INTO $catalog.$ns.plan_only VALUES (1, 'us', 1.0)")
        }
      }
      assert(
        findWriteExec(plan).isEmpty,
        s"plan-only mode must not split the write into Comet's two-operator shape:\n$plan")
      assert(
        !containsCometWriteExec(plan),
        s"plan-only mode must not offload the write to Comet:\n$plan")
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
      val provider = IcebergReflection
        .getLocationProvider(table)
        .getOrElse(fail("locationProvider"))
      assert(
        provider.getClass.getName == IcebergReflection.ClassNames.DEFAULT_LOCATION_PROVIDER,
        provider.getClass.getName)
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

  test("fall-back: custom TableOperations LocationProvider that no property reveals") {
    withTempIcebergDir { warehouseDir =>
      val locationCat = "location_provider_probe_cat"
      withSQLConf(
        s"spark.sql.catalog.$locationCat" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$locationCat.catalog-impl" ->
          classOf[DetectionCustomLocationHadoopCatalog].getName,
        s"spark.sql.catalog.$locationCat.warehouse" -> warehouseDir.getAbsolutePath) {
        spark.sql(s"""
          CREATE TABLE $locationCat.$ns.custom_location_provider (
            id INT,
            region STRING,
            amount DOUBLE
          ) USING iceberg
        """)
        val writeExec =
          planInsertWriteExec(s"$locationCat.$ns.custom_location_provider")
        assertUnsupportedContains(
          writeExec,
          "custom_location_provider",
          "table.locationProvider()",
          classOf[DetectionDelegatingLocationProvider].getName)
      }
    }
  }

  test("Compatible for a format-version=3 append") {
    assume(isSpark35Plus, "V3 tables require Iceberg 1.8.1+ (Spark 3.5 profile)")
    withDetectionCatalog { dir =>
      createTable(dir, "v3", partitionSpec = "", properties = Some("'format-version'='3'"))
      assertSupportLevelIs[Compatible]("v3")
    }
  }

  test("fall-back: format-version=4") {
    assume(icebergVersionAtLeast(1, 10), "V4 tables require Iceberg 1.10+")
    withDetectionCatalog { dir =>
      createTable(dir, "v4", partitionSpec = "", properties = Some("'format-version'='4'"))
      assertUnsupportedContains("v4", "format-version=4")
    }
  }

  test("fall-back: variant column in the write schema") {
    assume(isSpark40Plus, "VARIANT requires Spark 4.0+")
    assume(icebergVersionAtLeast(1, 10), "VARIANT columns require Iceberg 1.10+")
    withDetectionCatalog { _ =>
      spark.sql(s"""
        CREATE TABLE $catalog.$ns.variant_col (id INT, v VARIANT) USING iceberg
        TBLPROPERTIES ('format-version'='3')
      """)
      val writeExec = captureWriteExec("variant_col", allowWriteFailure = false) {
        spark.sql(s"""INSERT INTO $catalog.$ns.variant_col VALUES (1, parse_json('{"a": 1}'))""")
      }
      assertUnsupportedContains(writeExec, "variant_col", "column v has Iceberg type variant")
    }
  }

  test("fall-back: unknown column in the write schema") {
    assume(icebergVersionAtLeast(1, 10), "The unknown type requires Iceberg 1.10+")
    withDetectionCatalog { dir =>
      // Spark DDL cannot declare `unknown` (Iceberg plans it as Spark's NullType), so evolve the
      // schema through the Iceberg API.
      createTable(
        dir,
        "unknown_col",
        partitionSpec = "",
        properties = Some("'format-version'='3'"))
      addIcebergColumn(
        loadIcebergTable(spark, catalog, ns, "unknown_col"),
        "u",
        icebergUnknownType())
      spark.sql(s"REFRESH TABLE $catalog.$ns.unknown_col")
      val writeExec = captureWriteExec("unknown_col", allowWriteFailure = true) {
        spark.sql(s"INSERT INTO $catalog.$ns.unknown_col VALUES (1, 'us', 1.0, NULL)")
      }
      assertUnsupportedContains(writeExec, "unknown_col", "column u has Iceberg type unknown")
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

  test("fall-back: write.parquet.bloom-filter-max-bytes set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "bloom_max",
        partitionSpec = "",
        properties = Some("'write.parquet.bloom-filter-max-bytes'='524288'"))
      assertUnsupportedContains("bloom_max", "write.parquet.bloom-filter-max-bytes")
    }
  }

  test("fall-back: per-column bloom filter enabled") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "bloom_col",
        partitionSpec = "",
        properties = Some("'write.parquet.bloom-filter-enabled.column.id'='true'"))
      assertUnsupportedContains(
        "bloom_col",
        "write.parquet.bloom-filter-enabled.column.id",
        "true")
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

  test("fall-back: mixed-case data location scheme (native opens the location verbatim)") {
    // OpenDAL strips the scheme prefix from a path case-sensitively, so `S3://` cannot be opened
    // natively even though `s3://` can. The gate must match the scheme verbatim and decline it.
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "mixed_case_scheme",
        partitionSpec = "",
        properties =
          Some("'write.data.path'='S3://nonexistent-bucket/iceberg/db/mixed_case_scheme'"))
      assertUnsupportedContainsAllowingWriteFailure("mixed_case_scheme", "storage scheme", "S3")
    }
  }

  test("fall-back: hostless hdfs:/ data location is read as hdfs, not file") {
    // Hadoop normalises `hdfs:///p` to `hdfs:/p`; with no `://` the gate used to call it `file`.
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "hostless_hdfs",
        partitionSpec = "",
        properties = Some("'write.data.path'='hdfs:/iceberg/db/hostless_hdfs'"))
      assertUnsupportedContains(
        planInsertWriteExec(s"$catalog.$ns.hostless_hdfs"),
        "hostless_hdfs",
        "unsupported storage scheme: hdfs")
    }
  }

  test("fall-back: s3 data location without a bucket in its authority") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "hostless_s3",
        partitionSpec = "",
        properties = Some("'write.data.path'='s3:/nonexistent-bucket/iceberg/db/hostless_s3'"))
      assertUnsupportedContains(
        planInsertWriteExec(s"$catalog.$ns.hostless_s3"),
        "hostless_s3",
        "s3 data location has no bucket")
    }
  }

  test("storageScheme follows the native scheme_of rule") {
    // Keep in step with `scheme_of_extracts_scheme_from_all_uri_forms` in iceberg_common.rs.
    Seq(
      "hdfs:/warehouse/t" -> "hdfs",
      "hdfs:///warehouse/t" -> "hdfs",
      "hdfs://nn:8020/warehouse/t" -> "hdfs",
      "s3://bucket/key" -> "s3",
      "s3:/bucket/key" -> "s3",
      "blob:/bucket/key" -> "blob",
      "memory:/x" -> "memory",
      "file:///tmp/x" -> "file",
      "file:/tmp/x" -> "file",
      "/tmp/no-scheme" -> "file",
      "/tmp/a:b" -> "file",
      "S3://bucket/key" -> "S3").foreach { case (location, expected) =>
      assert(CometIcebergNativeWrite.storageScheme(location) == expected, location)
    }
  }

  test("hasBucketAuthority requires a non-empty host after //") {
    Seq("s3://bucket/key", "s3a://bucket", "gs://bucket/x").foreach { location =>
      assert(CometIcebergNativeWrite.hasBucketAuthority(location), location)
    }
    Seq("s3:/bucket/key", "s3:///bucket/key", "s3:bucket/key", "gs://").foreach { location =>
      assert(!CometIcebergNativeWrite.hasBucketAuthority(location), location)
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

  test("S3 setting allow-lists reject unknown keys deterministically without values") {
    val hadoopConf = new Configuration(false)
    val secret = "SECRET_VALUE_MUST_NOT_APPEAR"
    Seq(
      "fs.s3a.access.key",
      "fs.s3a.secret.key",
      "fs.s3a.session.token",
      "fs.s3a.endpoint",
      "fs.s3a.endpoint.region",
      "fs.s3a.path.style.access",
      "fs.s3a.bucket.target.access.key",
      "fs.s3a.bucket.target.path.style.access",
      // A per-bucket property for another bucket is not effective for this write.
      "fs.s3a.bucket.other.encryption.algorithm").foreach(hadoopConf.set(_, "supported"))
    hadoopConf.set("fs.s3a.encryption.key", secret)
    hadoopConf.set("fs.s3a.aws.credentials.provider", secret)
    hadoopConf.set("fs.s3a.bucket.target.encryption.algorithm", secret)

    val unsupportedHadoop =
      CometIcebergNativeWrite.unsupportedHadoopS3Settings(hadoopConf, Some("target"))
    assert(
      unsupportedHadoop == Seq(
        "fs.s3a.aws.credentials.provider",
        "fs.s3a.bucket.target.encryption.algorithm",
        "fs.s3a.encryption.key"),
      unsupportedHadoop)
    assert(!unsupportedHadoop.exists(_.contains(secret)), unsupportedHadoop)

    val supportedFileIO = Map(
      "s3.endpoint" -> "endpoint",
      "s3.access-key-id" -> "access",
      "s3.secret-access-key" -> secret,
      "s3.session-token" -> secret,
      "s3.region" -> "us-east-1",
      "client.region" -> "us-east-1",
      "s3.path-style-access" -> "true",
      "s3.sse.type" -> "kms",
      "s3.sse.key" -> "key-id",
      "s3.sse.md5" -> "md5",
      "client.assume-role.arn" -> "arn",
      "client.assume-role.external-id" -> "external",
      "client.assume-role.session-name" -> "session",
      "s3.allow-anonymous" -> "false",
      "s3.disable-ec2-metadata" -> "false",
      "s3.disable-config-load" -> "false",
      "s3.comet.credential.webIdentity.enabled" -> "true",
      "s3.comet.credential.webIdentity.maxAttempts" -> "5",
      "s3.comet.credential.webIdentity.minTtlSeconds" -> "300",
      "s3.comet.credential.webIdentity.refreshJitterSeconds" -> "60",
      "s3.session-token-expires-at-ms" -> "0")
    val unsupportedFileIO = CometIcebergNativeWrite.unsupportedS3FileIOProperties(
      supportedFileIO ++ Map(
        "s3.acl" -> secret,
        "s3.sse.type" -> "dsse-kms",
        "s3.write.tags.foo" -> secret,
        "s3.write.storage-class" -> secret,
        "s3.access-points.bucket" -> secret,
        "s3.remote-signing-enabled" -> secret,
        "client.factory" -> secret,
        "client.credentials-provider" -> secret,
        "unrelated.property" -> secret))
    assert(
      unsupportedFileIO == Seq(
        "client.credentials-provider",
        "client.factory",
        "s3.access-points.bucket",
        "s3.acl",
        "s3.remote-signing-enabled",
        "s3.sse.type",
        "s3.write.storage-class",
        "s3.write.tags.foo"),
      unsupportedFileIO)
    assert(!unsupportedFileIO.exists(_.contains(secret)), unsupportedFileIO)
    assert(
      CometIcebergNativeWrite.unsupportedS3FileIOProperties(Map("s3.sse.type" -> " kms ")) == Seq(
        "s3.sse.type"))

    val withCustomProvider = supportedFileIO ++ Map(
      "s3.comet.credential.provider.class" -> "provider",
      "s3.vendor.credential-scope" -> secret,
      "client.vendor.tenant-id" -> secret,
      // Standard Iceberg settings remain unsupported even when a provider is configured.
      "client.factory" -> secret,
      "client.assume-role.tags.department" -> secret,
      "s3.acl" -> secret,
      "s3.write.tags.foo" -> secret)
    val unsupportedWithCustomProvider =
      CometIcebergNativeWrite.unsupportedS3FileIOProperties(withCustomProvider)
    assert(
      unsupportedWithCustomProvider == Seq(
        "client.assume-role.tags.department",
        "client.factory",
        "s3.acl",
        "s3.write.tags.foo"),
      unsupportedWithCustomProvider)
    assert(
      !unsupportedWithCustomProvider.exists(_.contains(secret)),
      unsupportedWithCustomProvider)
  }

  test("a longer dotted bucket is not the target bucket") {
    val hadoopConf = new Configuration(false)
    hadoopConf.set("fs.s3a.bucket.target.access.key", "access")
    hadoopConf.set("fs.s3a.bucket.target.endpoint.region", "us-east-1")
    hadoopConf.set("fs.s3a.bucket.target.path.style.access", "true")
    // Bucket `target.other` plus suffix `endpoint`, not bucket `target` plus `other.endpoint`.
    hadoopConf.set("fs.s3a.bucket.target.other.endpoint", "https://other.example")
    hadoopConf.set("fs.s3a.bucket.target.other.encryption.algorithm", "SSE-KMS")
    hadoopConf.set("fs.s3a.bucket.target.encryption.algorithm", "SSE-KMS")

    assert(
      CometIcebergNativeWrite.unsupportedHadoopS3Settings(hadoopConf, Some("target")) == Seq(
        "fs.s3a.bucket.target.encryption.algorithm"))

    val longerBucket = new Configuration(false)
    longerBucket.set("fs.s3a.bucket.target.other.endpoint", "https://other.example")
    longerBucket.set("fs.s3a.bucket.target.other.encryption.algorithm", "SSE-KMS")
    assert(
      CometIcebergNativeWrite.unsupportedHadoopS3Settings(longerBucket, Some("target.other")) ==
        Seq("fs.s3a.bucket.target.other.encryption.algorithm"))
  }

  test("missing AWS SDK falls back instead of aborting S3 FileIO classification") {
    val secret = "SECRET_VALUE_MUST_NOT_APPEAR"
    val properties = Map(
      "s3.endpoint" -> "https://example",
      "s3.comet.credential.provider.class" -> "provider",
      "s3.vendor.credential-scope" -> secret,
      "client.vendor.tenant-id" -> secret,
      "s3.acl" -> secret,
      "s3.write.tags.foo" -> secret)
    val expected =
      Seq("client.vendor.tenant-id", "s3.acl", "s3.vendor.credential-scope", "s3.write.tags.foo")
    val loadFailures = Seq[Throwable](
      new NoClassDefFoundError("software/amazon/awssdk/auth/credentials/AwsCredentialsProvider"),
      new ClassNotFoundException("org.apache.iceberg.aws.s3.S3FileIOProperties"))
    loadFailures.foreach { failure =>
      val names = CometIcebergNativeWrite.icebergAwsPropertyNames(_ => throw failure)
      val unsupported =
        CometIcebergNativeWrite.unsupportedS3FileIOProperties(properties, names)
      assert(unsupported == expected, unsupported)
      assert(!unsupported.exists(_.contains(secret)), unsupported)
    }
  }

  test("Hadoop S3A built-in defaults are ignored but custom resources are effective") {
    def xml(key: String, value: String): java.io.ByteArrayInputStream =
      new java.io.ByteArrayInputStream(s"""<configuration>
           |  <property><name>$key</name><value>$value</value></property>
           |</configuration>""".stripMargin.getBytes(java.nio.charset.StandardCharsets.UTF_8))

    val key = "fs.s3a.encryption.algorithm"

    val coreDefaultOnly = new Configuration(false)
    coreDefaultOnly.addResource(xml(key, "SSE-KMS"), "core-default.xml")
    assert(coreDefaultOnly.get(key) == "SSE-KMS")
    assert(
      CometIcebergNativeWrite
        .unsupportedHadoopS3Settings(coreDefaultOnly, Some("target"))
        .isEmpty)

    val customDefault = new Configuration(false)
    customDefault.addResource(xml(key, "SSE-KMS"), "tenant-default.xml")
    assert(customDefault.get(key) == "SSE-KMS")
    assert(
      CometIcebergNativeWrite.unsupportedHadoopS3Settings(customDefault, Some("target")) == Seq(
        key))

    val site = new Configuration(false)
    site.addResource(xml(key, "SSE-KMS"), "probe-site.xml")
    assert(site.get(key) == "SSE-KMS")
    assert(CometIcebergNativeWrite.unsupportedHadoopS3Settings(site, Some("target")) == Seq(key))

    coreDefaultOnly.set(key, "SSE-S3")
    assert(
      CometIcebergNativeWrite.unsupportedHadoopS3Settings(coreDefaultOnly, Some("target")) == Seq(
        key))
  }

  test("catalog Hadoop S3A overrides use the FileIO configuration") {
    withTempIcebergDir { warehouseDir =>
      val catalogWithOverride = "s3_hadoop_override_cat"
      val hadoopKey = "fs.s3a.encryption.algorithm"
      withSQLConf(
        s"spark.sql.catalog.$catalogWithOverride" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$catalogWithOverride.type" -> "hadoop",
        s"spark.sql.catalog.$catalogWithOverride.warehouse" -> warehouseDir.getAbsolutePath,
        s"spark.sql.catalog.$catalogWithOverride.hadoop.$hadoopKey" -> "SSE-KMS") {
        spark.sql(s"""
          CREATE TABLE $catalogWithOverride.$ns.catalog_hadoop_override (
            id INT,
            region STRING,
            amount DOUBLE
          ) USING iceberg
          TBLPROPERTIES (
            'write.data.path'='s3a://probe-bucket/iceberg/db/catalog_hadoop_override'
          )
        """)

        // SparkCatalog retains the initialized FileIO even after its options change.
        withSQLConf(s"spark.sql.catalog.$catalogWithOverride.hadoop.$hadoopKey" -> "") {
          val writeExec =
            planInsertWriteExec(s"$catalogWithOverride.$ns.catalog_hadoop_override")
          val sparkWrite = IcebergReflection
            .getOuterSparkWrite(writeExec.batchWrite)
            .getOrElse(fail("could not unwrap SparkWrite"))
          val table = IcebergReflection
            .getTableFromSparkWrite(sparkWrite)
            .getOrElse(fail("could not extract Iceberg table"))
          val fileIOConf = IcebergReflection
            .getFileIOHadoopConf(table)
            .getOrElse(fail("FileIO did not expose its Hadoop configuration"))
          assert(fileIOConf.get(hadoopKey) == "SSE-KMS")
          assertUnsupportedContains(
            writeExec,
            "catalog_hadoop_override",
            s"unsupported Hadoop S3A setting: $hadoopKey")
        }
      }
    }
  }

  test("supported catalog Hadoop S3A overrides are forwarded to the native write") {
    withTempIcebergDir { warehouseDir =>
      val catalogWithOverride = "s3_hadoop_forwarding_cat"
      val endpoint = "https://s3.example.test"
      withSQLConf(
        "fs.s3a.endpoint" -> "https://session.example.test",
        s"spark.sql.catalog.$catalogWithOverride" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$catalogWithOverride.type" -> "hadoop",
        s"spark.sql.catalog.$catalogWithOverride.warehouse" -> warehouseDir.getAbsolutePath,
        s"spark.sql.catalog.$catalogWithOverride.hadoop.fs.s3a.endpoint" -> endpoint,
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        spark.sql(s"""
          CREATE TABLE $catalogWithOverride.$ns.catalog_hadoop_forwarding (
            id INT,
            region STRING,
            amount DOUBLE
          ) USING iceberg
          TBLPROPERTIES (
            'write.data.path'='s3a://probe-bucket/iceberg/db/catalog_hadoop_forwarding'
          )
        """)

        val plan = captureWritePlan("catalog_hadoop_forwarding", allowWriteFailure = true) {
          spark.sql(
            s"INSERT INTO $catalogWithOverride.$ns.catalog_hadoop_forwarding " +
              "VALUES (1, 'us', 1.0)")
        }
        val cometWrite = findCometWriteExec(plan)
          .getOrElse(fail(s"expected CometIcebergWriteExec in:\n$plan"))
        val properties = cometWrite.nativeOp.getIcebergWrite.getCommon.getCatalogPropertiesMap
        assert(properties.get("s3.endpoint") == endpoint, properties)
      }
    }
  }

  test("native write preserves FileIO values after catalog initialization") {
    withTempIcebergDir { warehouseDir =>
      val initializedCatalog = "s3_initialized_cat"
      val endpoint = "https://original.example.test"
      withSQLConf(
        "fs.s3a.endpoint" -> endpoint,
        s"spark.sql.catalog.$initializedCatalog" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$initializedCatalog.type" -> "hadoop",
        s"spark.sql.catalog.$initializedCatalog.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        spark.sql(s"""
          CREATE TABLE $initializedCatalog.$ns.initialized_s3 (id INT, region STRING, amount DOUBLE)
          USING iceberg
          TBLPROPERTIES ('write.data.path'='s3a://probe-bucket/iceberg/db/initialized_s3')
        """)

        withSQLConf(
          "fs.s3a.endpoint" -> "https://session-changed.example.test",
          "fs.s3a.encryption.algorithm" -> "SSE-KMS",
          s"spark.sql.catalog.$initializedCatalog.hadoop.fs.s3a.endpoint" ->
            "https://changed.example.test",
          s"spark.sql.catalog.$initializedCatalog.hadoop.fs.s3a.encryption.algorithm" ->
            "SSE-KMS") {
          // Planning only: inspect the native proto without issuing any S3 requests.
          val insert = spark.sessionState.sqlParser.parsePlan(
            s"INSERT INTO $initializedCatalog.$ns.initialized_s3 VALUES (1, 'us', 1.0)")
          val plan =
            spark.sessionState.executePlan(insert, CommandExecutionMode.SKIP).executedPlan
          val cometWrite = findCometWriteExec(plan)
            .getOrElse(fail(s"expected CometIcebergWriteExec in:\n$plan"))
          val properties = cometWrite.nativeOp.getIcebergWrite.getCommon.getCatalogPropertiesMap
          assert(properties.get("s3.endpoint") == endpoint, properties)
          assert(!properties.containsKey("s3.sse.type"), properties)
        }
      }
    }
  }

  test("S3FileIO ignores Hadoop options after catalog initialization") {
    withTempIcebergDir { warehouseDir =>
      val s3Catalog = "s3_non_hadoop_cat"
      withSQLConf(
        s"spark.sql.catalog.$s3Catalog" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$s3Catalog.catalog-impl" ->
          classOf[DetectionS3FileIOHadoopCatalog].getName,
        s"spark.sql.catalog.$s3Catalog.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        spark.sql(s"""
          CREATE TABLE $s3Catalog.$ns.initialized_s3 (id INT, region STRING, amount DOUBLE)
          USING iceberg
          TBLPROPERTIES ('write.data.path'='s3://probe-bucket/iceberg/db/initialized_s3')
        """)

        withSQLConf(
          "fs.s3a.endpoint" -> "https://session-changed.example.test",
          "fs.s3a.encryption.algorithm" -> "SSE-KMS",
          s"spark.sql.catalog.$s3Catalog.hadoop.fs.s3a.endpoint" ->
            "https://changed.example.test",
          s"spark.sql.catalog.$s3Catalog.hadoop.fs.s3a.encryption.algorithm" -> "SSE-KMS") {
          // Plan only: the real S3FileIO retains its default endpoint and no S3 requests run.
          val insert = spark.sessionState.sqlParser.parsePlan(
            s"INSERT INTO $s3Catalog.$ns.initialized_s3 VALUES (1, 'us', 1.0)")
          val plan =
            spark.sessionState.executePlan(insert, CommandExecutionMode.SKIP).executedPlan
          val cometWrite = findCometWriteExec(plan)
            .getOrElse(fail(s"expected CometIcebergWriteExec in:\n$plan"))
          assert(IcebergReflection.getFileIO(cometWrite.table).exists(_.isInstanceOf[S3FileIO]))
          assert(IcebergReflection.getFileIOHadoopConf(cometWrite.table).isEmpty)
          val properties = cometWrite.nativeOp.getIcebergWrite.getCommon.getCatalogPropertiesMap
          assert(properties.get("client.region") == "us-east-1", properties)
          assert(!properties.containsKey("s3.endpoint"), properties)
          assert(!properties.containsKey("s3.sse.type"), properties)
        }
      }
    }
  }

  test("ResolvingFileIO S3 delegate ignores wrapper Hadoop options") {
    withTempIcebergDir { warehouseDir =>
      val resolvingCatalog = "s3_resolving_hadoop_cat"
      withSQLConf(
        s"spark.sql.catalog.$resolvingCatalog" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$resolvingCatalog.type" -> "hadoop",
        s"spark.sql.catalog.$resolvingCatalog.warehouse" -> warehouseDir.getAbsolutePath,
        s"spark.sql.catalog.$resolvingCatalog.io-impl" -> classOf[ResolvingFileIO].getName,
        s"spark.sql.catalog.$resolvingCatalog.client.region" -> "us-east-1",
        s"spark.sql.catalog.$resolvingCatalog.hadoop.fs.s3a.endpoint" ->
          "https://catalog.example.test",
        s"spark.sql.catalog.$resolvingCatalog.hadoop.fs.s3a.encryption.algorithm" -> "SSE-KMS",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        Seq("s3", "s3a").foreach { scheme =>
          val tableName = s"resolving_$scheme"
          val location = s"$scheme://probe-bucket/iceberg/db/$tableName"
          spark.sql(s"""
            CREATE TABLE $resolvingCatalog.$ns.$tableName (id INT, region STRING, amount DOUBLE)
            USING iceberg
            TBLPROPERTIES ('write.data.path'='$location')
          """)

          // Planning only: resolve the real S3FileIO without issuing S3 requests.
          val insert = spark.sessionState.sqlParser.parsePlan(
            s"INSERT INTO $resolvingCatalog.$ns.$tableName VALUES (1, 'us', 1.0)")
          val plan =
            spark.sessionState.executePlan(insert, CommandExecutionMode.SKIP).executedPlan
          val cometWrite = findCometWriteExec(plan)
            .getOrElse(fail(s"expected CometIcebergWriteExec in:\n$plan"))
          val fileIO = IcebergReflection
            .getFileIO(cometWrite.table)
            .getOrElse(fail("could not resolve table FileIO"))
          assert(fileIO.isInstanceOf[ResolvingFileIO])
          assert(
            fileIO.asInstanceOf[ResolvingFileIO].getConf.get("fs.s3a.endpoint") ==
              "https://catalog.example.test")
          assert(
            IcebergReflection.resolveFileIOClass(fileIO, location).contains(classOf[S3FileIO]))
          assert(IcebergReflection.getFileIOHadoopConf(cometWrite.table).isEmpty)
          val properties = cometWrite.nativeOp.getIcebergWrite.getCommon.getCatalogPropertiesMap
          assert(properties.get("client.region") == "us-east-1", properties)
          assert(!properties.containsKey("s3.endpoint"), properties)
          assert(!properties.containsKey("s3.sse.type"), properties)
        }
      }
    }
  }

  test("fall-back: unsupported Hadoop S3A setting on an S3 data location") {
    val secret = "SECRET_VALUE_MUST_NOT_APPEAR"
    withSQLConf("fs.s3a.encryption.algorithm" -> secret) {
      withDetectionCatalog { dir =>
        createTable(
          dir,
          "s3a_hadoop_unsupported",
          partitionSpec = "",
          properties =
            Some("'write.data.path'='s3a://probe-bucket/iceberg/db/s3a_hadoop_unsupported'"))
        val writeExec = planInsertWriteExec(s"$catalog.$ns.s3a_hadoop_unsupported")
        val support = CometIcebergNativeWrite.getSupportLevel(writeExec)
        support match {
          case Unsupported(Some(reason)) =>
            assert(reason == "unsupported Hadoop S3A setting: fs.s3a.encryption.algorithm")
            assert(!reason.contains(secret), reason)
          case other => fail(s"expected Unsupported, got $other")
        }
        val planReasons =
          writeExec.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty)
        assert(planReasons.exists(_.contains("fs.s3a.encryption.algorithm")), planReasons)
        assert(!planReasons.exists(_.contains(secret)), planReasons)
        assert(!writeExec.toString.contains(secret), writeExec.toString)
      }
    }
  }

  test("S3-only setting gates do not affect a local data location") {
    withDetectionCatalog { dir =>
      createTable(dir, "local_with_s3_conf", partitionSpec = "")
      withSQLConf("fs.s3a.encryption.algorithm" -> "SSE-KMS") {
        assertSupportLevelIs[Compatible]("local_with_s3_conf")
      }
    }
  }

  test("fall-back: unsupported S3 FileIO properties") {
    withTempIcebergDir { warehouseDir =>
      val fileIOCat = "s3_file_io_probe_cat"
      val secret = "SECRET_VALUE_MUST_NOT_APPEAR"
      withSQLConf(
        s"spark.sql.catalog.$fileIOCat" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$fileIOCat.type" -> "hadoop",
        s"spark.sql.catalog.$fileIOCat.warehouse" -> warehouseDir.getAbsolutePath,
        s"spark.sql.catalog.$fileIOCat.io-impl" -> classOf[ResolvingFileIO].getName,
        s"spark.sql.catalog.$fileIOCat.s3.acl" -> secret,
        s"spark.sql.catalog.$fileIOCat.s3.sse.type" -> "dsse-kms",
        s"spark.sql.catalog.$fileIOCat.s3.write.tags.foo" -> secret) {
        spark.sql(s"""
          CREATE TABLE $fileIOCat.$ns.unsupported_file_io (
            id INT,
            region STRING,
            amount DOUBLE
          ) USING iceberg
          TBLPROPERTIES (
            'write.data.path'='s3a://probe-bucket/iceberg/db/unsupported_file_io'
          )
        """)
        val writeExec = planInsertWriteExec(s"$fileIOCat.$ns.unsupported_file_io")
        val support = CometIcebergNativeWrite.getSupportLevel(writeExec)
        support match {
          case Unsupported(Some(reason)) =>
            assert(
              reason ==
                "unsupported S3 FileIO settings: s3.acl, s3.sse.type, s3.write.tags.foo",
              reason)
            assert(!reason.contains(secret), reason)
          case other => fail(s"expected Unsupported, got $other")
        }
        val planReasons =
          writeExec.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty)
        assert(planReasons.exists(_.contains("s3.acl")), planReasons)
        assert(planReasons.exists(_.contains("s3.write.tags.foo")), planReasons)
        assert(!planReasons.exists(_.contains(secret)), planReasons)
      }
    }
  }

  test("custom credential-provider S3/client properties keep the native writer engaged") {
    withTempIcebergDir { warehouseDir =>
      val fileIOCat = "s3_provider_props_cat"
      val secret = "SECRET_PROVIDER_VALUE_MUST_NOT_APPEAR"
      val providerClass =
        classOf[org.apache.comet.cloud.s3.MinioCometS3CredentialProvider].getName
      withSQLConf(
        s"spark.sql.catalog.$fileIOCat" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$fileIOCat.type" -> "hadoop",
        s"spark.sql.catalog.$fileIOCat.warehouse" -> warehouseDir.getAbsolutePath,
        s"spark.sql.catalog.$fileIOCat.io-impl" -> classOf[ResolvingFileIO].getName,
        s"spark.sql.catalog.$fileIOCat.s3.comet.credential.provider.class" -> providerClass,
        s"spark.sql.catalog.$fileIOCat.s3.vendor.credential-scope" -> secret,
        s"spark.sql.catalog.$fileIOCat.client.vendor.tenant-id" -> "tenant-A",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        spark.sql(s"""
          CREATE TABLE $fileIOCat.$ns.provider_properties (
            id INT,
            region STRING,
            amount DOUBLE
          ) USING iceberg
          TBLPROPERTIES (
            'write.data.path'='s3a://probe-bucket/iceberg/db/provider_properties'
          )
        """)

        val plan = captureWritePlan("provider_properties", allowWriteFailure = true) {
          spark.sql(s"INSERT INTO $fileIOCat.$ns.provider_properties VALUES (1, 'us', 1.0)")
        }
        val cometWrite = findCometWriteExec(plan)
          .getOrElse(fail(s"expected CometIcebergWriteExec in:\n$plan"))
        val properties = cometWrite.nativeOp.getIcebergWrite.getCommon.getCatalogPropertiesMap
        assert(properties.get("s3.vendor.credential-scope") == secret, properties)
        assert(properties.get("client.vendor.tenant-id") == "tenant-A", properties)
        assert(!cometWrite.simpleString(Int.MaxValue).contains(secret))
      }
    }
  }

  test("Compatible when the data location scheme is memory") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "memory_scheme",
        partitionSpec = "",
        properties = Some("'write.data.path'='memory://nonexistent/iceberg/db/memory_scheme'"))
      assertSupportLevelIs[Compatible]("memory_scheme", allowWriteFailure = true)
    }
  }

  test("fall-back: gs data location under HadoopFileIO (fs.gs.* is not forwarded)") {
    // The hadoop catalog's table.io() is a HadoopFileIO. Planned only, never executed: running
    // the write would have the Hadoop GCS connector look for credentials over the network.
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "gs_hadoop_io",
        partitionSpec = "",
        properties = Some("'write.data.path'='gs://nonexistent/iceberg/db/gs_hadoop_io'"))
      assertUnsupportedContains(
        planInsertWriteExec(s"$catalog.$ns.gs_hadoop_io"),
        "gs_hadoop_io",
        "gs://",
        classOf[HadoopFileIO].getName)
    }
  }

  test("gs data location gate decides on the resolved FileIO class") {
    // Deterministic coverage of every branch; the ResolvingFileIO test below depends on which
    // delegate this classpath yields. GCSFileIO is loaded without initialization so the
    // optional GCS client libraries are never touched.
    val location = "gs://bucket/iceberg/db/t"
    val gcsFileIO =
      Class.forName(IcebergReflection.ClassNames.GCS_FILE_IO, false, getClass.getClassLoader)
    assert(CometIcebergNativeWrite.gcsDataLocationRejection(location, Some(gcsFileIO)).isEmpty)
    val hadoop =
      CometIcebergNativeWrite.gcsDataLocationRejection(location, Some(classOf[HadoopFileIO]))
    assert(
      hadoop.exists(r => r.contains("gs://") && r.contains(classOf[HadoopFileIO].getName)),
      hadoop)
    val unresolved = CometIcebergNativeWrite.gcsDataLocationRejection(location, None)
    assert(unresolved.exists(_.contains("gs://")), unresolved)
  }

  test("gs data location under ResolvingFileIO with a GCSFileIO that fails to initialize") {
    // ResolvingFileIO.ioClass maps gs:// to GCSFileIO, but the delegate it instantiates is a
    // HadoopFileIO whenever loading or initializing GCSFileIO throws an IllegalArgumentException,
    // so the gate must judge the instantiated delegate. Iceberg before 1.10 parses gcs.* in
    // GCSFileIO.initialize, so an unparseable chunk size takes that fallback wherever GCSFileIO
    // can be constructed (where the class does not load, the same fallback runs; where its
    // construction fails, Iceberg does not fall back and the delegate is unresolvable). 1.10+
    // defers the parsing to client construction, so there the property leaves the delegate
    // unchanged and the gate must follow whatever Iceberg instantiates.
    withTempIcebergDir { warehouseDir =>
      val location = "gs://nonexistent/iceberg/db/gs_resolving_bad"
      val badProperty = "gcs.channel.read.chunk-size-bytes" -> "invalid"
      def resolveWith(props: java.util.Map[String, String]): Option[Class[_]] = {
        val resolving = new ResolvingFileIO()
        resolving.setConf(new Configuration())
        resolving.initialize(props)
        try IcebergReflection.resolveFileIOClass(resolving, location)
        finally resolving.close()
      }
      val eagerInit = !icebergVersionAtLeast(1, 10)
      val delegate =
        resolveWith(java.util.Collections.singletonMap(badProperty._1, badProperty._2))
      logInfo(
        s"ResolvingFileIO delegate with $badProperty on this classpath: $delegate " +
          s"(eager initialization: $eagerInit)")
      if (eagerInit) {
        assert(delegate.forall(_ == classOf[HadoopFileIO]), delegate)
      } else {
        val unaffected = resolveWith(java.util.Collections.emptyMap[String, String]())
        assert(delegate == unaffected, s"$delegate differs from $unaffected without the property")
      }
      val badCat = "resolving_bad_io_cat"
      withSQLConf(
        s"spark.sql.catalog.$badCat" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$badCat.type" -> "hadoop",
        s"spark.sql.catalog.$badCat.warehouse" -> warehouseDir.getAbsolutePath,
        s"spark.sql.catalog.$badCat.io-impl" -> classOf[ResolvingFileIO].getName,
        s"spark.sql.catalog.$badCat.${badProperty._1}" -> badProperty._2) {
        spark.sql(s"""
          CREATE TABLE $badCat.$ns.gs_resolving_bad (
            id INT,
            region STRING,
            amount DOUBLE
          ) USING iceberg
          TBLPROPERTIES ('write.data.path'='$location')
        """)
        val support = CometIcebergNativeWrite.getSupportLevel(
          planInsertWriteExec(s"$badCat.$ns.gs_resolving_bad"))
        if (delegate.exists(_.getName == IcebergReflection.ClassNames.GCS_FILE_IO)) {
          assert(!eagerInit, "an unparseable chunk size must fail eager initialization")
          assert(
            support.isInstanceOf[Compatible],
            s"expected Compatible via GCSFileIO, got $support")
        } else {
          support match {
            case Unsupported(Some(reason)) =>
              assert(reason.contains("gs://") && !reason.contains("GCSFileIO"), reason)
            case other => fail(s"expected Unsupported with a reason, got $other")
          }
        }
      }
    }
  }

  test("gs data location under ResolvingFileIO is judged by the resolved delegate") {
    // ResolvingFileIO (the REST catalog default) instantiates GCSFileIO for gs:// when the GCS
    // client libraries are present and HadoopFileIO otherwise; the expectation follows whichever
    // this classpath yields.
    withTempIcebergDir { warehouseDir =>
      val location = "gs://nonexistent/iceberg/db/gs_resolving"
      val resolving = new ResolvingFileIO()
      resolving.setConf(new Configuration())
      resolving.initialize(java.util.Collections.emptyMap[String, String]())
      val delegate =
        try IcebergReflection.resolveFileIOClass(resolving, location)
        finally resolving.close()
      logInfo(s"ResolvingFileIO delegate for $location on this classpath: $delegate")
      val resolvingCat = "resolving_io_cat"
      withSQLConf(
        s"spark.sql.catalog.$resolvingCat" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$resolvingCat.type" -> "hadoop",
        s"spark.sql.catalog.$resolvingCat.warehouse" -> warehouseDir.getAbsolutePath,
        s"spark.sql.catalog.$resolvingCat.io-impl" -> classOf[ResolvingFileIO].getName) {
        spark.sql(s"""
          CREATE TABLE $resolvingCat.$ns.gs_resolving (
            id INT,
            region STRING,
            amount DOUBLE
          ) USING iceberg
          TBLPROPERTIES ('write.data.path'='$location')
        """)
        val writeExec = planInsertWriteExec(s"$resolvingCat.$ns.gs_resolving")
        delegate match {
          case Some(cls) if cls.getName == IcebergReflection.ClassNames.GCS_FILE_IO =>
            val support = CometIcebergNativeWrite.getSupportLevel(writeExec)
            assert(
              support.isInstanceOf[Compatible],
              s"expected Compatible via GCSFileIO, got $support")
          case Some(cls) =>
            assertUnsupportedContains(writeExec, "gs_resolving", "gs://", cls.getName)
          case None =>
            assertUnsupportedContains(writeExec, "gs_resolving", "gs://", "could not resolve")
        }
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
      val conf = spark.sessionState.conf
      conf.setConfString(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key, "true")
      conf.setConfString("fs.s3a.endpoint", "http://localhost:9000")
      conf.setConfString("fs.s3a.access.key", "probe-access-key")
      conf.setConfString("fs.s3a.path.style.access", "true")
      try {
        createTable(
          dir,
          "s3a_props",
          partitionSpec = "",
          properties = Some("'write.data.path'='s3a://probe-bucket/iceberg/db/s3a_props'"))
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

  test("fall-back: identity partition on a float or double column") {
    // iceberg-rust groups float partition values with an equality that treats -0.0 and 0.0 as one
    // value, where iceberg-java keeps them apart (#6138).
    withDetectionCatalog { _ =>
      Seq("float" -> "FLOAT", "double" -> "DOUBLE").foreach { case (typeName, sqlType) =>
        val table = s"part_$typeName"
        spark.sql(s"""
          CREATE TABLE $catalog.$ns.$table (id INT, v $sqlType)
          USING iceberg PARTITIONED BY (v)
        """)
        val writeExec = captureWriteExec(table, allowWriteFailure = false) {
          spark.sql(s"INSERT INTO $catalog.$ns.$table VALUES (1, CAST(1.5 AS $sqlType))")
        }
        assertUnsupportedContains(writeExec, table, "partition field v", typeName, "-0.0")
      }
    }
  }

  test("fall-back: identity partition on a nested double field") {
    // The source of a partition field can be nested inside a struct. `Schema.findField` resolves
    // a nested id too, so the rule must not fail open for it.
    withDetectionCatalog { _ =>
      spark.sql(s"""
        CREATE TABLE $catalog.$ns.part_nested (id INT, s STRUCT<v: DOUBLE>)
        USING iceberg PARTITIONED BY (s.v)
      """)
      val writeExec = captureWriteExec("part_nested", allowWriteFailure = false) {
        spark.sql(s"INSERT INTO $catalog.$ns.part_nested VALUES (1, named_struct('v', 1.5D))")
      }
      assertUnsupportedContains(writeExec, "part_nested", "partition field s.v", "double", "-0.0")
    }
  }

  test("fall-back: double identity partition beside a dropped partition field") {
    // A format-version-1 spec keeps a dropped partition field as a `void` transform, and that
    // field's source column can be dropped afterwards. The surviving double field must still be
    // found, whatever the dropped one does to the spec's partition type.
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "part_dropped",
        partitionSpec = "PARTITIONED BY (region, amount)",
        properties = Some("'format-version'='1'"))
      // Loaded afresh for each change: the insert in between commits through another handle.
      def table: org.apache.iceberg.Table =
        loadIcebergTable(spark, catalog, ns, "part_dropped")
          .asInstanceOf[org.apache.iceberg.Table]
      table.updateSpec().removeField("region").commit()
      spark.sql(s"REFRESH TABLE $catalog.$ns.part_dropped")
      assertUnsupportedContains("part_dropped", "partition field amount", "double", "-0.0")

      // Iceberg before 1.11 cannot plan a write once the `void` field's source column is gone.
      // On 1.11 iceberg-java plans it but cannot build a partition key for a spec that mixes that
      // field with a live one, so the write itself fails on either path; only the gate's decision
      // is checked.
      if (icebergVersionAtLeast(1, 11)) {
        table.updateSchema().deleteColumn("region").commit()
        spark.sql(s"REFRESH TABLE $catalog.$ns.part_dropped")
        val writeExec = captureWriteExec("part_dropped", allowWriteFailure = true) {
          spark.sql(s"INSERT INTO $catalog.$ns.part_dropped VALUES (2, 2.0)")
        }
        assertUnsupportedContains(
          writeExec,
          "part_dropped",
          "partition field amount",
          "double",
          "-0.0")
      }
    }
  }

  test("Compatible when a dropped double partition field remains as void") {
    // The `void` field only ever holds null, so there are no signed zeros to keep apart.
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "part_void",
        partitionSpec = "PARTITIONED BY (amount)",
        properties = Some("'format-version'='1'"))
      loadIcebergTable(spark, catalog, ns, "part_void")
        .asInstanceOf[org.apache.iceberg.Table]
        .updateSpec()
        .removeField("amount")
        .commit()
      spark.sql(s"REFRESH TABLE $catalog.$ns.part_void")
      assertSupportLevelIs[Compatible]("part_void")
    }
  }

  // A format-version-1 spec keeps a dropped partition field as a `void` transform, and the
  // field's source column can be dropped afterwards. Neither writer can write through a spec that
  // mixes such a field with a live one: iceberg-java fails building the partition key, and the
  // native writer fails resolving the spec's partition type. Declining keeps the failure
  // iceberg-java's own. The insert fails either way, so only the gate's decision is checked, on
  // an insert that is planned but not run.
  // https://github.com/apache/datafusion-comet/issues/6141
  test("fall-back: void partition field whose source column was dropped, beside a live field") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "void_dropped",
        partitionSpec = "PARTITIONED BY (region, id)",
        properties = Some("'format-version'='1'"))
      // Loaded afresh for each change, as each change commits a new metadata version.
      def table: org.apache.iceberg.Table =
        loadIcebergTable(spark, catalog, ns, "void_dropped")
          .asInstanceOf[org.apache.iceberg.Table]
      table.updateSpec().removeField("id").commit()
      table.updateSchema().deleteColumn("id").commit()
      spark.sql(s"REFRESH TABLE $catalog.$ns.void_dropped")
      val writeExec = planInsertWriteExec(s"$catalog.$ns.void_dropped", values = "('us', 1.0)")
      assertUnsupportedContains(writeExec, "void_dropped", "void", "source column", "dropped")
    }
  }

  test("Compatible when every partition field is void, even with its source column dropped") {
    // Such a spec writes unpartitioned, so the native writer needs no source column for it. On
    // Iceberg 1.8 neither writer can write to this table, since iceberg-java cannot bind the
    // original spec on the executors once its source column is gone, so only the gate's decision
    // is checked, on an insert that is planned but not run.
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "all_void_dropped",
        partitionSpec = "PARTITIONED BY (region)",
        properties = Some("'format-version'='1'"))
      def table: org.apache.iceberg.Table =
        loadIcebergTable(spark, catalog, ns, "all_void_dropped")
          .asInstanceOf[org.apache.iceberg.Table]
      table.updateSpec().removeField("region").commit()
      table.updateSchema().deleteColumn("region").commit()
      spark.sql(s"REFRESH TABLE $catalog.$ns.all_void_dropped")
      val writeExec = planInsertWriteExec(s"$catalog.$ns.all_void_dropped", values = "(1, 1.0)")
      val support = CometIcebergNativeWrite.getSupportLevel(writeExec)
      assert(support.isInstanceOf[Compatible], s"expected Compatible, got $support")
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

  private var catalog = "cat"
  private val ns = "db"

  private def withDetectionCatalog(f: File => Unit): Unit = withTempIcebergDir { warehouseDir =>
    // Spark caches catalog instances, including their initialized Hadoop configurations.
    // Each fixture needs a fresh catalog to observe the settings selected by this test.
    catalog = "cat_" + java.util.UUID.randomUUID().toString.replace("-", "")
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

  /**
   * Plans an INSERT into `qualifiedTable` without executing it and returns its IcebergWriteExec.
   * `CommandExecutionMode.SKIP` keeps `QueryExecution` from eagerly running the write command, so
   * a data location no filesystem on this classpath can reach never triggers a write.
   */
  private def planInsertWriteExec(
      qualifiedTable: String,
      values: String = "(1, 'us', 1.0)"): IcebergWriteExec = {
    val plan =
      spark.sessionState.sqlParser.parsePlan(s"INSERT INTO $qualifiedTable VALUES $values")
    findWriteExecOrFail(
      spark.sessionState.executePlan(plan, CommandExecutionMode.SKIP).executedPlan)
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

  /**
   * Runs Spark's transition insertion followed by [[EliminateRedundantTransitions]] over a
   * hand-built `CometIcebergWriteExec -> CometSparkToColumnarExec -> source` plan and returns the
   * write's final child.
   *
   * Hand-built rather than driven through SQL because the shape depends on a Spark-to-Arrow
   * conversion config admitting the write's source operator, and the set of admitted operators is
   * itself configurable. What matters is the rule's behaviour at that boundary, which this pins
   * directly.
   */
  private def writeChildAfterTransitionRules(source: SparkPlan): SparkPlan = {
    val child = CometSparkToColumnarExec(source)
    val output = Seq(
      AttributeReference(IcebergWriteExec.CommitMessageColumn, BinaryType, nullable = false)())
    val originalPlan = IcebergWriteExec(null, output, child)
    val write = CometIcebergWriteExec(
      Operator.newBuilder().build(),
      originalPlan,
      child,
      output,
      batchWrite = null,
      table = null,
      partitionSpecId = 0)
    val withTransitions = ApplyColumnarRulesAndInsertTransitions(Seq.empty, false).apply(write)
    // Spark must insert a columnar-to-row transition below the row-based write; if it stops doing
    // so the rest of the assertion is vacuous.
    assert(
      withTransitions.asInstanceOf[CometIcebergWriteExec].child.isInstanceOf[ColumnarToRowExec],
      s"expected an inserted ColumnarToRowExec below the write, got:\n$withTransitions")
    EliminateRedundantTransitions(spark)
      .apply(withTransitions)
      .asInstanceOf[CometIcebergWriteExec]
      .child
  }

  // https://github.com/apache/datafusion-comet/issues/5689: the write's input transition has to
  // be stripped before the generic `ColumnarToRowExec(CometSparkToColumnarExec)` cancellation
  // consumes it, otherwise that arm removes the Arrow bridge the write's FFI input depends on.
  // Both source representations are covered because the cancellation treats them differently:
  // over a row source it drops the bridge outright, over a Spark-columnar source it keeps a
  // transition but leaves the write reading Spark `ColumnarVector`s instead of `CometVector`s.
  test("row source keeps its Arrow bridge under the native Iceberg write") {
    val source = TransitionProbeLeaf(columnar = false)
    val child = writeChildAfterTransitionRules(source)
    assert(
      child == CometSparkToColumnarExec(source),
      s"expected the write to sit directly on CometSparkToColumnarExec, got:\n$child")
  }

  test("Spark-columnar source keeps its Arrow bridge under the native Iceberg write") {
    val source = TransitionProbeLeaf(columnar = true)
    val child = writeChildAfterTransitionRules(source)
    assert(
      child == CometSparkToColumnarExec(source),
      s"expected the write to sit directly on CometSparkToColumnarExec, got:\n$child")
  }
}

/**
 * Planning-only leaf used by the transition-boundary tests: `columnar` selects between the two
 * source representations that can sit under a `CometSparkToColumnarExec`. Never executed.
 */
case class TransitionProbeLeaf(columnar: Boolean) extends LeafExecNode {
  override def output: Seq[Attribute] = Seq(
    AttributeReference("id", IntegerType, nullable = false)())
  override def supportsColumnar: Boolean = columnar
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("planning-only node")
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    throw new UnsupportedOperationException("planning-only node")
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

/**
 * A catalog whose TableOperations supplies a custom LocationProvider directly, without setting
 * `write.location-provider.impl`. This is the path the native write gate must inspect explicitly.
 */
class DetectionCustomLocationHadoopCatalog extends HadoopCatalog {
  override protected def newTableOps(identifier: TableIdentifier): TableOperations =
    new DetectionLocationProviderTableOperations(super.newTableOps(identifier))
}

class DetectionLocationProviderTableOperations(delegate: TableOperations)
    extends TableOperations {
  override def current(): TableMetadata = delegate.current()
  override def refresh(): TableMetadata = delegate.refresh()
  override def commit(base: TableMetadata, metadata: TableMetadata): Unit =
    delegate.commit(base, metadata)
  override def io(): FileIO = delegate.io()
  override def encryption(): EncryptionManager = delegate.encryption()
  override def metadataFileLocation(fileName: String): String =
    delegate.metadataFileLocation(fileName)
  override def locationProvider(): LocationProvider =
    new DetectionDelegatingLocationProvider(delegate.locationProvider())
  override def temp(uncommittedMetadata: TableMetadata): TableOperations =
    new DetectionLocationProviderTableOperations(delegate.temp(uncommittedMetadata))
  override def newSnapshotId(): Long = delegate.newSnapshotId()
  override def requireStrictCleanup(): Boolean = delegate.requireStrictCleanup()
}

class DetectionDelegatingLocationProvider(delegate: LocationProvider) extends LocationProvider {
  override def newDataLocation(filename: String): String = delegate.newDataLocation(filename)
  override def newDataLocation(
      spec: PartitionSpec,
      partitionData: StructLike,
      filename: String): String =
    delegate.newDataLocation(spec, partitionData, filename)
}

/** A real S3FileIO for data, with local metadata managed by HadoopCatalog. Planning only. */
class DetectionS3FileIOHadoopCatalog extends HadoopCatalog {
  override protected def newTableOps(identifier: TableIdentifier): TableOperations =
    new DetectionS3FileIOTableOperations(super.newTableOps(identifier))
}

class DetectionS3FileIOTableOperations(delegate: TableOperations)
    extends DetectionLocationProviderTableOperations(delegate) {
  private val s3FileIO = new S3FileIO()
  s3FileIO.initialize(java.util.Collections.singletonMap("client.region", "us-east-1"))

  override def io(): FileIO = s3FileIO
  override def locationProvider(): LocationProvider = delegate.locationProvider()
  override def temp(uncommittedMetadata: TableMetadata): TableOperations =
    new DetectionS3FileIOTableOperations(delegate.temp(uncommittedMetadata))
}
