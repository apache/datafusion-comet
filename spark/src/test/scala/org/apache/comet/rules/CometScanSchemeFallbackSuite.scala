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

package org.apache.comet.rules

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, SaveMode}
import org.apache.spark.sql.comet.{CometIcebergNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

import org.apache.comet.{CometConf, CometIcebergTestBase, NativeBase}
import org.apache.comet.hadoop.fs.{FakeHDFSFileSystem, FakeHdfsSchemeFileSystem, FakeWasbSchemeFileSystem}
import org.apache.comet.iceberg.IcebergStorageSchemes

/**
 * Comet's native readers go through object_store, which understands a fixed set of URL schemes. A
 * custom Hadoop FileSystem scheme object_store can't parse (`fake://`) must NOT be claimed -- it
 * would fail at execution with "Unable to recognise URL". This suite applies `CometScanRule` to
 * the physical plan and asserts fallback (no execution), and unit-tests the two scheme gates
 * directly: `isNativelyReadableScheme` (Parquet) and `isIcebergReadableScheme` (Iceberg).
 * S3-compliant alias schemes like `blob` are opt-in via `fs.comet.s3Compliant.schemes`, and the
 * native probe is cached per probe URL so an authorityless URL can't poison the authority-bearing
 * form of the same scheme.
 */
class CometScanSchemeFallbackSuite extends CometTestBase with CometIcebergTestBase {

  private var fakeRootDir: File = _

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.hadoop.fs.fake.impl", "org.apache.comet.hadoop.fs.FakeHDFSFileSystem")
    // Back the `hdfs` scheme with a local FS so we can exercise an `hdfs://` path without a live
    // cluster. `hdfs` is natively readable by default, so this scan must be CLAIMED, not declined.
    conf.set("spark.hadoop.fs.hdfs.impl", "org.apache.comet.hadoop.fs.FakeHdfsSchemeFileSystem")
    conf.set("spark.hadoop.fs.defaultFS", FakeHDFSFileSystem.PREFIX)
    // Back `wasb` with a local FS so an Iceberg table can live under a `wasb://` warehouse. The
    // native Iceberg storage factory has no arm for it, so that scan must be DECLINED, not claimed.
    conf.set("spark.hadoop.fs.wasb.impl", classOf[FakeWasbSchemeFileSystem].getName)
    // Intentionally NOT setting CometConf.COMET_LIBHDFS_SCHEMES -- `fake` is not natively readable,
    // and `hdfs` must still be claimed by default (mirrors the native `is_hdfs_scheme` default).
    conf
  }

  override def beforeAll(): Unit = {
    fakeRootDir = Files.createTempDirectory(s"comet_scheme_${UUID.randomUUID().toString}").toFile
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    if (fakeRootDir != null) FileUtils.deleteDirectory(fakeRootDir)
    super.afterAll()
  }

  test("native scan declines a filesystem scheme object_store can't read (fake://)") {
    val path = s"${FakeHDFSFileSystem.PREFIX}${fakeRootDir.getAbsolutePath}/data"
    spark.range(0, 10).toDF("id").write.format("parquet").mode(SaveMode.Overwrite).save(path)

    // Clean Spark plan (Comet disabled), then apply CometScanRule directly -- no execution, we
    // only check whether the rule claims the scan. (var capture: withSQLConf returns Unit on 3.5.)
    var sparkPlan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      sparkPlan = spark.read.parquet(path).queryExecution.executedPlan
    }

    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      val transformed = CometScanRule(spark).apply(stripAQEPlan(sparkPlan))

      val cometScans = transformed.collect { case s: CometScanExec => s }
      val sparkScans = transformed.collect { case s: FileSourceScanExec => s }
      assert(
        cometScans.isEmpty,
        "`fake://` is not object_store-readable; the native scan must fall back to Spark, " +
          s"but Comet claimed it:\n$transformed")
      assert(
        sparkScans.size == 1,
        s"expected the scan to remain a Spark FileSourceScanExec:\n$transformed")
    }
  }

  test("both gates: a configured s3-compliant scheme (minio) is admitted") {
    // Neither object_store-native nor in the Iceberg allowlist: declined absent config, admitted
    // on both paths once opted in. A `blob` alias behaves identically on the parquet gate (opt-in,
    // not object_store-native); its Iceberg-gate coverage is below and its end-to-end parquet
    // coverage is in ParquetReadFromS3Suite.
    val uri = new URI("minio://bucket/key.parquet")
    assert(!CometScanRule.isNativelyReadableScheme(uri, Set.empty))
    assert(!CometScanRule.isIcebergReadableScheme(uri, Set.empty))
    assert(CometScanRule.isNativelyReadableScheme(uri, Set("minio")))
    assert(CometScanRule.isIcebergReadableScheme(uri, Set("minio")))
  }

  test("parquet gate: authorityless URI first does not poison the authority-bearing form") {
    // ObjectStoreScheme::parse keys on (scheme, host-presence): `gs:///` (no host) is unrecognized,
    // `gs://bucket/` is. The cache is keyed on the probe URL, so probing the authorityless form
    // first must NOT poison the whole scheme (the old single-scheme key did).
    CometScanRule.isNativelyReadableScheme(new URI("gs:///key.parquet"), Set.empty)
    assert(
      CometScanRule.isNativelyReadableScheme(new URI("gs://bucket/key.parquet"), Set.empty),
      "gs://bucket/... must be admitted even after an authorityless gs:// URI was probed first")
  }

  test("iceberg gate: native scheme lists match the JVM fallback constants") {
    // The gates load their scheme sets from the native storage factory over JNI. The JVM
    // fallback constants only stand in when the library cannot be loaded, so this pins them to
    // what native publishes: extending `builtin_storage_schemes` without updating the fallback
    // fails here rather than drifting silently.
    assume(NativeBase.isLoaded, "Comet native library not loaded")
    assert(
      IcebergStorageSchemes.read == IcebergStorageSchemes.FallbackRead,
      s"native read schemes ${IcebergStorageSchemes.read.toSeq.sorted} differ from the JVM " +
        s"fallback ${IcebergStorageSchemes.FallbackRead.toSeq.sorted}")
    assert(
      IcebergStorageSchemes.write == IcebergStorageSchemes.FallbackWrite,
      s"native write schemes ${IcebergStorageSchemes.write.toSeq.sorted} differ from the JVM " +
        s"fallback ${IcebergStorageSchemes.FallbackWrite.toSeq.sorted}")
    Seq("memory", "oss").foreach { scheme =>
      assert(
        IcebergStorageSchemes.read.contains(scheme),
        s"$scheme must be in the native read schemes ${IcebergStorageSchemes.read.toSeq.sorted}")
    }
    assert(
      !IcebergStorageSchemes.write.contains("oss"),
      s"oss must not be in the native write schemes ${IcebergStorageSchemes.write.toSeq.sorted}")
    Seq("abfs", "abfss", "wasb", "wasbs", "gcs").foreach { scheme =>
      assert(
        !IcebergStorageSchemes.read.contains(scheme),
        s"$scheme must not be in the native read schemes ${IcebergStorageSchemes.read.toSeq.sorted}")
      assert(
        !IcebergStorageSchemes.write.contains(scheme),
        s"$scheme must not be in the native write schemes " +
          s"${IcebergStorageSchemes.write.toSeq.sorted}")
    }
  }

  test("iceberg gate: builtin allowlist admitted, unbuildable schemes rejected") {
    // The Iceberg gate is the allowlist the native storage factory publishes over JNI. Narrower
    // than the Parquet gate: object_store recognizes http/abfs/wasb but iceberg-rust can't build
    // them, so reject up-front rather than fail during native setup.
    Seq(
      "file:///tmp/key.parquet",
      "memory:///key.parquet",
      "s3://bucket/key.parquet",
      "s3a://bucket/key.parquet",
      "gs://bucket/key.parquet",
      "oss://bucket/key.parquet").foreach { u =>
      assert(
        CometScanRule.isIcebergReadableScheme(new URI(u), Set.empty),
        s"$u must be iceberg-readable; icebergReadableSchemes has regressed")
    }
    Seq(
      "http://bucket.example.com/key.parquet",
      "https://bucket.example.com/key.parquet",
      "abfs://container@acct/key.parquet",
      "abfss://container@acct/key.parquet",
      "wasb://container@acct/key.parquet",
      "wasbs://container@acct/key.parquet").foreach { u =>
      assert(
        !CometScanRule.isIcebergReadableScheme(new URI(u), Set.empty),
        s"$u must not be iceberg-readable; storage_factory_for has no matching arm")
    }
  }

  test("parquet gate: mixed-bucket alias scan is declined (single object store per partition)") {
    // Native planning registers one object store per FilePartition and strips the authority from
    // every file's object key, so files in a second bucket would be read from the first. A scan
    // whose opt-in alias paths span multiple buckets must fall back.
    val schemes = Set("blob")
    def buckets(locations: String*): Set[String] =
      CometScanRule.aliasScanBuckets(
        CometScanRule.classifyRootPaths(locations.map(new URI(_)), Set.empty, schemes))

    assert(
      buckets("blob://bucket-a/k.parquet", "blob://bucket-b/k.parquet") ==
        Set("bucket-a", "bucket-b"),
      "two alias buckets must be reported so the Parquet gate falls back")
    // An alias bucket mixed with a plain-s3 bucket is still two object stores -> declined.
    assert(
      buckets("blob://bucket-a/k.parquet", "s3://bucket-b/k.parquet") ==
        Set("bucket-a", "bucket-b"))
    // Safe scans report at most one bucket: one alias path, or the same bucket via two paths.
    assert(buckets("blob://bucket-a/x.parquet") == Set("bucket-a"))
    assert(buckets("blob://bucket-a/x.parquet", "blob://bucket-a/y.parquet") == Set("bucket-a"))
    // No alias path present: plain multi-bucket s3:// is a pre-existing limitation, out of scope.
    assert(buckets("s3://bucket-a/k.parquet", "s3://bucket-b/k.parquet").isEmpty)
  }

  test("parquet gate: object_store rejects an actual path with an illegal character") {
    // The scheme cache is path-independent, but a valid `file` scheme can carry a path object_store
    // rejects: a directory name with a newline surfaces as `%0A` and `Path::from_url_path` fails.
    // Native execution would hard-error, so the real path is probed separately.
    assert(
      CometScanRule.objectStoreAcceptsPath(new URI("file:///tmp/warehouse/data")),
      "an ordinary local path must be accepted by object_store")
    assert(
      !CometScanRule.objectStoreAcceptsPath(new URI("file:///tmp/dir%0A/data")),
      "a directory name containing a newline (%0A) must be rejected so the scan falls back")
  }

  test("iceberg gate: hostless alias promotes bucket from path; other hostless schemes decline") {
    // iceberg-rust opens files by their raw location. Host-bearing locations always work. Hostless
    // ones work ONLY for opt-in S3-compliant aliases, which the native reader opens by promoting
    // the bucket from the first path segment (BlobHostPromotingS3Storage).
    // `hasOpenableAuthority` is the predicate `validateIcebergFileScanTasks` applies per data and
    // delete file, once its scheme has cleared `isIcebergReadableScheme` (covered above).
    val schemes = Set("blob")
    def openable(location: String, s3Compliant: Set[String] = schemes): Boolean =
      CometScanRule.hasOpenableAuthority(new URI(location), s3Compliant)
    // Host-bearing: openable regardless of scheme family; `file`/schemeless need no host.
    assert(openable("s3://bucket/k.parquet"))
    assert(openable("blob://bucket/k.parquet"))
    assert(openable("file:///tmp/k.parquet"))
    assert(openable("/tmp/k.parquet"))
    // Hostless alias: openable -- the bucket is promoted from the first path segment.
    assert(
      openable("blob:///bucket/k.parquet"),
      "hostless blob:/// must be openable: native promotes the first path segment to the bucket")
    assert(
      openable("blob:/bucket/k.parquet"),
      "the opaque single-slash blob:/ form promotes the same way")
    // Hostless alias with no promotable bucket segment: declined (nothing to promote).
    assert(
      !openable("blob:///"),
      "a hostless alias URL with no bucket segment cannot be promoted")
    // Hostless non-alias: still declined; s3/s3a/gs/oss have no bucket-from-path promotion.
    assert(
      !openable("s3:///bucket/k.parquet"),
      "authorityless s3:/// has no host and no alias promotion")
    // blob not opted in: no promotion, so the hostless form is not openable either.
    assert(
      !openable("blob:///bucket/k.parquet", Set.empty),
      "without opt-in, blob gets no bucket promotion, so a hostless blob location is unopenable")
  }

  test("iceberg scan declines a wasb table instead of failing at execution") {
    // End-to-end guard for the scheme gate: object_store recognizes `wasb`, but the native Iceberg
    // storage factory has no arm for it. The planner must decline the scan with a reason naming
    // the scheme so Spark reads the table, rather than claim it and fail at execution with
    // "Unsupported storage scheme: wasb".
    assume(icebergAvailable, "Iceberg not available in classpath")
    withTempIcebergDir { dir =>
      val warehouse = s"${FakeWasbSchemeFileSystem.PREFIX}${dir.getAbsolutePath}/warehouse"
      withSQLConf(
        "spark.sql.catalog.wasb_catalog" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.wasb_catalog.type" -> "hadoop",
        "spark.sql.catalog.wasb_catalog.warehouse" -> warehouse,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {
        spark.sql("CREATE TABLE wasb_catalog.db.wasb_table (id INT, name STRING) USING iceberg")
        spark.sql(
          "INSERT INTO wasb_catalog.db.wasb_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        try {
          val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
            "SELECT * FROM wasb_catalog.db.wasb_table ORDER BY id",
            "wasb")
          val nativeScans = cometPlan.collect { case s: CometIcebergNativeScanExec => s }
          assert(
            nativeScans.isEmpty,
            "`wasb://` has no native Iceberg storage factory arm; the scan must fall back to " +
              s"Spark, but Comet claimed it:\n$cometPlan")
        } finally {
          spark.sql("DROP TABLE wasb_catalog.db.wasb_table")
        }
      }
    }
  }

  test("native scan claims hdfs:// when libhdfs.schemes is unset (native-default lockstep)") {
    // Native `is_hdfs_scheme` treats `hdfs` as readable when `fs.comet.libhdfs.schemes` is unset,
    // so the JVM gate must agree and CLAIM `hdfs://`. Guards the `case None => Set("hdfs")` default
    // against the silent-fallback regression from #4525.
    val path = s"${FakeHdfsSchemeFileSystem.PREFIX}${fakeRootDir.getAbsolutePath}/hdfs-data"
    spark.range(0, 10).toDF("id").write.format("parquet").mode(SaveMode.Overwrite).save(path)

    var sparkPlan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      sparkPlan = spark.read.parquet(path).queryExecution.executedPlan
    }

    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      val transformed = CometScanRule(spark).apply(stripAQEPlan(sparkPlan))

      val cometScans = transformed.collect { case s: CometScanExec => s }
      val sparkScans = transformed.collect { case s: FileSourceScanExec => s }
      assert(
        cometScans.size == 1,
        "`hdfs://` is natively readable by default; Comet must claim the scan, " +
          s"but it fell back to Spark:\n$transformed")
      assert(sparkScans.isEmpty, s"expected no leftover Spark FileSourceScanExec:\n$transformed")
    }
  }
}
