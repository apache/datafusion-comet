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
import org.apache.iceberg.{DataFiles, FileFormat, Table}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, SaveMode}
import org.apache.spark.sql.comet.{CometBatchScanExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.comet.{CometConf, CometExplainInfo, CometIcebergTestBase}
import org.apache.comet.hadoop.fs.{FakeHDFSFileSystem, FakeHdfsSchemeFileSystem}

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

  test("iceberg gate: builtin allowlist admitted, unbuildable schemes rejected") {
    // The Iceberg gate is an explicit allowlist mirroring `storage_factory_for`'s arms (keep in
    // lockstep). Narrower than the Parquet gate: object_store recognizes http/abfs/wasb but
    // iceberg-rust can't build them, so reject up-front rather than fail during native setup.
    Seq(
      "file:///tmp/key.parquet",
      "s3://bucket/key.parquet",
      "s3a://bucket/key.parquet",
      "gs://bucket/key.parquet",
      "oss://bucket/key.parquet",
      // hdfs-native backend, not the libhdfs/JNI client the plain-Parquet path uses.
      "hdfs://nn:8020/warehouse/db/t/key.parquet",
      "hdfs://nameservice1/warehouse/db/t/key.parquet").foreach { u =>
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
    // hdfs: the authority is the NameNode, and the gate runs before the catalog properties that
    // could supply one, so a hostless location declines rather than guess.
    assert(openable("hdfs://nn:8020/warehouse/db/t/k.parquet"))
    assert(openable("hdfs://nameservice1/warehouse/db/t/k.parquet"))
    assert(
      !openable("hdfs:///warehouse/db/t/k.parquet"),
      "authorityless hdfs:/// carries no NameNode and gets no promotion")
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

  // --- Iceberg scans over HDFS data files ---------------------------------------------------
  //
  // These drive the real planner gate (`CometScanRule` on a `BatchScanExec` over a real Iceberg
  // table) rather than the static helpers above, because the NameNode decisions need the catalog
  // properties the rule assembles from the session Hadoop configuration. The data files are only
  // registered in the table's manifests, with the `hdfs://` paths the test names: planning reads
  // manifests, never the data files, so nothing here needs a NameNode and nothing is executed.

  /**
   * Plans `SELECT id` over an Iceberg table whose data files sit at `dataFiles`, applies
   * `CometScanRule` with native Iceberg scans enabled and `hadoopConf` set on the session, and
   * returns the scans the rule claimed plus the fallback reasons it left on the ones it declined.
   */
  private def planIcebergScanOverHdfs(
      dataFiles: Seq[String],
      hadoopConf: Seq[(String, String)] = Seq.empty): (Seq[CometBatchScanExec], Set[String]) = {
    // A catalog of its own, so no test reads another's cached catalog or warehouse.
    val catalog = s"hdfs_gate_${UUID.randomUUID().toString.take(8)}"
    var claimed = Seq.empty[CometBatchScanExec]
    var reasons = Set.empty[String]
    withHadoopCatalog(catalog) {
      spark.sql(s"CREATE TABLE $catalog.db.t (id INT) USING iceberg")
      val table = loadIcebergTable(spark, catalog, "db", "t").asInstanceOf[Table]
      val append = table.newAppend()
      dataFiles.foreach { path =>
        append.appendFile(
          DataFiles
            .builder(table.spec())
            .withPath(path)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(1024)
            .withRecordCount(1)
            .build())
      }
      append.commit()

      // The Spark plan first, with Comet off, then the rule alone: no execution.
      var sparkPlan: SparkPlan = null
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        sparkPlan = spark.sql(s"SELECT id FROM $catalog.db.t").queryExecution.executedPlan
      }
      val confs = Seq(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") ++ hadoopConf
      withSQLConf(confs: _*) {
        val transformed = CometScanRule(spark).apply(stripAQEPlan(sparkPlan))
        claimed = transformed.collect { case s: CometBatchScanExec => s }
        reasons = transformed
          .collect { case s: BatchScanExec =>
            s.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty[String])
          }
          .flatten
          .toSet
      }
    }
    (claimed, reasons)
  }

  test("iceberg scan: a configured HDFS nameservice that does not resolve is declined") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    // nn2 has no rpc-address, so the mapping to `hdfs.name-node` is all-or-nothing empty and the
    // executor would dial `ns1` as if it were a host.
    val (claimed, reasons) = planIcebergScanOverHdfs(
      Seq("hdfs://ns1/warehouse/db/t/a.parquet"),
      Seq(
        "dfs.nameservices" -> "ns1",
        "dfs.ha.namenodes.ns1" -> "nn1,nn2",
        "dfs.namenode.rpc-address.ns1.nn1" -> "nn1.example.com:8020"))
    assert(claimed.isEmpty, "an unresolvable nameservice must not be claimed natively")
    assert(
      reasons.exists(_.contains("HDFS nameservice 'ns1' could not be resolved")),
      s"expected the nameservice fall-back reason, got: $reasons")
  }

  test("iceberg scan: a nameservice listed only in dfs.nameservices is declined") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    val (claimed, reasons) = planIcebergScanOverHdfs(
      Seq("hdfs://ns1/warehouse/db/t/a.parquet"),
      Seq("dfs.nameservices" -> "ns1,ns2"))
    assert(claimed.isEmpty, "a configured nameservice without NameNodes must not be claimed")
    assert(
      reasons.exists(_.contains("HDFS nameservice 'ns1' could not be resolved")),
      s"expected the nameservice fall-back reason, got: $reasons")
  }

  test("iceberg scan: a resolvable HDFS nameservice is claimed with its NameNode list") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    // The control for the two tests above, and for the multi-authority one below: the same table
    // shape is claimed once the nameservice resolves, so the declines are down to the gate under
    // test. nn2's rpc-address has no port, which Hadoop (and now Comet) defaults to 8020.
    val (claimed, reasons) = planIcebergScanOverHdfs(
      Seq("hdfs://ns1/warehouse/db/t/a.parquet"),
      Seq(
        "dfs.nameservices" -> "ns1",
        "dfs.ha.namenodes.ns1" -> "nn1,nn2",
        "dfs.namenode.rpc-address.ns1.nn1" -> "nn1.example.com:9000",
        "dfs.namenode.rpc-address.ns1.nn2" -> "nn2.example.com"))
    assert(claimed.size == 1, s"expected the scan to be claimed, fell back with: $reasons")
    val metadata = claimed.head.nativeIcebergScanMetadata.get
    assert(
      metadata.catalogProperties.get("hdfs.name-node") ==
        Some("hdfs://nn1.example.com:9000,hdfs://nn2.example.com:8020"),
      s"unexpected catalog properties: ${metadata.catalogProperties}")
  }

  test("iceberg scan: a plain host:port HDFS location is claimed with no NameNode property") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    // Indistinguishable from a real NameNode, so the path authority is left to iceberg-rust.
    val (claimed, reasons) =
      planIcebergScanOverHdfs(Seq("hdfs://nn.example.com:8020/warehouse/db/t/a.parquet"))
    assert(claimed.size == 1, s"expected the scan to be claimed, fell back with: $reasons")
    assert(
      !claimed.head.nativeIcebergScanMetadata.get.catalogProperties.contains("hdfs.name-node"))
  }

  test("iceberg scan: data files on two HDFS authorities are declined") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    // One `hdfs.name-node` per scan, and it wins over every path authority, so the second
    // NameNode's files would be read from the first at the same relative path. The decline sits
    // inline in the scan case of `CometScanRule.apply`, with no static helper of its own, so a real
    // scan is the lowest layer that reaches it.
    val (claimed, reasons) = planIcebergScanOverHdfs(
      Seq(
        "hdfs://nn1.example.com:8020/warehouse/db/t/a.parquet",
        "hdfs://nn2.example.com:8020/warehouse/db/t/b.parquet"))
    assert(claimed.isEmpty, "files on two NameNodes must not be claimed natively")
    assert(
      reasons.exists(
        _.contains(
          "across multiple HDFS authorities (nn1.example.com:8020, nn2.example.com:8020)")),
      s"expected the multi-authority fall-back reason, got: $reasons")
  }
}
