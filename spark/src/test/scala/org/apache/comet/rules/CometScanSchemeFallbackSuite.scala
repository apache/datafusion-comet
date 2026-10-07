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

import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{CometTestBase, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.comet.{CometScanExec, CometScanUtils}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.hadoop.fs.{FakeHDFSFileSystem, FakeHdfsSchemeFileSystem}
import org.apache.comet.objectstore.NativeConfig

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
class CometScanSchemeFallbackSuite extends CometTestBase {

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

  test("parquet gate: mixed-bucket alias scan is declined (alias settings fit one bucket)") {
    // Alias settings under the hostless `default` authority resolve for one bucket only. A scan
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
    // No alias path present: plain multi-bucket s3:// is packed per bucket instead.
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

  /** Applies CometScanRule to the Spark plan of `read` and returns the scans it claimed. */
  private def claimedScans(read: => org.apache.spark.sql.DataFrame): Seq[CometScanExec] = {
    var sparkPlan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      sparkPlan = read.queryExecution.executedPlan
    }
    var claimed: Seq[CometScanExec] = Nil
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      claimed = CometScanRule(spark)
        .apply(stripAQEPlan(sparkPlan))
        .collect { case s: CometScanExec => s }
    }
    claimed
  }

  test("parquet gate: a scan over two object stores of different schemes is declined") {
    // The native scan forwards the object store options of one file's scheme, so stores of
    // different schemes cannot share a scan even when no partition mixes them.
    val localPath = s"file://${fakeRootDir.getAbsolutePath}/mixed-local"
    val hdfsPath = s"${FakeHdfsSchemeFileSystem.PREFIX}${fakeRootDir.getAbsolutePath}/mixed-hdfs"
    spark.range(0, 5).toDF("id").write.mode(SaveMode.Overwrite).parquet(localPath)
    spark.range(5, 10).toDF("id").write.mode(SaveMode.Overwrite).parquet(hdfsPath)

    assert(claimedScans(spark.read.parquet(localPath)).size == 1)
    assert(claimedScans(spark.read.parquet(hdfsPath)).size == 1)
    assert(
      claimedScans(spark.read.parquet(localPath, hdfsPath)).isEmpty,
      "a scan over file:// and hdfs:// stores must fall back to Spark")
    withSQLConf(
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      checkSparkAnswerAndFallbackReason(
        spark.read.parquet(localPath, hdfsPath),
        "Native Parquet scan reads paths with schemes file, hdfs, whose object store " +
          "settings differ, but forwards the settings of one scheme")
    }
  }

  test("multiStoreFallbackReason: scheme families, and stores of a bucketed scan") {
    def reason(
        bucketed: Boolean,
        locations: Seq[String],
        aliases: Set[String] = Set.empty,
        libhdfs: Set[String] = Set("hdfs")): Option[String] =
      CometScanUtils.multiStoreFallbackReason(
        "Native Parquet scan",
        locations.map(new URI(_)),
        aliases,
        libhdfs,
        bucketed)

    assert(reason(bucketed = true, Nil).isEmpty)
    // One store, however it is spelled, never falls back.
    assert(reason(bucketed = true, Seq("s3a://bucket-a/t/p=1", "s3://bucket-a/t/p=2")).isEmpty)
    assert(reason(bucketed = true, Seq("/tmp/t/p=1", "file:///tmp/t/p=2")).isEmpty)
    // Several stores of one scheme family: packed per store, unless the scan is bucketed.
    assert(reason(bucketed = false, Seq("s3a://bucket-a/t", "s3a://bucket-b/t")).isEmpty)
    assert(reason(bucketed = false, Seq("gs://bucket-a/t", "gs://bucket-b/t")).isEmpty)
    assert(reason(bucketed = false, Seq("hdfs://nn1/t", "hdfs://nn2/t")).isEmpty)
    val bucketedS3 = reason(bucketed = true, Seq("s3a://bucket-a/t/p=1", "s3a://bucket-b/t/p=2"))
    assert(
      bucketedS3.contains(
        "Native Parquet scan of a bucketed table reads paths in object stores s3://bucket-a, " +
          "s3://bucket-b, but reads each table bucket through one store"))
    val bucketedHdfs = reason(bucketed = true, Seq("hdfs://nn1/t/p=1", "hdfs://nn2/t/p=2"))
    assert(bucketedHdfs.exists(_.contains("hdfs://nn1 (libhdfs), hdfs://nn2 (libhdfs)")))
    // Scheme families whose settings differ fall back, bucketed or not.
    val mixed = reason(bucketed = false, Seq("s3a://bucket-a/t", "gs://bucket-b/t"))
    assert(
      mixed.contains(
        "Native Parquet scan reads paths with schemes gs, s3a, whose object store settings " +
          "differ, but forwards the settings of one scheme"))
    // s3 read through libhdfs and s3a read natively, in one bucket.
    val libhdfsS3 =
      reason(bucketed = false, Seq("s3://bucket-a/t", "s3a://bucket-a/u"), libhdfs = Set("s3"))
    assert(libhdfsS3.exists(_.contains("schemes s3, s3a")))
    // An alias and s3a in one bucket share a store but not the alias settings.
    val alias =
      reason(
        bucketed = false,
        Seq("blob://bucket-a/t", "s3a://bucket-a/u"),
        aliases = Set("blob"))
    assert(
      alias.contains(
        "Native Parquet scan reads paths with schemes blob, s3a, whose object store settings " +
          "differ, but forwards the settings of one scheme"))
  }

  test("packFilesPerStore: no partition mixes stores, and one store packs as Spark does") {
    val maxSplitBytes = 100L
    val random = new Random(6746)
    def file(store: String, name: String, length: Long): PartitionedFile =
      PartitionedFile(
        InternalRow.empty,
        SparkPath.fromUrlString(s"s3a://$store/t/$name.parquet"),
        0,
        length,
        Array.empty[String],
        0,
        length)
    def storeOf(file: PartitionedFile): String = file.pathUri.getAuthority
    def pack(files: Seq[PartitionedFile]): Seq[FilePartition] =
      FilePartition.getFilePartitions(spark, files, maxSplitBytes)
    def layout(partitions: Seq[FilePartition]): Seq[(Int, Seq[String])] =
      partitions.map(p => (p.index, p.files.map(_.filePath.toString).toSeq))

    // A small open cost lets several files share a partition.
    withSQLConf(SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1") {
      var sparkMixedRounds = 0
      (1 to 200).foreach { round =>
        val numStores = 1 + random.nextInt(3)
        def randomFile(i: Int): PartitionedFile =
          file(s"bucket-${random.nextInt(numStores)}", s"f$i", 1L + random.nextInt(150))
        val files = (0 until random.nextInt(30)).map(randomFile).sortBy(-_.length)
        if (pack(files).exists(_.files.map(storeOf).distinct.length > 1)) {
          sparkMixedRounds += 1
        }
        val partitions = CometScanUtils.packFilesPerStore(files, storeOf)(pack)

        withClue(s"round $round, layout ${layout(partitions)}: ") {
          assert(partitions.map(_.index) == partitions.indices)
          assert(partitions.forall(_.files.map(storeOf).distinct.length == 1))
          assert(
            partitions.flatMap(_.files.map(_.filePath.toString)).sorted ==
              files.map(_.filePath.toString).sorted)
          if (files.map(storeOf).distinct.size <= 1) {
            assert(layout(partitions) == layout(pack(files)))
          }
          // Each store's files are packed as Spark packs them on their own.
          files.map(storeOf).distinct.foreach { store =>
            assert(layout(partitions.filter(p => storeOf(p.files.head) == store)).map(_._2) ==
              layout(pack(files.filter(f => storeOf(f) == store))).map(_._2))
          }
        }
      }
      assert(sparkMixedRounds > 0, "Spark's packing never mixed stores, so nothing was tested")
    }
  }

  test("libhdfs s3 and native s3a files of one bucket land in separate partitions") {
    def file(path: String): PartitionedFile =
      PartitionedFile(
        InternalRow.empty,
        SparkPath.fromUrlString(path),
        0,
        1,
        Array.empty[String],
        0,
        1)
    def storeOf(file: PartitionedFile): NativeConfig.ObjectStoreKey =
      NativeConfig.objectStoreKey(file.pathUri, Set.empty, Set("s3"))
    val files = Seq(file("s3://bucket/1.parquet"), file("s3a://bucket/2.parquet"))
    def paths(partitions: Seq[FilePartition]): Seq[Seq[String]] =
      partitions.map(_.files.map(_.filePath.toString).toSeq)

    withSQLConf(SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1") {
      val packed = CometScanUtils.packFilesPerStore(files, storeOf) { files =>
        FilePartition.getFilePartitions(spark, files, 100L)
      }
      assert(paths(packed) == Seq(Seq("s3://bucket/1.parquet"), Seq("s3a://bucket/2.parquet")))
    }
    val split =
      CometScanUtils.splitPartitionsByStore(Seq(FilePartition(0, files.toArray)), storeOf)
    assert(paths(split) == Seq(Seq("s3://bucket/1.parquet"), Seq("s3a://bucket/2.parquet")))
  }

  test("splitPartitionsByStore: each partition is split per store, keeping file order") {
    // The partition value is the number in the file name.
    def file(path: String): PartitionedFile =
      PartitionedFile(
        InternalRow(path.split('/').last.takeWhile(_.isDigit).toInt),
        SparkPath.fromUrlString(path),
        0,
        1,
        Array.empty[String],
        0,
        1)
    def storeOf(file: PartitionedFile): String = file.pathUri.getAuthority
    def layout(partitions: Seq[FilePartition]): Seq[(Int, Seq[String])] =
      partitions.map(p => (p.index, p.files.map(_.filePath.toString).toSeq))

    val single = Seq(
      FilePartition(0, Array(file("s3a://a/1.csv"), file("s3a://a/2.csv"))),
      FilePartition(1, Array(file("s3a://b/3.csv"))))
    assert(CometScanUtils.splitPartitionsByStore(single, storeOf) == single)

    val mixed = Seq(
      FilePartition(
        0,
        Array(file("s3a://a/1.csv"), file("s3a://b/2.csv"), file("s3a://a/3.csv"))),
      FilePartition(1, Array(file("s3a://b/4.csv"))))
    val split = CometScanUtils.splitPartitionsByStore(mixed, storeOf)
    assert(
      layout(split) == Seq(
        0 -> Seq("s3a://a/1.csv", "s3a://a/3.csv"),
        1 -> Seq("s3a://b/2.csv"),
        2 -> Seq("s3a://b/4.csv")))
    assert(split.flatMap(_.files.map(_.partitionValues.getInt(0))) == Seq(1, 3, 2, 4))
  }
}
