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
import java.net.URI
import java.nio.file.Files
import java.util.UUID

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.{MiniDFSCluster, MiniDFSNNTopology}
import org.apache.hadoop.util.VersionInfo
import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.{CometIcebergNativeScanExec, CometIcebergWriteExec, IcebergCommitExec, IcebergWriteExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.CometSparkSessionExtensions.isSpark42Plus
import org.apache.comet.iceberg.IcebergReflection

/**
 * End-to-end coverage of the native Iceberg scan and write against an `hdfs://` warehouse, backed
 * by in-process `MiniDFSCluster`s.
 *
 * This is the only test that exercises iceberg-rust's `hdfs-native` backend for real. It matters
 * because that backend is a second, independent HDFS client: the plain-Parquet native scan
 * reaches HDFS through libhdfs/JNI (`fs.comet.libhdfs.schemes`), while an Iceberg table on HDFS
 * is opened by a pure-Rust RPC client that shares nothing with it but the NameNode endpoints
 * Comet hands it. A unit test over the scheme allowlists cannot tell whether that client actually
 * connects, reads, or finishes a write.
 *
 * What is covered:
 *   - scans of tables on a single-NameNode cluster, whose locations carry a real `host:port`
 *     authority, so iceberg-rust needs no NameNode declaration;
 *   - native writes on the same cluster (unpartitioned and partitioned `INSERT`, a second
 *     `INSERT`, a copy-on-write `UPDATE`). Each asserts the `CometIcebergWriteExec` /
 *     `IcebergCommitExec` plan shape, that every committed file exists under the table's data
 *     location on HDFS with exactly the length its manifest entry records, that the file, row and
 *     byte counts `CometIcebergWriteExec` reports agree with what the NameNode holds, and that
 *     the native scan and plain Spark read the same rows back. The length check is the one that
 *     matters for `FileWrite::close`: the pinned iceberg-rust errors with "Wrote N bytes but
 *     storage reports M" when the two differ, and this is the first test to run it against a real
 *     NameNode;
 *   - a two-NameNode HA cluster addressed by nameservice (`hdfs://<ns>/...`), which is not a
 *     host, so the scan and the write only connect through the `hdfs.name-node.<ns>` declaration
 *     Comet derives from the session Hadoop configuration (the pinned iceberg-rust resolves a
 *     portless authority only through it). It is read before and after a failover.
 *
 * Which profiles run it. The cluster needs the `hadoop-client-minicluster` that `pom.xml` pins
 * per Spark profile (`hadoop.version`: 3.4 -> 3.3.4, 3.5 -> 3.3.4, 4.0 -> 3.4.1, 4.1 -> 3.4.2,
 * 4.2 -> 3.5.0) to match the `hadoop-client-api`/`runtime` Spark supplies. Measured by running
 * the scan tests on every profile:
 *   - Spark 3.4, 3.5, 4.0 and 4.1: the cluster starts and every test runs, none canceled. A
 *     cluster that fails to start there is a broken fixture, so `beforeAll` fails the suite with
 *     the cause instead of cancelling the tests.
 *   - Spark 4.2: nothing runs (all tests are cancelled), for two independent reasons. There is no
 *     Iceberg runtime for 4.2 (`icebergAvailable` is false there, which alone cancelled the three
 *     original tests), and the cluster cannot start either: `MiniDFSCluster.Builder` throws
 *     `NoClassDefFoundError: org/junit/jupiter/api/Assertions`, because Hadoop 3.5.0's
 *     `GenericTestUtils` asserts with JUnit 5, which the shaded minicluster leaves out and the
 *     test classpath does not carry. With junit-jupiter-api 5.10.3 and its three small
 *     dependencies put on the JVM class path the cluster starts, so a test-scoped
 *     `junit-jupiter-api` dependency on the 4.2 profile is the fix once an Iceberg runtime
 *     exists. Until then this suite skips there and starts nothing.
 */
class CometIcebergHdfsSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometIcebergTestBase
    with WithHdfsCluster {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      // `UPDATE` on an Iceberg table is a row-level rewrite that these extensions plan.
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

  /**
   * The only profile on which this suite does not run (see the class docstring): no Iceberg
   * runtime, and no MiniDFSCluster. Anywhere else the Iceberg runtime and the cluster are
   * required, and their absence fails the suite rather than cancelling it.
   */
  private def profileSkipped: Boolean = isSpark42Plus

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      assume(!profileSkipped, "Spark 4.2 has no Iceberg runtime and no usable MiniDFSCluster")
      testFun
    }
  }

  /**
   * The nameservice of the HA cluster. Not the `ns1` of `simpleHATopology`, to be unmistakable.
   */
  private val haNameservice = "cometha"

  private var haCluster: MiniDFSCluster = _
  private var haBaseDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!profileSkipped) {
      assert(icebergAvailable, "the Iceberg runtime is missing from the test classpath")
      try startHdfsCluster()
      catch {
        case e @ (_: Exception | _: LinkageError) =>
          throw new IllegalStateException(
            "MiniDFSCluster failed to start, so none of the HDFS tests can run. " +
              s"Spark ${org.apache.spark.SPARK_VERSION}, hadoop-client-api " +
              s"${VersionInfo.getVersion}, hadoop-client-minicluster loaded from " +
              s"${classOf[MiniDFSCluster].getProtectionDomain.getCodeSource.getLocation}: $e",
            e)
      }
    }
  }

  override def afterAll(): Unit = {
    try stopHaCluster()
    finally {
      try stopHdfsCluster()
      finally super.afterAll()
    }
  }

  /** `hdfs://localhost:<port>` -- the authority iceberg-rust dials as the NameNode. */
  private def hdfsUri: String = s"hdfs://localhost:$getDFSPort"

  private def assertSingleNativeScan(cometPlan: SparkPlan): CometIcebergNativeScanExec = {
    val scans = collect(cometPlan) { case scan: CometIcebergNativeScanExec => scan }
    assert(
      scans.length == 1,
      s"Expected exactly 1 CometIcebergNativeScanExec but found ${scans.length}. " +
        s"Plan:\n$cometPlan")
    scans.head
  }

  /**
   * Runs `f` with a Hadoop-catalog Iceberg warehouse rooted at `baseUri`, which is the
   * single-NameNode cluster by default. The catalog name is unique per test so Spark's catalog
   * cache cannot hand back a warehouse from an earlier test, and the warehouse directory is
   * removed afterwards (`cleanup`).
   */
  private def withHdfsIcebergCatalog(baseUri: String = hdfsUri, cleanup: Boolean = true)(
      f: String => Unit): Unit = {
    val catalog = s"hdfs_cat_${UUID.randomUUID().toString.replace("-", "")}"
    val warehouse = s"$baseUri/warehouse/${UUID.randomUUID()}"
    try {
      withSQLConf(
        s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$catalog.type" -> "hadoop",
        s"spark.sql.catalog.$catalog.warehouse" -> warehouse,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {
        f(catalog)
      }
    } finally {
      if (cleanup) getFileSystem.delete(new Path(warehouse), true)
    }
  }

  /**
   * Runs `sqlText` with Comet's native Iceberg writer enabled and returns the executed plan of
   * every query that ran. Inline `VALUES` only reach the native writer if the local table scan is
   * columnar too, hence the last setting.
   */
  private def runNativeWrite(sqlText: String): Seq[SparkPlan] = {
    // `withSQLConf` returns Unit before Spark 4.0, hence the var.
    var plans: Seq[SparkPlan] = Seq.empty
    withSQLConf(
      CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "true",
      CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      plans = capturePlans(spark)(spark.sql(sqlText))
    }
    plans
  }

  private def nativeWrites(plans: Seq[SparkPlan]): Seq[CometIcebergWriteExec] =
    plans.flatMap(p => collectWithSubqueries(p) { case w: CometIcebergWriteExec => w })

  /**
   * The write really went through Comet's native writer, in the split-operator shape: a native
   * file writer, exactly one commit, and no row-based `IcebergWriteExec` left beside it.
   */
  private def assertNativeWrite(plans: Seq[SparkPlan]): Unit = {
    val jvmWrites = plans.flatMap(p => collectWithSubqueries(p) { case w: IcebergWriteExec => w })
    val commits = plans.flatMap(p => collectWithSubqueries(p) { case c: IcebergCommitExec => c })
    val dump = plans.mkString("\n--\n")
    assert(nativeWrites(plans).nonEmpty, s"expected a CometIcebergWriteExec. Plans:\n$dump")
    assert(
      jvmWrites.isEmpty,
      s"the JVM IcebergWriteExec ran beside the native one. Plans:\n$dump")
    assert(commits.length == 1, s"expected exactly 1 IcebergCommitExec, got ${commits.length}")
  }

  /**
   * The native writer's own accounting of what it wrote agrees with the NameNode: the files, the
   * rows, and the bytes. `written` are the files this write added, with their HDFS lengths.
   */
  private def assertWriterReported(
      plans: Seq[SparkPlan],
      written: Seq[(Path, Long)],
      rows: Long): Unit = {
    def reported(metric: String): Long = nativeWrites(plans).map(_.metrics(metric).value).sum
    assert(reported("numFiles") == written.size, s"numFiles vs $written")
    assert(reported("numOutputRows") == rows, "numOutputRows")
    assert(
      reported("numOutputBytes") == written.map(_._2).sum,
      s"the writer reports ${reported("numOutputBytes")} bytes, HDFS holds $written")
  }

  private def tableLocation(catalog: String, table: String): String = {
    val icebergTable = loadIcebergTable(spark, catalog, "db", table)
    icebergTable.getClass.getMethod("location").invoke(icebergTable).toString
  }

  private def snapshotCount(catalog: String, table: String): Long =
    spark.sql(s"SELECT count(*) FROM $catalog.db.$table.snapshots").collect().head.getLong(0)

  /** Every data file the table's current snapshot references, with the size it records. */
  private def committedFiles(catalog: String, table: String): Seq[(Path, Long)] =
    spark
      .sql(s"SELECT file_path, file_size_in_bytes FROM $catalog.db.$table.files")
      .collect()
      .toSeq
      .map(row => (new Path(row.getString(0)), row.getLong(1)))

  /** Every regular file under `root` on the cluster. */
  private def listFiles(fs: FileSystem, root: Path): Seq[FileStatus] = {
    val it = fs.listFiles(root, true)
    val files = Seq.newBuilder[FileStatus]
    while (it.hasNext) files += it.next()
    files.result()
  }

  /**
   * Checks the table's committed data files against what the NameNode and DataNode actually hold
   * (read through `fs`), and returns them with their HDFS lengths.
   *
   * Every file the table references must exist under the table's `data/` directory with exactly
   * the length its manifest entry records. That is the end-to-end check on iceberg-rust's
   * `FileWrite::close`: the writer reports the byte count it wrote, and a `DataFile` that
   * disagrees with storage would corrupt every later split-planning and rewrite.
   *
   * With `exact`, `data/` must hold nothing else either: no leftovers of an aborted attempt. A
   * copy-on-write rewrite leaves the replaced files behind until they expire, so it opts out.
   */
  private def assertCommittedFilesOnHdfs(
      catalog: String,
      table: String,
      fs: FileSystem = getFileSystem,
      exact: Boolean = true): Seq[(Path, Long)] = {
    val dataDir = new Path(tableLocation(catalog, table), "data")
    assert(dataDir.toUri.getScheme == "hdfs", s"expected an hdfs:// data location, got $dataDir")

    val committed = committedFiles(catalog, table)
    assert(committed.nonEmpty, s"$catalog.db.$table references no data files")
    val onHdfs = committed.map { case (path, recordedLength) =>
      assert(
        path.toUri.getPath.startsWith(dataDir.toUri.getPath),
        s"$path is not under the table's data directory $dataDir")
      assert(recordedLength > 0, s"$path is recorded as empty")
      val length = fs.getFileStatus(path).getLen
      assert(
        length == recordedLength,
        s"$path: the manifest records $recordedLength bytes but HDFS holds $length")
      (path, length)
    }

    val onCluster = listFiles(fs, dataDir).map(_.getPath.toUri.getPath).toSet
    val referenced = committed.map(_._1.toUri.getPath).toSet
    assert(referenced.subsetOf(onCluster), s"missing from HDFS: ${referenced -- onCluster}")
    if (exact) {
      val extra = onCluster -- referenced
      assert(extra.isEmpty, s"unreferenced files under $dataDir: $extra")
    }
    onHdfs
  }

  /** The files in `after` that `before` did not have. */
  private def addedFiles(
      before: Seq[(Path, Long)],
      after: Seq[(Path, Long)]): Seq[(Path, Long)] = {
    val known = before.map(_._1.toUri.getPath).toSet
    after.filterNot { case (path, _) => known.contains(path.toUri.getPath) }
  }

  test("native Iceberg scan reads a table stored on HDFS") {
    withHdfsIcebergCatalog() { catalog =>
      spark.sql(s"CREATE TABLE $catalog.db.t (id INT, name STRING, value DOUBLE) USING iceberg")
      spark.sql(
        s"INSERT INTO $catalog.db.t VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.3), " +
          "(3, 'Charlie', 30.7)")

      // The data location must actually be on HDFS, or this suite would silently degrade into a
      // duplicate of the local-filesystem coverage.
      val dataLocation = IcebergReflection
        .getDataLocation(loadIcebergTable(spark, catalog, "db", "t"))
        .getOrElse(fail("could not resolve the Iceberg data location"))
      assert(
        dataLocation.startsWith("hdfs://"),
        s"expected an hdfs:// data location, got $dataLocation")

      val (_, cometPlan) = checkSparkAnswer(s"SELECT * FROM $catalog.db.t ORDER BY id")
      assertSingleNativeScan(cometPlan)

      spark.sql(s"DROP TABLE $catalog.db.t")
    }
  }

  test("native Iceberg scan on HDFS applies a pushed-down filter") {
    withHdfsIcebergCatalog() { catalog =>
      spark.sql(s"CREATE TABLE $catalog.db.f (id INT, name STRING) USING iceberg")
      spark.sql(
        s"INSERT INTO $catalog.db.f VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

      val (_, cometPlan) =
        checkSparkAnswer(s"SELECT id, name FROM $catalog.db.f WHERE id > 3 ORDER BY id")
      assertSingleNativeScan(cometPlan)

      spark.sql(s"DROP TABLE $catalog.db.f")
    }
  }

  test("native Iceberg scan reads a partitioned table across multiple HDFS data files") {
    withHdfsIcebergCatalog() { catalog =>
      spark.sql(
        s"CREATE TABLE $catalog.db.p (id INT, part STRING) USING iceberg PARTITIONED BY (part)")
      // Separate inserts so each partition lands in its own data file: a single-file read would
      // not prove the operator cache serves more than one path from one NameNode.
      spark.sql(s"INSERT INTO $catalog.db.p VALUES (1, 'x'), (2, 'x')")
      spark.sql(s"INSERT INTO $catalog.db.p VALUES (3, 'y'), (4, 'y')")

      val (_, cometPlan) = checkSparkAnswer(s"SELECT * FROM $catalog.db.p ORDER BY id")
      assertSingleNativeScan(cometPlan)

      spark.sql(s"DROP TABLE $catalog.db.p")
    }
  }

  test("native Iceberg INSERT writes its data files to HDFS and reads them back") {
    withHdfsIcebergCatalog() { catalog =>
      spark.sql(s"CREATE TABLE $catalog.db.w (id INT, name STRING, value DOUBLE) USING iceberg")

      val plans = runNativeWrite(
        s"INSERT INTO $catalog.db.w VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.3), " +
          "(3, 'Charlie', 30.7)")
      assertNativeWrite(plans)
      assert(snapshotCount(catalog, "w") == 1L)
      assertWriterReported(plans, assertCommittedFilesOnHdfs(catalog, "w"), rows = 3)

      // Spark's own reader (Comet off) and the native scan agree on what was written.
      val (_, cometPlan) = checkSparkAnswer(s"SELECT * FROM $catalog.db.w ORDER BY id")
      assertSingleNativeScan(cometPlan)
      val rows = spark.sql(s"SELECT id, name, value FROM $catalog.db.w ORDER BY id").collect()
      assert(
        rows.map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).toSeq ==
          Seq((1, "Alice", 10.5), (2, "Bob", 20.3), (3, "Charlie", 30.7)))
    }
  }

  test("native Iceberg INSERT into a partitioned HDFS table, twice") {
    withHdfsIcebergCatalog() { catalog =>
      spark.sql(
        s"CREATE TABLE $catalog.db.pw (id INT, region STRING, amount DOUBLE) USING iceberg " +
          "PARTITIONED BY (region)")

      val first = runNativeWrite(
        s"INSERT INTO $catalog.db.pw VALUES (1, 'us-east', 10.5), (2, 'us-east', 20.3), " +
          "(3, 'eu', 30.7)")
      assertNativeWrite(first)
      val afterFirst = assertCommittedFilesOnHdfs(catalog, "pw")
      assertWriterReported(first, afterFirst, rows = 3)

      val second =
        runNativeWrite(s"INSERT INTO $catalog.db.pw VALUES (4, 'eu', 1.5), (5, 'ap', 2.5)")
      assertNativeWrite(second)
      assert(snapshotCount(catalog, "pw") == 2L)
      val afterSecond = assertCommittedFilesOnHdfs(catalog, "pw")
      assertWriterReported(second, addedFiles(afterFirst, afterSecond), rows = 2)

      // The native writer created the partition directories on the cluster.
      assert(
        afterSecond.map(_._1.getParent.getName).toSet ==
          Set("region=us-east", "region=eu", "region=ap"),
        s"unexpected partition directories: $afterSecond")

      val (_, cometPlan) = checkSparkAnswer(s"SELECT * FROM $catalog.db.pw ORDER BY id")
      assertSingleNativeScan(cometPlan)
      checkSparkAnswer(s"SELECT id FROM $catalog.db.pw WHERE region = 'eu' ORDER BY id")
    }
  }

  test("native Iceberg copy-on-write UPDATE rewrites its data files on HDFS") {
    withHdfsIcebergCatalog() { catalog =>
      spark.sql(
        s"CREATE TABLE $catalog.db.u (id INT, region STRING, amount DOUBLE) USING iceberg " +
          "PARTITIONED BY (region) TBLPROPERTIES ('write.update.mode'='copy-on-write')")
      val insert = runNativeWrite(
        s"INSERT INTO $catalog.db.u VALUES (1, 'us-east', 10.0), (2, 'us-east', 20.0), " +
          "(3, 'eu', 30.0)")
      assertNativeWrite(insert)
      val before = assertCommittedFilesOnHdfs(catalog, "u")
      assertWriterReported(insert, before, rows = 3)

      // The scan feeding the rewrite and the writer behind it both talk to HDFS natively.
      val update =
        runNativeWrite(s"UPDATE $catalog.db.u SET amount = amount * 2 WHERE id = 2")
      assertNativeWrite(update)
      assert(snapshotCount(catalog, "u") == 2L)
      val after = assertCommittedFilesOnHdfs(catalog, "u", exact = false)

      // Only the partition holding the updated row was rewritten: its two rows are in a new
      // file, and the other partition's file is still the one the INSERT wrote.
      val rewritten = addedFiles(before, after)
      assert(rewritten.map(_._1.getParent.getName) == Seq("region=us-east"), s"$rewritten")
      assertWriterReported(update, rewritten, rows = 2)
      val untouched = after.map(_._1.toUri.getPath).toSet -- rewritten.map(_._1.toUri.getPath)
      assert(untouched.size == 1 && untouched.head.contains("region=eu"), s"$untouched")

      val (_, cometPlan) = checkSparkAnswer(s"SELECT * FROM $catalog.db.u ORDER BY id")
      assertSingleNativeScan(cometPlan)
      val amounts = spark
        .sql(s"SELECT id, amount FROM $catalog.db.u ORDER BY id")
        .collect()
        .map(r => (r.getInt(0), r.getDouble(1)))
        .toSeq
      assert(amounts == Seq((1, 10.0), (2, 40.0), (3, 30.0)))
    }
  }

  private def stopHaCluster(): Unit = {
    try if (haCluster != null) haCluster.shutdown(true)
    finally {
      haCluster = null
      if (haBaseDir != null) FileUtils.deleteQuietly(haBaseDir)
      haBaseDir = null
    }
  }

  /**
   * Starts a two-NameNode MiniDFSCluster with `nn1` active, and returns the client configuration
   * a Spark job needs to address it by nameservice: exactly the keys a production `hdfs-site.xml`
   * carries, so that Comet's derivation of `hdfs.name-node.<ns>` is what lets the native client
   * dial.
   */
  private def startHaCluster(): Seq[(String, String)] = {
    val conf = new Configuration()
    conf.set("dfs.namenode.metrics.logger.period.seconds", "0")
    conf.set("dfs.datanode.metrics.logger.period.seconds", "0")
    conf.setIfUnset("dfs.namenode.rpc-bind-host", "localhost")
    // A second cluster in this JVM must not format the first one's `target/test/data/dfs`.
    haBaseDir = Files.createTempDirectory("comet_ha_dfs_").toFile
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, haBaseDir.getAbsolutePath)

    val topology = new MiniDFSNNTopology().addNameservice(
      new MiniDFSNNTopology.NSConf(haNameservice)
        .addNN(new MiniDFSNNTopology.NNConf("nn1"))
        .addNN(new MiniDFSNNTopology.NNConf("nn2")))
    haCluster = new MiniDFSCluster.Builder(conf).nnTopology(topology).numDataNodes(1).build()
    haCluster.transitionToActive(0)

    def rpcAddress(nn: String): String = {
      val key = s"dfs.namenode.rpc-address.$haNameservice.$nn"
      val address = haCluster.getConfiguration(0).get(key)
      assert(address != null && address.nonEmpty, s"the HA cluster did not configure $key")
      address
    }
    Seq(
      "dfs.nameservices" -> haNameservice,
      s"dfs.ha.namenodes.$haNameservice" -> "nn1,nn2",
      s"dfs.namenode.rpc-address.$haNameservice.nn1" -> rpcAddress("nn1"),
      s"dfs.namenode.rpc-address.$haNameservice.nn2" -> rpcAddress("nn2"),
      s"dfs.client.failover.proxy.provider.$haNameservice" ->
        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
  }

  test("native Iceberg scan and write resolve an HA nameservice and follow a failover") {
    val haClientConf = startHaCluster()
    val logicalUri = new URI(s"hdfs://$haNameservice")
    val haConf = haClientConf.toMap
    val expectedNameNodes = Seq("nn1", "nn2")
      .map(nn => "hdfs://" + haConf(s"dfs.namenode.rpc-address.$haNameservice.$nn"))
      .mkString(",")

    val hadoopConf = new Configuration()
    haClientConf.foreach { case (k, v) => hadoopConf.set(k, v) }
    try {
      // The nameservice keys reach the session Hadoop configuration (planning, the Iceberg
      // catalog, the commit) the way `spark.hadoop.*` settings would in a deployment.
      withSQLConf(haClientConf: _*) {
        withHdfsIcebergCatalog(baseUri = logicalUri.toString, cleanup = false) { catalog =>
          // A location with no host:port, which iceberg-rust cannot dial by itself.
          spark.sql(s"CREATE TABLE $catalog.db.ha (id INT, name STRING) USING iceberg")
          spark.sql(s"INSERT INTO $catalog.db.ha VALUES (1, 'a'), (2, 'b'), (3, 'c')")
          val location = tableLocation(catalog, "ha")
          assert(
            location.startsWith(s"hdfs://$haNameservice/"),
            s"expected a nameservice location, got $location")
          val fs = FileSystem.get(logicalUri, hadoopConf)
          // Written by the JVM writer, through the HA client.
          val before = assertCommittedFilesOnHdfs(catalog, "ha", fs)

          // Succeeding is not enough: the declaration Comet derived is what the client was given.
          // The per-nameservice key, since iceberg-rust reads the global `hdfs.name-node` only
          // for authority-less paths.
          def assertDeclared(properties: Map[String, String]): Unit = {
            assert(
              properties.get(s"hdfs.name-node.$haNameservice") == Some(expectedNameNodes),
              s"$properties")
            assert(!properties.contains("hdfs.name-node"), s"$properties")
          }

          def scanAndCheck(): Unit = {
            val query = s"SELECT * FROM $catalog.db.ha ORDER BY id"
            // Checked on the planned scan before anything runs, so a lost declaration fails here
            // with the properties in the message, not as a NameNode resolution error on an
            // executor after the native client's retries.
            assertDeclared(
              assertSingleNativeScan(
                spark
                  .sql(query)
                  .queryExecution
                  .executedPlan).nativeIcebergScanMetadata.catalogProperties)
            val (_, cometPlan) = checkSparkAnswer(query)
            assertDeclared(
              assertSingleNativeScan(cometPlan).nativeIcebergScanMetadata.catalogProperties)
          }

          scanAndCheck()

          // Fail over: nn1 goes standby and nn2 takes over. The first NameNode on the native
          // client's list is now a standby, and the list must carry it to the new active.
          haCluster.transitionToStandby(0)
          haCluster.transitionToActive(1)
          assert(
            haCluster.getNameNode(0).isStandbyState && haCluster.getNameNode(1).isActiveState)
          scanAndCheck()

          // The native writer finds the new active the same way, and what it writes is on the
          // cluster with the length it reported.
          val plans = runNativeWrite(s"INSERT INTO $catalog.db.ha VALUES (4, 'd')")
          assertNativeWrite(plans)
          val after = assertCommittedFilesOnHdfs(catalog, "ha", fs)
          assertWriterReported(plans, addedFiles(before, after), rows = 1)
          scanAndCheck()
          assert(
            spark.sql(s"SELECT count(*) FROM $catalog.db.ha").collect().head.getLong(0) == 4L)

          // A NameNode that is gone rather than standby: connections to nn1 are now refused, and
          // both the cached read client and a fresh write client must still reach nn2.
          haCluster.shutdownNameNode(0)
          scanAndCheck()
          val goneBefore = assertCommittedFilesOnHdfs(catalog, "ha", fs)
          val gonePlans = runNativeWrite(s"INSERT INTO $catalog.db.ha VALUES (5, 'e')")
          assertNativeWrite(gonePlans)
          val goneAfter = assertCommittedFilesOnHdfs(catalog, "ha", fs)
          assertWriterReported(gonePlans, addedFiles(goneBefore, goneAfter), rows = 1)
          scanAndCheck()
          assert(
            spark.sql(s"SELECT count(*) FROM $catalog.db.ha").collect().head.getLong(0) == 5L)
        }
      }
    } finally {
      // Evict the cached client of the nameservice before its cluster goes away.
      try FileSystem.get(logicalUri, hadoopConf).close()
      catch { case _: Exception => () }
      stopHaCluster()
    }
  }
}
