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
import org.apache.hadoop.crypto.key.{KeyProvider, KeyProviderFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.{MiniDFSCluster, MiniDFSNNTopology}
import org.apache.hadoop.hdfs.client.HdfsAdmin
import org.apache.hadoop.util.VersionInfo
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.CometSparkSessionExtensions.isSpark42Plus
import org.apache.comet.iceberg.IcebergReflection

/**
 * End-to-end coverage of the native Iceberg scan against an `hdfs://` warehouse, backed by
 * in-process `MiniDFSCluster`s.
 *
 * This is the only test that exercises iceberg-rust's `hdfs-native` backend for real. It matters
 * because that backend is a second, independent HDFS client: the plain-Parquet native scan
 * reaches HDFS through libhdfs/JNI (`fs.comet.libhdfs.schemes`), while an Iceberg table on HDFS
 * is opened by a pure-Rust RPC client that shares nothing with it but the NameNode endpoints
 * Comet hands it. A unit test over the scheme allowlists cannot tell whether that client actually
 * connects and reads. Writes to HDFS use Iceberg's own writer and are not covered here.
 *
 * What is covered:
 *   - scans of tables on a single-NameNode cluster, whose locations carry a real `host:port`
 *     authority, so iceberg-rust needs no NameNode declaration;
 *   - a two-NameNode HA cluster addressed by nameservice (`hdfs://<ns>/...`), which is not a
 *     host, so the scan only connects through the `hdfs.name-node.<ns>` declaration Comet derives
 *     from the session Hadoop configuration (iceberg-rust resolves a portless authority only
 *     through it). It is read before and after a failover, and once the first NameNode is shut
 *     down.
 *
 * Which profiles run it. The cluster needs the `hadoop-client-minicluster` that `pom.xml` pins
 * per Spark profile (`hadoop.version`: 3.4 -> 3.3.4, 3.5 -> 3.3.4, 4.0 -> 3.4.1, 4.1 -> 3.4.2,
 * 4.2 -> 3.5.0) to match the `hadoop-client-api`/`runtime` Spark supplies. Measured by running
 * the scan tests on every profile:
 *   - Spark 3.4, 3.5, 4.0 and 4.1: the cluster starts and every test runs, none canceled. A
 *     cluster that fails to start there is a broken fixture, so `beforeAll` fails the suite with
 *     the cause instead of cancelling the tests.
 *   - Spark 4.2: nothing runs (all tests are cancelled), for two independent reasons. There is no
 *     Iceberg runtime for 4.2 (`icebergAvailable` is false there), and the cluster cannot start
 *     either: `MiniDFSCluster.Builder` throws `NoClassDefFoundError:
 *     org/junit/jupiter/api/Assertions`, because Hadoop 3.5.0's `GenericTestUtils` asserts with
 *     JUnit 5, which the shaded minicluster leaves out and the test classpath does not carry.
 *     With junit-jupiter-api 5.10.3 and its three small dependencies put on the JVM class path
 *     the cluster starts, so a test-scoped `junit-jupiter-api` dependency on the 4.2 profile is
 *     the fix once an Iceberg runtime exists. Until then this suite skips there and starts
 *     nothing.
 */
class CometIcebergHdfsSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometIcebergTestBase
    with WithHdfsCluster {

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
      try stopEncryptedCluster()
      finally {
        try stopHdfsCluster()
        finally super.afterAll()
      }
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

  private def tableLocation(catalog: String, table: String): String = {
    val icebergTable = loadIcebergTable(spark, catalog, "db", table)
    icebergTable.getClass.getMethod("location").invoke(icebergTable).toString
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

  test("native Iceberg scan resolves an HA nameservice and follows a failover") {
    val haClientConf = startHaCluster()
    val logicalUri = new URI(s"hdfs://$haNameservice")
    val haConf = haClientConf.toMap
    // What Comet declares for the nameservice: its NameNodes in `dfs.ha.namenodes` order, as
    // their rpc-addresses read.
    val expectedNameNodes = Seq("nn1", "nn2")
      .map(nn => haConf(s"dfs.namenode.rpc-address.$haNameservice.$nn"))
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

          // Succeeding is not enough: the declaration Comet derived is what the client was given.
          // The per-nameservice key, since iceberg-rust reads the global `hdfs.name-node` only
          // for authority-less paths.
          def assertDeclared(properties: Map[String, String]): Unit = {
            assert(
              properties.get(s"hdfs.name-node.$haNameservice") == Some(expectedNameNodes),
              s"$properties")
            assert(!properties.contains("hdfs.name-node"), s"$properties")
          }

          def scanAndCheck(expectedRows: Long): Unit = {
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
            assert(
              spark.sql(s"SELECT count(*) FROM $catalog.db.ha").collect().head.getLong(0) ==
                expectedRows)
          }

          scanAndCheck(3L)

          // Fail over: nn1 goes standby and nn2 takes over. The first NameNode on the native
          // client's list is now a standby, and the list must carry it to the new active.
          haCluster.transitionToStandby(0)
          haCluster.transitionToActive(1)
          assert(
            haCluster.getNameNode(0).isStandbyState && haCluster.getNameNode(1).isActiveState)
          scanAndCheck(3L)

          // A NameNode that is gone rather than standby: connections to nn1 are now refused, and
          // the cached native client must still reach nn2, for the files it knows and for one
          // Iceberg's own writer adds through the HA client meanwhile.
          haCluster.shutdownNameNode(0)
          spark.sql(s"INSERT INTO $catalog.db.ha VALUES (4, 'd')")
          scanAndCheck(4L)
        }
      }
    } finally {
      // Evict the cached client of the nameservice before its cluster goes away.
      try FileSystem.get(logicalUri, hadoopConf).close()
      catch { case _: Exception => () }
      stopHaCluster()
    }
  }

  private var ezCluster: MiniDFSCluster = _
  private var ezBaseDir: File = _

  private def stopEncryptedCluster(): Unit = {
    try if (ezCluster != null) ezCluster.shutdown(true)
    finally {
      ezCluster = null
      if (ezBaseDir != null) FileUtils.deleteQuietly(ezBaseDir)
      ezBaseDir = null
    }
  }

  /**
   * Starts a cluster whose NameNode has a key provider (a `jceks` keystore on the local disk, as
   * Hadoop's own encryption tests use), with one key and an encryption zone at `/ez`. The key is
   * created before the NameNode starts, since it loads the keystore once.
   */
  private def startEncryptedCluster(): Unit = {
    val conf = new Configuration()
    conf.set("dfs.namenode.metrics.logger.period.seconds", "0")
    conf.set("dfs.datanode.metrics.logger.period.seconds", "0")
    conf.setIfUnset("dfs.namenode.rpc-bind-host", "localhost")
    ezBaseDir = Files.createTempDirectory("comet_ez_dfs_").toFile
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, ezBaseDir.getAbsolutePath)
    val providerUri = s"jceks://file${new File(ezBaseDir, "ez.jceks").toURI.getPath}"
    conf.set("hadoop.security.key.provider.path", providerUri)
    val provider = KeyProviderFactory.get(new URI(providerUri), conf)
    provider.createKey("ez-key", new KeyProvider.Options(conf))
    provider.flush()

    ezCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()
    val fs = ezCluster.getFileSystem
    fs.mkdirs(new Path("/ez"))
    new HdfsAdmin(fs.getUri, ezCluster.getConfiguration(0))
      .createEncryptionZone(new Path("/ez"), "ez-key")
  }

  test("native Iceberg scan falls back for a table in an HDFS encryption zone") {
    startEncryptedCluster()
    try {
      val ezUri = s"hdfs://localhost:${ezCluster.getNameNodePort}"
      // Iceberg's own writer encrypts through the key provider the NameNode advertises; the
      // native reader cannot decrypt, so the scan must be declined at planning, not fail on an
      // executor.
      withHdfsIcebergCatalog(baseUri = s"$ezUri/ez", cleanup = false) { catalog =>
        spark.sql(s"CREATE TABLE $catalog.db.enc (id INT, name STRING) USING iceberg")
        spark.sql(s"INSERT INTO $catalog.db.enc VALUES (1, 'a'), (2, 'b')")
        val dataLocation = IcebergReflection
          .getDataLocation(loadIcebergTable(spark, catalog, "db", "enc"))
          .getOrElse(fail("could not resolve the Iceberg data location"))
        // The fixture really is a zone: the NameNode flags the data directory as encrypted.
        assert(
          ezCluster.getFileSystem.getFileStatus(new Path(dataLocation)).isEncrypted,
          s"$dataLocation is not in the encryption zone")

        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
          s"SELECT * FROM $catalog.db.enc ORDER BY id",
          "encryption zone")
        val scans = collect(cometPlan) { case scan: CometIcebergNativeScanExec => scan }
        assert(
          scans.isEmpty,
          s"an encryption-zone table must fall back, but was claimed:\n$cometPlan")
      }
      // The gate is per location, not per cluster: a table outside the zone on the same
      // NameNode still scans natively.
      withHdfsIcebergCatalog(baseUri = ezUri, cleanup = false) { catalog =>
        spark.sql(s"CREATE TABLE $catalog.db.plain (id INT) USING iceberg")
        spark.sql(s"INSERT INTO $catalog.db.plain VALUES (1), (2)")
        val (_, cometPlan) = checkSparkAnswer(s"SELECT * FROM $catalog.db.plain ORDER BY id")
        assertSingleNativeScan(cometPlan)
      }
    } finally stopEncryptedCluster()
  }
}
