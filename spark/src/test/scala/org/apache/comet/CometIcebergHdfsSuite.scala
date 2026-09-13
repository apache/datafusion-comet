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

import java.util.UUID

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.iceberg.IcebergReflection

/**
 * End-to-end coverage of the native Iceberg scan against an `hdfs://` warehouse, backed by an
 * in-process `MiniDFSCluster`.
 *
 * This is the only test that exercises iceberg-rust's `hdfs-native` backend for real. It matters
 * because that backend is a second, independent HDFS client: the plain-Parquet native scan
 * reaches HDFS through libhdfs/JNI (`fs.comet.libhdfs.schemes`), while an Iceberg table on HDFS
 * is opened by a pure-Rust RPC client that shares nothing with it but the `$HADOOP_CONF_DIR` XML.
 * A unit test over the scheme allowlists cannot tell whether that client actually connects.
 *
 * The cluster is a single NameNode, so table locations carry a real `host:port` authority and
 * iceberg-rust needs no `hdfs.name-node` property; the HA translation that supplies one is
 * covered by `CometIcebergNativeScanSuite`.
 */
class CometIcebergHdfsSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometIcebergTestBase
    with WithHdfsCluster {

  override def beforeAll(): Unit = {
    super.beforeAll()
    startHdfsCluster()
  }

  override def afterAll(): Unit = {
    try stopHdfsCluster()
    finally super.afterAll()
  }

  /** `hdfs://localhost:<port>` -- the authority iceberg-rust dials as the NameNode. */
  private def hdfsUri: String = s"hdfs://localhost:$getDFSPort"

  private def assertSingleNativeScan(cometPlan: SparkPlan): Unit = {
    val scans = collect(cometPlan) { case scan: CometIcebergNativeScanExec => scan }
    assert(
      scans.length == 1,
      s"Expected exactly 1 CometIcebergNativeScanExec but found ${scans.length}. " +
        s"Plan:\n$cometPlan")
  }

  /**
   * Runs `f` with a Hadoop-catalog Iceberg warehouse rooted on the MiniDFS cluster. The catalog
   * name is unique per test so Spark's catalog cache cannot hand back a warehouse from an earlier
   * test.
   */
  private def withHdfsIcebergCatalog(f: String => Unit): Unit = {
    val catalog = s"hdfs_cat_${UUID.randomUUID().toString.replace("-", "")}"
    val warehouse = s"$hdfsUri/warehouse/${UUID.randomUUID()}"
    withSQLConf(
      s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog.warehouse" -> warehouse,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {
      f(catalog)
    }
  }

  test("native Iceberg scan reads a table stored on HDFS") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withHdfsIcebergCatalog { catalog =>
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
    assume(icebergAvailable, "Iceberg not available in classpath")

    withHdfsIcebergCatalog { catalog =>
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
    assume(icebergAvailable, "Iceberg not available in classpath")

    withHdfsIcebergCatalog { catalog =>
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
}
