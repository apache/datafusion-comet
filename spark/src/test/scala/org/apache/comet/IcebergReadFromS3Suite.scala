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

import org.apache.spark.SparkConf
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.iceberg.RESTCatalogHelper

class IcebergReadFromS3Suite extends CometS3TestBase with RESTCatalogHelper {

  override protected val testBucketName = "test-iceberg-bucket"

  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.catalog.Catalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf

    conf.set("spark.sql.catalog.s3_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.s3_catalog.type", "hadoop")
    conf.set("spark.sql.catalog.s3_catalog.warehouse", s"s3a://$testBucketName/warehouse")

    conf.set(CometConf.COMET_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.set(CometConf.COMET_ICEBERG_NATIVE_ENABLED.key, "true")

    conf
  }

  /** Collects all CometIcebergNativeScanExec nodes from a plan */
  private def collectIcebergNativeScans(plan: SparkPlan): Seq[CometIcebergNativeScanExec] = {
    collect(plan) { case scan: CometIcebergNativeScanExec =>
      scan
    }
  }

  /**
   * Helper to verify query correctness and that exactly one CometIcebergNativeScanExec is used.
   */
  private def checkIcebergNativeScan(query: String): Unit = {
    val (_, cometPlan) = checkSparkAnswer(query)
    val icebergScans = collectIcebergNativeScans(cometPlan)
    assert(
      icebergScans.length == 1,
      s"Expected exactly 1 CometIcebergNativeScanExec but found ${icebergScans.length}. Plan:\n$cometPlan")
  }

  test("create and query simple Iceberg table from MinIO") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    spark.sql("""
      CREATE TABLE s3_catalog.db.simple_table (
        id INT,
        name STRING,
        value DOUBLE
      ) USING iceberg
    """)

    spark.sql("""
      INSERT INTO s3_catalog.db.simple_table
      VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.3), (3, 'Charlie', 30.7)
    """)

    checkIcebergNativeScan("SELECT * FROM s3_catalog.db.simple_table ORDER BY id")

    spark.sql("DROP TABLE s3_catalog.db.simple_table")
  }

  test("read partitioned Iceberg table from MinIO") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    spark.sql("""
      CREATE TABLE s3_catalog.db.partitioned_table (
        id INT,
        category STRING,
        value DOUBLE
      ) USING iceberg
      PARTITIONED BY (category)
    """)

    spark.sql("""
      INSERT INTO s3_catalog.db.partitioned_table VALUES
      (1, 'A', 10.5), (2, 'B', 20.3), (3, 'C', 30.7),
      (4, 'A', 15.2), (5, 'B', 25.8), (6, 'C', 35.0)
    """)

    checkIcebergNativeScan("SELECT * FROM s3_catalog.db.partitioned_table ORDER BY id")
    checkIcebergNativeScan(
      "SELECT * FROM s3_catalog.db.partitioned_table WHERE category = 'A' ORDER BY id")

    spark.sql("DROP TABLE s3_catalog.db.partitioned_table")
  }

  test("filter pushdown to S3-backed Iceberg table") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    spark.sql("""
      CREATE TABLE s3_catalog.db.filter_test (
        id INT,
        name STRING,
        value DOUBLE
      ) USING iceberg
    """)

    spark.sql("""
      INSERT INTO s3_catalog.db.filter_test VALUES
      (1, 'Alice', 10.5), (2, 'Bob', 20.3), (3, 'Charlie', 30.7),
      (4, 'Diana', 15.2), (5, 'Eve', 25.8)
    """)

    checkIcebergNativeScan("SELECT * FROM s3_catalog.db.filter_test WHERE id = 3")
    checkIcebergNativeScan("SELECT * FROM s3_catalog.db.filter_test WHERE value > 20.0")
    checkIcebergNativeScan("SELECT * FROM s3_catalog.db.filter_test WHERE name = 'Alice'")

    spark.sql("DROP TABLE s3_catalog.db.filter_test")
  }

  test("multiple files in S3 - verify no duplicates") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withSQLConf("spark.sql.files.maxRecordsPerFile" -> "50") {
      spark.sql("""
        CREATE TABLE s3_catalog.db.multifile_test (
          id INT,
          data STRING
        ) USING iceberg
      """)

      spark.sql("""
        INSERT INTO s3_catalog.db.multifile_test
        SELECT id, CONCAT('data_', CAST(id AS STRING)) as data
        FROM range(200)
      """)

      checkIcebergNativeScan("SELECT COUNT(DISTINCT id) FROM s3_catalog.db.multifile_test")
      checkIcebergNativeScan(
        "SELECT * FROM s3_catalog.db.multifile_test WHERE id < 10 ORDER BY id")

      spark.sql("DROP TABLE s3_catalog.db.multifile_test")
    }
  }

  test("large scale partitioned table - 100 partitions with many files") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withSQLConf(
      "spark.sql.files.maxRecordsPerFile" -> "50",
      "spark.sql.adaptive.enabled" -> "false") {
      spark.sql("""
        CREATE TABLE s3_catalog.db.large_partitioned_test (
          id INT,
          data STRING,
          partition_id INT
        ) USING iceberg
        PARTITIONED BY (partition_id)
      """)

      spark.sql("""
        INSERT INTO s3_catalog.db.large_partitioned_test
        SELECT
          id,
          CONCAT('data_', CAST(id AS STRING)) as data,
          (id % 100) as partition_id
        FROM range(500000)
      """)

      checkIcebergNativeScan(
        "SELECT COUNT(DISTINCT id) FROM s3_catalog.db.large_partitioned_test")
      checkIcebergNativeScan(
        "SELECT * FROM s3_catalog.db.large_partitioned_test WHERE id < 10 ORDER BY id")
      checkIcebergNativeScan(
        "SELECT SUM(id) FROM s3_catalog.db.large_partitioned_test WHERE partition_id = 0")
      checkIcebergNativeScan(
        "SELECT SUM(id) FROM s3_catalog.db.large_partitioned_test WHERE partition_id IN (0, 50, 99)")

      spark.sql("DROP TABLE s3_catalog.db.large_partitioned_test PURGE")
    }
  }

  test("MOR table with deletes in S3") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    spark.sql("""
      CREATE TABLE s3_catalog.db.mor_delete_test (
        id INT,
        name STRING,
        value DOUBLE
      ) USING iceberg
      TBLPROPERTIES (
        'write.delete.mode' = 'merge-on-read',
        'write.merge.mode' = 'merge-on-read'
      )
    """)

    spark.sql("""
      INSERT INTO s3_catalog.db.mor_delete_test VALUES
      (1, 'Alice', 10.5), (2, 'Bob', 20.3), (3, 'Charlie', 30.7),
      (4, 'Diana', 15.2), (5, 'Eve', 25.8)
    """)

    spark.sql("DELETE FROM s3_catalog.db.mor_delete_test WHERE id IN (2, 4)")

    checkIcebergNativeScan("SELECT * FROM s3_catalog.db.mor_delete_test ORDER BY id")

    spark.sql("DROP TABLE s3_catalog.db.mor_delete_test")
  }

  test("REST catalog credential vending rejects wrong credentials") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    val wrongCreds = Map(
      "s3.access-key-id" -> "WRONG_ACCESS_KEY",
      "s3.secret-access-key" -> "WRONG_SECRET_KEY",
      "s3.endpoint" -> minioContainer.getS3URL,
      "s3.path-style-access" -> "true")
    val warehouse = s"s3a://$testBucketName/warehouse-bad-creds"

    withRESTCatalog(vendedCredentials = wrongCreds, warehouseLocation = Some(warehouse)) {
      (restUri, _, _) =>
        withSQLConf(
          "spark.sql.catalog.bad_cat" -> "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.bad_cat.catalog-impl" -> "org.apache.iceberg.rest.RESTCatalog",
          "spark.sql.catalog.bad_cat.uri" -> restUri,
          "spark.sql.catalog.bad_cat.warehouse" -> warehouse) {

          spark.sql("CREATE NAMESPACE bad_cat.db")

          // CREATE TABLE succeeds (metadata only, no S3 access needed)
          spark.sql("CREATE TABLE bad_cat.db.test (id INT) USING iceberg")

          // INSERT fails because S3FileIO uses the wrong vended credentials
          val e = intercept[Exception] {
            spark.sql("INSERT INTO bad_cat.db.test VALUES (1)")
          }
          assert(e.getMessage.contains("403"), s"Expected S3 403 error but got: ${e.getMessage}")
        }
    }
  }

  test("REST catalog credential vending with native Iceberg scan on S3") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    val vendedCreds = Map(
      "s3.access-key-id" -> userName,
      "s3.secret-access-key" -> password,
      "s3.endpoint" -> minioContainer.getS3URL,
      "s3.path-style-access" -> "true")
    val warehouse = s"s3a://$testBucketName/warehouse-vending"

    withRESTCatalog(vendedCredentials = vendedCreds, warehouseLocation = Some(warehouse)) {
      (restUri, _, _) =>
        withSQLConf(
          "spark.sql.catalog.vend_cat" -> "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.vend_cat.catalog-impl" -> "org.apache.iceberg.rest.RESTCatalog",
          "spark.sql.catalog.vend_cat.uri" -> restUri,
          "spark.sql.catalog.vend_cat.warehouse" -> warehouse,
          CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {

          spark.sql("CREATE NAMESPACE vend_cat.db")

          spark.sql("""
            CREATE TABLE vend_cat.db.simple (
              id INT, name STRING, value DOUBLE
            ) USING iceberg
          """)
          spark.sql("""
            INSERT INTO vend_cat.db.simple
            VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.3), (3, 'Charlie', 30.7)
          """)
          checkIcebergNativeScan("SELECT * FROM vend_cat.db.simple ORDER BY id")

          spark.sql("DROP TABLE vend_cat.db.simple")
          spark.sql("DROP NAMESPACE vend_cat.db")
        }
    }
  }
}
