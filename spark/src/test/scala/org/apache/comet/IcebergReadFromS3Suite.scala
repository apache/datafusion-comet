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

class IcebergReadFromS3Suite extends CometS3TestBase {

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
}
