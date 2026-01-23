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
import java.nio.file.Files

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.iceberg.RESTCatalogHelper

/**
 * Test suite for native Iceberg scan using FileScanTasks and iceberg-rust.
 *
 * Note: Requires Iceberg dependencies to be added to pom.xml
 */
class CometIcebergNativeSuite extends CometTestBase with RESTCatalogHelper {

  // Skip these tests if Iceberg is not available in classpath
  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.catalog.Catalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  /** Collects all CometIcebergNativeScanExec nodes from a plan */
  private def collectIcebergNativeScans(plan: SparkPlan): Seq[CometIcebergNativeScanExec] = {
    collect(plan) { case scan: CometIcebergNativeScanExec =>
      scan
    }
  }

  /**
   * Helper to verify query correctness and that exactly one CometIcebergNativeScanExec is used.
   * This ensures both correct results and that the native Iceberg scan operator is being used.
   */
  private def checkIcebergNativeScan(query: String): Unit = {
    val (_, cometPlan) = checkSparkAnswer(query)
    val icebergScans = collectIcebergNativeScans(cometPlan)
    assert(
      icebergScans.length == 1,
      s"Expected exactly 1 CometIcebergNativeScanExec but found ${icebergScans.length}. Plan:\n$cometPlan")
  }

  test("create and query simple Iceberg table with Hadoop catalog") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.hadoop_catalog" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.hadoop_catalog.type" -> "hadoop",
        "spark.sql.catalog.hadoop_catalog.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE hadoop_catalog.db.test_table (
            id INT,
            name STRING,
            value DOUBLE
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO hadoop_catalog.db.test_table
          VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.3), (3, 'Charlie', 30.7)
        """)

        checkIcebergNativeScan("SELECT * FROM hadoop_catalog.db.test_table ORDER BY id")

        spark.sql("DROP TABLE hadoop_catalog.db.test_table")
      }
    }
  }

  test("filter pushdown - equality predicates") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.filter_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.filter_cat.type" -> "hadoop",
        "spark.sql.catalog.filter_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE filter_cat.db.filter_test (
            id INT,
            name STRING,
            value DOUBLE,
            active BOOLEAN
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO filter_cat.db.filter_test VALUES
          (1, 'Alice', 10.5, true),
          (2, 'Bob', 20.3, false),
          (3, 'Charlie', 30.7, true),
          (4, 'Diana', 15.2, false),
          (5, 'Eve', 25.8, true)
        """)

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.filter_test WHERE id = 3")

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.filter_test WHERE name = 'Bob'")

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.filter_test WHERE active = true")

        spark.sql("DROP TABLE filter_cat.db.filter_test")
      }
    }
  }

  test("filter pushdown - comparison operators") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.filter_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.filter_cat.type" -> "hadoop",
        "spark.sql.catalog.filter_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE filter_cat.db.comparison_test (
            id INT,
            value DOUBLE
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO filter_cat.db.comparison_test VALUES
          (1, 10.5), (2, 20.3), (3, 30.7), (4, 15.2), (5, 25.8)
        """)

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.comparison_test WHERE value > 20.0")

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.comparison_test WHERE value >= 20.3")

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.comparison_test WHERE value < 20.0")

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.comparison_test WHERE value <= 20.3")

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.comparison_test WHERE id != 3")

        spark.sql("DROP TABLE filter_cat.db.comparison_test")
      }
    }
  }

  test("filter pushdown - AND/OR combinations") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.filter_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.filter_cat.type" -> "hadoop",
        "spark.sql.catalog.filter_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE filter_cat.db.logical_test (
            id INT,
            category STRING,
            value DOUBLE
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO filter_cat.db.logical_test VALUES
          (1, 'A', 10.5), (2, 'B', 20.3), (3, 'A', 30.7),
          (4, 'B', 15.2), (5, 'A', 25.8), (6, 'C', 35.0)
        """)

        checkIcebergNativeScan(
          "SELECT * FROM filter_cat.db.logical_test WHERE category = 'A' AND value > 20.0")

        checkIcebergNativeScan(
          "SELECT * FROM filter_cat.db.logical_test WHERE category = 'B' OR value > 30.0")

        checkIcebergNativeScan("""SELECT * FROM filter_cat.db.logical_test
             WHERE (category = 'A' AND value > 20.0) OR category = 'C'""")

        spark.sql("DROP TABLE filter_cat.db.logical_test")
      }
    }
  }

  test("filter pushdown - NULL checks") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.filter_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.filter_cat.type" -> "hadoop",
        "spark.sql.catalog.filter_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE filter_cat.db.null_test (
            id INT,
            optional_value DOUBLE
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO filter_cat.db.null_test VALUES
          (1, 10.5), (2, NULL), (3, 30.7), (4, NULL), (5, 25.8)
        """)

        checkIcebergNativeScan(
          "SELECT * FROM filter_cat.db.null_test WHERE optional_value IS NULL")

        checkIcebergNativeScan(
          "SELECT * FROM filter_cat.db.null_test WHERE optional_value IS NOT NULL")

        spark.sql("DROP TABLE filter_cat.db.null_test")
      }
    }
  }

  test("filter pushdown - IN list") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.filter_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.filter_cat.type" -> "hadoop",
        "spark.sql.catalog.filter_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE filter_cat.db.in_test (
            id INT,
            name STRING
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO filter_cat.db.in_test VALUES
          (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'),
          (4, 'Diana'), (5, 'Eve'), (6, 'Frank')
        """)

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.in_test WHERE id IN (2, 4, 6)")

        checkIcebergNativeScan(
          "SELECT * FROM filter_cat.db.in_test WHERE name IN ('Alice', 'Charlie', 'Eve')")

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.in_test WHERE id IS NOT NULL")

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.in_test WHERE id NOT IN (1, 3, 5)")

        spark.sql("DROP TABLE filter_cat.db.in_test")
      }
    }
  }

  test("verify filters are pushed to native scan") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.filter_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.filter_cat.type" -> "hadoop",
        "spark.sql.catalog.filter_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE filter_cat.db.filter_debug (
            id INT,
            value DOUBLE
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO filter_cat.db.filter_debug VALUES
          (1, 10.5), (2, 20.3), (3, 30.7), (4, 15.2), (5, 25.8)
        """)

        checkIcebergNativeScan("SELECT * FROM filter_cat.db.filter_debug WHERE id > 2")

        spark.sql("DROP TABLE filter_cat.db.filter_debug")
      }
    }
  }

  test("small table - verify no duplicate rows (1 file)") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.small_table (
            id INT,
            name STRING
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.small_table
          VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
        """)

        checkIcebergNativeScan("SELECT * FROM test_cat.db.small_table ORDER BY id")
        checkIcebergNativeScan("SELECT COUNT(DISTINCT id) FROM test_cat.db.small_table")

        spark.sql("DROP TABLE test_cat.db.small_table")
      }
    }
  }

  test("medium table - verify correct partition count (multiple files)") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
        "spark.sql.files.maxRecordsPerFile" -> "10") {

        spark.sql("""
          CREATE TABLE test_cat.db.medium_table (
            id INT,
            value DOUBLE
          ) USING iceberg
        """)

        // Insert 100 rows - should create multiple files with maxRecordsPerFile=10
        spark.sql("""
          INSERT INTO test_cat.db.medium_table
          SELECT id, CAST(id * 1.5 AS DOUBLE) as value
          FROM range(100)
        """)

        // Verify results match Spark native (catches duplicates across partitions)
        checkIcebergNativeScan("SELECT * FROM test_cat.db.medium_table ORDER BY id")
        checkIcebergNativeScan("SELECT COUNT(DISTINCT id) FROM test_cat.db.medium_table")
        checkIcebergNativeScan("SELECT SUM(value) FROM test_cat.db.medium_table")

        spark.sql("DROP TABLE test_cat.db.medium_table")
      }
    }
  }

  test("large table - verify no duplicates with many files") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
        "spark.sql.files.maxRecordsPerFile" -> "100") {

        spark.sql("""
          CREATE TABLE test_cat.db.large_table (
            id BIGINT,
            category STRING,
            value DOUBLE
          ) USING iceberg
        """)

        // Insert 10,000 rows - with maxRecordsPerFile=100, creates ~100 files
        spark.sql("""
          INSERT INTO test_cat.db.large_table
          SELECT
            id,
            CASE WHEN id % 3 = 0 THEN 'A' WHEN id % 3 = 1 THEN 'B' ELSE 'C' END as category,
            CAST(id * 2.5 AS DOUBLE) as value
          FROM range(10000)
        """)

        checkIcebergNativeScan("SELECT COUNT(DISTINCT id) FROM test_cat.db.large_table")
        checkIcebergNativeScan("SELECT SUM(value) FROM test_cat.db.large_table")
        checkIcebergNativeScan(
          "SELECT category, COUNT(*) FROM test_cat.db.large_table GROUP BY category ORDER BY category")

        spark.sql("DROP TABLE test_cat.db.large_table")
      }
    }
  }

  test("partitioned table - verify key-grouped partitioning") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.partitioned_table (
            id INT,
            category STRING,
            value DOUBLE
          ) USING iceberg
          PARTITIONED BY (category)
        """)

        spark.sql("""
          INSERT INTO test_cat.db.partitioned_table VALUES
          (1, 'A', 10.5), (2, 'B', 20.3), (3, 'C', 30.7),
          (4, 'A', 15.2), (5, 'B', 25.8), (6, 'C', 35.0),
          (7, 'A', 12.1), (8, 'B', 22.5), (9, 'C', 32.9)
        """)

        checkIcebergNativeScan("SELECT * FROM test_cat.db.partitioned_table ORDER BY id")
        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.partitioned_table WHERE category = 'A' ORDER BY id")
        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.partitioned_table WHERE category = 'B' ORDER BY id")
        checkIcebergNativeScan(
          "SELECT category, COUNT(*) FROM test_cat.db.partitioned_table GROUP BY category ORDER BY category")

        spark.sql("DROP TABLE test_cat.db.partitioned_table")
      }
    }
  }

  test("empty table - verify graceful handling") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.empty_table (
            id INT,
            name STRING
          ) USING iceberg
        """)

        checkIcebergNativeScan("SELECT * FROM test_cat.db.empty_table")
        checkIcebergNativeScan("SELECT * FROM test_cat.db.empty_table WHERE id > 0")

        spark.sql("DROP TABLE test_cat.db.empty_table")
      }
    }
  }

  // MOR (Merge-On-Read) delete file tests.
  // Delete files are extracted from FileScanTasks and handled by iceberg-rust's ArrowReader,
  // which automatically applies both positional and equality deletes during scan execution.
  test("MOR table with POSITIONAL deletes - verify deletes are applied") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.positional_delete_test (
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
          INSERT INTO test_cat.db.positional_delete_test
          VALUES
            (1, 'Alice', 10.5), (2, 'Bob', 20.3), (3, 'Charlie', 30.7),
            (4, 'Diana', 15.2), (5, 'Eve', 25.8), (6, 'Frank', 35.0),
            (7, 'Grace', 12.1), (8, 'Hank', 22.5)
        """)

        spark.sql("DELETE FROM test_cat.db.positional_delete_test WHERE id IN (2, 4, 6)")

        checkIcebergNativeScan("SELECT * FROM test_cat.db.positional_delete_test ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.positional_delete_test")
      }
    }
  }

  test("MOR table with EQUALITY deletes - verify deletes are applied") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Create table with equality delete columns specified
        // This forces Spark to use equality deletes instead of positional deletes
        spark.sql("""
          CREATE TABLE test_cat.db.equality_delete_test (
            id INT,
            category STRING,
            value DOUBLE
          ) USING iceberg
          TBLPROPERTIES (
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.equality-delete-columns' = 'id'
          )
        """)

        spark.sql("""
          INSERT INTO test_cat.db.equality_delete_test
          VALUES
            (1, 'A', 10.5), (2, 'B', 20.3), (3, 'A', 30.7),
            (4, 'B', 15.2), (5, 'A', 25.8), (6, 'C', 35.0)
        """)

        spark.sql("DELETE FROM test_cat.db.equality_delete_test WHERE id IN (2, 4)")

        checkIcebergNativeScan("SELECT * FROM test_cat.db.equality_delete_test ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.equality_delete_test")
      }
    }
  }

  test("MOR table with multiple delete operations - mixed delete types") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.multi_delete_test (
            id INT,
            data STRING
          ) USING iceberg
          TBLPROPERTIES (
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
          )
        """)

        spark.sql("""
          INSERT INTO test_cat.db.multi_delete_test
          SELECT id, CONCAT('data_', CAST(id AS STRING)) as data
          FROM range(100)
        """)

        spark.sql("DELETE FROM test_cat.db.multi_delete_test WHERE id < 10")
        spark.sql("DELETE FROM test_cat.db.multi_delete_test WHERE id > 90")
        spark.sql("DELETE FROM test_cat.db.multi_delete_test WHERE id % 10 = 5")

        checkIcebergNativeScan("SELECT * FROM test_cat.db.multi_delete_test ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.multi_delete_test")
      }
    }
  }

  test("verify no duplicate rows across multiple partitions") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
        // Create multiple files to ensure multiple partitions
        "spark.sql.files.maxRecordsPerFile" -> "50") {

        spark.sql("""
          CREATE TABLE test_cat.db.multipart_test (
            id INT,
            data STRING
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.multipart_test
          SELECT id, CONCAT('data_', CAST(id AS STRING)) as data
          FROM range(500)
        """)

        // Critical: COUNT(*) vs COUNT(DISTINCT id) catches duplicates across partitions
        checkIcebergNativeScan("SELECT COUNT(DISTINCT id) FROM test_cat.db.multipart_test")
        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.multipart_test WHERE id < 10 ORDER BY id")
        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.multipart_test WHERE id >= 490 ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.multipart_test")
      }
    }
  }

  test("filter pushdown with multi-partition table") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
        "spark.sql.files.maxRecordsPerFile" -> "20") {

        spark.sql("""
          CREATE TABLE test_cat.db.filter_multipart (
            id INT,
            category STRING,
            value DOUBLE
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.filter_multipart
          SELECT
            id,
            CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END as category,
            CAST(id * 1.5 AS DOUBLE) as value
          FROM range(200)
        """)

        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.filter_multipart WHERE id > 150 ORDER BY id")
        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.filter_multipart WHERE category = 'even' AND id < 50 ORDER BY id")
        checkIcebergNativeScan(
          "SELECT COUNT(DISTINCT id) FROM test_cat.db.filter_multipart WHERE id BETWEEN 50 AND 100")
        checkIcebergNativeScan(
          "SELECT SUM(value) FROM test_cat.db.filter_multipart WHERE category = 'odd'")

        spark.sql("DROP TABLE test_cat.db.filter_multipart")
      }
    }
  }

  test("date partitioned table with date range queries") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.date_partitioned (
            id INT,
            event_date DATE,
            value STRING
          ) USING iceberg
          PARTITIONED BY (days(event_date))
        """)

        spark.sql("""
          INSERT INTO test_cat.db.date_partitioned VALUES
          (1, DATE '2024-01-01', 'a'), (2, DATE '2024-01-02', 'b'),
          (3, DATE '2024-01-03', 'c'), (4, DATE '2024-01-15', 'd'),
          (5, DATE '2024-01-16', 'e'), (6, DATE '2024-02-01', 'f')
        """)

        checkIcebergNativeScan("SELECT * FROM test_cat.db.date_partitioned ORDER BY id")
        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.date_partitioned WHERE event_date = DATE '2024-01-01'")
        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.date_partitioned WHERE event_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-03' ORDER BY id")
        checkIcebergNativeScan(
          "SELECT event_date, COUNT(*) FROM test_cat.db.date_partitioned GROUP BY event_date ORDER BY event_date")

        spark.sql("DROP TABLE test_cat.db.date_partitioned")
      }
    }
  }

  test("bucket partitioned table") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.bucket_partitioned (
            id INT,
            value DOUBLE
          ) USING iceberg
          PARTITIONED BY (bucket(4, id))
        """)

        spark.sql("""
          INSERT INTO test_cat.db.bucket_partitioned
          SELECT id, CAST(id * 1.5 AS DOUBLE) as value
          FROM range(100)
        """)

        checkIcebergNativeScan("SELECT * FROM test_cat.db.bucket_partitioned ORDER BY id")
        checkIcebergNativeScan("SELECT COUNT(DISTINCT id) FROM test_cat.db.bucket_partitioned")
        checkIcebergNativeScan("SELECT SUM(value) FROM test_cat.db.bucket_partitioned")
        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.bucket_partitioned WHERE id < 20 ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.bucket_partitioned")
      }
    }
  }

  test("partition pruning - bucket transform verifies files are skipped") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.bucket_pruning (
            id INT,
            data STRING
          ) USING iceberg
          PARTITIONED BY (bucket(8, id))
        """)

        (0 until 8).foreach { bucket =>
          spark.sql(s"""
            INSERT INTO test_cat.db.bucket_pruning
            SELECT id, CONCAT('data_', CAST(id AS STRING)) as data
            FROM range(${bucket * 100}, ${(bucket + 1) * 100})
          """)
        }

        val specificIds = Seq(5, 15, 25)
        val df = spark.sql(s"""
          SELECT * FROM test_cat.db.bucket_pruning
          WHERE id IN (${specificIds.mkString(",")})
        """)

        val scanNodes = df.queryExecution.executedPlan
          .collectLeaves()
          .collect { case s: CometIcebergNativeScanExec => s }

        assert(scanNodes.nonEmpty, "Expected at least one CometIcebergNativeScanExec node")

        val metrics = scanNodes.head.metrics

        val result = df.collect()
        assert(result.length == specificIds.length)

        // With bucket partitioning, pruning occurs at the file level, not manifest level
        // Bucket transforms use hash-based bucketing, so manifests may contain files from
        // multiple buckets. Iceberg can skip individual files based on bucket metadata,
        // but cannot skip entire manifests.
        assert(
          metrics("resultDataFiles").value < 8,
          "Bucket pruning should skip some files, but read " +
            s"${metrics("resultDataFiles").value} out of 8")
        assert(
          metrics("skippedDataFiles").value > 0,
          "Expected skipped data files due to bucket pruning, got" +
            s"${metrics("skippedDataFiles").value}")

        spark.sql("DROP TABLE test_cat.db.bucket_pruning")
      }
    }
  }

  test("partition pruning - truncate transform verifies files are skipped") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.truncate_pruning (
            id INT,
            message STRING
          ) USING iceberg
          PARTITIONED BY (truncate(5, message))
        """)

        val prefixes = Seq("alpha", "bravo", "charlie", "delta", "echo")
        prefixes.zipWithIndex.foreach { case (prefix, idx) =>
          spark.sql(s"""
            INSERT INTO test_cat.db.truncate_pruning
            SELECT
              id,
              CONCAT('$prefix', '_suffix_', CAST(id AS STRING)) as message
            FROM range(${idx * 10}, ${(idx + 1) * 10})
          """)
        }

        val df = spark.sql("""
          SELECT * FROM test_cat.db.truncate_pruning
          WHERE message LIKE 'alpha%'
        """)

        val scanNodes = df.queryExecution.executedPlan
          .collectLeaves()
          .collect { case s: CometIcebergNativeScanExec => s }

        assert(scanNodes.nonEmpty, "Expected at least one CometIcebergNativeScanExec node")

        val metrics = scanNodes.head.metrics

        val result = df.collect()
        assert(result.length == 10)
        assert(result.forall(_.getString(1).startsWith("alpha")))

        // Partition pruning occurs at the manifest level, not file level
        // Each INSERT creates one manifest, so we verify skippedDataManifests
        assert(
          metrics("resultDataFiles").value == 1,
          s"Truncate pruning should only read 1 file, read ${metrics("resultDataFiles").value}")
        assert(
          metrics("skippedDataManifests").value == 4,
          s"Expected 4 skipped manifests, got ${metrics("skippedDataManifests").value}")

        spark.sql("DROP TABLE test_cat.db.truncate_pruning")
      }
    }
  }

  test("partition pruning - hour transform verifies files are skipped") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.hour_pruning (
            id INT,
            event_time TIMESTAMP,
            data STRING
          ) USING iceberg
          PARTITIONED BY (hour(event_time))
        """)

        (0 until 6).foreach { hour =>
          spark.sql(s"""
            INSERT INTO test_cat.db.hour_pruning
            SELECT
              id,
              CAST('2024-01-01 $hour:00:00' AS TIMESTAMP) as event_time,
              CONCAT('event_', CAST(id AS STRING)) as data
            FROM range(${hour * 10}, ${(hour + 1) * 10})
          """)
        }

        val df = spark.sql("""
          SELECT * FROM test_cat.db.hour_pruning
          WHERE event_time >= CAST('2024-01-01 04:00:00' AS TIMESTAMP)
        """)

        val scanNodes = df.queryExecution.executedPlan
          .collectLeaves()
          .collect { case s: CometIcebergNativeScanExec => s }

        assert(scanNodes.nonEmpty, "Expected at least one CometIcebergNativeScanExec node")

        val metrics = scanNodes.head.metrics

        val result = df.collect()
        assert(result.length == 20)

        // Partition pruning occurs at the manifest level, not file level
        // Each INSERT creates one manifest, so we verify skippedDataManifests
        assert(
          metrics("resultDataFiles").value == 2,
          s"Hour pruning should read 2 files (hours 4-5), read ${metrics("resultDataFiles").value}")
        assert(
          metrics("skippedDataManifests").value == 4,
          s"Expected 4 skipped manifests (hours 0-3), got ${metrics("skippedDataManifests").value}")

        spark.sql("DROP TABLE test_cat.db.hour_pruning")
      }
    }
  }

  test("schema evolution - add column") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.schema_evolution (
            id INT,
            name STRING
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.schema_evolution VALUES (1, 'Alice'), (2, 'Bob')
        """)

        spark.sql("ALTER TABLE test_cat.db.schema_evolution ADD COLUMN age INT")

        spark.sql("""
          INSERT INTO test_cat.db.schema_evolution VALUES (3, 'Charlie', 30), (4, 'Diana', 25)
        """)

        checkIcebergNativeScan("SELECT * FROM test_cat.db.schema_evolution ORDER BY id")
        checkIcebergNativeScan("SELECT id, name FROM test_cat.db.schema_evolution ORDER BY id")
        checkIcebergNativeScan(
          "SELECT id, age FROM test_cat.db.schema_evolution WHERE age IS NOT NULL ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.schema_evolution")
      }
    }
  }

  test("schema evolution - drop column") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.drop_column_test (
            id INT,
            name STRING,
            age INT
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.drop_column_test VALUES (1, 'Alice', 30), (2, 'Bob', 25)
        """)

        // Drop the age column
        spark.sql("ALTER TABLE test_cat.db.drop_column_test DROP COLUMN age")

        // Insert new data without the age column
        spark.sql("""
          INSERT INTO test_cat.db.drop_column_test VALUES (3, 'Charlie'), (4, 'Diana')
        """)

        // Read all data - must handle old files (with age) and new files (without age)
        checkIcebergNativeScan("SELECT * FROM test_cat.db.drop_column_test ORDER BY id")
        checkIcebergNativeScan("SELECT id, name FROM test_cat.db.drop_column_test ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.drop_column_test")
      }
    }
  }

  test("migration - basic read after migration (fallback for no field ID)") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        val sourceName = "parquet_source"
        val destName = "test_cat.db.iceberg_dest"
        val dataPath = s"${warehouseDir.getAbsolutePath}/source_data"

        // Step 1: Create regular Parquet table (without field IDs)
        spark
          .range(10)
          .selectExpr(
            "CAST(id AS INT) as id",
            "CONCAT('name_', CAST(id AS STRING)) as name",
            "CAST(id * 2 AS DOUBLE) as value")
          .write
          .mode("overwrite")
          .option("path", dataPath)
          .saveAsTable(sourceName)

        // Step 2: Snapshot the Parquet table into Iceberg using SparkActions API
        try {
          val actionsClass = Class.forName("org.apache.iceberg.spark.actions.SparkActions")
          val getMethod = actionsClass.getMethod("get")
          val actions = getMethod.invoke(null)
          val snapshotMethod = actions.getClass.getMethod("snapshotTable", classOf[String])
          val snapshotAction = snapshotMethod.invoke(actions, sourceName)
          val asMethod = snapshotAction.getClass.getMethod("as", classOf[String])
          val snapshotWithDest = asMethod.invoke(snapshotAction, destName)
          val executeMethod = snapshotWithDest.getClass.getMethod("execute")
          executeMethod.invoke(snapshotWithDest)

          // Step 3: Read the Iceberg table - Parquet files have no field IDs, so position-based mapping is used
          checkIcebergNativeScan(s"SELECT * FROM $destName ORDER BY id")
          checkIcebergNativeScan(s"SELECT id, name FROM $destName ORDER BY id")
          checkIcebergNativeScan(s"SELECT value FROM $destName WHERE id < 5 ORDER BY id")

          spark.sql(s"DROP TABLE $destName")
          spark.sql(s"DROP TABLE $sourceName")
        } catch {
          case _: ClassNotFoundException =>
            cancel("Iceberg Actions API not available - requires iceberg-spark-runtime")
        }
      }
    }
  }

  test("migration - hive-style partitioned table has partition values") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        val sourceName = "parquet_partitioned_source"
        val destName = "test_cat.db.iceberg_partitioned"
        val dataPath = s"${warehouseDir.getAbsolutePath}/partitioned_data"

        // Hive-style partitioning stores partition values in directory paths, not in data files
        spark
          .range(10)
          .selectExpr(
            "CAST(id AS INT) as partition_col",
            "CONCAT('data_', CAST(id AS STRING)) as data")
          .write
          .mode("overwrite")
          .partitionBy("partition_col")
          .option("path", dataPath)
          .saveAsTable(sourceName)

        try {
          val actionsClass = Class.forName("org.apache.iceberg.spark.actions.SparkActions")
          val getMethod = actionsClass.getMethod("get")
          val actions = getMethod.invoke(null)
          val snapshotMethod = actions.getClass.getMethod("snapshotTable", classOf[String])
          val snapshotAction = snapshotMethod.invoke(actions, sourceName)
          val asMethod = snapshotAction.getClass.getMethod("as", classOf[String])
          val snapshotWithDest = asMethod.invoke(snapshotAction, destName)
          val executeMethod = snapshotWithDest.getClass.getMethod("execute")
          executeMethod.invoke(snapshotWithDest)

          // Partition columns must have actual values from manifests, not NULL
          checkIcebergNativeScan(s"SELECT * FROM $destName ORDER BY partition_col")
          checkIcebergNativeScan(
            s"SELECT partition_col, data FROM $destName WHERE partition_col < 5 ORDER BY partition_col")

          spark.sql(s"DROP TABLE $destName")
          spark.sql(s"DROP TABLE $sourceName")
        } catch {
          case _: ClassNotFoundException =>
            cancel("Iceberg Actions API not available - requires iceberg-spark-runtime")
        }
      }
    }
  }

  test("projection - column subset, reordering, and duplication") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Create table with multiple columns
        spark.sql("""
          CREATE TABLE test_cat.db.proj_test (
            id INT,
            name STRING,
            value DOUBLE,
            flag BOOLEAN
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.proj_test
          VALUES (1, 'Alice', 10.5, true),
                 (2, 'Bob', 20.3, false),
                 (3, 'Charlie', 30.7, true)
        """)

        // Test 1: Column subset (only 2 of 4 columns)
        checkIcebergNativeScan("SELECT name, value FROM test_cat.db.proj_test ORDER BY id")

        // Test 2: Reordered columns (reverse order)
        checkIcebergNativeScan("SELECT value, name, id FROM test_cat.db.proj_test ORDER BY id")

        // Test 3: Duplicate columns
        checkIcebergNativeScan(
          "SELECT id, name, id AS id2 FROM test_cat.db.proj_test ORDER BY id")

        // Test 4: Single column
        checkIcebergNativeScan("SELECT name FROM test_cat.db.proj_test ORDER BY name")

        // Test 5: Different ordering with subset
        checkIcebergNativeScan("SELECT flag, id FROM test_cat.db.proj_test ORDER BY id")

        // Test 6: Multiple duplicates
        checkIcebergNativeScan(
          "SELECT name, value, name AS name2, value AS value2 FROM test_cat.db.proj_test ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.proj_test")
      }
    }
  }

  test("complex type - array") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.array_test (
            id INT,
            name STRING,
            values ARRAY<INT>
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.array_test
          VALUES (1, 'Alice', array(1, 2, 3)), (2, 'Bob', array(4, 5, 6))
        """)

        checkIcebergNativeScan("SELECT * FROM test_cat.db.array_test ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.array_test")
      }
    }
  }

  test("complex type - map") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.map_test (
            id INT,
            name STRING,
            properties MAP<STRING, INT>
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.map_test
          VALUES (1, 'Alice', map('age', 30, 'score', 95)), (2, 'Bob', map('age', 25, 'score', 87))
        """)

        checkIcebergNativeScan("SELECT * FROM test_cat.db.map_test ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.map_test")
      }
    }
  }

  test("complex type - struct") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.struct_test (
            id INT,
            name STRING,
            address STRUCT<city: STRING, zip: INT>
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.struct_test
          VALUES (1, 'Alice', struct('NYC', 10001)), (2, 'Bob', struct('LA', 90001))
        """)

        checkIcebergNativeScan("SELECT * FROM test_cat.db.struct_test ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.struct_test")
      }
    }
  }

  test("UUID type - native Iceberg UUID column (reproduces type mismatch)") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        import org.apache.iceberg.catalog.TableIdentifier
        import org.apache.iceberg.spark.SparkCatalog
        import org.apache.iceberg.types.Types
        import org.apache.iceberg.{PartitionSpec, Schema}

        // Use Iceberg API to create table with native UUID type
        // (not possible via Spark SQL CREATE TABLE)
        // Get Spark's catalog instance to ensure the table is visible to Spark
        val sparkCatalog = spark.sessionState.catalogManager
          .catalog("test_cat")
          .asInstanceOf[SparkCatalog]

        spark.sql("CREATE NAMESPACE IF NOT EXISTS test_cat.db")

        // UUID is stored as FixedSizeBinary(16) but must be presented as Utf8 to Spark
        val schema = new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "uuid", Types.UUIDType.get()))
        val tableIdent = TableIdentifier.of("db", "uuid_test")
        sparkCatalog.icebergCatalog.createTable(tableIdent, schema, PartitionSpec.unpartitioned())

        spark.sql("""
          INSERT INTO test_cat.db.uuid_test VALUES
          (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
          (2, 'b1ffcd88-8d1a-3de7-aa5c-5aa8ac269a00'),
          (3, 'c2aade77-7e0b-2cf6-99e4-4998bc158b22')
        """)

        checkIcebergNativeScan("SELECT * FROM test_cat.db.uuid_test ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.uuid_test")
      }
    }
  }

  test("verify all Iceberg planning metrics are populated") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    val icebergPlanningMetricNames = Seq(
      "totalPlanningDuration",
      "totalDataManifest",
      "scannedDataManifests",
      "skippedDataManifests",
      "resultDataFiles",
      "skippedDataFiles",
      "totalDataFileSize",
      "totalDeleteManifests",
      "scannedDeleteManifests",
      "skippedDeleteManifests",
      "totalDeleteFileSize",
      "resultDeleteFiles",
      "equalityDeleteFiles",
      "indexedDeleteFiles",
      "positionalDeleteFiles",
      "skippedDeleteFiles")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.metrics_test (
            id INT,
            value DOUBLE
          ) USING iceberg
        """)

        // Create multiple files to ensure non-zero manifest/file counts
        spark
          .range(10000)
          .selectExpr("CAST(id AS INT)", "CAST(id * 1.5 AS DOUBLE) as value")
          .coalesce(1)
          .write
          .format("iceberg")
          .mode("append")
          .saveAsTable("test_cat.db.metrics_test")

        spark
          .range(10001, 20000)
          .selectExpr("CAST(id AS INT)", "CAST(id * 1.5 AS DOUBLE) as value")
          .coalesce(1)
          .write
          .format("iceberg")
          .mode("append")
          .saveAsTable("test_cat.db.metrics_test")

        val df = spark.sql("SELECT * FROM test_cat.db.metrics_test WHERE id < 10000")

        // Must extract metrics before collect() because planning happens at plan creation
        val scanNodes = df.queryExecution.executedPlan
          .collectLeaves()
          .collect { case s: CometIcebergNativeScanExec => s }

        assert(scanNodes.nonEmpty, "Expected at least one CometIcebergNativeScanExec node")

        val metrics = scanNodes.head.metrics

        icebergPlanningMetricNames.foreach { metricName =>
          assert(metrics.contains(metricName), s"metric $metricName was not found")
        }

        // Planning metrics are populated during plan creation, so they're already available
        assert(metrics("totalDataManifest").value > 0, "totalDataManifest should be > 0")
        assert(metrics("resultDataFiles").value > 0, "resultDataFiles should be > 0")
        assert(metrics("totalDataFileSize").value > 0, "totalDataFileSize should be > 0")

        df.collect()

        assert(metrics("output_rows").value == 10000)
        assert(metrics("num_splits").value > 0)
        // ImmutableSQLMetric prevents these from being reset to 0 after execution
        assert(
          metrics("totalDataManifest").value > 0,
          "totalDataManifest should still be > 0 after execution")
        assert(
          metrics("resultDataFiles").value > 0,
          "resultDataFiles should still be > 0 after execution")

        spark.sql("DROP TABLE test_cat.db.metrics_test")
      }
    }
  }

  test("verify manifest pruning metrics") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Partition by category to enable manifest-level pruning
        spark.sql("""
          CREATE TABLE test_cat.db.pruning_test (
            id INT,
            category STRING,
            value DOUBLE
          ) USING iceberg
          PARTITIONED BY (category)
        """)

        // Each category gets its own manifest entry
        spark.sql("""
          INSERT INTO test_cat.db.pruning_test
          SELECT id, 'A' as category, CAST(id * 1.5 AS DOUBLE) as value
          FROM range(1000)
        """)

        spark.sql("""
          INSERT INTO test_cat.db.pruning_test
          SELECT id, 'B' as category, CAST(id * 2.0 AS DOUBLE) as value
          FROM range(1000, 2000)
        """)

        spark.sql("""
          INSERT INTO test_cat.db.pruning_test
          SELECT id, 'C' as category, CAST(id * 2.5 AS DOUBLE) as value
          FROM range(2000, 3000)
        """)

        // Filter should prune B and C partitions at manifest level
        val df = spark.sql("SELECT * FROM test_cat.db.pruning_test WHERE category = 'A'")

        val scanNodes = df.queryExecution.executedPlan
          .collectLeaves()
          .collect { case s: CometIcebergNativeScanExec => s }

        assert(scanNodes.nonEmpty, "Expected at least one CometIcebergNativeScanExec node")

        val metrics = scanNodes.head.metrics

        // Iceberg prunes entire manifests when all files in a manifest don't match the filter
        assert(
          metrics("resultDataFiles").value == 1,
          s"Expected 1 result data file, got ${metrics("resultDataFiles").value}")
        assert(
          metrics("scannedDataManifests").value == 1,
          s"Expected 1 scanned manifest, got ${metrics("scannedDataManifests").value}")
        assert(
          metrics("skippedDataManifests").value == 2,
          s"Expected 2 skipped manifests, got ${metrics("skippedDataManifests").value}")

        // Verify the query actually returns correct results
        val result = df.collect()
        assert(metrics("output_rows").value == 1000)
        assert(result.length == 1000, s"Expected 1000 rows, got ${result.length}")

        spark.sql("DROP TABLE test_cat.db.pruning_test")
      }
    }
  }

  test("verify delete file metrics - MOR table") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Equality delete columns force MOR behavior instead of COW
        spark.sql("""
          CREATE TABLE test_cat.db.delete_metrics (
            id INT,
            category STRING,
            value DOUBLE
          ) USING iceberg
          TBLPROPERTIES (
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read',
            'write.delete.equality-delete-columns' = 'id'
          )
        """)

        spark.sql("""
          INSERT INTO test_cat.db.delete_metrics
          VALUES
            (1, 'A', 10.5), (2, 'B', 20.3), (3, 'A', 30.7),
            (4, 'B', 15.2), (5, 'A', 25.8), (6, 'C', 35.0)
        """)

        spark.sql("DELETE FROM test_cat.db.delete_metrics WHERE id IN (2, 4, 6)")

        val df = spark.sql("SELECT * FROM test_cat.db.delete_metrics")

        val scanNodes = df.queryExecution.executedPlan
          .collectLeaves()
          .collect { case s: CometIcebergNativeScanExec => s }

        assert(scanNodes.nonEmpty, "Expected at least one CometIcebergNativeScanExec node")

        val metrics = scanNodes.head.metrics

        // Iceberg may convert equality deletes to positional deletes internally
        assert(
          metrics("resultDeleteFiles").value > 0,
          s"Expected result delete files > 0, got ${metrics("resultDeleteFiles").value}")
        assert(
          metrics("totalDeleteFileSize").value > 0,
          s"Expected total delete file size > 0, got ${metrics("totalDeleteFileSize").value}")

        val hasDeletes = metrics("positionalDeleteFiles").value > 0 ||
          metrics("equalityDeleteFiles").value > 0
        assert(hasDeletes, "Expected either positional or equality delete files > 0")

        val result = df.collect()
        assert(metrics("output_rows").value == 3)
        assert(result.length == 3, s"Expected 3 rows after deletes, got ${result.length}")

        spark.sql("DROP TABLE test_cat.db.delete_metrics")
      }
    }
  }

  test("verify output_rows metric reflects row-level filtering in scan") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
        // Create relatively small files to get multiple row groups per file
        "spark.sql.files.maxRecordsPerFile" -> "1000") {

        spark.sql("""
          CREATE TABLE test_cat.db.filter_metric_test (
            id INT,
            category STRING,
            value DOUBLE
          ) USING iceberg
        """)

        // Insert 10,000 rows with mixed category values
        // This ensures row groups will have mixed data that can't be completely eliminated
        spark.sql("""
          INSERT INTO test_cat.db.filter_metric_test
          SELECT
            id,
            CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END as category,
            CAST(id * 1.5 AS DOUBLE) as value
          FROM range(10000)
        """)

        // Apply a highly selective filter on id that will filter ~99% of rows
        // This filter requires row-level evaluation because:
        // - Row groups contain ranges of IDs (0-999, 1000-1999, etc.)
        // - The first row group (0-999) cannot be fully eliminated by stats alone
        // - Row-level filtering must apply "id < 100" to filter out rows 100-999
        val df = spark.sql("""
          SELECT * FROM test_cat.db.filter_metric_test
          WHERE id < 100
        """)

        val scanNodes = df.queryExecution.executedPlan
          .collectLeaves()
          .collect { case s: CometIcebergNativeScanExec => s }

        assert(scanNodes.nonEmpty, "Expected at least one CometIcebergNativeScanExec node")

        val metrics = scanNodes.head.metrics

        // Execute the query to populate metrics
        val result = df.collect()

        // The filter "id < 100" should match exactly 100 rows (0-99)
        assert(result.length == 100, s"Expected 100 rows after filter, got ${result.length}")

        // CRITICAL: Verify output_rows metric matches the filtered count
        // If row-level filtering is working, this should be 100
        // If only row group filtering is working, this would be ~1000 (entire first row group)
        assert(
          metrics("output_rows").value == 100,
          s"Expected output_rows=100 (filtered count), got ${metrics("output_rows").value}. " +
            "This indicates row-level filtering may not be working correctly.")

        // Verify the filter actually selected the right rows
        val ids = result.map(_.getInt(0)).sorted
        assert(ids.head == 0, s"Expected first id=0, got ${ids.head}")
        assert(ids.last == 99, s"Expected last id=99, got ${ids.last}")
        assert(ids.forall(_ < 100), "All IDs should be < 100")

        spark.sql("DROP TABLE test_cat.db.filter_metric_test")
      }
    }
  }

  test("schema evolution - read old snapshot after column drop (VERSION AS OF)") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
        // Force LOCAL mode to use iceberg-rust
        "spark.sql.iceberg.read.data-planning-mode" -> "local") {

        // This test verifies that Comet correctly handles reading old snapshots after schema changes,
        // which is a form of backward schema evolution. This corresponds to these Iceberg Java tests:
        // - TestIcebergSourceHadoopTables::testSnapshotReadAfterDropColumn
        // - TestIcebergSourceHadoopTables::testSnapshotReadAfterAddAndDropColumn
        // - TestIcebergSourceHiveTables::testSnapshotReadAfterDropColumn
        // - TestIcebergSourceHiveTables::testSnapshotReadAfterAddAndDropColumn
        // - TestSnapshotSelection::testSnapshotSelectionByTagWithSchemaChange

        // Step 1: Create table with columns (id, data, category)
        spark.sql("""
          CREATE TABLE test_cat.db.schema_evolution_test (
            id INT,
            data STRING,
            category STRING
          ) USING iceberg
        """)

        // Step 2: Write data with all three columns
        spark.sql("""
          INSERT INTO test_cat.db.schema_evolution_test
          VALUES (1, 'x', 'A'), (2, 'y', 'A'), (3, 'z', 'B')
        """)

        // Get snapshot ID before schema change
        val snapshotIdBefore = spark
          .sql("SELECT snapshot_id FROM test_cat.db.schema_evolution_test.snapshots ORDER BY committed_at DESC LIMIT 1")
          .collect()(0)
          .getLong(0)

        // Verify data is correct before schema change
        checkIcebergNativeScan("SELECT * FROM test_cat.db.schema_evolution_test ORDER BY id")

        // Step 3: Drop the "data" column
        spark.sql("ALTER TABLE test_cat.db.schema_evolution_test DROP COLUMN data")

        // Step 4: Read the old snapshot (before column was dropped) using VERSION AS OF
        // This requires using the snapshot's schema, not the current table schema
        checkIcebergNativeScan(
          s"SELECT * FROM test_cat.db.schema_evolution_test VERSION AS OF $snapshotIdBefore ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.schema_evolution_test")
      }
    }
  }

  test("schema evolution - branch read after adding DATE column") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
        "spark.sql.iceberg.read.data-planning-mode" -> "local") {

        // Reproduces: TestSelect::readAndWriteWithBranchAfterSchemaChange
        // Error: "Iceberg scan error: Unexpected => unexpected target column type Date32"
        //
        // Issue: When reading old data from a branch after the table schema evolved to add
        // a DATE column, the schema adapter fails to handle Date32 type conversion.

        // Step 1: Create table with (id, data, float_col)
        spark.sql("""
          CREATE TABLE test_cat.db.date_branch_test (
            id BIGINT,
            data STRING,
            float_col FLOAT
          ) USING iceberg
        """)

        // Step 2: Insert data
        spark.sql("""
          INSERT INTO test_cat.db.date_branch_test
          VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', CAST('NaN' AS FLOAT))
        """)

        // Step 3: Create a branch at this point using Iceberg API
        val catalog = spark.sessionState.catalogManager.catalog("test_cat")
        val ident =
          org.apache.spark.sql.connector.catalog.Identifier.of(Array("db"), "date_branch_test")
        val sparkTable = catalog
          .asInstanceOf[org.apache.iceberg.spark.SparkCatalog]
          .loadTable(ident)
          .asInstanceOf[org.apache.iceberg.spark.source.SparkTable]
        val table = sparkTable.table()
        val snapshotId = table.currentSnapshot().snapshotId()
        table.manageSnapshots().createBranch("test_branch", snapshotId).commit()

        // Step 4: Evolve schema - drop float_col, add date_col
        spark.sql("ALTER TABLE test_cat.db.date_branch_test DROP COLUMN float_col")
        spark.sql("ALTER TABLE test_cat.db.date_branch_test ADD COLUMN date_col DATE")

        // Step 5: Insert more data with the new schema
        spark.sql("""
          INSERT INTO test_cat.db.date_branch_test
          VALUES (4, 'd', DATE '2024-04-04'), (5, 'e', DATE '2024-05-05')
        """)

        // Step 6: Read from the branch using VERSION AS OF
        // This reads old data (id, data, float_col) but applies the current schema (id, data, date_col)
        // The old data files don't have date_col, so it should be NULL
        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.date_branch_test VERSION AS OF 'test_branch' ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.date_branch_test")
      }
    }
  }

  // Complex type filter tests
  test("complex type filter - struct column IS NULL") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.struct_filter_test (
            id INT,
            name STRING,
            address STRUCT<city: STRING, zip: INT>
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.struct_filter_test
          VALUES
            (1, 'Alice', struct('NYC', 10001)),
            (2, 'Bob', struct('LA', 90001)),
            (3, 'Charlie', NULL)
        """)

        // Test filtering on struct IS NULL - this should fall back to Spark
        // (iceberg-rust doesn't support IS NULL on complex type columns yet)
        checkSparkAnswer(
          "SELECT * FROM test_cat.db.struct_filter_test WHERE address IS NULL ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.struct_filter_test")
      }
    }
  }

  test("complex type filter - struct field filter") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.struct_field_filter_test (
            id INT,
            name STRING,
            address STRUCT<city: STRING, zip: INT>
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.struct_field_filter_test
          VALUES
            (1, 'Alice', struct('NYC', 10001)),
            (2, 'Bob', struct('LA', 90001)),
            (3, 'Charlie', struct('NYC', 10002))
        """)

        // Test filtering on struct field - this should use native scan now!
        // iceberg-rust supports nested field filters like address.city = 'NYC'
        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.struct_field_filter_test WHERE address.city = 'NYC' ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.struct_field_filter_test")
      }
    }
  }

  test("complex type filter - entire struct value") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.struct_value_filter_test (
            id INT,
            name STRING,
            address STRUCT<city: STRING, zip: INT>
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.struct_value_filter_test
          VALUES
            (1, 'Alice', named_struct('city', 'NYC', 'zip', 10001)),
            (2, 'Bob', named_struct('city', 'LA', 'zip', 90001)),
            (3, 'Charlie', named_struct('city', 'NYC', 'zip', 10001))
        """)

        // Test filtering on entire struct value - this falls back to Spark
        // (Iceberg Java doesn't push down this type of filter)
        checkSparkAnswer(
          "SELECT * FROM test_cat.db.struct_value_filter_test WHERE address = named_struct('city', 'NYC', 'zip', 10001) ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.struct_value_filter_test")
      }
    }
  }

  test("complex type filter - array column IS NULL") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.array_filter_test (
            id INT,
            name STRING,
            values ARRAY<INT>
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.array_filter_test
          VALUES
            (1, 'Alice', array(1, 2, 3)),
            (2, 'Bob', array(4, 5, 6)),
            (3, 'Charlie', NULL)
        """)

        // Test filtering on array IS NULL - this should fall back to Spark
        // (iceberg-rust doesn't support IS NULL on complex type columns yet)
        checkSparkAnswer(
          "SELECT * FROM test_cat.db.array_filter_test WHERE values IS NULL ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.array_filter_test")
      }
    }
  }

  test("complex type filter - array element filter") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.array_element_filter_test (
            id INT,
            name STRING,
            values ARRAY<INT>
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.array_element_filter_test
          VALUES
            (1, 'Alice', array(1, 2, 3)),
            (2, 'Bob', array(4, 5, 6)),
            (3, 'Charlie', array(1, 7, 8))
        """)

        // Test filtering with array_contains - this should fall back to Spark
        // (Iceberg Java only pushes down NOT NULL, which fails in iceberg-rust)
        checkSparkAnswer(
          "SELECT * FROM test_cat.db.array_element_filter_test WHERE array_contains(values, 1) ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.array_element_filter_test")
      }
    }
  }

  test("complex type filter - entire array value") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.array_value_filter_test (
            id INT,
            name STRING,
            values ARRAY<INT>
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.array_value_filter_test
          VALUES
            (1, 'Alice', array(1, 2, 3)),
            (2, 'Bob', array(4, 5, 6)),
            (3, 'Charlie', array(1, 2, 3))
        """)

        // Test filtering on entire array value - this should fall back to Spark
        // (Iceberg Java only pushes down NOT NULL, which fails in iceberg-rust)
        checkSparkAnswer(
          "SELECT * FROM test_cat.db.array_value_filter_test WHERE values = array(1, 2, 3) ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.array_value_filter_test")
      }
    }
  }

  test("complex type filter - map column IS NULL") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.map_filter_test (
            id INT,
            name STRING,
            properties MAP<STRING, INT>
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.map_filter_test
          VALUES
            (1, 'Alice', map('age', 30, 'score', 95)),
            (2, 'Bob', map('age', 25, 'score', 87)),
            (3, 'Charlie', NULL)
        """)

        // Test filtering on map IS NULL - this should fall back to Spark
        // (iceberg-rust doesn't support IS NULL on complex type columns yet)
        checkSparkAnswer(
          "SELECT * FROM test_cat.db.map_filter_test WHERE properties IS NULL ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.map_filter_test")
      }
    }
  }

  test("complex type filter - map key access filter") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE test_cat.db.map_key_filter_test (
            id INT,
            name STRING,
            properties MAP<STRING, INT>
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO test_cat.db.map_key_filter_test
          VALUES
            (1, 'Alice', map('age', 30, 'score', 95)),
            (2, 'Bob', map('age', 25, 'score', 87)),
            (3, 'Charlie', map('age', 30, 'score', 80))
        """)

        // Test filtering with map key access - this should fall back to Spark
        // (Iceberg Java only pushes down NOT NULL, which fails in iceberg-rust)
        checkSparkAnswer(
          "SELECT * FROM test_cat.db.map_key_filter_test WHERE properties['age'] = 30 ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.map_key_filter_test")
      }
    }
  }

  // Test to reproduce "Field X not found in schema" errors
  // Mimics TestAggregatePushDown.testNaN() where aggregate output schema differs from table schema
  test("partitioned table with aggregates - reproduces Field not found error") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Create table partitioned by id, like TestAggregatePushDown.testNaN
        spark.sql("""
          CREATE TABLE test_cat.db.agg_test (
            id INT,
            data FLOAT
          ) USING iceberg
          PARTITIONED BY (id)
        """)

        spark.sql("""
          INSERT INTO test_cat.db.agg_test VALUES
          (1, CAST('NaN' AS FLOAT)),
          (1, CAST('NaN' AS FLOAT)),
          (2, 2.0),
          (2, CAST('NaN' AS FLOAT)),
          (3, CAST('NaN' AS FLOAT)),
          (3, 1.0)
        """)

        // This aggregate query's output schema is completely different from table schema
        // When iceberg-rust tries to look up partition field 'id' (field 1 in table schema),
        // it needs to find it in the full table schema, not the aggregate output schema
        checkIcebergNativeScan(
          "SELECT count(*), max(data), min(data), count(data) FROM test_cat.db.agg_test")

        spark.sql("DROP TABLE test_cat.db.agg_test")
      }
    }
  }

  test("MOR partitioned table with timestamp_ntz - reproduces NULL partition issue") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Create partitioned table like TestRewritePositionDeleteFiles.testTimestampNtz
        spark.sql("""
          CREATE TABLE test_cat.db.timestamp_ntz_partition_test (
            id LONG,
            ts TIMESTAMP_NTZ,
            c1 STRING,
            c2 STRING
          ) USING iceberg
          PARTITIONED BY (ts)
          TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
          )
        """)

        // Insert data into multiple partitions
        spark.sql("""
          INSERT INTO test_cat.db.timestamp_ntz_partition_test
          VALUES
            (1, TIMESTAMP_NTZ '2023-01-01 15:30:00', 'a', 'b'),
            (2, TIMESTAMP_NTZ '2023-01-02 15:30:00', 'c', 'd'),
            (3, TIMESTAMP_NTZ '2023-01-03 15:30:00', 'e', 'f')
        """)

        // Delete some rows to create position delete files
        spark.sql("DELETE FROM test_cat.db.timestamp_ntz_partition_test WHERE id = 2")

        // Query should work with NULL partition handling
        checkIcebergNativeScan(
          "SELECT * FROM test_cat.db.timestamp_ntz_partition_test ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.timestamp_ntz_partition_test")
      }
    }
  }

  test("MOR partitioned table with decimal - reproduces NULL partition issue") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Create partitioned table like TestRewritePositionDeleteFiles.testDecimalPartition
        spark.sql("""
          CREATE TABLE test_cat.db.decimal_partition_test (
            id LONG,
            dec DECIMAL(18, 10),
            c1 STRING,
            c2 STRING
          ) USING iceberg
          PARTITIONED BY (dec)
          TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
          )
        """)

        // Insert data into multiple partitions
        spark.sql("""
          INSERT INTO test_cat.db.decimal_partition_test
          VALUES
            (1, 1.0, 'a', 'b'),
            (2, 2.0, 'c', 'd'),
            (3, 3.0, 'e', 'f')
        """)

        // Delete some rows to create position delete files
        spark.sql("DELETE FROM test_cat.db.decimal_partition_test WHERE id = 2")

        // Query should work with NULL partition handling
        checkIcebergNativeScan("SELECT * FROM test_cat.db.decimal_partition_test ORDER BY id")

        spark.sql("DROP TABLE test_cat.db.decimal_partition_test")
      }
    }
  }

  test("REST catalog with native Iceberg scan") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withRESTCatalog { (restUri, _, warehouseDir) =>
      withSQLConf(
        "spark.sql.catalog.rest_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.rest_cat.catalog-impl" -> "org.apache.iceberg.rest.RESTCatalog",
        "spark.sql.catalog.rest_cat.uri" -> restUri,
        "spark.sql.catalog.rest_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
        CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {

        // Create namespace first (REST catalog requires explicit namespace creation)
        spark.sql("CREATE NAMESPACE rest_cat.db")

        // Create a table via REST catalog
        spark.sql("""
          CREATE TABLE rest_cat.db.test_table (
            id INT,
            name STRING,
            value DOUBLE
          ) USING iceberg
        """)

        spark.sql("""
          INSERT INTO rest_cat.db.test_table
          VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.3), (3, 'Charlie', 30.7)
        """)

        checkIcebergNativeScan("SELECT * FROM rest_cat.db.test_table ORDER BY id")

        spark.sql("DROP TABLE rest_cat.db.test_table")
        spark.sql("DROP NAMESPACE rest_cat.db")
      }
    }
  }

  // Helper to create temp directory
  def withTempIcebergDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory("comet-iceberg-test").toFile
    try {
      f(dir)
    } finally {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }
      deleteRecursively(dir)
    }
  }
}
