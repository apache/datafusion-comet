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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.CometListenerBusUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{InSubqueryExec, ReusedSubqueryExec, SparkPlan, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, BroadcastQueryStageExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, TimestampType}

import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark42Plus}
import org.apache.comet.iceberg.RESTCatalogHelper
import org.apache.comet.testing.{FuzzDataGenerator, SchemaGenOptions}

/**
 * Test suite for native Iceberg scan using FileScanTasks and iceberg-rust.
 *
 * Note: Requires Iceberg dependencies to be added to pom.xml
 */
class CometIcebergNativeSuite
    extends CometTestBase
    with RESTCatalogHelper
    with AdaptiveSparkPlanHelper {

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

  test("bytes_scanned includes delete file I/O") {
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
          CREATE TABLE test_cat.db.delete_bytes_test (
            id INT,
            value DOUBLE
          ) USING iceberg
          TBLPROPERTIES (
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
          )
        """)

        spark
          .range(1000)
          .selectExpr("CAST(id AS INT)", "CAST(id * 1.5 AS DOUBLE) as value")
          .coalesce(1)
          .write
          .format("iceberg")
          .mode("append")
          .saveAsTable("test_cat.db.delete_bytes_test")

        // Scan before deletes: data files only
        val dfBefore = spark.sql("SELECT * FROM test_cat.db.delete_bytes_test")
        val scanBefore = dfBefore.queryExecution.executedPlan
          .collectLeaves()
          .collect { case s: CometIcebergNativeScanExec => s }
          .head
        dfBefore.collect()
        val bytesBefore = scanBefore.metrics("bytes_scanned").value
        assert(bytesBefore > 0, s"bytes_scanned before deletes should be > 0, got $bytesBefore")

        // Create position delete files
        spark.sql("DELETE FROM test_cat.db.delete_bytes_test WHERE id < 100")

        // Scan after deletes: data files + delete files
        val dfAfter = spark.sql("SELECT * FROM test_cat.db.delete_bytes_test")
        val scanAfter = dfAfter.queryExecution.executedPlan
          .collectLeaves()
          .collect { case s: CometIcebergNativeScanExec => s }
          .head
        dfAfter.collect()
        val bytesAfter = scanAfter.metrics("bytes_scanned").value

        assert(
          bytesAfter > bytesBefore,
          s"bytes_scanned should increase after deletes: before=$bytesBefore, after=$bytesAfter")

        spark.sql("DROP TABLE test_cat.db.delete_bytes_test")
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
        assert(
          metrics("bytes_scanned").value > 0,
          "bytes_scanned should be > 0 after reading data files")
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

  // Regression test for https://github.com/apache/datafusion-comet/issues/3856
  // Fixed in https://github.com/apache/iceberg-rust/pull/2301
  test("migration - INT96 timestamp") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}
        import org.apache.spark.sql.functions.monotonically_increasing_id
        import org.apache.spark.sql.types._

        val dataPath = s"${warehouseDir.getAbsolutePath}/int96_data"
        val numRows = 50
        val r = new scala.util.Random(42)

        // Exercise INT96 coercion in flat columns, structs, arrays, and maps
        val fuzzSchema = StructType(
          Seq(
            StructField("ts", TimestampType, nullable = true),
            StructField("value", DoubleType, nullable = true),
            StructField(
              "ts_struct",
              StructType(
                Seq(
                  StructField("inner_ts", TimestampType, nullable = true),
                  StructField("inner_val", DoubleType, nullable = true))),
              nullable = true),
            StructField(
              "ts_array",
              ArrayType(TimestampType, containsNull = true),
              nullable = true),
            StructField("ts_map", MapType(IntegerType, TimestampType), nullable = true)))

        // Default FuzzDataGenerator baseDate is year 3333, outside the i64 nanosecond
        // range (~1677-2262). This triggers the INT96 overflow bug if coercion is missing.
        val dataGenOptions = DataGenOptions(allowNull = false)
        val fuzzDf =
          FuzzDataGenerator.generateDataFrame(r, spark, fuzzSchema, numRows, dataGenOptions)

        val df = fuzzDf.withColumn("id", monotonically_increasing_id())

        // Write Parquet with INT96 timestamps
        withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "INT96") {
          df.write.mode("overwrite").parquet(dataPath)
        }

        // Verify all timestamp columns in the Parquet file use INT96
        val parquetFiles = new java.io.File(dataPath)
          .listFiles()
          .filter(f => f.getName.endsWith(".parquet"))
        assert(parquetFiles.nonEmpty, "Expected at least one Parquet file")

        val parquetFile = parquetFiles.head
        val reader = org.apache.parquet.hadoop.ParquetFileReader.open(
          org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
            new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath),
            spark.sessionState.newHadoopConf()))
        try {
          val parquetSchema = reader.getFooter.getFileMetaData.getSchema
          val int96Columns = parquetSchema.getColumns.asScala
            .filter(_.getPrimitiveType.getPrimitiveTypeName ==
              org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96)
            .map(_.getPath.mkString("."))
          // Expect INT96 for: ts, ts_struct.inner_ts, ts_array.list.element, ts_map.value
          assert(
            int96Columns.size >= 4,
            s"Expected at least 4 INT96 columns but found ${int96Columns.size}: ${int96Columns.mkString(", ")}")
        } finally {
          reader.close()
        }

        // Create an unpartitioned Iceberg table and import the Parquet files
        spark.sql("CREATE NAMESPACE IF NOT EXISTS test_cat.db")
        spark.sql("""
          CREATE TABLE test_cat.db.int96_test (
            ts TIMESTAMP,
            value DOUBLE,
            ts_struct STRUCT<inner_ts: TIMESTAMP, inner_val: DOUBLE>,
            ts_array ARRAY<TIMESTAMP>,
            ts_map MAP<INT, TIMESTAMP>,
            id BIGINT
          ) USING iceberg
        """)

        try {
          val tableUtilClass = Class.forName("org.apache.iceberg.spark.SparkTableUtil")
          val sparkCatalog = spark.sessionState.catalogManager
            .catalog("test_cat")
            .asInstanceOf[org.apache.iceberg.spark.SparkCatalog]
          val ident =
            org.apache.spark.sql.connector.catalog.Identifier.of(Array("db"), "int96_test")
          val sparkTable = sparkCatalog
            .loadTable(ident)
            .asInstanceOf[org.apache.iceberg.spark.source.SparkTable]
          val table = sparkTable.table()

          val stagingDir = s"${warehouseDir.getAbsolutePath}/staging"

          spark.sql(s"""CREATE TABLE parquet_temp USING parquet LOCATION '$dataPath'""")
          val sourceIdent = new org.apache.spark.sql.catalyst.TableIdentifier("parquet_temp")

          val importMethod = tableUtilClass.getMethod(
            "importSparkTable",
            classOf[org.apache.spark.sql.SparkSession],
            classOf[org.apache.spark.sql.catalyst.TableIdentifier],
            classOf[org.apache.iceberg.Table],
            classOf[String])
          importMethod.invoke(null, spark, sourceIdent, table, stagingDir)

          val distinctCount = spark
            .sql("SELECT COUNT(DISTINCT id) FROM test_cat.db.int96_test")
            .collect()(0)
            .getLong(0)
          assert(
            distinctCount == numRows,
            s"Expected $numRows distinct IDs but got $distinctCount")

          // Spark's Iceberg reader returns null for INT96 timestamps inside structs,
          // so we can't use checkIcebergNativeScan (which compares against Spark) for
          // ts_struct. Instead, compare Comet's read against the raw Parquet source.
          checkIcebergNativeScan(
            "SELECT id, ts, value, ts_array, ts_map FROM test_cat.db.int96_test ORDER BY id")
          checkIcebergNativeScan("SELECT id, ts FROM test_cat.db.int96_test ORDER BY id")
          checkIcebergNativeScan("SELECT id, ts_array FROM test_cat.db.int96_test ORDER BY id")
          checkIcebergNativeScan("SELECT id, ts_map FROM test_cat.db.int96_test ORDER BY id")

          // Validate ts_struct against raw Parquet since Spark's Iceberg reader can't read it
          val icebergStructDf = spark
            .sql("SELECT id, ts_struct FROM test_cat.db.int96_test ORDER BY id")
            .collect()
          val parquetStructDf = spark.read
            .parquet(dataPath)
            .select("id", "ts_struct")
            .orderBy("id")
            .collect()
          assert(
            icebergStructDf.sameElements(parquetStructDf),
            "ts_struct mismatch between Comet Iceberg read and raw Parquet")

          spark.sql("DROP TABLE test_cat.db.int96_test")
          spark.sql("DROP TABLE parquet_temp")
        } catch {
          case _: ClassNotFoundException =>
            cancel("SparkTableUtil not available")
        }
      }
    }
  }

  test("REST catalog with native Iceberg scan") {
    assume(!isSpark42Plus, "https://github.com/apache/datafusion-comet/issues/4142")
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

  test("runtime filtering - multiple DPP filters on two partition columns") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.runtime_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.runtime_cat.type" -> "hadoop",
        "spark.sql.catalog.runtime_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Create table partitioned by TWO columns: (data, bucket(8, id))
        // This mimics Iceberg's testMultipleRuntimeFilters
        spark.sql("""
          CREATE TABLE runtime_cat.db.multi_dpp_fact (
            id BIGINT,
            data STRING,
            date DATE,
            ts TIMESTAMP
          ) USING iceberg
          PARTITIONED BY (data, bucket(8, id))
        """)

        // Insert data - 99 rows with varying data and id values
        val df = spark
          .range(1, 100)
          .selectExpr(
            "id",
            "CAST(DATE_ADD(DATE '1970-01-01', CAST(id % 4 AS INT)) AS STRING) as data",
            "DATE_ADD(DATE '1970-01-01', CAST(id % 4 AS INT)) as date",
            "CAST(DATE_ADD(DATE '1970-01-01', CAST(id % 4 AS INT)) AS TIMESTAMP) as ts")
        df.coalesce(1)
          .write
          .format("iceberg")
          .option("fanout-enabled", "true")
          .mode("append")
          .saveAsTable("runtime_cat.db.multi_dpp_fact")

        // Create dimension table with specific id=1, data='1970-01-02'
        spark
          .createDataFrame(Seq((1L, java.sql.Date.valueOf("1970-01-02"), "1970-01-02")))
          .toDF("id", "date", "data")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("dim")

        // Join on BOTH partition columns - this creates TWO DPP filters
        val query =
          """SELECT /*+ BROADCAST(d) */ f.*
            |FROM runtime_cat.db.multi_dpp_fact f
            |JOIN dim d ON f.id = d.id AND f.data = d.data
            |WHERE d.date = DATE '1970-01-02'""".stripMargin

        // Verify plan has 2 dynamic pruning expressions
        val df2 = spark.sql(query)
        val planStr = df2.queryExecution.executedPlan.toString
        // Count "dynamicpruningexpression(" to avoid matching "dynamicpruning#N" references
        val dppCount = "dynamicpruningexpression\\(".r.findAllIn(planStr).length
        assert(dppCount == 2, s"Expected 2 DPP expressions but found $dppCount in:\n$planStr")

        // Verify native Iceberg scan is used and DPP actually pruned partitions
        val (_, cometPlan) = checkSparkAnswer(query)
        val icebergScans = collectIcebergNativeScans(cometPlan)
        assert(
          icebergScans.nonEmpty,
          s"Expected CometIcebergNativeScanExec but found none. Plan:\n$cometPlan")
        // With 4 data values x 8 buckets = up to 32 partitions total
        // DPP on (data='1970-01-02', bucket(id=1)) should prune to 1
        val numPartitions = icebergScans.head.numPartitions
        assert(numPartitions == 1, s"Expected DPP to prune to 1 partition but got $numPartitions")

        // Verify AQE DPP used CometSubqueryBroadcastExec with broadcast reuse
        if (isSpark35Plus) {
          val subqueries = collectIcebergDPPSubqueries(cometPlan)
          assert(subqueries.size == 2, s"Expected 2 DPP subqueries but got ${subqueries.size}")
          subqueries.foreach { sub =>
            assert(
              sub.isInstanceOf[CometSubqueryBroadcastExec],
              s"Expected CometSubqueryBroadcastExec but got ${sub.getClass.getSimpleName}")
          }
        }

        spark.sql("DROP TABLE runtime_cat.db.multi_dpp_fact")
      }
    }
  }

  test("runtime filtering - join with dynamic partition pruning") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.runtime_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.runtime_cat.type" -> "hadoop",
        "spark.sql.catalog.runtime_cat.warehouse" -> warehouseDir.getAbsolutePath,
        // Prevent fact table from being broadcast (force dimension to be broadcast)
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Create partitioned Iceberg table (fact table) with 3 partitions
        // Add enough data to prevent broadcast
        spark.sql("""
          CREATE TABLE runtime_cat.db.fact_table (
            id BIGINT,
            data STRING,
            date DATE
          ) USING iceberg
          PARTITIONED BY (date)
        """)

        // Insert data across multiple partitions
        spark.sql("""
          INSERT INTO runtime_cat.db.fact_table VALUES
          (1, 'a', DATE '1970-01-01'),
          (2, 'b', DATE '1970-01-02'),
          (3, 'c', DATE '1970-01-02'),
          (4, 'd', DATE '1970-01-03'),
          (5, 'e', DATE '1970-01-01'),
          (6, 'f', DATE '1970-01-02'),
          (7, 'g', DATE '1970-01-03'),
          (8, 'h', DATE '1970-01-01')
        """)

        // Create dimension table (Parquet) in temp directory
        spark
          .createDataFrame(Seq((1L, java.sql.Date.valueOf("1970-01-02"))))
          .toDF("id", "date")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("dim")

        // This join should trigger dynamic partition pruning
        // Use BROADCAST hint to force dimension table to be broadcast
        val query =
          """SELECT /*+ BROADCAST(d) */ f.* FROM runtime_cat.db.fact_table f
            |JOIN dim d ON f.date = d.date AND d.id = 1
            |ORDER BY f.id""".stripMargin

        // Verify the initial plan contains dynamic pruning expression
        val df = spark.sql(query)
        val initialPlan = df.queryExecution.executedPlan
        val planStr = initialPlan.toString
        assert(
          planStr.contains("dynamicpruning"),
          s"Expected dynamic pruning in plan but got:\n$planStr")

        // Should now use native Iceberg scan with DPP
        checkIcebergNativeScan(query)

        // Verify DPP actually pruned partitions (should only scan 1 of 3 partitions)
        val (_, cometPlan) = checkSparkAnswer(query)
        val icebergScans = collectIcebergNativeScans(cometPlan)
        assert(
          icebergScans.nonEmpty,
          s"Expected CometIcebergNativeScanExec but found none. Plan:\n$cometPlan")
        val numPartitions = icebergScans.head.numPartitions
        assert(numPartitions == 1, s"Expected DPP to prune to 1 partition but got $numPartitions")

        // Verify AQE DPP used CometSubqueryBroadcastExec with broadcast reuse
        if (isSpark35Plus) {
          val subqueries = collectIcebergDPPSubqueries(cometPlan)
          assert(subqueries.nonEmpty, s"Expected DPP subqueries in plan:\n$cometPlan")
          subqueries.foreach { sub =>
            assert(
              sub.isInstanceOf[CometSubqueryBroadcastExec],
              s"Expected CometSubqueryBroadcastExec but got ${sub.getClass.getSimpleName}")
            val csb = sub.asInstanceOf[CometSubqueryBroadcastExec]
            assert(
              csb.child.isInstanceOf[AdaptiveSparkPlanExec],
              "Expected AdaptiveSparkPlanExec child but got " +
                s"${csb.child.getClass.getSimpleName}")
            val aspe = csb.child.asInstanceOf[AdaptiveSparkPlanExec]
            val hasReuse = collect(aspe) { case r: ReusedExchangeExec => r }.nonEmpty ||
              collect(aspe) { case b: BroadcastQueryStageExec => b }.nonEmpty
            assert(
              hasReuse,
              "DPP subquery's ASPE should contain ReusedExchangeExec or " +
                s"BroadcastQueryStageExec for broadcast reuse:\n${cometPlan.treeString}")
          }
        }

        spark.sql("DROP TABLE runtime_cat.db.fact_table")
      }
    }
  }

  // Regression test for a user reported issue
  test("double partitioning with range filter on top-level partition") {
    assume(icebergAvailable, "Iceberg not available")

    // Generate Iceberg table without Comet enabled
    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.files.maxRecordsPerFile" -> "50") {

        // timestamp + geohash with multi-column partitioning
        spark.sql("""
          CREATE TABLE test_cat.db.geolocation_trips (
            outputTimestamp TIMESTAMP,
            geohash7 STRING,
            tripId STRING
          ) USING iceberg
          PARTITIONED BY (hours(outputTimestamp), truncate(3, geohash7))
          TBLPROPERTIES (
            'format-version' = '2',
            'write.distribution-mode' = 'range',
            'write.target-file-size-bytes' = '1073741824'
          )
        """)
        val schema = FuzzDataGenerator.generateSchema(
          SchemaGenOptions(primitiveTypes = Seq(TimestampType, StringType, StringType)))

        val random = new scala.util.Random(42)
        // Set baseDate to match our filter range (around 2024-01-01)
        val options = testing.DataGenOptions(
          allowNull = false,
          baseDate = 1704067200000L
        ) // 2024-01-01 00:00:00

        val df = FuzzDataGenerator
          .generateDataFrame(random, spark, schema, 1000, options)
          .toDF("outputTimestamp", "geohash7", "tripId")

        df.writeTo("test_cat.db.geolocation_trips").append()
      }

      // Query using Comet native Iceberg scan
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Filter for a range that does not align with hour boundaries
        // Partitioning is hours(outputTimestamp), so filter in middle of hours forces residual filter
        val startMs = 1704067200000L + 30 * 60 * 1000L // 2024-01-01 01:30:00 (30 min into hour)
        val endMs = 1704078000000L - 15 * 60 * 1000L // 2024-01-01 03:45:00 (15 min before hour)

        checkIcebergNativeScan(s"""
          SELECT COUNT(DISTINCT(tripId)) FROM test_cat.db.geolocation_trips
          WHERE timestamp_millis($startMs) <= outputTimestamp
            AND outputTimestamp < timestamp_millis($endMs)
        """)

        spark.sql("DROP TABLE test_cat.db.geolocation_trips")
      }
    }
  }

  // Regression test for https://github.com/apache/datafusion-comet/issues/3860
  // Fixed in https://github.com/apache/iceberg-rust/pull/2307
  test("filter with nested types in migrated table") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        val dataPath = s"${warehouseDir.getAbsolutePath}/nested_data"

        // Write Parquet WITHOUT Iceberg (simulates pre-migration data)
        // id is last so its leaf index is after all nested type leaves
        spark
          .sql("""
          SELECT
            named_struct('age', id * 10, 'score', id * 1.5) AS info,
            array(id, id + 1) AS tags,
            map('key', id) AS props,
            id
          FROM range(10)
        """)
          .write
          .parquet(dataPath)

        spark.sql("CREATE NAMESPACE IF NOT EXISTS test_cat.db")
        spark.sql("""
          CREATE TABLE test_cat.db.nested_migrate (
            info STRUCT<age: BIGINT, score: DOUBLE>,
            tags ARRAY<BIGINT>,
            props MAP<STRING, BIGINT>,
            id BIGINT
          ) USING iceberg
        """)

        try {
          val tableUtilClass = Class.forName("org.apache.iceberg.spark.SparkTableUtil")
          val sparkCatalog = spark.sessionState.catalogManager
            .catalog("test_cat")
            .asInstanceOf[org.apache.iceberg.spark.SparkCatalog]
          val ident =
            org.apache.spark.sql.connector.catalog.Identifier.of(Array("db"), "nested_migrate")
          val sparkTable = sparkCatalog
            .loadTable(ident)
            .asInstanceOf[org.apache.iceberg.spark.source.SparkTable]
          val table = sparkTable.table()

          val stagingDir = s"${warehouseDir.getAbsolutePath}/staging"
          spark.sql(s"""CREATE TABLE parquet_temp USING parquet LOCATION '$dataPath'""")
          val sourceIdent = new org.apache.spark.sql.catalyst.TableIdentifier("parquet_temp")

          val importMethod = tableUtilClass.getMethod(
            "importSparkTable",
            classOf[org.apache.spark.sql.SparkSession],
            classOf[org.apache.spark.sql.catalyst.TableIdentifier],
            classOf[org.apache.iceberg.Table],
            classOf[String])
          importMethod.invoke(null, spark, sourceIdent, table, stagingDir)

          // Select only flat columns to avoid Spark's Iceberg reader returning
          // null for struct fields in migrated tables (separate Spark bug)
          checkIcebergNativeScan("SELECT id FROM test_cat.db.nested_migrate ORDER BY id")

          // Filter on root column with nested types in migrated table:
          // Parquet files lack Iceberg field IDs, so iceberg-rust falls back to
          // name mapping where column_map resolution was broken for nested types
          checkIcebergNativeScan(
            "SELECT id FROM test_cat.db.nested_migrate WHERE id > 5 ORDER BY id")

          spark.sql("DROP TABLE test_cat.db.nested_migrate")
          spark.sql("DROP TABLE parquet_temp")
        } catch {
          case _: ClassNotFoundException =>
            cancel("SparkTableUtil not available")
        }
      }
    }
  }

  test("task-level inputMetrics.bytesRead is populated for Iceberg native scan") {
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
          CREATE TABLE test_cat.db.task_metrics_test (
            id INT,
            value DOUBLE
          ) USING iceberg
        """)

        spark
          .range(10000)
          .selectExpr("CAST(id AS INT)", "CAST(id * 1.5 AS DOUBLE) as value")
          .repartition(5)
          .write
          .format("iceberg")
          .mode("append")
          .saveAsTable("test_cat.db.task_metrics_test")

        val bytesReadValues = mutable.ArrayBuffer.empty[Long]
        val recordsReadValues = mutable.ArrayBuffer.empty[Long]

        val listener = new SparkListener {
          override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
            val im = taskEnd.taskMetrics.inputMetrics
            if (im.bytesRead > 0) {
              bytesReadValues.synchronized {
                bytesReadValues += im.bytesRead
                recordsReadValues += im.recordsRead
              }
            }
          }
        }
        spark.sparkContext.addSparkListener(listener)

        try {
          val query = "SELECT * FROM test_cat.db.task_metrics_test"

          // Same drain-run-drain pattern as CometTaskMetricsSuite's shuffle test
          CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)

          // Baseline: iceberg-Java scan (Comet native disabled)
          withSQLConf(CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "false") {
            bytesReadValues.clear()
            recordsReadValues.clear()
            spark.sql(query).collect()
            CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
          }
          val sparkBytes = bytesReadValues.sum
          val sparkRecords = recordsReadValues.sum

          // Comet native Iceberg scan
          bytesReadValues.clear()
          recordsReadValues.clear()
          val df = spark.sql(query)

          val scanNodes = df.queryExecution.executedPlan
            .collectLeaves()
            .collect { case s: CometIcebergNativeScanExec => s }
          assert(scanNodes.nonEmpty, "Expected CometIcebergNativeScanExec in plan")

          df.collect()
          CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)

          val cometBytes = bytesReadValues.sum
          val cometRecords = recordsReadValues.sum

          // Both paths should report metrics
          assert(sparkBytes > 0, s"Spark bytesRead should be > 0, got $sparkBytes")
          assert(sparkRecords > 0, s"Spark recordsRead should be > 0, got $sparkRecords")
          assert(cometBytes > 0, s"Comet bytesRead should be > 0, got $cometBytes")
          assert(cometRecords > 0, s"Comet recordsRead should be > 0, got $cometRecords")

          assert(
            cometRecords == sparkRecords,
            s"recordsRead mismatch: comet=$cometRecords, spark=$sparkRecords")

          // SQL-level metric should match task-level metric
          val sqlBytes = scanNodes.head.metrics("bytes_scanned").value
          assert(
            sqlBytes == cometBytes,
            s"SQL bytes_scanned ($sqlBytes) should match task bytesRead ($cometBytes)")
        } finally {
          spark.sparkContext.removeSparkListener(listener)
          spark.sql("DROP TABLE test_cat.db.task_metrics_test")
        }
      }
    }
  }

  // ---- AQE DPP broadcast reuse tests ----

  private def collectIcebergDPPSubqueries(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) { case scan: CometIcebergNativeScanExec => scan }
      .flatMap(_.runtimeFilters)
      .collect { case DynamicPruningExpression(e: InSubqueryExec) =>
        e.plan
      }
  }

  /** Extracts the broadcast-side child from a CometBroadcastHashJoinExec. */
  private def broadcastChild(join: CometBroadcastHashJoinExec): SparkPlan = {
    join.buildSide match {
      case BuildLeft => join.left
      case BuildRight => join.right
    }
  }

  test("AQE DPP - CometSubqueryBroadcastExec replaces SubqueryAdaptiveBroadcastExec") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.dpp_reuse_fact (
            id BIGINT, data STRING, date DATE
          ) USING iceberg PARTITIONED BY (date)
        """)
        spark.sql("""
          INSERT INTO aqe_cat.db.dpp_reuse_fact VALUES
          (1, 'a', DATE '1970-01-01'), (2, 'b', DATE '1970-01-02'),
          (3, 'c', DATE '1970-01-02'), (4, 'd', DATE '1970-01-03')
        """)

        spark
          .createDataFrame(Seq((1L, java.sql.Date.valueOf("1970-01-02"))))
          .toDF("id", "date")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("aqe_dim")

        val query =
          """SELECT /*+ BROADCAST(d) */ f.* FROM aqe_cat.db.dpp_reuse_fact f
            |JOIN aqe_dim d ON f.date = d.date AND d.id = 1""".stripMargin
        val (_, cometPlan) = checkSparkAnswer(query)

        assertNoLeftoverCSAB(cometPlan)

        if (isSpark35Plus) {
          // Verify CometSubqueryBroadcastExec replaced SubqueryAdaptiveBroadcastExec
          val subqueries = collectIcebergDPPSubqueries(cometPlan)
          assert(subqueries.nonEmpty, s"Expected DPP subqueries in plan:\n$cometPlan")
          subqueries.foreach { sub =>
            assert(
              sub.isInstanceOf[CometSubqueryBroadcastExec],
              s"Expected CometSubqueryBroadcastExec but got ${sub.getClass.getSimpleName}")
          }

          // Verify broadcast reuse: subquery child should be an ASPE that contains a
          // BroadcastQueryStageExec (via AQE stageCache, possibly wrapped in
          // ReusedExchangeExec). Reference equality (eq) on the join's BQS does not
          // hold because convertSAB wraps a fresh exchange in a new ASPE; the actual
          // reuse manifests as ReusedExchangeExec inside the ASPE's final plan.
          subqueries.foreach {
            case csb: CometSubqueryBroadcastExec =>
              assert(
                csb.child.isInstanceOf[AdaptiveSparkPlanExec],
                "Expected AdaptiveSparkPlanExec child but got " +
                  s"${csb.child.getClass.getSimpleName}")
              val aspe = csb.child.asInstanceOf[AdaptiveSparkPlanExec]
              val hasReuse = collect(aspe) { case r: ReusedExchangeExec => r }.nonEmpty ||
                collect(aspe) { case b: BroadcastQueryStageExec => b }.nonEmpty
              assert(
                hasReuse,
                "DPP subquery's ASPE should contain ReusedExchangeExec or " +
                  s"BroadcastQueryStageExec for broadcast reuse:\n${cometPlan.treeString}")
            case _ =>
          }

          // Verify correct results and partition pruning
          val icebergScans = collectIcebergNativeScans(cometPlan)
          assert(icebergScans.nonEmpty, "Expected CometIcebergNativeScanExec in plan")
          assert(
            icebergScans.head.numPartitions == 1,
            s"Expected DPP to prune to 1 partition but got ${icebergScans.head.numPartitions}")
        }

        spark.sql("DROP TABLE aqe_cat.db.dpp_reuse_fact")
      }
    }
  }

  test("AQE DPP - multiple DPP filters reuse same broadcast") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.multi_dpp_reuse (
            id BIGINT, data STRING, date DATE, ts TIMESTAMP
          ) USING iceberg PARTITIONED BY (data, bucket(8, id))
        """)
        val df = spark
          .range(1, 100)
          .selectExpr(
            "id",
            "CAST(DATE_ADD(DATE '1970-01-01', CAST(id % 4 AS INT)) AS STRING) as data",
            "DATE_ADD(DATE '1970-01-01', CAST(id % 4 AS INT)) as date",
            "CAST(DATE_ADD(DATE '1970-01-01', CAST(id % 4 AS INT)) AS TIMESTAMP) as ts")
        df.coalesce(1)
          .write
          .format("iceberg")
          .option("fanout-enabled", "true")
          .mode("append")
          .saveAsTable("aqe_cat.db.multi_dpp_reuse")

        spark
          .createDataFrame(Seq((1L, java.sql.Date.valueOf("1970-01-02"), "1970-01-02")))
          .toDF("id", "date", "data")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("aqe_multi_dim")

        val query =
          """SELECT /*+ BROADCAST(d) */ f.*
            |FROM aqe_cat.db.multi_dpp_reuse f
            |JOIN aqe_multi_dim d ON f.id = d.id AND f.data = d.data
            |WHERE d.date = DATE '1970-01-02'""".stripMargin
        val (_, cometPlan) = checkSparkAnswer(query)

        assertNoLeftoverCSAB(cometPlan)

        if (isSpark35Plus) {
          // Both DPP filters should use CometSubqueryBroadcastExec
          val subqueries = collectIcebergDPPSubqueries(cometPlan)
          assert(subqueries.size == 2, s"Expected 2 DPP subqueries but got ${subqueries.size}")
          subqueries.foreach { sub =>
            assert(
              sub.isInstanceOf[CometSubqueryBroadcastExec],
              s"Expected CometSubqueryBroadcastExec but got ${sub.getClass.getSimpleName}")
          }

          // Both should reuse the dim broadcast. Each subquery child is an ASPE that
          // contains a BroadcastQueryStageExec (or ReusedExchangeExec) - AQE stageCache
          // dedupes via canonical form rather than Java reference identity.
          subqueries.foreach {
            case csb: CometSubqueryBroadcastExec =>
              assert(
                csb.child.isInstanceOf[AdaptiveSparkPlanExec],
                "Expected AdaptiveSparkPlanExec child but got " +
                  s"${csb.child.getClass.getSimpleName}")
              val aspe = csb.child.asInstanceOf[AdaptiveSparkPlanExec]
              val hasReuse = collect(aspe) { case r: ReusedExchangeExec => r }.nonEmpty ||
                collect(aspe) { case b: BroadcastQueryStageExec => b }.nonEmpty
              assert(
                hasReuse,
                "DPP subquery's ASPE should contain ReusedExchangeExec or " +
                  s"BroadcastQueryStageExec:\n${cometPlan.treeString}")
            case _ =>
          }
        }

        spark.sql("DROP TABLE aqe_cat.db.multi_dpp_reuse")
      }
    }
  }

  test("AQE DPP - two separate broadcast joins disambiguated by buildKeys") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dim1Dir = new File(warehouseDir, "dim1_parquet")
      val dim2Dir = new File(warehouseDir, "dim2_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Fact table partitioned by TWO columns: date and category
        spark.sql("""
          CREATE TABLE aqe_cat.db.two_join_fact (
            id BIGINT, date DATE, category STRING, value INT
          ) USING iceberg PARTITIONED BY (date, category)
        """)
        spark.sql("""
          INSERT INTO aqe_cat.db.two_join_fact VALUES
          (1, DATE '2024-01-01', 'A', 10),
          (2, DATE '2024-01-01', 'B', 20),
          (3, DATE '2024-01-02', 'A', 30),
          (4, DATE '2024-01-02', 'B', 40),
          (5, DATE '2024-01-03', 'A', 50),
          (6, DATE '2024-01-03', 'C', 60)
        """)

        // Dim1: filters on date
        spark
          .createDataFrame(Seq((java.sql.Date.valueOf("2024-01-02"), "keep")))
          .toDF("date", "label")
          .write
          .parquet(dim1Dir.getAbsolutePath)
        spark.read.parquet(dim1Dir.getAbsolutePath).createOrReplaceTempView("date_dim")

        // Dim2: filters on category
        spark
          .createDataFrame(Seq(("A", "keep")))
          .toDF("category", "label")
          .write
          .parquet(dim2Dir.getAbsolutePath)
        spark.read.parquet(dim2Dir.getAbsolutePath).createOrReplaceTempView("cat_dim")

        // Two separate broadcast joins, each creates its own DPP filter
        val query =
          """SELECT /*+ BROADCAST(d1), BROADCAST(d2) */ f.*
            |FROM aqe_cat.db.two_join_fact f
            |JOIN date_dim d1 ON f.date = d1.date
            |JOIN cat_dim d2 ON f.category = d2.category
            |WHERE d1.label = 'keep' AND d2.label = 'keep'""".stripMargin

        val (_, cometPlan) = checkSparkAnswer(query)

        assertNoLeftoverCSAB(cometPlan)

        if (isSpark35Plus) {
          // Should have DPP subqueries for both joins
          val subqueries = collectIcebergDPPSubqueries(cometPlan)
          assert(subqueries.nonEmpty, s"Expected DPP subqueries in plan:\n$cometPlan")

          // Each should be CometSubqueryBroadcastExec with an ASPE child wrapping the
          // build-side broadcast (reuse manifests as ReusedExchangeExec inside the ASPE).
          subqueries.foreach { sub =>
            assert(
              sub.isInstanceOf[CometSubqueryBroadcastExec],
              s"Expected CometSubqueryBroadcastExec but got ${sub.getClass.getSimpleName}")
          }

          val subqueryCsbs = subqueries.collect { case csb: CometSubqueryBroadcastExec => csb }
          subqueryCsbs.foreach { csb =>
            assert(
              csb.child.isInstanceOf[AdaptiveSparkPlanExec],
              "Expected AdaptiveSparkPlanExec child but got " +
                s"${csb.child.getClass.getSimpleName}")
            val aspe = csb.child.asInstanceOf[AdaptiveSparkPlanExec]
            val hasReuse = collect(aspe) { case r: ReusedExchangeExec => r }.nonEmpty ||
              collect(aspe) { case b: BroadcastQueryStageExec => b }.nonEmpty
            assert(
              hasReuse,
              "DPP subquery's ASPE should contain ReusedExchangeExec or " +
                s"BroadcastQueryStageExec:\n${cometPlan.treeString}")
          }

          // buildKeys disambiguation: the two DPP subqueries should not share canonical
          // form (different join keys -> different broadcasts). Compare canonicalized
          // plans rather than Java reference identity.
          if (subqueryCsbs.size >= 2) {
            val distinctCanonical = subqueryCsbs.map(_.child.canonicalized).distinct
            assert(
              distinctCanonical.size == subqueryCsbs.size,
              s"Expected ${subqueryCsbs.size} distinct broadcasts (by canonical form) " +
                s"but got ${distinctCanonical.size}. buildKeys disambiguation may not be working.")
          }

          // Verify correct results: date=2024-01-02 AND category=A returns row (3, 2024-01-02, A, 30)
          val icebergScans = collectIcebergNativeScans(cometPlan)
          assert(icebergScans.nonEmpty, "Expected CometIcebergNativeScanExec in plan")
          assert(
            icebergScans.head.numPartitions == 1,
            s"Expected DPP to prune to 1 partition but got ${icebergScans.head.numPartitions}")
        }

        spark.sql("DROP TABLE aqe_cat.db.two_join_fact")
      }
    }
  }

  test("AQE DPP - graceful fallback when broadcast join is not Comet") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
        // Disable Comet BHJ so the join stays as Spark's BroadcastHashJoinExec.
        // The rule cannot find CometBroadcastHashJoinExec and must handle gracefully.
        CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED.key -> "false",
        CometConf.COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.key -> "false") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.fallback_fact (
            id BIGINT, data STRING, date DATE
          ) USING iceberg PARTITIONED BY (date)
        """)
        spark.sql("""
          INSERT INTO aqe_cat.db.fallback_fact VALUES
          (1, 'a', DATE '1970-01-01'), (2, 'b', DATE '1970-01-02'),
          (3, 'c', DATE '1970-01-02'), (4, 'd', DATE '1970-01-03')
        """)

        spark
          .createDataFrame(Seq((1L, java.sql.Date.valueOf("1970-01-02"))))
          .toDF("id", "date")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("fallback_dim")

        // Query should still produce correct results even without Comet BHJ
        val query =
          """SELECT /*+ BROADCAST(d) */ f.* FROM aqe_cat.db.fallback_fact f
            |JOIN fallback_dim d ON f.date = d.date AND d.id = 1
            |ORDER BY f.id""".stripMargin
        val (_, cometPlan) = checkSparkAnswer(query)
        assertNoLeftoverCSAB(cometPlan)

        spark.sql("DROP TABLE aqe_cat.db.fallback_fact")
      }
    }
  }

  test("AQE DPP - empty broadcast result prunes all partitions") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.empty_dpp_fact (
            id BIGINT, data STRING, date DATE
          ) USING iceberg PARTITIONED BY (date)
        """)
        spark.sql("""
          INSERT INTO aqe_cat.db.empty_dpp_fact VALUES
          (1, 'a', DATE '1970-01-01'), (2, 'b', DATE '1970-01-02'),
          (3, 'c', DATE '1970-01-03')
        """)

        // Dim table with a value that matches NO fact partitions
        spark
          .createDataFrame(Seq((1L, java.sql.Date.valueOf("2099-12-31"))))
          .toDF("id", "date")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("empty_dim")

        val query =
          """SELECT /*+ BROADCAST(d) */ f.* FROM aqe_cat.db.empty_dpp_fact f
            |JOIN empty_dim d ON f.date = d.date AND d.id = 1""".stripMargin

        // Should return empty result, DPP prunes all partitions
        val result = spark.sql(query).collect()
        assert(result.isEmpty, s"Expected empty result but got ${result.length} rows")

        val (_, cometPlan) = checkSparkAnswer(query)

        assertNoLeftoverCSAB(cometPlan)

        if (isSpark35Plus) {
          // Verify the rule still converted the SAB
          val subqueries = collectIcebergDPPSubqueries(cometPlan)
          subqueries.foreach { sub =>
            assert(
              sub.isInstanceOf[CometSubqueryBroadcastExec],
              s"Expected CometSubqueryBroadcastExec but got ${sub.getClass.getSimpleName}")
          }
        }

        spark.sql("DROP TABLE aqe_cat.db.empty_dpp_fact")
      }
    }
  }

  test("AQE DPP - no broadcast join (SMJ) handles SAB gracefully") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        // Disable broadcast to force sort-merge join, no broadcast join for DPP to reuse
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.smj_fact (
            id BIGINT, data STRING, date DATE
          ) USING iceberg PARTITIONED BY (date)
        """)
        spark.sql("""
          INSERT INTO aqe_cat.db.smj_fact VALUES
          (1, 'a', DATE '1970-01-01'), (2, 'b', DATE '1970-01-02'),
          (3, 'c', DATE '1970-01-02'), (4, 'd', DATE '1970-01-03')
        """)

        spark
          .createDataFrame(Seq((1L, java.sql.Date.valueOf("1970-01-02"))))
          .toDF("id", "date")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("smj_dim")

        // No BROADCAST hint + threshold=-1 forces SMJ. DPP may still create SABs
        // but there is no broadcast join for our rule to find.
        val query =
          """SELECT f.* FROM aqe_cat.db.smj_fact f
            |JOIN smj_dim d ON f.date = d.date
            |WHERE d.id = 1
            |ORDER BY f.id""".stripMargin

        // Should produce correct results regardless of DPP path
        val (_, cometPlan) = checkSparkAnswer(query)
        // Even when no BHJ exists, no CSAB should survive the rule. On 3.5+ the rule
        // emits TrueLiteral or aggregate SubqueryExec; on 3.4 the wrapper is never
        // created in the first place. A leftover CSAB would crash at runtime.
        assertNoLeftoverCSAB(cometPlan)

        spark.sql("DROP TABLE aqe_cat.db.smj_fact")
      }
    }
  }

  // Asserts no leftover CometSubqueryAdaptiveBroadcastExec in the plan. Any survivor
  // would throw at execution because doExecute is intentionally unimplemented.
  private def assertNoLeftoverCSAB(plan: SparkPlan): Unit = {
    val remaining = collectWithSubqueries(plan) { case s: CometSubqueryAdaptiveBroadcastExec =>
      s
    }
    assert(
      remaining.isEmpty,
      "Expected no unconverted CometSubqueryAdaptiveBroadcastExec, " +
        s"found ${remaining.size}:\n${plan.treeString}")
  }

  test("AQE DPP - ReuseAdaptiveSubquery wraps CSAB in ReusedSubqueryExec (UNION ALL)") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Two fact tables with identical partition schema; UNION ALL pushes a single
        // logical DPP through to both scans. Their SABs share buildPlan and canonicalize
        // identically, so ReuseAdaptiveSubquery wraps one in ReusedSubqueryExec.
        spark.sql("""
          CREATE TABLE aqe_cat.db.fact1 (id BIGINT, fact_date DATE)
          USING iceberg PARTITIONED BY (fact_date)
        """)
        spark.sql("""
          CREATE TABLE aqe_cat.db.fact2 (id BIGINT, fact_date DATE)
          USING iceberg PARTITIONED BY (fact_date)
        """)
        spark.sql("""
          INSERT INTO aqe_cat.db.fact1 VALUES
          (1, DATE '2024-01-01'), (2, DATE '2024-01-02'), (3, DATE '2024-01-03')
        """)
        spark.sql("""
          INSERT INTO aqe_cat.db.fact2 VALUES
          (4, DATE '2024-01-01'), (5, DATE '2024-01-02'), (6, DATE '2024-01-03')
        """)

        spark
          .createDataFrame(Seq((9L, java.sql.Date.valueOf("2024-01-02"))))
          .toDF("dim_id", "dim_date")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("union_dim")

        val query =
          """SELECT /*+ BROADCAST(d) */ f.id, f.fact_date
            |FROM (
            |  SELECT id, fact_date FROM aqe_cat.db.fact1
            |  UNION ALL
            |  SELECT id, fact_date FROM aqe_cat.db.fact2
            |) f
            |JOIN union_dim d ON f.fact_date = d.dim_date
            |WHERE d.dim_id > 7""".stripMargin
        val (_, cometPlan) = checkSparkAnswer(query)

        // No CSAB should survive on either version (would crash at execution otherwise).
        assertNoLeftoverCSAB(cometPlan)

        if (isSpark35Plus) {
          // Subquery dedup: exactly 1 CometSubqueryBroadcastExec shared across both
          // fact scans, plus at least 1 ReusedSubqueryExec pointer.
          val cometSubqueries = collectWithSubqueries(cometPlan) {
            case s: CometSubqueryBroadcastExec => s
          }
          assert(
            cometSubqueries.size == 1,
            "Expected exactly 1 CometSubqueryBroadcastExec (shared), got " +
              s"${cometSubqueries.size}:\n${cometPlan.treeString}")
          val reusedCsbs = collectWithSubqueries(cometPlan) {
            case r @ ReusedSubqueryExec(_: CometSubqueryBroadcastExec) => r
          }
          assert(
            reusedCsbs.nonEmpty,
            "Expected at least one ReusedSubqueryExec(CometSubqueryBroadcastExec):" +
              s"\n${cometPlan.treeString}")
        }

        spark.sql("DROP TABLE aqe_cat.db.fact1")
        spark.sql("DROP TABLE aqe_cat.db.fact2")
      }
    }
  }

  test("AQE DPP - cross-stage scalar subquery with broadcast reuse") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.scalar_fact (
            id BIGINT, value INT, fact_date DATE
          ) USING iceberg PARTITIONED BY (fact_date)
        """)
        spark.sql("""
          INSERT INTO aqe_cat.db.scalar_fact VALUES
          (1, 10, DATE '2024-01-01'), (2, 20, DATE '2024-01-02'),
          (3, 30, DATE '2024-01-02'), (4, 40, DATE '2024-01-03')
        """)

        spark
          .createDataFrame(Seq((1L, java.sql.Date.valueOf("2024-01-02"), "US")))
          .toDF("dim_id", "dim_date", "country")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("scalar_dim")

        // Same DPP shape in main query and in an uncorrelated scalar subquery.
        // Main: BHJ over fact x dim. Scalar: independent scan x dim with same DPP.
        // The scalar subquery is a separate ASPE, so the cross-stage search via
        // context.qe.executedPlan AND subqueryCache dedup must both work.
        // BROADCAST hints on BOTH the main and scalar joins are intentional. The V1
        // analog (CometExecSuite "AQE DPP: uncorrelated scalar subquery with broadcast
        // reuse") relies on `withDppTables` running ANALYZE TABLE to populate row-level
        // stats, which lets Spark's optimizer naturally pick BuildRight (dim broadcast)
        // in both contexts. Iceberg has no clean ANALYZE-equivalent (its stats come
        // from manifest summaries), and Spark's default size estimate for the small
        // post-aggregate fact tempts AQE to broadcast fact in the scalar subquery -
        // BuildLeft instead of BuildRight - which leaves no dim broadcast for the
        // SAB to match against, falling through to TrueLiteral. The hint forces the
        // same broadcast direction Spark picks naturally for V1, so the rule and
        // subqueryCache deduplication path under test actually fires.
        val query =
          """SELECT /*+ BROADCAST(d) */
            |  f.fact_date,
            |  SUM(f.value) AS s,
            |  (SELECT /*+ BROADCAST(d2) */ SUM(f2.value)
            |   FROM aqe_cat.db.scalar_fact f2
            |   JOIN scalar_dim d2 ON f2.fact_date = d2.dim_date
            |   WHERE d2.country = 'US') AS total
            |FROM aqe_cat.db.scalar_fact f
            |JOIN scalar_dim d ON f.fact_date = d.dim_date
            |WHERE d.country = 'US'
            |GROUP BY 1""".stripMargin
        val (_, cometPlan) = checkSparkAnswer(query)

        assertNoLeftoverCSAB(cometPlan)

        // Cross-plan dedup is a known 3.4 limitation (each ASPE sees only its own
        // plan at prep-rule time, so the scalar subquery's SAB cannot find the main
        // query's BHJ). Strict shape checks only on 3.5+.
        if (isSpark35Plus) {
          val countBroadcasts = collectWithSubqueries(cometPlan) {
            case _: CometSubqueryBroadcastExec => 1
          }.sum
          val countReused = collectWithSubqueries(cometPlan) {
            case ReusedSubqueryExec(_: CometSubqueryBroadcastExec) => 1
          }.sum

          assert(
            countBroadcasts == 1,
            "Expected 1 CometSubqueryBroadcastExec (shared across plans), " +
              s"got $countBroadcasts:\n${cometPlan.treeString}")
          assert(
            countReused == 1,
            s"Expected 1 ReusedSubqueryExec, got $countReused:\n${cometPlan.treeString}")
        }

        spark.sql("DROP TABLE aqe_cat.db.scalar_fact")
      }
    }
  }

  test("AQE DPP - exchange reuse disabled falls through to aggregate SubqueryExec") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        // exchangeReuseEnabled=false drops case 1; onlyInBroadcast=false (the default
        // when stats favor running anyway) makes the rule pick case 3, not case 2.
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.noreuse_fact (
            id BIGINT, value INT, fact_date DATE
          ) USING iceberg PARTITIONED BY (fact_date)
        """)
        // Larger fact table so PartitionPruning.pruningHasBenefit evaluates DPP
        // worth inserting even with REUSE_BROADCAST_ONLY=false.
        spark
          .range(1, 200)
          .selectExpr(
            "id",
            "cast(id as int) as value",
            "DATE_ADD(DATE '2024-01-01', CAST(id % 4 AS INT)) as fact_date")
          .write
          .format("iceberg")
          .mode("append")
          .saveAsTable("aqe_cat.db.noreuse_fact")

        spark
          .createDataFrame(Seq((1L, java.sql.Date.valueOf("2024-01-02"))))
          .toDF("dim_id", "dim_date")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("noreuse_dim")

        val query =
          """SELECT /*+ BROADCAST(d) */ f.* FROM aqe_cat.db.noreuse_fact f
            |JOIN noreuse_dim d ON f.fact_date = d.dim_date
            |WHERE d.dim_id = 1
            |ORDER BY f.id""".stripMargin
        val (_, cometPlan) = checkSparkAnswer(query)

        assertNoLeftoverCSAB(cometPlan)

        // With exchange reuse off, no CometSubqueryBroadcastExec is created.
        // Case 3 emits an aggregate SubqueryExec (or Spark may inline TrueLiteral).
        val csbCount = collectWithSubqueries(cometPlan) { case _: CometSubqueryBroadcastExec =>
          1
        }.sum
        assert(
          csbCount == 0,
          "Expected 0 CometSubqueryBroadcastExec when exchange reuse is off, " +
            s"got $csbCount:\n${cometPlan.treeString}")

        spark.sql("DROP TABLE aqe_cat.db.noreuse_fact")
      }
    }
  }

  test("AQE DPP - broadcast exchange count is 1 across multiple DPP filters") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.exchg_fact (
            id BIGINT, data STRING, fact_date DATE
          ) USING iceberg PARTITIONED BY (data, fact_date)
        """)
        val df = spark
          .range(1, 50)
          .selectExpr(
            "id",
            "CAST(DATE_ADD(DATE '2024-01-01', CAST(id % 4 AS INT)) AS STRING) as data",
            "DATE_ADD(DATE '2024-01-01', CAST(id % 4 AS INT)) as fact_date")
        df.coalesce(1)
          .write
          .format("iceberg")
          .option("fanout-enabled", "true")
          .mode("append")
          .saveAsTable("aqe_cat.db.exchg_fact")

        spark
          .createDataFrame(Seq((1L, "2024-01-02", java.sql.Date.valueOf("2024-01-02"))))
          .toDF("dim_id", "data", "dim_date")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("exchg_dim")

        val query =
          """SELECT /*+ BROADCAST(d) */ f.*
            |FROM aqe_cat.db.exchg_fact f
            |JOIN exchg_dim d ON f.data = d.data AND f.fact_date = d.dim_date""".stripMargin
        val (_, cometPlan) = checkSparkAnswer(query)

        assertNoLeftoverCSAB(cometPlan)

        if (isSpark35Plus) {
          // Two DPP filters from the same join must not duplicate the dim broadcast.
          // Count broadcasts across the whole plan including subquery contexts.
          val cometBroadcasts = collectWithSubqueries(cometPlan) {
            case b: CometBroadcastExchangeExec => b
          }
          assert(
            cometBroadcasts.size == 1,
            "Expected exactly 1 CometBroadcastExchangeExec across whole plan, " +
              s"got ${cometBroadcasts.size}:\n${cometPlan.treeString}")
        }

        spark.sql("DROP TABLE aqe_cat.db.exchg_fact")
      }
    }
  }

  // SPARK-34637: DPP-side broadcast query stage must be created before the main
  // join's broadcast stage. Iceberg is V2 BatchScan, exercising the equivalent of
  // Spark's V2 path.
  test("AQE DPP - V2 broadcast query stage creation order (SPARK-34637)") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "10MB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.q34637_fact (
            store_id INT, units_sold INT
          ) USING iceberg PARTITIONED BY (store_id)
        """)
        spark.sql("""
          INSERT INTO aqe_cat.db.q34637_fact VALUES
          (1, 70), (1, 70), (15, 70), (15, 70), (2, 30), (3, 40)
        """)

        // Mirrors the SPARK-34637 query: WITH clause grouped by store_id, self-joined.
        // The DPP-side broadcast stage must be created before the main BHJ's stage,
        // otherwise AQE's stageCache cannot dedupe.
        val query =
          """WITH v AS (
            |  SELECT f.store_id FROM aqe_cat.db.q34637_fact f
            |  WHERE f.units_sold = 70 GROUP BY f.store_id
            |)
            |SELECT * FROM v v1 JOIN v v2 ON v1.store_id = v2.store_id""".stripMargin
        val (_, cometPlan) = checkSparkAnswer(query)

        assertNoLeftoverCSAB(cometPlan)

        if (isSpark35Plus) {
          val cometSubqueries = collectWithSubqueries(cometPlan) {
            case s: CometSubqueryBroadcastExec => s
          }
          assert(
            cometSubqueries.nonEmpty,
            s"Expected CometSubqueryBroadcastExec for DPP, got 0:\n${cometPlan.treeString}")
        }

        spark.sql("DROP TABLE aqe_cat.db.q34637_fact")
      }
    }
  }

  // SPARK-32509 (Iceberg port): DPP filter exists but no matching BHJ
  // (AUTO_BROADCASTJOIN_THRESHOLD=-1 forces SMJ). The unused DPP filter degrades to
  // `Literal.TrueLiteral`. The invariant under test: the two scans of the same table
  // canonicalize identically AFTER DPP degradation, so the fact data is read exactly once.
  // This depends on CometIcebergNativeScanExec.doCanonicalize stripping
  // DynamicPruningExpression(TrueLiteral) - analogous to FileSourceScanExec.doCanonicalize
  // on V1.
  //
  // The "exactly once" property manifests differently across Spark versions:
  //   - 3.5+: EnsureRequirements inserts a hash shuffle for SMJ; AQE recognizes the matching
  //     canonical form on the peer side and emits ReusedExchangeExec -> 1 shuffle, 1 reuse.
  //     Same shape as V1's CometExecSuite version of this test.
  //   - 3.4: planner doesn't insert shuffles for this query shape (V2-specific outputPartitioning
  //     interaction with EnsureRequirements). Result: 0 shuffles. Still "data read once" - just
  //     via a different mechanism.
  // Either shape satisfies the invariant; assertion accepts both.
  test("AQE DPP - unused DPP filter and exchange reuse (SPARK-32509)") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.q32509_fact (
            store_id INT, units_sold INT
          ) USING iceberg PARTITIONED BY (store_id)
        """)
        // Match V1's withDppTables shape: 31 rows across many distinct store_ids -> many file
        // partitions on Iceberg's PARTITIONED BY (store_id). Multiple partitions ensure SMJ
        // sees enough work to need shuffles (Spark's planner can skip shuffles for trivially
        // small inputs). Only store_id=15 has units_sold=70, mirroring V1, so the self-join
        // WHERE units_sold=70 produces exactly one (15, 15) row.
        spark
          .range(1, 32)
          .selectExpr(
            "cast(id as int) as store_id",
            "cast(case when id = 15 then 70 else (id * 10) end as int) as units_sold")
          .write
          .format("iceberg")
          .mode("append")
          .saveAsTable("aqe_cat.db.q32509_fact")

        val query =
          """WITH v1 AS (
            |  SELECT f.store_id FROM aqe_cat.db.q32509_fact f WHERE f.units_sold = 70
            |)
            |SELECT * FROM v1 a JOIN v1 b ON a.store_id = b.store_id""".stripMargin
        val (_, cometPlan) = checkSparkAnswer(query)

        assertNoLeftoverCSAB(cometPlan)

        // Accept either shape (see test docstring): single shuffle with reuse, or no
        // shuffles at all. Both prove the two scans canonicalize identically after DPP
        // degrades to TrueLiteral, so fact data is read exactly once.
        val shuffleExchanges = collect(cometPlan) {
          case e: ShuffleExchangeExec => e
          case e: CometShuffleExchangeExec => e
        }
        val reusedExchanges = collect(cometPlan) { case r: ReusedExchangeExec => r }

        val singleShuffleWithReuse =
          shuffleExchanges.size == 1 && reusedExchanges.size == 1
        val noShuffles = shuffleExchanges.isEmpty && reusedExchanges.isEmpty
        assert(
          singleShuffleWithReuse || noShuffles,
          "Expected fact data read exactly once: either (1 shuffle + 1 ReusedExchange) " +
            s"or (0 shuffles), got (${shuffleExchanges.size} shuffles, " +
            s"${reusedExchanges.size} reused):\n${cometPlan.treeString}")

        spark.sql("DROP TABLE aqe_cat.db.q32509_fact")
      }
    }
  }

  // Multi-key BHJ where SAB build keys must keep their position so the index lookup
  // selects the right DPP value. A bug in convertSAB key handling would either pick
  // the wrong column or produce SubqueryExec instead of broadcast reuse.
  test("AQE DPP - avoid reordering broadcast join keys") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dim2Dir = new File(warehouseDir, "dim2_parquet")
      val dim3Dir = new File(warehouseDir, "dim3_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.large (
            id BIGINT, A BIGINT, B BIGINT
          ) USING iceberg PARTITIONED BY (A)
        """)
        spark.sql("""
          INSERT INTO aqe_cat.db.large
          SELECT id, id + 1, id + 2 FROM range(100)
        """)

        spark
          .createDataFrame((0 until 10).map(i => (i.toLong, (i + 1).toLong, (i + 2).toLong)))
          .toDF("id", "C", "D")
          .write
          .parquet(dim2Dir.getAbsolutePath)
        spark.read.parquet(dim2Dir.getAbsolutePath).createOrReplaceTempView("dimTwo")

        spark
          .createDataFrame(
            (0 until 10).map(i => (i.toLong, (i + 1).toLong, (i + 2).toLong, (i + 3).toLong)))
          .toDF("id", "E", "F", "G")
          .write
          .parquet(dim3Dir.getAbsolutePath)
        spark.read.parquet(dim3Dir.getAbsolutePath).createOrReplaceTempView("dimThree")

        // Compose a query that triggers DPP via a BROADCAST join with multi-key
        // build side, and a leading SMJ that consumes (A, B) in one order while the
        // BHJ joins on (B, A) in another. If our rule reorders buildKeys, the DPP
        // value picked from the broadcast row will be wrong or DPP will degrade to
        // SubqueryExec.
        val query =
          """SELECT /*+ BROADCAST(prod) */ fact.id
            |FROM aqe_cat.db.large fact
            |LEFT JOIN dimTwo dim2 ON fact.A = dim2.C AND fact.B = dim2.D
            |JOIN dimThree prod ON fact.B = prod.F AND fact.A = prod.E
            |WHERE prod.G > 5""".stripMargin
        val (_, cometPlan) = checkSparkAnswer(query)

        assertNoLeftoverCSAB(cometPlan)

        if (isSpark35Plus) {
          val dpExprs = collect(cometPlan) { case s: CometIcebergNativeScanExec => s }
            .flatMap(_.runtimeFilters.collect { case d: DynamicPruningExpression =>
              d.child
            })
          val hasSubquery = dpExprs.exists {
            case InSubqueryExec(_, _: SubqueryExec, _, _, _, _) => true
            case _ => false
          }
          val hasBroadcast = dpExprs.exists {
            case InSubqueryExec(_, _: CometSubqueryBroadcastExec, _, _, _, _) => true
            case _ => false
          }
          assert(!hasSubquery, s"Should not have SubqueryExec DPP:\n${cometPlan.treeString}")
          assert(hasBroadcast, s"Should have broadcast DPP:\n${cometPlan.treeString}")
        }

        spark.sql("DROP TABLE aqe_cat.db.large")
      }
    }
  }

  // Join key is non-atomic (struct/array of partition column). Our sabKeyIds extraction
  // via references.map(_.exprId) must traverse the wrapper so the BHJ key lookup matches.
  test("AQE DPP - non-atomic type (struct/array) join key") {
    assume(icebergAvailable, "Iceberg not available")
    withTempIcebergDir { warehouseDir =>
      val dimDir = new File(warehouseDir, "dim_parquet")
      withSQLConf(
        "spark.sql.catalog.aqe_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.aqe_cat.type" -> "hadoop",
        "spark.sql.catalog.aqe_cat.warehouse" -> warehouseDir.getAbsolutePath,
        "spark.sql.autoBroadcastJoinThreshold" -> "1KB",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        spark.sql("""
          CREATE TABLE aqe_cat.db.nonatomic_fact (
            date_id INT, store_id INT, units_sold INT
          ) USING iceberg PARTITIONED BY (store_id)
        """)
        spark
          .range(100)
          .selectExpr(
            "cast(id as int) as date_id",
            "cast(id % 10 as int) as store_id",
            "cast(id * 2 as int) as units_sold")
          .write
          .format("iceberg")
          .mode("append")
          .saveAsTable("aqe_cat.db.nonatomic_fact")

        spark
          .range(10)
          .selectExpr("cast(id as int) as store_id", "cast(id as string) as country")
          .write
          .parquet(dimDir.getAbsolutePath)
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("nonatomic_dim")

        Seq("struct", "array").foreach { dataType =>
          val query =
            s"""SELECT /*+ BROADCAST(d) */ f.date_id, f.store_id
               |FROM aqe_cat.db.nonatomic_fact f
               |JOIN nonatomic_dim d
               |  ON $dataType(f.store_id) = $dataType(d.store_id)
               |WHERE d.country = '3'""".stripMargin
          val (_, cometPlan) = checkSparkAnswer(query)
          assertNoLeftoverCSAB(cometPlan)
        }

        spark.sql("DROP TABLE aqe_cat.db.nonatomic_fact")
      }
    }
  }
}
