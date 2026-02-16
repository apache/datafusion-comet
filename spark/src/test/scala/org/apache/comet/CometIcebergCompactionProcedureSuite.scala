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

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometNativeCompaction

/**
 * Integration tests for CALL rewrite_data_files() procedure intercepted by CometCompactionRule.
 * Verifies that the SQL procedure path routes through native compaction when enabled.
 */
class CometIcebergCompactionProcedureSuite extends CometTestBase {

  private val icebergExtensions =
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    val existing = conf.get("spark.sql.extensions", "")
    val extensions =
      if (existing.isEmpty) icebergExtensions
      else s"$existing,$icebergExtensions"
    conf.set("spark.sql.extensions", extensions)
  }

  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.catalog.Catalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  private def withTempIcebergDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory("comet-procedure-test").toFile
    try {
      f(dir)
    } finally {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) file.listFiles().foreach(deleteRecursively)
        file.delete()
      }
      deleteRecursively(dir)
    }
  }

  private def catalogConf(warehouseDir: File, compactionEnabled: Boolean): Map[String, String] =
    Map(
      "spark.sql.catalog.proc_cat" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.proc_cat.type" -> "hadoop",
      "spark.sql.catalog.proc_cat.warehouse" -> warehouseDir.getAbsolutePath,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_ICEBERG_COMPACTION_ENABLED.key -> compactionEnabled.toString)

  private def createFragmentedTable(tableName: String, rowCount: Int): Unit = {
    spark.sql(s"""
      CREATE TABLE $tableName (
        id BIGINT,
        name STRING,
        value DOUBLE
      ) USING iceberg
    """)
    for (i <- 1 to rowCount) {
      spark.sql(s"INSERT INTO $tableName VALUES ($i, 'name_$i', ${i * 1.5})")
    }
  }

  // ============== SQL Procedure Tests ==============

  test("CALL rewrite_data_files uses native compaction when enabled") {
    assume(icebergAvailable, "Iceberg not available")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(catalogConf(warehouseDir, compactionEnabled = true).toSeq: _*) {
        createFragmentedTable("proc_cat.db.proc_table", 10)

        val filesBefore =
          spark.sql("SELECT file_path FROM proc_cat.db.proc_table.files").count()
        assert(filesBefore >= 5, s"Expected fragmented files, got $filesBefore")

        val rowsBefore =
          spark.sql("SELECT count(*) FROM proc_cat.db.proc_table").collect()(0).getLong(0)

        val result =
          spark.sql("CALL proc_cat.system.rewrite_data_files(table => 'db.proc_table')")
        val resultRow = result.collect()

        assert(resultRow.length == 1, "Procedure should return one result row")
        val fields = result.schema.fieldNames.toSeq
        Seq("rewritten_data_files_count", "added_data_files_count", "rewritten_bytes_count")
          .foreach(f => assert(fields.contains(f), s"Missing field $f in $fields"))

        val rewrittenCount = resultRow(0).getInt(0)
        val addedCount = resultRow(0).getInt(1)
        assert(rewrittenCount > 0, "Should rewrite files")
        assert(addedCount > 0, "Should add compacted files")

        spark.sql("REFRESH TABLE proc_cat.db.proc_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM proc_cat.db.proc_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        val filesAfter =
          spark.sql("SELECT file_path FROM proc_cat.db.proc_table.files").count()
        assert(
          filesAfter < filesBefore,
          s"File count should decrease: $filesBefore -> $filesAfter")

        spark.sql("DROP TABLE proc_cat.db.proc_table")
      }
    }
  }

  test("CALL rewrite_data_files falls back to Spark when config disabled") {
    assume(icebergAvailable, "Iceberg not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(catalogConf(warehouseDir, compactionEnabled = false).toSeq: _*) {
        createFragmentedTable("proc_cat.db.fallback_table", 10)

        val rowsBefore =
          spark.sql("SELECT count(*) FROM proc_cat.db.fallback_table").collect()(0).getLong(0)

        val result =
          spark.sql("CALL proc_cat.system.rewrite_data_files(table => 'db.fallback_table')")
        val resultRow = result.collect()

        assert(resultRow.length == 1, "Spark procedure should return one result row")

        spark.sql("REFRESH TABLE proc_cat.db.fallback_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM proc_cat.db.fallback_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE proc_cat.db.fallback_table")
      }
    }
  }

  test("CALL rewrite_data_files with binpack strategy uses native compaction") {
    assume(icebergAvailable, "Iceberg not available")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(catalogConf(warehouseDir, compactionEnabled = true).toSeq: _*) {
        createFragmentedTable("proc_cat.db.binpack_table", 10)

        val rowsBefore =
          spark.sql("SELECT count(*) FROM proc_cat.db.binpack_table").collect()(0).getLong(0)

        val result = spark.sql(
          "CALL proc_cat.system.rewrite_data_files(table => 'db.binpack_table', strategy => 'binpack')")
        val resultRow = result.collect()

        assert(resultRow.length == 1)
        assert(resultRow(0).getInt(0) > 0, "Should rewrite files with binpack")

        spark.sql("REFRESH TABLE proc_cat.db.binpack_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM proc_cat.db.binpack_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE proc_cat.db.binpack_table")
      }
    }
  }

  test("CALL rewrite_data_files with sort strategy falls back to Spark") {
    assume(icebergAvailable, "Iceberg not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(catalogConf(warehouseDir, compactionEnabled = true).toSeq: _*) {
        createFragmentedTable("proc_cat.db.sort_table", 10)

        val rowsBefore =
          spark.sql("SELECT count(*) FROM proc_cat.db.sort_table").collect()(0).getLong(0)

        // Sort strategy not supported by native compaction, should fall back to Spark
        val result = spark.sql(
          "CALL proc_cat.system.rewrite_data_files(table => 'db.sort_table', strategy => 'sort', sort_order => 'id')")
        val resultRow = result.collect()

        assert(resultRow.length == 1, "Spark fallback should still return results")

        spark.sql("REFRESH TABLE proc_cat.db.sort_table")
        val rowsAfter =
          spark.sql("SELECT count(*) FROM proc_cat.db.sort_table").collect()(0).getLong(0)
        assert(rowsAfter == rowsBefore, s"Row count changed: $rowsBefore -> $rowsAfter")

        spark.sql("DROP TABLE proc_cat.db.sort_table")
      }
    }
  }

  test("CALL rewrite_data_files preserves data correctness") {
    assume(icebergAvailable, "Iceberg not available")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(catalogConf(warehouseDir, compactionEnabled = true).toSeq: _*) {
        createFragmentedTable("proc_cat.db.correct_table", 15)

        val dataBefore = spark
          .sql("SELECT id, name, value FROM proc_cat.db.correct_table ORDER BY id")
          .collect()
          .map(_.toString())

        spark.sql("CALL proc_cat.system.rewrite_data_files(table => 'db.correct_table')")

        spark.sql("REFRESH TABLE proc_cat.db.correct_table")
        val dataAfter = spark
          .sql("SELECT id, name, value FROM proc_cat.db.correct_table ORDER BY id")
          .collect()
          .map(_.toString())

        assert(dataBefore.toSeq == dataAfter.toSeq, "Data must be identical after procedure call")

        spark.sql("DROP TABLE proc_cat.db.correct_table")
      }
    }
  }
}
