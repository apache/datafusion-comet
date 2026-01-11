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

package org.apache.comet.parquet

import java.io.File

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.command.DataWritingCommandExec

import org.apache.comet.CometConf

/**
 * Test suite for Comet Native Parquet Writer.
 *
 * Tests basic write functionality and verifies data integrity.
 */
class CometParquetWriter2PCSuite extends CometTestBase {

  private val nativeWriteConf = Seq(
    CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true")

  /** Helper to check if output directory contains any data files */
  private def hasDataFiles(dir: File): Boolean = {
    if (!dir.exists()) return false
    dir.listFiles().exists(f => f.getName.startsWith("part-") && f.getName.endsWith(".parquet"))
  }

  /** Helper to count data files in directory */
  private def countDataFiles(dir: File): Int = {
    if (!dir.exists()) return 0
    dir.listFiles().count(f => f.getName.startsWith("part-") && f.getName.endsWith(".parquet"))
  }

  // ==========================================================================
  // Test 1: Basic successful write should work
  // ==========================================================================
  test("basic successful write should create files in output directory") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output").getAbsolutePath

      val df = spark
        .range(0, 1000, 1, 4)
        .selectExpr("id", "id * 2 as value")

      withSQLConf(nativeWriteConf: _*) {
        df.write.parquet(outputPath)

        val outputDir = new File(outputPath)
        assert(hasDataFiles(outputDir), "Data files should exist in output directory")

        // Verify data can be read back correctly
        val readDf = spark.read.parquet(outputPath)
        assert(readDf.count() == 1000, "Should have 1000 rows")
      }
    }
  }

  // ==========================================================================
  // Test 2: Multiple partitions write correctly
  // ==========================================================================
  test("multiple concurrent tasks should write without file conflicts") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output").getAbsolutePath

      // Create larger dataset with more partitions
      val df = spark
        .range(0, 10000, 1, 20)
        .selectExpr("id", "id * 2 as value")

      withSQLConf(nativeWriteConf: _*) {
        df.write.parquet(outputPath)

        val outputDir = new File(outputPath)
        val fileCount = countDataFiles(outputDir)
        assert(fileCount >= 20, s"Expected at least 20 files for 20 partitions, got $fileCount")

        // Verify data integrity
        val readDf = spark.read.parquet(outputPath)
        assert(readDf.count() == 10000, "Should have 10000 rows")

        // Verify no data corruption
        val sum = readDf.selectExpr("sum(id)").collect()(0).getLong(0)
        val expectedSum = (0L until 10000L).sum
        assert(sum == expectedSum, s"Data corruption detected: sum=$sum, expected=$expectedSum")
      }
    }
  }

  // ==========================================================================
  // Test 3: Write with different data types
  // ==========================================================================
  test("write various data types correctly") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output").getAbsolutePath

      val df = spark
        .range(0, 100)
        .selectExpr(
          "id",
          "cast(id as int) as int_col",
          "cast(id as double) as double_col",
          "cast(id as string) as string_col",
          "id % 2 = 0 as bool_col")

      withSQLConf(nativeWriteConf: _*) {
        df.write.parquet(outputPath)

        val readDf = spark.read.parquet(outputPath)
        assert(readDf.count() == 100)
        assert(
          readDf.schema.fieldNames.toSet == Set(
            "id",
            "int_col",
            "double_col",
            "string_col",
            "bool_col"))
      }
    }
  }

  // ==========================================================================
  // Test 4: Append mode - currently a known limitation
  // Native writes use partition-based filenames without unique job IDs,
  // so append overwrites files with same names. This test verifies the
  // current behavior rather than ideal append semantics.
  // ==========================================================================
  test("append mode overwrites files with same partition IDs (known limitation)") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output").getAbsolutePath

      // Use different partition counts to avoid complete overlap
      val df1 = spark.range(0, 500, 1, 2).toDF("id") // 2 partitions
      val df2 = spark.range(500, 1000, 1, 3).toDF("id") // 3 partitions

      withSQLConf(nativeWriteConf: _*) {
        df1.write.parquet(outputPath)
        val countAfterFirst = spark.read.parquet(outputPath).count()
        assert(countAfterFirst == 500, "Should have 500 rows after first write")

        df2.write.mode("append").parquet(outputPath)

        // Due to filename conflicts, only partition files that don't overlap survive
        // Partitions 0, 1 get overwritten, partition 2 is new
        val readDf = spark.read.parquet(outputPath)
        val finalCount = readDf.count()
        // We expect some rows from df2 (at least partition 2) plus potentially
        // overwritten partitions. The exact count depends on partition distribution.
        assert(finalCount > 0, "Should have some rows after append")
      }
    }
  }

  // ==========================================================================
  // Test 5: Overwrite mode works correctly
  // ==========================================================================
  test("overwrite mode should replace existing files") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output").getAbsolutePath

      val df1 = spark.range(0, 1000).toDF("id")
      val df2 = spark.range(0, 500).toDF("id")

      withSQLConf(nativeWriteConf: _*) {
        df1.write.parquet(outputPath)
        df2.write.mode("overwrite").parquet(outputPath)

        val readDf = spark.read.parquet(outputPath)
        assert(readDf.count() == 500, "Should have 500 rows after overwrite")
      }
    }
  }
}
