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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.internal.SQLConf

/**
 * Pure scan benchmark to isolate Parquet reading performance. No joins, no aggregations - just
 * reading data.
 *
 * This measures the actual difference between: - Spark's JVM-based ParquetFileFormat reader -
 * Comet's native DataFusion ParquetExec reader
 */
class CometScanOnlyBenchmark extends CometTestBase {

  private val warmupRuns = 2
  private val benchmarkRuns = 5

  // scalastyle:off println
  test("pure scan benchmark: Spark JVM vs Comet Native Parquet reader") {
    withTempDir { dir =>
      val dataDir = new File(dir, "data")

      // Create test data - 5M rows with multiple columns
      val numRows = 5000000

      println("\n" + "=" * 70)
      println("  PURE SCAN BENCHMARK: Spark vs Comet Native Reader")
      println("=" * 70)
      println(s"  Creating test data: $numRows rows...")

      spark
        .range(numRows)
        .selectExpr(
          "id",
          "id % 100 as int_col",
          "id * 1.5 as double_col",
          "concat('string_value_', id % 1000) as string_col",
          "id % 2 = 0 as bool_col")
        .write
        .parquet(dataDir.getAbsolutePath)

      val fileSize = dataDir.listFiles().filter(_.getName.endsWith(".parquet")).map(_.length).sum
      println(f"  Data size: ${fileSize / 1024.0 / 1024.0}%.1f MB")
      println("-" * 70)

      // Simple scan query - just read all data
      val scanQuery = s"SELECT * FROM parquet.`${dataDir.getAbsolutePath}`"

      // Aggregation query to force full scan
      val aggQuery =
        s"SELECT COUNT(*), SUM(int_col), AVG(double_col) FROM parquet.`${dataDir.getAbsolutePath}`"

      // Warmup
      println("  Warming up...")
      for (_ <- 1 to warmupRuns) {
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          spark.sql(aggQuery).collect()
        }
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION) {
          spark.sql(aggQuery).collect()
        }
      }

      // Benchmark Spark (JVM reader)
      println(s"  Running Spark benchmark ($benchmarkRuns runs)...")
      val sparkTimes = (1 to benchmarkRuns).map { _ =>
        System.gc()
        val start = System.nanoTime()
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          spark.sql(aggQuery).collect()
        }
        (System.nanoTime() - start) / 1000000
      }

      // Benchmark Comet (native reader)
      println(s"  Running Comet benchmark ($benchmarkRuns runs)...")
      val cometTimes = (1 to benchmarkRuns).map { _ =>
        System.gc()
        val start = System.nanoTime()
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          spark.sql(aggQuery).collect()
        }
        (System.nanoTime() - start) / 1000000
      }

      // Calculate statistics
      val sparkAvg = sparkTimes.sum.toDouble / benchmarkRuns
      val sparkMin = sparkTimes.min
      val sparkMax = sparkTimes.max

      val cometAvg = cometTimes.sum.toDouble / benchmarkRuns
      val cometMin = cometTimes.min
      val cometMax = cometTimes.max

      val speedup = sparkAvg / cometAvg

      println("-" * 70)
      println("  RESULTS:")
      println("-" * 70)
      println(f"  Spark (JVM reader):   avg=${sparkAvg}%.0f ms  (min=$sparkMin, max=$sparkMax)")
      println(f"  Comet (native reader): avg=${cometAvg}%.0f ms  (min=$cometMin, max=$cometMax)")
      println("-" * 70)
      println(f"  SPEEDUP: ${speedup}%.2fx")
      println("=" * 70)

      // Also output raw data for analysis
      println("\n  Raw timings (ms):")
      println(s"  Spark:  ${sparkTimes.mkString(", ")}")
      println(s"  Comet:  ${cometTimes.mkString(", ")}")
      println()
    }
  }

  test("scan with filter pushdown: Spark vs Comet") {
    withTempDir { dir =>
      val dataDir = new File(dir, "data")

      val numRows = 5000000

      println("\n" + "=" * 70)
      println("  SCAN + FILTER PUSHDOWN BENCHMARK")
      println("=" * 70)

      spark
        .range(numRows)
        .selectExpr("id", "id % 100 as category", "id * 2.0 as amount")
        .write
        .parquet(dataDir.getAbsolutePath)

      // Filter that can be pushed down to Parquet (selects ~10% of data)
      val filterQuery =
        s"""SELECT SUM(amount) FROM parquet.`${dataDir.getAbsolutePath}`
           |WHERE category < 10""".stripMargin

      // Warmup
      for (_ <- 1 to warmupRuns) {
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          spark.sql(filterQuery).collect()
        }
      }

      // Benchmark
      val sparkTimes2 = (1 to benchmarkRuns).map { _ =>
        System.gc()
        val start = System.nanoTime()
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          spark.sql(filterQuery).collect()
        }
        (System.nanoTime() - start) / 1000000
      }
      val sparkTime = sparkTimes2.sum.toDouble / benchmarkRuns

      val cometTimes2 = (1 to benchmarkRuns).map { _ =>
        System.gc()
        val start = System.nanoTime()
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          spark.sql(filterQuery).collect()
        }
        (System.nanoTime() - start) / 1000000
      }
      val cometTime = cometTimes2.sum.toDouble / benchmarkRuns

      println(f"  Spark (JVM):   $sparkTime%.0f ms")
      println(f"  Comet (native): $cometTime%.0f ms")
      println(f"  SPEEDUP: ${sparkTime / cometTime}%.2fx")
      println("=" * 70 + "\n")
    }
  }
  // scalastyle:on println
}
