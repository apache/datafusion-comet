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
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark comparing DPP query performance:
 *   1. Spark native (no Comet) 2. Comet with DPP fallback (old behavior) 3. Comet with DPP
 *      support (new behavior - native scan)
 *
 * Run with: mvn test -pl spark -Pspark-3.4
 * -Dsuites=org.apache.comet.CometNativeScanDPPBenchmark
 */
class CometNativeScanDPPBenchmark extends CometTestBase {

  private val numRows = 1000000 // 1M rows
  private val numPartitions = 100

  // scalastyle:off println
  test("benchmark: DPP query - Spark vs Comet native scan") {
    withTempDir { dir =>
      val factDir = new File(dir, "fact")
      val dimDir = new File(dir, "dim")

      // Create large partitioned fact table
      spark
        .range(numRows)
        .selectExpr(
          "id",
          s"id % $numPartitions as part_key",
          "id * 2 as value",
          "concat('data_', id) as data")
        .write
        .partitionBy("part_key")
        .parquet(factDir.getAbsolutePath)

      // Create small dimension table (will be broadcast)
      // Only includes 5 partition keys out of 100
      spark
        .createDataFrame((0 until 5).map(i => (i.toLong, s"dim_$i")))
        .toDF("dim_key", "dim_name")
        .write
        .parquet(dimDir.getAbsolutePath)

      spark.read.parquet(factDir.getAbsolutePath).createOrReplaceTempView("fact")
      spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("dim")

      val query =
        """SELECT /*+ BROADCAST(dim) */ SUM(f.value), COUNT(*)
          |FROM fact f
          |JOIN dim d ON f.part_key = d.dim_key""".stripMargin

      println("\n" + "=" * 70)
      println("  DPP BENCHMARK: Spark vs Comet Native Scan")
      println("=" * 70)
      println(s"  Fact table: $numRows rows, $numPartitions partitions")
      println(s"  Dimension: 5 keys (DPP prunes to 5% of partitions)")
      println("-" * 70)

      // Warmup
      spark.sql("SELECT COUNT(*) FROM fact").collect()

      // 1. Pure Spark (no Comet)
      var sparkTime = 0L
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "false",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val start = System.nanoTime()
        spark.sql(query).collect()
        sparkTime = (System.nanoTime() - start) / 1000000

        // Verify it's using Spark's FileSourceScanExec
        val plan = spark.sql(query).queryExecution.executedPlan
        val sparkScans = collect(plan) { case s: FileSourceScanExec => s }
        assert(sparkScans.nonEmpty, "Expected FileSourceScanExec for Spark mode")

        println(f"  Spark (JVM scan):        $sparkTime%6d ms  [FileSourceScanExec]")
      }

      // 2. Comet with DPP fallback (simulating old behavior)
      var cometFallbackTime = 0L
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_ICEBERG_COMPAT,
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val start = System.nanoTime()
        spark.sql(query).collect()
        cometFallbackTime = (System.nanoTime() - start) / 1000000

        // With fallback enabled, DPP queries should use Spark scan
        val plan = spark.sql(query).queryExecution.executedPlan
        val sparkScans = collect(plan) { case s: FileSourceScanExec => s }
        val nativeScans = collect(plan) { case s: CometNativeScanExec => s }

        val scanType =
          if (nativeScans.nonEmpty) "CometNativeScanExec"
          else if (sparkScans.nonEmpty) "FileSourceScanExec (fallback)"
          else "Unknown"

        println(f"  Comet (DPP fallback):    $cometFallbackTime%6d ms  [$scanType]")
      }

      // 3. Comet with DPP support (new behavior - native scan)
      var cometNativeTime = 0L
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "false",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION,
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val start = System.nanoTime()
        spark.sql(query).collect()
        cometNativeTime = (System.nanoTime() - start) / 1000000

        // With DPP support, should use CometNativeScanExec
        val plan = spark.sql(query).queryExecution.executedPlan
        val nativeScans = collect(plan) { case s: CometNativeScanExec => s }

        val scanType =
          if (nativeScans.nonEmpty) "CometNativeScanExec (native)"
          else "Unknown"

        println(f"  Comet (DPP native):      $cometNativeTime%6d ms  [$scanType]")
      }

      println("-" * 70)

      if (cometNativeTime > 0 && sparkTime > 0) {
        val speedupVsSpark = sparkTime.toDouble / cometNativeTime
        val speedupVsFallback =
          if (cometFallbackTime > 0) cometFallbackTime.toDouble / cometNativeTime else 0.0

        println(f"  Speedup vs Spark:        $speedupVsSpark%.2fx")
        if (speedupVsFallback > 0) {
          println(f"  Speedup vs Fallback:     $speedupVsFallback%.2fx")
        }
      }

      println("=" * 70 + "\n")
    }
  }

  test("verify: DPP uses native scan and prunes partitions correctly") {
    withTempDir { dir =>
      val factDir = new File(dir, "fact")
      val dimDir = new File(dir, "dim")

      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "false",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION,
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {

        // Create fact table with 10 partitions
        spark
          .range(10000)
          .selectExpr("id", "id % 10 as part_key", "id * 2 as value")
          .write
          .partitionBy("part_key")
          .parquet(factDir.getAbsolutePath)

        // Dimension table with only 2 keys
        spark
          .createDataFrame(Seq((1L, "one"), (2L, "two")))
          .toDF("dim_key", "dim_name")
          .write
          .parquet(dimDir.getAbsolutePath)

        spark.read.parquet(factDir.getAbsolutePath).createOrReplaceTempView("fact_verify")
        spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("dim_verify")

        val query =
          """SELECT /*+ BROADCAST(dim_verify) */ f.id, f.value, d.dim_name
            |FROM fact_verify f
            |JOIN dim_verify d ON f.part_key = d.dim_key
            |ORDER BY f.id""".stripMargin

        val df = spark.sql(query)

        // Verify plan uses CometNativeScanExec
        val plan = df.queryExecution.executedPlan
        val nativeScans = collect(stripAQEPlan(plan)) { case s: CometNativeScanExec => s }

        println("\n" + "=" * 70)
        println("  VERIFICATION: Native Scan with DPP")
        println("=" * 70)

        if (nativeScans.nonEmpty) {
          println("  ✓ Using CometNativeScanExec (native DataFusion scan)")

          // Check the number of partitions being scanned
          val numPartitionsScanned = nativeScans.head.perPartitionData.length
          println(s"  ✓ Partitions scanned: $numPartitionsScanned (expected: 2 out of 10)")

          // DPP should prune to only 2 partitions (part_key IN (1, 2))
          assert(
            numPartitionsScanned <= 2,
            s"Expected DPP to prune to 2 partitions but got $numPartitionsScanned")
        } else {
          println("  ✗ NOT using CometNativeScanExec")
          println(s"  Plan: ${plan.toString.take(500)}")
        }

        // Verify results are correct
        val result = df.collect()
        println(s"  ✓ Result rows: ${result.length} (expected: 2000)")
        assert(result.length == 2000, s"Expected 2000 rows but got ${result.length}")

        // Verify all results have part_key IN (1, 2)
        result.foreach { row =>
          val id = row.getLong(0)
          val partKey = id % 10
          assert(
            partKey == 1 || partKey == 2,
            s"Expected part_key IN (1,2) but got $partKey for id=$id")
        }
        println("  ✓ All rows have correct partition keys (1 or 2)")

        println("=" * 70 + "\n")
      }
    }
  }
  // scalastyle:on println
}
