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

package org.apache.spark.sql.benchmark

import java.io.File

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Benchmark for Dynamic Partition Pruning (DPP) with Comet native scan.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometDPPBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometDPPBenchmark-*results.txt".
 */
object CometDPPBenchmark extends CometBenchmarkBase {

  override def runCometBenchmark(args: Array[String]): Unit = {
    runDPPStarSchemaBenchmark(5000000, 50)
  }

  def runDPPStarSchemaBenchmark(numRows: Long, numPartitions: Int): Unit = {
    val benchmark = new Benchmark(
      s"Star-Schema DPP Query ($numRows rows, $numPartitions partitions)",
      numRows,
      output = output)

    withTempPath { dir =>
      val factDir = new File(dir, "fact")
      val dimDir = new File(dir, "dim")

      // Create partitioned fact table
      spark
        .range(numRows)
        .selectExpr("id", s"id % $numPartitions as part_key", "id * 1.5 as amount")
        .write
        .partitionBy("part_key")
        .parquet(factDir.getAbsolutePath)

      // Create dimension table with 5 keys (10% selectivity)
      spark
        .createDataFrame((0L until 5L).map(i => (i, s"Category_$i")))
        .toDF("dim_key", "dim_name")
        .write
        .parquet(dimDir.getAbsolutePath)

      spark.read.parquet(factDir.getAbsolutePath).createOrReplaceTempView("fact_table")
      spark.read.parquet(dimDir.getAbsolutePath).createOrReplaceTempView("dim_table")

      val query =
        """SELECT /*+ BROADCAST(dim_table) */
          |  SUM(f.amount) as total,
          |  COUNT(*) as cnt
          |FROM fact_table f
          |JOIN dim_table d ON f.part_key = d.dim_key""".stripMargin

      // Spark with DPP
      benchmark.addCase("Spark (JVM) with DPP") { _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100MB") {
          spark.sql(query).noop()
        }
      }

      // Spark without DPP
      benchmark.addCase("Spark (JVM) without DPP") { _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100MB") {
          spark.sql(query).noop()
        }
      }

      // Comet native scan with DPP
      benchmark.addCase("Comet (Native) with DPP") { _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "false",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100MB") {
          spark.sql(query).noop()
        }
      }

      // Comet native scan without DPP
      benchmark.addCase("Comet (Native) without DPP") { _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "false",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100MB") {
          spark.sql(query).noop()
        }
      }

      benchmark.run()
    }
  }
}
