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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Row
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Measures wide-decimal hash shuffle routing with COUNT, independently of decimal AVG support.
 * Run this benchmark on both revisions to compare the routing change. Fixture creation, result
 * validation and route reporting are untimed. The session uses local[1]; `--reverse` reverses
 * case order, and `--validate-only` checks the fixture and results without collecting timings.
 */
object CometWideDecimalShuffleBenchmark extends CometBenchmarkBase {
  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = 1024 * 1024
    val groups = 10000
    val partitions = 4
    val filePartitionBytes = 16 * 1024 * 1024
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> partitions.toString,
      // Keep each file in one split, and prevent Spark from combining files into one task.
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> filePartitionBytes.toString,
      SQLConf.FILES_OPEN_COST_IN_BYTES.key -> filePartitionBytes.toString,
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true") {
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          val query = "SELECT k, COUNT(v) FROM parquetV1Table GROUP BY k"
          var expected = Seq.empty[Row]
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            prepareTable(
              dir,
              spark
                .range(0L, rows.toLong, 1L, partitions)
                .selectExpr(
                  s"CAST(id % $groups AS DECIMAL(38, 2)) AS k",
                  "CAST(id % 97 AS INT) AS v"))
            assert(spark.table("parquetV1Table").rdd.getNumPartitions == partitions)
            expected = spark.sql(query).collect().toSeq.sortBy(_.getDecimal(0))
          }
          val benchmark = new Benchmark("wide_decimal_hash_shuffle", rows, output = output)
          val modes = Seq("Spark", "native", "auto", "jvm")
          for (mode <- (if (mainArgs.contains("--reverse")) modes.reverse else modes)) {
            val configs = Seq(
              CometConf.COMET_ENABLED.key -> (mode != "Spark").toString,
              CometConf.COMET_SHUFFLE_MODE.key -> (if (mode == "Spark") "native" else mode))
            withSQLConf(configs: _*) {
              val df = spark.sql(query)
              assert(df.collect().toSeq.sortBy(_.getDecimal(0)) == expected)
              val plan = df.queryExecution.executedPlan
              val routes = plan.collect {
                case exchange: CometShuffleExchangeExec => exchange.shuffleType.toString
                case _: ShuffleExchangeExec => "Spark shuffle"
              }
              assert(routes.size == 1, plan.treeString)
              benchmark.out.println(s"Wide-decimal shuffle mode=$mode: ${routes.head}")
              benchmark.out.println(plan.treeString)
            }
            benchmark.addCase(mode) { _ =>
              withSQLConf(configs: _*) {
                spark.sql(query).collect()
              }
            }
          }
          if (!mainArgs.contains("--validate-only")) benchmark.run()
        }
      }
    }
  }
}
