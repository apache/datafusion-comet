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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometBroadcastHashJoinExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.functions.{broadcast, col, count, sum}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Measures a complete local native-shuffle to broadcast-join query with uncached inputs and a
 * final aggregation. Pass --on-first to reverse case order.
 *
 * To run with four local workers:
 * {{{
 * COMET_BENCHMARK_MASTER='local[4]' SPARK_GENERATE_BENCHMARK_FILES=1 make \
 *   benchmark-org.apache.spark.sql.benchmark.CometBroadcastLocalShuffleBenchmark BENCH_HEAP=4g
 * }}}
 */
object CometBroadcastLocalShuffleBenchmark extends CometBenchmarkBase {

  private val buildRows = 65536L
  private val probeRows = buildRows * 16

  private def query(): DataFrame = {
    val build = spark
      .range(0, buildRows, 1, 128)
      .selectExpr(
        "CAST(id AS INT) AS k",
        "CASE WHEN id % 7 = 0 THEN NULL ELSE CAST(id * 3 AS INT) END AS v")
      .repartition(64, col("k"))
    val probe = spark
      .range(0, probeRows, 1, 8)
      .selectExpr(s"CAST(id % $buildRows AS INT) AS k")
    probe.join(broadcast(build), "k").agg(count("*"), sum("v"))
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("WARN")
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_BATCH_SIZE.key -> "8192",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val expectedSum = (0L until buildRows).filter(_ % 7 != 0).map(_ * 3).sum * 16
      val expected = Seq(Row(probeRows, expectedSum))
      val streams = Seq(false, true).map { enabled =>
        var count = 0L
        withSQLConf(CometConf.COMET_BROADCAST_PRODUCER_COALESCE_ENABLED.key -> enabled.toString) {
          val df = query()
          assert(df.collect().toSeq == expected)
          val plan = df.queryExecution.executedPlan
          assert(collect(plan) { case join: CometBroadcastHashJoinExec => join }.nonEmpty, plan)
          assert(
            collect(plan) {
              case shuffle: CometShuffleExchangeExec
                  if shuffle.shuffleType == CometNativeShuffle =>
                shuffle
            }.nonEmpty,
            plan)
          val exchanges = collect(plan) { case exchange: CometBroadcastExchangeExec => exchange }
          assert(exchanges.size == 1, plan)
          val exchange = exchanges.head
          assert(exchange.metrics("numOutputRows").value == buildRows)
          count = exchange.metrics("numCoalescedBatches").value
          println(s"enabled=$enabled, broadcast input streams=$count, result=$expected")
          println(plan.treeString)
        }
        count
      }
      assert(streams(1) > 0 && streams(1) < streams(0), streams)

      val modes = CometBenchmarkBase.onOffModes(mainArgs.contains("--on-first"))
      runBenchmark("local native shuffle to broadcast join") {
        val benchmark = new Benchmark(
          "local native shuffle to broadcast join",
          probeRows,
          minNumIters = 5,
          output = output)
        modes.foreach { case (name, enabled) =>
          benchmark.addCase(name) { _ =>
            withSQLConf(
              CometConf.COMET_BROADCAST_PRODUCER_COALESCE_ENABLED.key -> enabled.toString) {
              assert(query().collect().toSeq == expected)
            }
          }
        }
        benchmark.run()
      }
    }
  }
}
