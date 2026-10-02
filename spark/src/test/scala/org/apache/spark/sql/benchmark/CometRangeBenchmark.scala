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
import org.apache.spark.sql.comet.{CometHashAggregateExec, CometPlan, CometRangeExec, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{RangeExec, SparkPlan}
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.CometConf

/**
 * Compares three ways to run a query over `spark.range`:
 *
 *   - Spark: Comet disabled.
 *   - SparkToColumnar: Spark's `RangeExec`, whose rows `CometSparkToColumnarExec` converts to
 *     Arrow for the native operators above it.
 *   - CometRange: `CometRangeExec`, which generates the values in native code.
 *
 * The cases sweep how much work sits above the range, from a sum that Spark's whole-stage codegen
 * fuses with the range loop to a high-cardinality aggregate. Every arm's result and plan are
 * checked before it is timed, and the Spark arm runs again at the end of each case to show the
 * noise. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometRangeBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometRangeBenchmark-**results.txt".
 */
object CometRangeBenchmark extends CometBenchmarkBase {

  private val numRows = 64L * 1024 * 1024

  private case class Arm(name: String, confs: Seq[(String, String)], check: SparkPlan => Unit)

  private val cometConfs =
    Seq(CometConf.COMET_ENABLED.key -> "true", CometConf.COMET_EXEC_ENABLED.key -> "true")

  private val arms = Seq(
    Arm(
      "Spark",
      Seq(CometConf.COMET_ENABLED.key -> "false"),
      plan => {
        assert(collect(plan) { case r: RangeExec => r }.nonEmpty, plan)
        assert(collect(plan) { case c: CometPlan => c }.isEmpty, plan)
      }),
    Arm(
      "SparkToColumnar",
      cometConfs ++ Seq(
        CometConf.COMET_EXEC_RANGE_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true"),
      plan => {
        assert(collect(plan) { case c: CometSparkToColumnarExec => c }.nonEmpty, plan)
        assert(collect(plan) { case a: CometHashAggregateExec => a }.nonEmpty, plan)
      }),
    Arm(
      "CometRange",
      cometConfs ++ Seq(
        CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "false"),
      plan => {
        assert(collect(plan) { case r: CometRangeExec => r }.nonEmpty, plan)
        assert(collect(plan) { case a: CometHashAggregateExec => a }.nonEmpty, plan)
      }))

  private def groupedCount(keys: Long): DataFrame =
    spark
      .range(numRows)
      .groupBy((col("id") % keys).as("k"))
      .count()
      .agg(sum("count"))

  private val cases: Seq[(String, () => DataFrame)] = Seq(
    "range -> sum" -> (() => spark.range(numRows).selectExpr("sum(id)")),
    "range -> filter -> sum" -> (() =>
      spark.range(numRows).filter("id % 3 = 0").selectExpr("sum(id)")),
    "range -> xxhash64 -> sum" -> (() => spark.range(numRows).selectExpr("sum(xxhash64(id))")),
    "range -> cast to string -> sum(length)" -> (() =>
      spark.range(numRows).selectExpr("sum(length(CAST(id AS STRING)))")),
    "range -> group by 100 keys" -> (() => groupedCount(100)),
    "range -> group by 1M keys" -> (() => groupedCount(1000000)))

  private def runArm(arm: Arm, query: () => DataFrame): (Seq[Row], SparkPlan) = {
    var result: (Seq[Row], SparkPlan) = null
    withSQLConf(arm.confs: _*) {
      val df = query()
      result = (df.collect().toSeq, df.queryExecution.executedPlan)
    }
    result
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    cases.foreach { case (name, query) =>
      runBenchmark(name) {
        // Check every arm's plan and result before timing anything.
        val results = arms.map(runArm(_, query))
        arms.zip(results).foreach { case (arm, (rows, plan)) =>
          arm.check(plan)
          assert(
            rows == results.head._1,
            s"${arm.name} returned $rows, expected ${results.head._1}")
        }

        val benchmark = new Benchmark(name, numRows, output = output)
        (arms :+ arms.head.copy(name = "Spark (repeat)")).foreach { arm =>
          benchmark.addCase(arm.name) { _ =>
            withSQLConf(arm.confs: _*) {
              query().collect()
            }
          }
        }
        benchmark.run()
      }
    }
  }
}
