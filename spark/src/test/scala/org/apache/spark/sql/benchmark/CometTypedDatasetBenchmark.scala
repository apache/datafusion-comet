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
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.comet.{CometHashAggregateExec, CometPlan, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions.{col, count, length, lit, sum}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

// Top-level, so the encoders need no outer pointer, which is the ordinary user shape.
case class TypedDatasetBenchRec(a: Long, b: String)

case class TypedDatasetBenchWide(a: Long, b: String, c: Long, d: String)

/**
 * Compares three ways to run a query over the output of a typed Dataset operation:
 *
 *   - Spark: Comet disabled.
 *   - Comet: the default. The typed operation runs in Spark, and Comet takes over again at the
 *     shuffle above it, so the operators in between stay on Spark.
 *   - Comet, converted: `spark.comet.convert.typedDataset.enabled`, which converts the output of
 *     the typed operation to Arrow so the operators above it run natively.
 *
 * The cases sweep how much work sits above the typed operation, from an aggregate over 100 groups
 * that Spark's whole-stage codegen fuses with the operation to one over a million groups. Every
 * arm's result and plan are checked before it is timed, and the Comet arm runs again at the end
 * of each case to show the noise. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometTypedDatasetBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometTypedDatasetBenchmark-**results.txt".
 */
object CometTypedDatasetBenchmark extends CometBenchmarkBase {

  private val numRows = 4L * 1024 * 1024
  private val loKeys = 100
  private val hiKeys = 1024 * 1024

  import spark.implicits._

  private case class Arm(name: String, confs: Seq[(String, String)], check: SparkPlan => Unit)

  // No AQE and one shuffle partition, so every arm plans the same shape on every iteration.
  private val planConfs =
    Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", SQLConf.SHUFFLE_PARTITIONS.key -> "1")

  private def cometConfs(convert: Boolean) = planConfs ++ Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_CONVERT_FROM_TYPED_DATASET_ENABLED.key -> convert.toString)

  private def conversions(plan: SparkPlan): Seq[CometSparkToColumnarExec] =
    collect(plan) { case c: CometSparkToColumnarExec => c }

  private val arms = Seq(
    Arm(
      "Spark",
      planConfs :+ (CometConf.COMET_ENABLED.key -> "false"),
      plan => assert(collect(plan) { case c: CometPlan => c }.isEmpty, plan)),
    Arm("Comet", cometConfs(convert = false), plan => assert(conversions(plan).isEmpty, plan)),
    Arm(
      "Comet, converted",
      cometConfs(convert = true),
      plan => {
        assert(conversions(plan).nonEmpty, plan)
        assert(
          collect(plan) {
            case a: CometHashAggregateExec if a.modes.contains(Partial) => a
          }.nonEmpty,
          plan)
      }))

  /** The table read back as a typed Dataset, grouping on `keys`. */
  private def typed(keys: String): Dataset[TypedDatasetBenchRec] =
    spark
      .table("parquetV1Table")
      .select(col("id").as("a"), col(keys).as("b"))
      .as[TypedDatasetBenchRec]

  /** Totals the per-group results in `s`, so each case returns one row. */
  private def total(grouped: DataFrame): DataFrame = grouped.agg(sum("s"), count(lit(1)))

  private def mapThenGroup(keys: String): DataFrame =
    total(
      typed(keys).map(r => TypedDatasetBenchRec(r.a + 1, r.b)).groupBy("b").agg(sum("a").as("s")))

  private def mapThenFilterThenGroup(keys: String): DataFrame =
    total(
      typed(keys)
        .map(r => TypedDatasetBenchRec(r.a * 2, r.b))
        .filter(col("a") % 3 === 0)
        .groupBy("b")
        .agg(sum("a").as("s")))

  private val cases: Seq[(String, () => DataFrame)] = Seq(
    "map -> group by 100 keys" -> (() => mapThenGroup("lo")),
    "map -> group by 1M keys" -> (() => mapThenGroup("hi")),
    "map -> filter -> group by 100 keys" -> (() => mapThenFilterThenGroup("lo")),
    "map -> filter -> group by 1M keys" -> (() => mapThenFilterThenGroup("hi")),
    "map -> group by long key, 1M keys" -> (() =>
      total(
        typed("lo")
          .map(r => TypedDatasetBenchRec(r.a % hiKeys, r.b))
          .groupBy("a")
          .agg(sum(length(col("b"))).as("s")))),
    "map, 4 columns -> group by 1M keys" -> (() =>
      total(
        typed("hi")
          .map(r => TypedDatasetBenchWide(r.a + 1, r.b, r.a * 2, r.b + "!"))
          .groupBy("b")
          .agg((sum("a") + sum("c") + sum(length(col("d")))).as("s")))),
    "mapPartitions -> group by 1M keys" -> (() =>
      total(
        typed("hi")
          .mapPartitions(_.map(r => TypedDatasetBenchRec(r.a + 1, r.b)))
          .groupBy("b")
          .agg(sum("a").as("s")))))

  private def runArm(arm: Arm, query: () => DataFrame): (Seq[Row], SparkPlan) = {
    var result: (Seq[Row], SparkPlan) = null
    withSQLConf(arm.confs: _*) {
      val df = query()
      result = (df.collect().toSeq, df.queryExecution.executedPlan)
    }
    result
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark
            .range(numRows)
            .selectExpr(
              "id",
              s"CAST(id % $loKeys AS STRING) AS lo",
              s"CAST(id % $hiKeys AS STRING) AS hi"))

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
            (arms :+ arms(1).copy(name = "Comet (repeat)")).foreach { arm =>
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
  }
}
