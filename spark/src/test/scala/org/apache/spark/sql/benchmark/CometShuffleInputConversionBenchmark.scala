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
import org.apache.spark.sql.comet.{CometPlan, CometSparkToColumnarExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions.{col, count, length, lit, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, DecimalType, IntegerType, LongType, StructType}

import org.apache.comet.CometConf

// Top-level, so the encoder needs no outer pointer.
case class ShuffleInputBenchRec(a: Long, b: String)

/**
 * Compares three ways to run a shuffle whose input comes from a Spark operator:
 *
 *   - Spark: Comet disabled.
 *   - Comet: the default. Comet's JVM columnar shuffle buffers the rows and converts them to
 *     Arrow natively, one partition at a time.
 *   - Comet, converted: `spark.comet.convert.shuffleInput.enabled`, which converts the rows with
 *     `CometSparkToColumnarExec` and uses native shuffle.
 *
 * Each case partitions its rows into 200 partitions and aggregates them. Most hash-partition on a
 * number, because the conversion leaves a shuffle that hashes a string on the JVM columnar
 * shuffle, and one range-partitions. Every arm's result and plan are checked before it is timed,
 * and the Comet arm runs again at the end of each case to show the noise. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometShuffleInputConversionBenchmark
 * }}}
 * Results will be written to
 * "spark/benchmarks/CometShuffleInputConversionBenchmark-**results.txt".
 */
object CometShuffleInputConversionBenchmark extends CometBenchmarkBase {

  private val numRows = 4L * 1024 * 1024
  private val numPartitions = 200

  import spark.implicits._

  private case class Arm(name: String, confs: Seq[(String, String)], check: SparkPlan => Unit)

  // No AQE, so every arm plans the same shape on every iteration.
  private val planConfs = Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false")

  private def cometConfs(convert: Boolean) = planConfs ++ Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    // Spark scans the RDDs, as it does by default, so the shuffle reads a Spark operator.
    CometConf.COMET_CONVERT_FROM_RDD_ENABLED.key -> "false",
    CometConf.COMET_CONVERT_FROM_SHUFFLE_INPUT_ENABLED.key -> convert.toString)

  private def shuffles(plan: SparkPlan): Seq[CometShuffleExchangeExec] =
    collect(plan) { case s: CometShuffleExchangeExec => s }

  private val arms = Seq(
    Arm(
      "Spark",
      planConfs :+ (CometConf.COMET_ENABLED.key -> "false"),
      plan => assert(collect(plan) { case c: CometPlan => c }.isEmpty, plan)),
    Arm(
      "Comet",
      cometConfs(convert = false),
      plan => assert(shuffles(plan).exists(_.shuffleType == CometColumnarShuffle), plan)),
    Arm(
      "Comet, converted",
      cometConfs(convert = true),
      plan =>
        assert(
          shuffles(plan).exists(s =>
            s.shuffleType == CometNativeShuffle && s.child
              .isInstanceOf[CometSparkToColumnarExec]),
          plan)))

  private val rowSchema = new StructType()
    .add("k", IntegerType)
    .add("l", LongType)
    .add("s", DataTypes.StringType)
    .add("d", DataTypes.DoubleType)
    .add("m", DecimalType(18, 2))

  /** `numRows` rows from an RDD, generated as the query runs. */
  private def rddRows(): DataFrame = {
    val rows = spark.sparkContext.range(0, numRows, 1, 1).map { id =>
      val i = id.toInt
      Row(
        i % 1000,
        id,
        s"value-${i * 31 % 100003}",
        i + 0.5d,
        java.math.BigDecimal.valueOf(i * 7919L % 1000000000L, 2))
    }
    spark.createDataFrame(rows, rowSchema)
  }

  /** Totals the per-group results in `s`, so each case returns one row. */
  private def total(grouped: DataFrame): DataFrame = grouped.agg(sum("s"), count(lit(1)))

  private val cases: Seq[(String, () => DataFrame)] = Seq(
    "RDD rows: int, long, double" -> (() =>
      total(
        rddRows()
          .select("k", "l", "d")
          .repartition(numPartitions, col("k"))
          .groupBy("k")
          .agg((sum("l") + sum("d")).as("s")))),
    "RDD rows: int, long, string, double, decimal(18,2)" -> (() =>
      total(
        rddRows()
          .repartition(numPartitions, col("k"))
          .groupBy("k")
          .agg((sum("l") + sum(length(col("s"))) + sum("d") + sum("m")).as("s")))),
    "RDD rows: int, long, string, double, decimal(18,2), range partitioned" -> (() =>
      total(
        rddRows()
          .repartitionByRange(numPartitions, col("l"))
          .select((col("k") + col("l") + length(col("s")) + col("d").cast("long") + col("m"))
            .as("s")))),
    "map over a Parquet scan" -> (() =>
      total(
        spark
          .table("parquetV1Table")
          .select(col("id").as("a"), col("key").as("b"))
          .as[ShuffleInputBenchRec]
          .map(r => ShuffleInputBenchRec(r.a % 1000, r.b))
          .repartition(numPartitions, col("a"))
          .groupBy("a")
          .agg(sum(length(col("b"))).as("s")))))

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
          spark.range(numRows).selectExpr("id", "CAST(id % 1000 AS STRING) AS key"))

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
