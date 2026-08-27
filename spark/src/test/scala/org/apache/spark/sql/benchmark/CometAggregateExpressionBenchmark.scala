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

import org.apache.spark.sql.comet.CometHashAggregateExec
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

case class AggExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Comprehensive benchmark for Comet aggregate functions. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometAggregateExpressionBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometAggregateFunctionBenchmark-**results.txt".
 */
object CometAggregateExpressionBenchmark extends CometBenchmarkBase {

  /**
   * Run with `--wide-decimal-shuffle` on both revisions to measure the cost of routing wide
   * decimal hash keys through Spark's partition assignments. Fixture generation, result checks,
   * and plan reporting are not timed. The benchmark session uses local[1]. Add `--reverse` to
   * reverse the order of the native and auto cases, or `--validate-only` to check the fixture,
   * results and plans without collecting timings.
   */
  private def wideDecimalShuffleBenchmark(reverse: Boolean, validateOnly: Boolean): Unit = {
    val rows = 1024 * 1024
    val groups = 10000
    val partitions = 4
    val filePartitionBytes = 16 * 1024 * 1024
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> partitions.toString,
      // Keep each file in one split, and prevent Spark from combining files into one task.
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> filePartitionBytes.toString,
      SQLConf.FILES_OPEN_COST_IN_BYTES.key -> filePartitionBytes.toString) {
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark
              .range(0L, rows.toLong, 1L, partitions)
              .selectExpr(
                s"CAST(id % $groups AS DECIMAL(38, 2)) AS k",
                "CAST(id % 97 AS DECIMAL(20, 2)) AS v"))
          val query = "SELECT k, AVG(v) FROM parquetV1Table GROUP BY k"
          val expected = withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            val input = spark.table("parquetV1Table")
            assert(input.inputFiles.length == partitions)
            val inputPartitions = input.rdd.getNumPartitions
            assert(
              inputPartitions == partitions,
              s"Expected $partitions inputs, got $inputPartitions")
            val df = spark.sql(query)
            val result = df.collect()
            assert(result.length == groups)
            println(
              "Wide-decimal Spark baseline plan:\n" + df.queryExecution.executedPlan.treeString)
            result.toSet
          }
          val modes = Seq("native", "auto")
          for (mode <- (if (reverse) modes.reverse else modes)) {
            val configs = Map(
              CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
              CometConf.COMET_SHUFFLE_MODE.key -> mode)
            withSQLConf(
              (configs.toSeq ++ Seq(
                CometConf.COMET_ENABLED.key -> "true",
                CometConf.COMET_EXEC_ENABLED.key -> "true")): _*) {
              val df = spark.sql(query)
              val result = df.collect()
              assert(result.length == groups)
              assert(result.toSet == expected, s"Wide-decimal results differ from Spark: $mode")
              val plan = df.queryExecution.executedPlan
              val cometShuffles = plan.collect { case exchange: CometShuffleExchangeExec =>
                exchange.shuffleType.toString
              }
              val sparkShuffles = plan.collect { case _: ShuffleExchangeExec => 1 }.sum
              val nativeAggregates = plan.collect { case _: CometHashAggregateExec => 1 }.sum
              assert(cometShuffles.size + sparkShuffles == 1)
              // Report the actual routing, including the expected Spark aggregate fallback in
              // native-only mode. A faster result does not justify an incompatible hash key.
              println(s"Wide-decimal shuffle mode=$mode, rows=$rows, groups=$groups, " +
                s"inputPartitions=$partitions, shufflePartitions=$partitions, " +
                s"filePartitionBytes=$filePartitionBytes, fileOpenCostBytes=$filePartitionBytes, " +
                s"master=${spark.sparkContext.master}, sparkVersion=${spark.version}, " +
                s"resultMatchesSpark=true, cometShuffles=${cometShuffles.mkString(",")}, " +
                s"sparkShuffles=$sparkShuffles, nativeAggregates=$nativeAggregates")
              println(plan.treeString)
            }
            if (!validateOnly) {
              runExpressionBenchmark(s"wide_decimal_shuffle_$mode", rows, query, configs)
            }
          }
        }
      }
    }
  }

  private val basicAggregates = List(
    AggExprConfig("count", "SELECT COUNT(*) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("count_col", "SELECT COUNT(c_int) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "count_distinct",
      "SELECT COUNT(DISTINCT c_int) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("min_int", "SELECT MIN(c_int) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("max_int", "SELECT MAX(c_int) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("min_double", "SELECT MIN(c_double) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("max_double", "SELECT MAX(c_double) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("sum_int", "SELECT SUM(c_int) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("sum_long", "SELECT SUM(c_long) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("sum_double", "SELECT SUM(c_double) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("avg_int", "SELECT AVG(c_int) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("avg_double", "SELECT AVG(c_double) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("first", "SELECT FIRST(c_int) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "first_ignore_nulls",
      "SELECT FIRST(c_int, true) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("last", "SELECT LAST(c_int) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "last_ignore_nulls",
      "SELECT LAST(c_int, true) FROM parquetV1Table GROUP BY grp"))

  private val statisticalAggregates = List(
    AggExprConfig("var_samp", "SELECT VAR_SAMP(c_double) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("var_pop", "SELECT VAR_POP(c_double) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("stddev_samp", "SELECT STDDEV_SAMP(c_double) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("stddev_pop", "SELECT STDDEV_POP(c_double) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "covar_samp",
      "SELECT COVAR_SAMP(c_double, c_double2) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "covar_pop",
      "SELECT COVAR_POP(c_double, c_double2) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("corr", "SELECT CORR(c_double, c_double2) FROM parquetV1Table GROUP BY grp"))

  private val bitwiseAggregates = List(
    AggExprConfig("bit_and", "SELECT BIT_AND(c_long) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("bit_or", "SELECT BIT_OR(c_long) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("bit_xor", "SELECT BIT_XOR(c_long) FROM parquetV1Table GROUP BY grp"))

  // Additional structural tests (multiple group keys, multiple aggregates)
  private val multiKeyAggregates = List(
    AggExprConfig("sum_multi_key", "SELECT SUM(c_int) FROM parquetV1Table GROUP BY grp, grp2"),
    AggExprConfig("avg_multi_key", "SELECT AVG(c_double) FROM parquetV1Table GROUP BY grp, grp2"))

  private val multiAggregates = List(
    AggExprConfig("sum_sum", "SELECT SUM(c_int), SUM(c_long) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("min_max", "SELECT MIN(c_int), MAX(c_int) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "count_sum_avg",
      "SELECT COUNT(*), SUM(c_int), AVG(c_double) FROM parquetV1Table GROUP BY grp"))

  // Decimal aggregates
  private val decimalAggregates = List(
    AggExprConfig("sum_decimal", "SELECT SUM(c_decimal) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("avg_decimal", "SELECT AVG(c_decimal) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("min_decimal", "SELECT MIN(c_decimal) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig("max_decimal", "SELECT MAX(c_decimal) FROM parquetV1Table GROUP BY grp"))

  // High cardinality tests
  private val highCardinalityAggregates = List(
    AggExprConfig(
      "sum_high_card",
      "SELECT SUM(c_int) FROM parquetV1Table GROUP BY high_card_grp"),
    AggExprConfig(
      "count_distinct_high_card",
      "SELECT COUNT(DISTINCT c_int) FROM parquetV1Table GROUP BY high_card_grp"))

  // Exact percentile. Only the single-percentage, default-frequency, numeric-input form runs
  // natively through Comet's Spark-compatible percentile UDAF; other forms fall back to Spark.
  private val percentileAggregates = List(
    AggExprConfig(
      "percentile_int_median",
      "SELECT percentile(c_int, 0.5) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "percentile_long_median",
      "SELECT percentile(c_long, 0.5) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "percentile_double_median",
      "SELECT percentile(c_double, 0.5) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "percentile_double_p90",
      "SELECT percentile(c_double, 0.9) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "percentile_double_global",
      "SELECT percentile(c_double, 0.5) FROM parquetV1Table"),
    AggExprConfig(
      "percentile_double_high_card",
      "SELECT percentile(c_double, 0.5) FROM parquetV1Table GROUP BY high_card_grp"))

  // approx_count_distinct (Spark's HyperLogLogPlusPlus). c_int has ~10000 distinct values.
  private val approxCountDistinctAggregates = List(
    AggExprConfig(
      "approx_count_distinct_int",
      "SELECT approx_count_distinct(c_int) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "approx_count_distinct_string",
      "SELECT approx_count_distinct(CAST(c_int AS STRING)) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "approx_count_distinct_global",
      "SELECT approx_count_distinct(c_int) FROM parquetV1Table"),
    AggExprConfig(
      "approx_count_distinct_high_card",
      "SELECT approx_count_distinct(c_int) FROM parquetV1Table GROUP BY high_card_grp"))

  // Approximate percentile (Greenwald-Khanna). All numeric input types and the
  // scalar, array, and explicit-accuracy forms run natively.
  private val approxPercentileAggregates = List(
    AggExprConfig(
      "approx_percentile_int_median",
      "SELECT approx_percentile(c_int, 0.5) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "approx_percentile_long_median",
      "SELECT approx_percentile(c_long, 0.5) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "approx_percentile_double_median",
      "SELECT approx_percentile(c_double, 0.5) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "approx_percentile_double_p90",
      "SELECT approx_percentile(c_double, 0.9) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "approx_percentile_double_array",
      "SELECT approx_percentile(c_double, array(0.25, 0.5, 0.75)) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "approx_percentile_double_accuracy",
      "SELECT approx_percentile(c_double, 0.5, 100) FROM parquetV1Table GROUP BY grp"),
    AggExprConfig(
      "approx_percentile_double_global",
      "SELECT approx_percentile(c_double, 0.5) FROM parquetV1Table"),
    AggExprConfig(
      "approx_percentile_double_high_card",
      "SELECT approx_percentile(c_double, 0.5) FROM parquetV1Table GROUP BY high_card_grp"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    if (mainArgs.contains("--wide-decimal-shuffle")) {
      wideDecimalShuffleBenchmark(
        mainArgs.contains("--reverse"),
        mainArgs.contains("--validate-only"))
      return
    }
    val values = 1024 * 1024

    runBenchmarkWithTable("Aggregate function benchmarks", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CAST(value % 1000 AS INT) AS grp,
                CAST(value % 100 AS INT) AS grp2,
                CAST(value % 100000 AS INT) AS high_card_grp,
                CASE WHEN value % 100 = 0 THEN NULL ELSE CAST((value % 10000) - 5000 AS INT) END AS c_int,
                CASE WHEN value % 100 = 1 THEN NULL ELSE CAST(value * 1000 AS LONG) END AS c_long,
                CASE WHEN value % 100 = 2 THEN NULL ELSE CAST((value % 10000) / 100.0 AS DOUBLE) END AS c_double,
                CASE WHEN value % 100 = 3 THEN NULL ELSE CAST((value % 5000) / 50.0 AS DOUBLE) END AS c_double2,
                CASE WHEN value % 100 = 4 THEN NULL ELSE CAST((value % 10000 - 5000) / 100.0 AS DECIMAL(18, 10)) END AS c_decimal
              FROM $tbl
            """))

          val allAggregates = basicAggregates ++ statisticalAggregates ++ bitwiseAggregates ++
            multiKeyAggregates ++ multiAggregates ++ decimalAggregates ++
            highCardinalityAggregates ++ percentileAggregates ++ approxPercentileAggregates ++
            approxCountDistinctAggregates

          allAggregates.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
