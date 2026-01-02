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

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
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
            multiKeyAggregates ++ multiAggregates ++ decimalAggregates ++ highCardinalityAggregates

          allAggregates.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
