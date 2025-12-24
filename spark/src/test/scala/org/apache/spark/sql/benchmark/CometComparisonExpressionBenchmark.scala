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

case class ComparisonExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

// spotless:off
/**
 * Comprehensive benchmark for Comet comparison and predicate expressions. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometComparisonExpressionBenchmark`
 * Results will be written to "spark/benchmarks/CometComparisonExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometComparisonExpressionBenchmark extends CometBenchmarkBase {

  private val comparisonExpressions = List(
    ComparisonExprConfig("equal_to", "SELECT c_int = c_int2 FROM parquetV1Table"),
    ComparisonExprConfig("not_equal_to", "SELECT c_int != c_int2 FROM parquetV1Table"),
    ComparisonExprConfig("less_than", "SELECT c_int < c_int2 FROM parquetV1Table"),
    ComparisonExprConfig("less_than_or_equal", "SELECT c_int <= c_int2 FROM parquetV1Table"),
    ComparisonExprConfig("greater_than", "SELECT c_int > c_int2 FROM parquetV1Table"),
    ComparisonExprConfig("greater_than_or_equal", "SELECT c_int >= c_int2 FROM parquetV1Table"),
    ComparisonExprConfig("equal_null_safe", "SELECT c_int <=> c_int2 FROM parquetV1Table"),
    ComparisonExprConfig("is_null", "SELECT c_int IS NULL FROM parquetV1Table"),
    ComparisonExprConfig("is_not_null", "SELECT c_int IS NOT NULL FROM parquetV1Table"),
    ComparisonExprConfig("is_nan_float", "SELECT isnan(c_float) FROM parquetV1Table"),
    ComparisonExprConfig("is_nan_double", "SELECT isnan(c_double) FROM parquetV1Table"),
    ComparisonExprConfig("and", "SELECT (c_int > 0) AND (c_int2 < 100) FROM parquetV1Table"),
    ComparisonExprConfig("or", "SELECT (c_int > 0) OR (c_int2 < 100) FROM parquetV1Table"),
    ComparisonExprConfig("not", "SELECT NOT (c_int > 0) FROM parquetV1Table"),
    ComparisonExprConfig(
      "in_list",
      "SELECT c_int IN (1, 10, 100, 1000, 10000) FROM parquetV1Table"),
    ComparisonExprConfig(
      "not_in_list",
      "SELECT c_int NOT IN (1, 10, 100, 1000, 10000) FROM parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = getBenchmarkRows(1024 * 1024 * 5) // 5M rows default

    runBenchmarkWithTable("Comparison expression benchmarks", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 10 = 0 THEN NULL ELSE CAST((value % 100000) - 50000 AS INT) END AS c_int,
                CASE WHEN value % 10 = 1 THEN NULL ELSE CAST((value % 1000) AS INT) END AS c_int2,
                CASE
                  WHEN value % 50 = 2 THEN NULL
                  WHEN value % 50 = 3 THEN CAST('NaN' AS FLOAT)
                  ELSE CAST((value % 10000) / 100.0 AS FLOAT)
                END AS c_float,
                CASE
                  WHEN value % 50 = 4 THEN NULL
                  WHEN value % 50 = 5 THEN CAST('NaN' AS DOUBLE)
                  ELSE CAST((value % 10000) / 100.0 AS DOUBLE)
                END AS c_double
              FROM $tbl
            """))

          comparisonExpressions.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
