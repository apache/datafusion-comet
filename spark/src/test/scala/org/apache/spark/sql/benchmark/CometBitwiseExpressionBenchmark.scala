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

case class BitwiseExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

// spotless:off
/**
 * Comprehensive benchmark for Comet bitwise expressions. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometBitwiseExpressionBenchmark`
 * Results will be written to "spark/benchmarks/CometBitwiseExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometBitwiseExpressionBenchmark extends CometBenchmarkBase {

  private val bitwiseExpressions = List(
    BitwiseExprConfig("bitwise_and", "SELECT c_int & c_int2 FROM parquetV1Table"),
    BitwiseExprConfig("bitwise_or", "SELECT c_int | c_int2 FROM parquetV1Table"),
    BitwiseExprConfig("bitwise_xor", "SELECT c_int ^ c_int2 FROM parquetV1Table"),
    BitwiseExprConfig("bitwise_not", "SELECT ~c_int FROM parquetV1Table"),
    BitwiseExprConfig("shift_left", "SELECT SHIFTLEFT(c_int, 3) FROM parquetV1Table"),
    BitwiseExprConfig("shift_right", "SELECT SHIFTRIGHT(c_int, 3) FROM parquetV1Table"),
    BitwiseExprConfig(
      "shift_right_unsigned",
      "SELECT SHIFTRIGHTUNSIGNED(c_int, 3) FROM parquetV1Table"),
    BitwiseExprConfig("bit_count", "SELECT BIT_COUNT(c_long) FROM parquetV1Table"),
    BitwiseExprConfig("bit_get", "SELECT BIT_GET(c_long, c_pos) FROM parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = getBenchmarkRows(1024 * 1024 * 5) // 5M rows default

    runBenchmarkWithTable("Bitwise expression benchmarks", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 100 = 0 THEN NULL ELSE CAST(value % 1000000 AS INT) END AS c_int,
                CASE WHEN value % 100 = 1 THEN NULL ELSE CAST((value * 7) % 1000000 AS INT) END AS c_int2,
                CASE WHEN value % 100 = 2 THEN NULL ELSE CAST(value * 1000 AS LONG) END AS c_long,
                CASE WHEN value % 100 = 3 THEN NULL ELSE CAST(value % 64 AS INT) END AS c_pos
              FROM $tbl
            """))

          bitwiseExpressions.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
