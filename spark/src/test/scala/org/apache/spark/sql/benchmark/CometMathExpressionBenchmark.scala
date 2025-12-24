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

case class MathExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

// spotless:off
/**
 * Comprehensive benchmark for Comet math expressions. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometMathExpressionBenchmark`
 * Results will be written to "spark/benchmarks/CometMathExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometMathExpressionBenchmark extends CometBenchmarkBase {

  // Unary math functions
  private val unaryMathFunctions = List(
    "abs" -> "ABS(c_double)",
    "ceil" -> "CEIL(c_double)",
    "floor" -> "FLOOR(c_double)",
    "round" -> "ROUND(c_double)",
    "round_scale" -> "ROUND(c_double, 2)",
    "hex_int" -> "HEX(c_int)",
    "hex_long" -> "HEX(c_long)",
    "unhex" -> "UNHEX(c_hex_str)",
    "unary_minus" -> "-(c_double)")

  // Binary math functions
  private val binaryMathFunctions =
    List("atan2" -> "ATAN2(c_double, c_double2)", "pmod" -> "PMOD(c_int, c_int2)")

  // Logarithm functions
  private val logFunctions = List(
    "log" -> "LN(c_positive_double)",
    "log10" -> "LOG10(c_positive_double)",
    "log2" -> "LOG2(c_positive_double)")

  private def generateConfigs(funcs: List[(String, String)]): List[MathExprConfig] = {
    funcs.map { case (name, expr) =>
      MathExprConfig(name, s"SELECT $expr FROM parquetV1Table")
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 * 5 // 5M rows

    // Benchmark unary and binary math functions
    runBenchmarkWithTable("Math expression benchmarks", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 100 = 0 THEN NULL ELSE CAST((value - 2500000) AS INT) END AS c_int,
                CASE WHEN value % 100 = 1 THEN NULL ELSE CAST((value % 1000) - 500 AS INT) END AS c_int2,
                CASE WHEN value % 100 = 2 THEN NULL ELSE CAST(value * 1000 AS LONG) END AS c_long,
                CASE
                  WHEN value % 100 = 3 THEN NULL
                  WHEN value % 100 = 4 THEN CAST('NaN' AS DOUBLE)
                  WHEN value % 100 = 5 THEN CAST('Infinity' AS DOUBLE)
                  WHEN value % 100 = 6 THEN CAST('-Infinity' AS DOUBLE)
                  ELSE CAST((value - 2500000) / 100.0 AS DOUBLE)
                END AS c_double,
                CASE WHEN value % 100 = 7 THEN NULL ELSE CAST((value % 1000) - 500 AS DOUBLE) END AS c_double2,
                CASE WHEN value % 100 = 8 THEN NULL ELSE CAST(ABS((value - 2500000) / 100.0) + 1.0 AS DOUBLE) END AS c_positive_double,
                CASE WHEN value % 100 = 9 THEN NULL ELSE HEX(CAST(value % 1000000 AS LONG)) END AS c_hex_str
              FROM $tbl
            """))

          (generateConfigs(unaryMathFunctions) ++
            generateConfigs(binaryMathFunctions) ++
            generateConfigs(logFunctions)).foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
