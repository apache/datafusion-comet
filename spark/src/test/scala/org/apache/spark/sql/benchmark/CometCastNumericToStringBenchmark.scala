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

case class CastNumericToStringConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet cast from numeric types to String. To run this
 * benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCastNumericToStringBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometCastNumericToStringBenchmark-**results.txt".
 */
object CometCastNumericToStringBenchmark extends CometBenchmarkBase {

  private val castFunctions = Seq("CAST", "TRY_CAST")
  private val sourceTypes =
    Seq(
      ("BOOLEAN", "c_bool"),
      ("BYTE", "c_byte"),
      ("SHORT", "c_short"),
      ("INT", "c_int"),
      ("LONG", "c_long"),
      ("FLOAT", "c_float"),
      ("DOUBLE", "c_double"),
      ("DECIMAL(10,2)", "c_decimal"))

  private val castConfigs = for {
    castFunc <- castFunctions
    (sourceType, colName) <- sourceTypes
  } yield CastNumericToStringConfig(
    s"$castFunc $sourceType to String",
    s"SELECT $castFunc($colName AS STRING) FROM parquetV1Table")

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 // 1M rows

    // Generate input data once with all numeric types
    runBenchmarkWithTable("Numeric to String casts", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // Data distribution: 1% NULL per column
          // - c_bool: 50/50 true/false
          // - c_byte: full range -64 to 63
          // - c_short: full range -16384 to 16383
          // - c_int/c_long: large values centered around 0
          // - c_float/c_double: 3% special values (NaN/Infinity), rest centered around 0
          // - c_decimal: values from -25000.00 to +25000.00
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 100 = 0 THEN NULL ELSE (value % 2 = 0) END AS c_bool,
                CASE WHEN value % 100 = 1 THEN NULL ELSE CAST((value % 128) - 64 AS BYTE) END AS c_byte,
                CASE WHEN value % 100 = 2 THEN NULL ELSE CAST((value % 32768) - 16384 AS SHORT) END AS c_short,
                CASE WHEN value % 100 = 3 THEN NULL ELSE CAST(value - 2500000 AS INT) END AS c_int,
                CASE WHEN value % 100 = 4 THEN NULL ELSE CAST(value * 1000000 AS LONG) END AS c_long,
                CASE
                  WHEN value % 100 = 5 THEN NULL
                  WHEN value % 100 = 6 THEN CAST('NaN' AS FLOAT)
                  WHEN value % 100 = 7 THEN CAST('Infinity' AS FLOAT)
                  WHEN value % 100 = 8 THEN CAST('-Infinity' AS FLOAT)
                  ELSE CAST((value - 2500000) / 1000.0 AS FLOAT)
                END AS c_float,
                CASE
                  WHEN value % 100 = 9 THEN NULL
                  WHEN value % 100 = 10 THEN CAST('NaN' AS DOUBLE)
                  WHEN value % 100 = 11 THEN CAST('Infinity' AS DOUBLE)
                  WHEN value % 100 = 12 THEN CAST('-Infinity' AS DOUBLE)
                  ELSE CAST((value - 2500000) / 100.0 AS DOUBLE)
                END AS c_double,
                CASE WHEN value % 100 = 13 THEN NULL ELSE CAST((value - 2500000) / 100.0 AS DECIMAL(10,2)) END AS c_decimal
              FROM $tbl
            """))

          castConfigs.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
