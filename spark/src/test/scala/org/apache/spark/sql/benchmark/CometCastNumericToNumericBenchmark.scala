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

case class CastNumericToNumericConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet cast between numeric types. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCastNumericToNumericBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometCastNumericToNumericBenchmark-**results.txt".
 */
object CometCastNumericToNumericBenchmark extends CometBenchmarkBase {

  private val castFunctions = Seq("CAST", "TRY_CAST")

  // Integer widening conversions
  private val integerWideningPairs = Seq(
    ("BYTE", "c_byte", "SHORT"),
    ("BYTE", "c_byte", "INT"),
    ("BYTE", "c_byte", "LONG"),
    ("SHORT", "c_short", "INT"),
    ("SHORT", "c_short", "LONG"),
    ("INT", "c_int", "LONG"))

  // Integer narrowing conversions
  private val integerNarrowingPairs = Seq(
    ("LONG", "c_long", "INT"),
    ("LONG", "c_long", "SHORT"),
    ("LONG", "c_long", "BYTE"),
    ("INT", "c_int", "SHORT"),
    ("INT", "c_int", "BYTE"),
    ("SHORT", "c_short", "BYTE"))

  // Floating point conversions
  private val floatPairs = Seq(("FLOAT", "c_float", "DOUBLE"), ("DOUBLE", "c_double", "FLOAT"))

  // Integer to floating point conversions
  private val intToFloatPairs = Seq(
    ("BYTE", "c_byte", "FLOAT"),
    ("SHORT", "c_short", "FLOAT"),
    ("INT", "c_int", "FLOAT"),
    ("LONG", "c_long", "FLOAT"),
    ("INT", "c_int", "DOUBLE"),
    ("LONG", "c_long", "DOUBLE"))

  // Floating point to integer conversions
  private val floatToIntPairs = Seq(
    ("FLOAT", "c_float", "INT"),
    ("FLOAT", "c_float", "LONG"),
    ("DOUBLE", "c_double", "INT"),
    ("DOUBLE", "c_double", "LONG"))

  // Decimal conversions
  private val decimalPairs = Seq(
    ("INT", "c_int", "DECIMAL(10,2)"),
    ("LONG", "c_long", "DECIMAL(20,4)"),
    ("DOUBLE", "c_double", "DECIMAL(15,5)"),
    ("DECIMAL(10,2)", "c_decimal", "INT"),
    ("DECIMAL(10,2)", "c_decimal", "LONG"),
    ("DECIMAL(10,2)", "c_decimal", "DOUBLE"))

  private def generateConfigs(
      pairs: Seq[(String, String, String)]): Seq[CastNumericToNumericConfig] = {
    for {
      castFunc <- castFunctions
      (sourceType, colName, targetType) <- pairs
    } yield CastNumericToNumericConfig(
      s"$castFunc $sourceType to $targetType",
      s"SELECT $castFunc($colName AS $targetType) FROM parquetV1Table")
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 // 1M rows

    // Generate input data once with all numeric types
    runBenchmarkWithTable("Numeric to Numeric casts", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // Data distribution: 1% NULL per column
          // - c_byte: full range -64 to 63
          // - c_short: full range -16384 to 16383
          // - c_int: centered around 0 (-2.5M to +2.5M)
          // - c_long: large positive values (0 to ~5 billion)
          // - c_float/c_double: 4% special values (NaN/Infinity), rest centered around 0
          // - c_decimal: values from -25000.00 to +25000.00
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 100 = 0 THEN NULL ELSE CAST((value % 128) - 64 AS BYTE) END AS c_byte,
                CASE WHEN value % 100 = 1 THEN NULL ELSE CAST((value % 32768) - 16384 AS SHORT) END AS c_short,
                CASE WHEN value % 100 = 2 THEN NULL ELSE CAST(value - 2500000 AS INT) END AS c_int,
                CASE WHEN value % 100 = 3 THEN NULL ELSE CAST(value * 1000 AS LONG) END AS c_long,
                CASE
                  WHEN value % 100 = 4 THEN NULL
                  WHEN value % 100 = 5 THEN CAST('NaN' AS FLOAT)
                  WHEN value % 100 = 6 THEN CAST('Infinity' AS FLOAT)
                  WHEN value % 100 = 7 THEN CAST('-Infinity' AS FLOAT)
                  ELSE CAST((value - 2500000) / 100.0 AS FLOAT)
                END AS c_float,
                CASE
                  WHEN value % 100 = 8 THEN NULL
                  WHEN value % 100 = 9 THEN CAST('NaN' AS DOUBLE)
                  WHEN value % 100 = 10 THEN CAST('Infinity' AS DOUBLE)
                  WHEN value % 100 = 11 THEN CAST('-Infinity' AS DOUBLE)
                  ELSE CAST((value - 2500000) / 100.0 AS DOUBLE)
                END AS c_double,
                CASE WHEN value % 100 = 12 THEN NULL ELSE CAST((value - 2500000) / 100.0 AS DECIMAL(10,2)) END AS c_decimal
              FROM $tbl
            """))

          // Run all benchmark categories
          (generateConfigs(integerWideningPairs) ++
            generateConfigs(integerNarrowingPairs) ++
            generateConfigs(floatPairs) ++
            generateConfigs(intToFloatPairs) ++
            generateConfigs(floatToIntPairs) ++
            generateConfigs(decimalPairs)).foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
