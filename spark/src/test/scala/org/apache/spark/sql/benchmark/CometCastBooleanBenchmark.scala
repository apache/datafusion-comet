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

case class CastBooleanConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet cast operations involving Boolean type. To run this
 * benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCastBooleanBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometCastBooleanBenchmark-**results.txt".
 */
object CometCastBooleanBenchmark extends CometBenchmarkBase {

  private val castFunctions = Seq("CAST", "TRY_CAST")

  // Boolean to String
  private val boolToStringConfigs = for {
    castFunc <- castFunctions
  } yield CastBooleanConfig(
    s"$castFunc Boolean to String",
    s"SELECT $castFunc(c_bool AS STRING) FROM parquetV1Table")

  // Boolean to numeric types
  private val boolToNumericTypes =
    Seq("BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DECIMAL(10,2)")
  private val boolToNumericConfigs = for {
    castFunc <- castFunctions
    targetType <- boolToNumericTypes
  } yield CastBooleanConfig(
    s"$castFunc Boolean to $targetType",
    s"SELECT $castFunc(c_bool AS $targetType) FROM parquetV1Table")

  // Numeric to Boolean
  private val numericTypes = Seq(
    ("BYTE", "c_byte"),
    ("SHORT", "c_short"),
    ("INT", "c_int"),
    ("LONG", "c_long"),
    ("FLOAT", "c_float"),
    ("DOUBLE", "c_double"),
    ("DECIMAL(10,2)", "c_decimal"))

  private val numericToBoolConfigs = for {
    castFunc <- castFunctions
    (sourceType, colName) <- numericTypes
  } yield CastBooleanConfig(
    s"$castFunc $sourceType to Boolean",
    s"SELECT $castFunc($colName AS BOOLEAN) FROM parquetV1Table")

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 // 1M rows

    // Generate boolean data for boolean-to-other casts
    runBenchmarkWithTable("Boolean to other types casts", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // Data distribution: 1% NULL, 50/50 true/false
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT CASE
                WHEN value % 100 = 0 THEN NULL
                ELSE (value % 2 = 0)
              END AS c_bool
              FROM $tbl
            """))

          (boolToStringConfigs ++ boolToNumericConfigs).foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }

    // Generate numeric data for numeric-to-boolean casts
    runBenchmarkWithTable("Numeric to Boolean casts", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // Data distribution: 1% NULL per column, values in {-1, 0, 1} (~33% each)
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                CASE WHEN value % 100 = 0 THEN NULL ELSE CAST((value % 3) - 1 AS BYTE) END AS c_byte,
                CASE WHEN value % 100 = 1 THEN NULL ELSE CAST((value % 3) - 1 AS SHORT) END AS c_short,
                CASE WHEN value % 100 = 2 THEN NULL ELSE CAST((value % 3) - 1 AS INT) END AS c_int,
                CASE WHEN value % 100 = 3 THEN NULL ELSE CAST((value % 3) - 1 AS LONG) END AS c_long,
                CASE WHEN value % 100 = 4 THEN NULL ELSE CAST((value % 3) - 1 AS FLOAT) END AS c_float,
                CASE WHEN value % 100 = 5 THEN NULL ELSE CAST((value % 3) - 1 AS DOUBLE) END AS c_double,
                CASE WHEN value % 100 = 6 THEN NULL ELSE CAST((value % 3) - 1 AS DECIMAL(10,2)) END AS c_decimal
              FROM $tbl
            """))

          numericToBoolConfigs.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
