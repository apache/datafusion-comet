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

case class CastTemporalToNumericConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet cast from temporal types to numeric types. To run
 * this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCastTemporalToNumericBenchmark
 * }}}
 * Results will be written to
 * "spark/benchmarks/CometCastTemporalToNumericBenchmark-**results.txt".
 */
object CometCastTemporalToNumericBenchmark extends CometBenchmarkBase {

  private val castFunctions = Seq("CAST", "TRY_CAST")

  // DATE to numeric types
  private val dateToNumericTypes = Seq("BYTE", "SHORT", "INT", "LONG")
  private val dateToNumericConfigs = for {
    castFunc <- castFunctions
    targetType <- dateToNumericTypes
  } yield CastTemporalToNumericConfig(
    s"$castFunc Date to $targetType",
    s"SELECT $castFunc(c_date AS $targetType) FROM parquetV1Table")

  // TIMESTAMP to numeric types
  private val timestampToNumericTypes = Seq("BYTE", "SHORT", "INT", "LONG")
  private val timestampToNumericConfigs = for {
    castFunc <- castFunctions
    targetType <- timestampToNumericTypes
  } yield CastTemporalToNumericConfig(
    s"$castFunc Timestamp to $targetType",
    s"SELECT $castFunc(c_timestamp AS $targetType) FROM parquetV1Table")

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 // 1M rows

    // Generate DATE data once for all date-to-numeric benchmarks
    runBenchmarkWithTable("Date to Numeric casts", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // Data distribution: 1% NULL, dates spanning ~10 years from 2020-01-01
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT CASE
                WHEN value % 100 = 0 THEN NULL
                ELSE DATE_ADD('2020-01-01', CAST(value % 3650 AS INT))
              END AS c_date
              FROM $tbl
            """))

          dateToNumericConfigs.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }

    // Generate TIMESTAMP data once for all timestamp-to-numeric benchmarks
    runBenchmarkWithTable("Timestamp to Numeric casts", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // Data distribution: 1% NULL, timestamps spanning ~1 year from 2020-01-01
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT CASE
                WHEN value % 100 = 0 THEN NULL
                ELSE TIMESTAMP_MICROS(1577836800000000 + value % 31536000000000)
              END AS c_timestamp
              FROM $tbl
            """))

          timestampToNumericConfigs.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
