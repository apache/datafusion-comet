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

case class CastStringToTemporalConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet cast from String to temporal types. To run this
 * benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCastStringToTemporalBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometCastStringToTemporalBenchmark-**results.txt".
 */
object CometCastStringToTemporalBenchmark extends CometBenchmarkBase {

  // Configuration for String to temporal cast benchmarks
  private val dateCastConfigs = List(
    CastStringToTemporalConfig(
      "Cast String to Date",
      "SELECT CAST(c1 AS DATE) FROM parquetV1Table"),
    CastStringToTemporalConfig(
      "Try_Cast String to Date",
      "SELECT TRY_CAST(c1 AS DATE) FROM parquetV1Table"))

  private val timestampCastConfigs = List(
    CastStringToTemporalConfig(
      "Cast String to Timestamp",
      "SELECT CAST(c1 AS TIMESTAMP) FROM parquetV1Table"),
    CastStringToTemporalConfig(
      "Try_Cast String to Timestamp",
      "SELECT TRY_CAST(c1 AS TIMESTAMP) FROM parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 // 1M rows

    // Generate date data once with ~10% invalid values
    runBenchmarkWithTable("date data generation", values) { v =>
      withTempPath { dateDir =>
        withTempTable("parquetV1Table") {
          // Data distribution: 10% invalid strings, 90% valid date strings spanning ~10 years
          prepareTable(
            dateDir,
            spark.sql(s"""
              SELECT CASE
                WHEN value % 10 = 0 THEN 'invalid-date'
                ELSE CAST(DATE_ADD('2020-01-01', CAST(value % 3650 AS INT)) AS STRING)
              END AS c1
              FROM $tbl
            """))

          // Run date cast benchmarks with the same data
          dateCastConfigs.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }

    // Generate timestamp data once with ~10% invalid values
    runBenchmarkWithTable("timestamp data generation", values) { v =>
      withTempPath { timestampDir =>
        withTempTable("parquetV1Table") {
          // Data distribution: 10% invalid strings, 90% valid timestamp strings (1970 epoch range)
          prepareTable(
            timestampDir,
            spark.sql(s"""
              SELECT CASE
                WHEN value % 10 = 0 THEN 'not-a-timestamp'
                ELSE CAST(TIMESTAMP_MICROS(value % 9999999999) AS STRING)
              END AS c1
              FROM $tbl
            """))

          // Run timestamp cast benchmarks with the same data
          timestampCastConfigs.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
