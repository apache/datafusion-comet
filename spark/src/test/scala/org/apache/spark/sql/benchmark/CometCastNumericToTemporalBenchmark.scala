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

case class CastNumericToTemporalConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet cast from numeric types to temporal types. To run
 * this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCastNumericToTemporalBenchmark
 * }}}
 * Results will be written to
 * "spark/benchmarks/CometCastNumericToTemporalBenchmark-**results.txt".
 */
object CometCastNumericToTemporalBenchmark extends CometBenchmarkBase {

  private val castFunctions = Seq("CAST", "TRY_CAST")

  // INT to DATE (days since epoch)
  private val intToDateConfigs = for {
    castFunc <- castFunctions
  } yield CastNumericToTemporalConfig(
    s"$castFunc Int to Date",
    s"SELECT $castFunc(c_int AS DATE) FROM parquetV1Table")

  // LONG to TIMESTAMP (microseconds since epoch)
  private val longToTimestampConfigs = for {
    castFunc <- castFunctions
  } yield CastNumericToTemporalConfig(
    s"$castFunc Long to Timestamp",
    s"SELECT $castFunc(c_long AS TIMESTAMP) FROM parquetV1Table")

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 // 1M rows

    // Generate data once for INT to DATE conversions
    runBenchmarkWithTable("Int to Date casts", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // Data distribution: 1% NULL, days since epoch spanning ~100 years (1920-2020)
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT CASE
                WHEN value % 100 = 0 THEN NULL
                ELSE CAST((value % 36500) - 18000 AS INT)
              END AS c_int
              FROM $tbl
            """))

          intToDateConfigs.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }

    // Generate data once for LONG to TIMESTAMP conversions
    runBenchmarkWithTable("Long to Timestamp casts", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // Data distribution: 1% NULL, microseconds since epoch spanning ~1 year from 2020-01-01
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT CASE
                WHEN value % 100 = 0 THEN NULL
                ELSE 1577836800000000 + (value % 31536000000000)
              END AS c_long
              FROM $tbl
            """))

          longToTimestampConfigs.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
