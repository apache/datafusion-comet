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

case class CastTemporalToStringConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet cast from temporal types to String. To run this
 * benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCastTemporalToStringBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometCastTemporalToStringBenchmark-**results.txt".
 */
object CometCastTemporalToStringBenchmark extends CometBenchmarkBase {

  private val castFunctions = Seq("CAST", "TRY_CAST")

  private val dateCastConfigs = for {
    castFunc <- castFunctions
  } yield CastTemporalToStringConfig(
    s"$castFunc Date to String",
    s"SELECT $castFunc(c_date AS STRING) FROM parquetV1Table")

  private val timestampCastConfigs = for {
    castFunc <- castFunctions
  } yield CastTemporalToStringConfig(
    s"$castFunc Timestamp to String",
    s"SELECT $castFunc(c_timestamp AS STRING) FROM parquetV1Table")

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 // 1M rows

    // Generate temporal data once for date benchmarks
    runBenchmarkWithTable("Date to String casts", values) { v =>
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

          dateCastConfigs.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }

    // Generate temporal data once for timestamp benchmarks
    runBenchmarkWithTable("Timestamp to String casts", values) { v =>
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

          timestampCastConfigs.foreach { config =>
            runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
          }
        }
      }
    }
  }
}
