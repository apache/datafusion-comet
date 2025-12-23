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

import org.apache.spark.sql.internal.SQLConf

case class CastStringToTemporalConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet cast from String to temporal types. To run this
 * benchmark: `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometCastStringToTemporalBenchmark` Results will be
 * written to "spark/benchmarks/CometCastStringToTemporalBenchmark-**results.txt".
 */
object CometCastStringToTemporalBenchmark extends CometBenchmarkBase {

  /**
   * Generic method to run a cast benchmark with the given configuration.
   */
  def runCastBenchmark(config: CastStringToTemporalConfig, values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Generate date strings like "2020-01-01", "2020-01-02", etc.
        // This covers the full range for date parsing
        prepareTable(
          dir,
          spark.sql(
            s"SELECT CAST(DATE_ADD('2020-01-01', CAST(value % 3650 AS INT)) AS STRING) AS c1 FROM $tbl"))

        runExpressionBenchmark(config.name, values, config.query, config.extraCometConfigs)
      }
    }
  }

  /**
   * Benchmark for String to Timestamp with timestamp-formatted strings.
   */
  def runTimestampCastBenchmark(config: CastStringToTemporalConfig, values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Generate timestamp strings like "2020-01-01 12:34:56", etc.
        prepareTable(
          dir,
          spark.sql(
            s"SELECT CAST(TIMESTAMP_MICROS(value % 9999999999) AS STRING) AS c1 FROM $tbl"))

        runExpressionBenchmark(config.name, values, config.query, config.extraCometConfigs)
      }
    }
  }

  // Configuration for String to temporal cast benchmarks
  private val castConfigs = List(
    // Date
    CastStringToTemporalConfig(
      "Cast String to Date (LEGACY)",
      "SELECT CAST(c1 AS DATE) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "false")),
    CastStringToTemporalConfig(
      "Cast String to Date (ANSI)",
      "SELECT CAST(c1 AS DATE) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "true")))

  private val timestampCastConfigs = List(
    // Timestamp (only UTC timezone is compatible)
    CastStringToTemporalConfig(
      "Cast String to Timestamp (LEGACY, UTC)",
      "SELECT CAST(c1 AS TIMESTAMP) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "false")),
    CastStringToTemporalConfig(
      "Cast String to Timestamp (ANSI, UTC)",
      "SELECT CAST(c1 AS TIMESTAMP) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "true")))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 * 10 // 10M rows

    // Run date casts
    castConfigs.foreach { config =>
      runBenchmarkWithTable(config.name, values) { v =>
        runCastBenchmark(config, v)
      }
    }

    // Run timestamp casts
    timestampCastConfigs.foreach { config =>
      runBenchmarkWithTable(config.name, values) { v =>
        runTimestampCastBenchmark(config, v)
      }
    }
  }
}
