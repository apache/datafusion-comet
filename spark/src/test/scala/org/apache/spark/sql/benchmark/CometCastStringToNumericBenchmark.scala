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

case class CastStringToNumericConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet cast from String to numeric types. To run this
 * benchmark: `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometCastStringToNumericBenchmark` Results will be
 * written to "spark/benchmarks/CometCastStringToNumericBenchmark-**results.txt".
 */
object CometCastStringToNumericBenchmark extends CometBenchmarkBase {

  /**
   * Generic method to run a cast benchmark with the given configuration.
   */
  def runCastBenchmark(config: CastStringToNumericConfig, values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Generate numeric strings with decimal points: "123.45", "-456.78", etc.
        prepareTable(
          dir,
          spark.sql(s"SELECT CAST((value - 500000) / 100.0 AS STRING) AS c1 FROM $tbl"))

        runExpressionBenchmark(config.name, values, config.query, config.extraCometConfigs)
      }
    }
  }

  // Configuration for all String to numeric cast benchmarks
  private val castConfigs = List(
    // Boolean
    CastStringToNumericConfig(
      "Cast String to Boolean (LEGACY)",
      "SELECT CAST(c1 AS BOOLEAN) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "false")),
    CastStringToNumericConfig(
      "Cast String to Boolean (ANSI)",
      "SELECT CAST(c1 AS BOOLEAN) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "true")),
    // Byte
    CastStringToNumericConfig(
      "Cast String to Byte (LEGACY)",
      "SELECT CAST(c1 AS BYTE) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "false")),
    CastStringToNumericConfig(
      "Cast String to Byte (ANSI)",
      "SELECT CAST(c1 AS BYTE) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "true")),
    // Short
    CastStringToNumericConfig(
      "Cast String to Short (LEGACY)",
      "SELECT CAST(c1 AS SHORT) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "false")),
    CastStringToNumericConfig(
      "Cast String to Short (ANSI)",
      "SELECT CAST(c1 AS SHORT) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "true")),
    // Integer
    CastStringToNumericConfig(
      "Cast String to Integer (LEGACY)",
      "SELECT CAST(c1 AS INT) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "false")),
    CastStringToNumericConfig(
      "Cast String to Integer (ANSI)",
      "SELECT CAST(c1 AS INT) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "true")),
    // Long
    CastStringToNumericConfig(
      "Cast String to Long (LEGACY)",
      "SELECT CAST(c1 AS LONG) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "false")),
    CastStringToNumericConfig(
      "Cast String to Long (ANSI)",
      "SELECT CAST(c1 AS LONG) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "true")),
    // Float (Incompatible - but still useful to benchmark)
    CastStringToNumericConfig(
      "Cast String to Float (LEGACY)",
      "SELECT CAST(c1 AS FLOAT) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "false")),
    // Double (Incompatible - but still useful to benchmark)
    CastStringToNumericConfig(
      "Cast String to Double (LEGACY)",
      "SELECT CAST(c1 AS DOUBLE) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "false")),
    // Decimal (Incompatible - but still useful to benchmark)
    CastStringToNumericConfig(
      "Cast String to Decimal(10,2) (LEGACY)",
      "SELECT CAST(c1 AS DECIMAL(10,2)) FROM parquetV1Table",
      Map(SQLConf.ANSI_ENABLED.key -> "false")))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 * 10 // 10M rows

    castConfigs.foreach { config =>
      runBenchmarkWithTable(config.name, values) { v =>
        runCastBenchmark(config, v)
      }
    }
  }
}
