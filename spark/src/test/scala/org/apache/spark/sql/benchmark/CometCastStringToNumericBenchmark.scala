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

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

case class CastStringToNumericConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet cast from String to numeric types. To run this
 * benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCastStringToNumericBenchmark
 * }}}
 */
object CometCastStringToNumericBenchmark extends CometBenchmarkBase {

  private val castFunctions = Seq("CAST", "TRY_CAST")
  private val targetTypes =
    Seq(
      "BOOLEAN",
      "BYTE",
      "SHORT",
      "INT",
      "LONG",
      "FLOAT",
      "DOUBLE",
      "DECIMAL(10,2)",
      "DECIMAL(38,19)")

  private val castConfigs = for {
    castFunc <- castFunctions
    targetType <- targetTypes
  } yield CastStringToNumericConfig(
    s"$castFunc String to $targetType",
    s"SELECT $castFunc(c1 AS $targetType) FROM parquetV1Table",
    Map(
      SQLConf.ANSI_ENABLED.key -> "false",
      CometConf.getExprAllowIncompatConfigKey(classOf[Cast]) -> "true"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024 // 1M rows

    // Generate input data once for all benchmarks
    runBenchmarkWithTable("String to numeric casts", values) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // Data distribution:
          // - 2% NULL, 2% 'NaN', 2% 'Infinity', 2% '-Infinity'
          // - 12% small integers (0-98)
          // - 40% medium integers (0-999,998)
          // - 40% decimals centered around 0 (approx -5000.00 to +5000.00)
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT CASE
                WHEN value % 50 = 0 THEN NULL
                WHEN value % 50 = 1 THEN 'NaN'
                WHEN value % 50 = 2 THEN 'Infinity'
                WHEN value % 50 = 3 THEN '-Infinity'
                WHEN value % 50 < 10 THEN CAST(value % 99 AS STRING)
                WHEN value % 50 < 30 THEN CAST(value % 999999 AS STRING)
                ELSE CAST((value - 500000) / 100.0 AS STRING)
              END AS c1
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
