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

import org.apache.comet.CometConf

/**
 * Configuration for a map expression benchmark.
 *
 * @param name
 *   Name for the benchmark
 * @param query
 *   SQL query to benchmark
 * @param extraCometConfigs
 *   Additional Comet configurations for the scan+exec case
 */
case class MapExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet map expressions. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometMapExpressionBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometMapExpressionBenchmark-**results.txt".
 */
object CometMapExpressionBenchmark extends CometBenchmarkBase {

  private val mapExpressions = List(
    MapExprConfig(
      "create_map",
      "select map(c1, c1, c2, c2, c3, c3, c4, c4, c5, c5) from parquetV1Table"))

  override def runCometBenchmark(args: Array[String]): Unit = {
    runBenchmarkWithTable("Map expressions", 1024) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(
              "SELECT " +
                "(value + 0) AS C1, " +
                "(value + 10) AS C2, " +
                "(value + 20) AS C3, " +
                "(value + 30) AS C4, " +
                s"(value + 40) AS C5 FROM $tbl"))

          val extraConfigs = Map(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true")

          mapExpressions.foreach { config =>
            val allConfigs = extraConfigs ++ config.extraCometConfigs
            runBenchmark(config.name) {
              runExpressionBenchmark(config.name, v, config.query, allConfigs)
            }
          }
        }
      }
    }
  }
}
