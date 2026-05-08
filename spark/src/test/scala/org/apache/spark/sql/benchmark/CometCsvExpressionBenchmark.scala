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

import org.apache.spark.sql.catalyst.expressions.CsvToStructs

import org.apache.comet.CometConf

/**
 * Configuration for a CSV expression benchmark.
 *
 * @param name
 *   Name for the benchmark
 * @param query
 *   SQL query to benchmark
 * @param extraCometConfigs
 *   Additional Comet configurations for the scan+exec case
 */
case class CsvExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

// spotless:off
/**
 * Benchmark to measure performance of Comet CSV expressions. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometCsvExpressionBenchmark` Results will be written
 * to "spark/benchmarks/CometCsvExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometCsvExpressionBenchmark extends CometBenchmarkBase {

  /**
   * Generic method to run a CSV expression benchmark with the given configuration.
   */
  def runCsvExprBenchmark(config: CsvExprConfig, values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"SELECT CAST(value AS STRING) AS c1, CAST(value AS INT) AS c2, CAST(value AS LONG) AS c3 FROM $tbl"))

        val extraConfigs = Map(
          CometConf.getExprAllowIncompatConfigKey(
            classOf[CsvToStructs]) -> "true") ++ config.extraCometConfigs

        runExpressionBenchmark(config.name, values, config.query, extraConfigs)
      }
    }
  }

  // Configuration for all CSV expression benchmarks
  private val csvExpressions = List(
    CsvExprConfig("to_csv", "SELECT to_csv(struct(c1, c2, c3)) FROM parquetV1Table"))

  override def runCometBenchmark(args: Array[String]): Unit = {
    val values = 1024 * 1024

    csvExpressions.foreach { config =>
      runBenchmarkWithTable(config.name, values) { value =>
        runCsvExprBenchmark(config, value)
      }
    }
  }
}
