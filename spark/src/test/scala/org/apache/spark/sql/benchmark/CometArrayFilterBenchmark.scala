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

import org.apache.spark.benchmark.Benchmark

import org.apache.comet.CometConf

// spotless:off
/**
 * Benchmark to measure performance of Comet array expressions. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometArrayFilterBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometArrayFilterBenchmark-**results.txt".
 */
// spotless:on
object CometArrayFilterBenchmark extends CometBenchmarkBase {

  def runExprBenchmark(config: ArrayFilterExprConfig, values: Int, arraySize: Int): Unit = {
    val benchmark =
      new Benchmark(s"${config.name} (size $arraySize)", values, output = output)
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"SELECT sequence(0, cast(rand(42) * $arraySize as int)) AS arr " +
              s"FROM range($values)"))

        benchmark.addCase(s"Spark ${config.name}") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql(config.query).noop()
          }
        }

        benchmark.addCase(s"Comet (Native) ${config.name}") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
            spark.sql(config.query).noop()
          }
        }

        benchmark.addCase(s"Comet (Codegen) ${config.name}") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true") {
            spark.sql(config.query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def runCometBenchmark(args: Array[String]): Unit = {
    val values = 4 * 1024 * 1024

    val config =
      ArrayFilterExprConfig("array_filter", "SELECT filter(arr, x -> x > 2) FROM parquetV1Table")

    runExprBenchmark(config, values, 100)
  }
}

case class ArrayFilterExprConfig(name: String, query: String)
