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

object CometArrayFilterBenchmark extends CometBenchmarkBase {

  def runExprBenchmark(config: ArrayFilterExprConfig, values: Int, arraySize: Int): Unit = {
    val benchmark =
      new Benchmark(config.name, values, output = output)
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"""
               |SELECT
               |  sequence(0, cast(rand(42) * $arraySize as int)) AS arr,
               |  cast(rand(42) * $arraySize as int) AS threshold,
               |  sequence(0, cast(rand(42) * 5 as int)) AS short_arr,
               |  sequence(0, cast(rand(42) * 1000 as int)) AS large_arr,
               |  transform(
               |    sequence(0, cast(rand(42) * $arraySize as int)),
               |    i -> concat('str_val_', cast(i as string))
               |  ) AS arr_str,
               |  transform(
               |    sequence(0, cast(rand(42) * $arraySize as int)),
               |    i -> IF(i % 5 = 0, NULL, i)
               |  ) AS arr_with_nulls,
               |  transform(
               |    sequence(0, cast(rand(42) * 10 as int)),
               |    i -> sequence(0, cast(rand(42) * 5 as int))
               |  ) AS nested_arr
               |FROM range($values)
               |""".stripMargin))

        benchmark.addCase(s"Spark ${config.name}") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql(config.query).noop()
          }
        }

        benchmark.addCase(s"Comet (Native) ${config.name}") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_HIGHER_ORDER_FUNCTION_NATIVE_ENABLED.key -> "true",
            CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
            spark.sql(config.query).noop()
          }
        }

        benchmark.addCase(s"Comet (Codegen) ${config.name}") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_HIGHER_ORDER_FUNCTION_NATIVE_ENABLED.key -> "false",
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
    val arraySize = 100

    val configs = Seq(
      ArrayFilterExprConfig("int literal", "SELECT filter(arr, x -> x > 2) FROM parquetV1Table"),
      ArrayFilterExprConfig(
        "capture outer column",
        "SELECT filter(arr, x -> x > threshold) FROM parquetV1Table"),
      ArrayFilterExprConfig(
        "compound predicate (AND / range)",
        "SELECT filter(arr, x -> x >= 20 AND x <= 80) FROM parquetV1Table"),
      ArrayFilterExprConfig(
        "arithmetic expression in lambda",
        "SELECT filter(arr, x -> (x % 2 = 0) AND (x * 3 > 100)) FROM parquetV1Table"),
      ArrayFilterExprConfig(
        "string length predicate",
        "SELECT filter(arr_str, x -> length(x) > 10) FROM parquetV1Table"),
      ArrayFilterExprConfig(
        "string equality comparison",
        "SELECT filter(arr_str, x -> x = 'str_val_42') FROM parquetV1Table"),
      ArrayFilterExprConfig(
        "array with nulls (IS NOT NULL check)",
        "SELECT filter(arr_with_nulls, x -> x IS NOT NULL AND x > 10) FROM parquetV1Table"),
      ArrayFilterExprConfig(
        "nested array (size check)",
        "SELECT filter(nested_arr, inner_arr -> size(inner_arr) > 2) FROM parquetV1Table"),
      ArrayFilterExprConfig(
        "chained filters (pipeline)",
        "SELECT size(filter(filter(arr, x -> x > 20), x -> x < 80)) FROM parquetV1Table"),
      ArrayFilterExprConfig(
        "short arrays (JNI overhead)",
        "SELECT filter(short_arr, x -> x > 2) FROM parquetV1Table"),
      ArrayFilterExprConfig(
        "large arrays (SIMD)",
        "SELECT filter(large_arr, x -> x > 500) FROM parquetV1Table"))

    configs.foreach(config => runExprBenchmark(config, values, arraySize))
  }
}

case class ArrayFilterExprConfig(name: String, query: String)
