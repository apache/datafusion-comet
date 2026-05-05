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

/**
 * Benchmark to measure performance of Comet get_json_object expression. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometGetJsonObjectBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometGetJsonObjectBenchmark-**results.txt".
 */
object CometGetJsonObjectBenchmark extends CometBenchmarkBase {

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val numRows = 1024 * 1024
    runBenchmarkWithTable("get_json_object", numRows) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          import spark.implicits._

          prepareTable(
            dir,
            spark
              .range(numRows)
              .map { i =>
                val name = s"user_$i"
                val age = (i % 80 + 18).toInt
                val nested = s"""{"city":"city_${i % 100}","zip":"${10000 + i % 90000}"}"""
                val items =
                  (0 until (i % 5 + 1).toInt).map(j => s""""item_$j"""").mkString("[", ",", "]")
                s"""{"name":"$name","age":$age,"address":$nested,"items":$items}"""
              }
              .toDF("c1"))

          val extraConfigs =
            Map("spark.comet.expression.GetJsonObject.allowIncompatible" -> "true")

          val benchmarks = List(
            StringExprConfig(
              "simple field",
              "select get_json_object(c1, '$.name') from parquetV1Table",
              extraConfigs),
            StringExprConfig(
              "numeric field",
              "select get_json_object(c1, '$.age') from parquetV1Table",
              extraConfigs),
            StringExprConfig(
              "nested field",
              "select get_json_object(c1, '$.address.city') from parquetV1Table",
              extraConfigs),
            StringExprConfig(
              "array element",
              "select get_json_object(c1, '$.items[0]') from parquetV1Table",
              extraConfigs),
            StringExprConfig(
              "nested object",
              "select get_json_object(c1, '$.address') from parquetV1Table",
              extraConfigs))

          benchmarks.foreach { config =>
            runBenchmark(config.name) {
              runExpressionBenchmark(config.name, v, config.query, config.extraCometConfigs)
            }
          }
        }
      }
    }
  }
}
