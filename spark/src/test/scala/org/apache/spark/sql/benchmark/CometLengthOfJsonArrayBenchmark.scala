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

import org.apache.spark.sql.catalyst.expressions.LengthOfJsonArray

import org.apache.comet.CometConf

/**
 * Benchmark to measure performance of Comet json_array_length expression. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometLengthOfJsonArrayBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometLengthOfJsonArray-**results.txt".
 */
object CometLengthOfJsonArrayBenchmark extends CometBenchmarkBase {

  override def runCometBenchmark(args: Array[String]): Unit = {
    val numRows = 1024 * 1024
    runBenchmarkWithTable("json_array_length", numRows) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          import spark.implicits._
          prepareTable(
            dir,
            spark
              .range(numRows)
              .map { i =>
                val arrayLength = (i % 100).toInt
                (0 until arrayLength)
                  .map(j => s""""item_${i}_$j"""")
                  .mkString("[", ",", "]")
              }
              .toDF("c1"))

          val extraConfigs =
            Map(CometConf.getExprAllowIncompatConfigKey(classOf[LengthOfJsonArray]) -> "true")

          val benchmarks = List(
            StringExprConfig(
              "get json array length",
              "select json_array_length(c1) from parquetV1Table",
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
