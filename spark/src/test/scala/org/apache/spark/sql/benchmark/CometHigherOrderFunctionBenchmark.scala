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

// spotless:off
/**
 * Benchmark to measure `filter` with a lambda, and the `array_compact` / `array_remove` forms a
 * lambda can often be rewritten to. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometHigherOrderFunctionBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometHigherOrderFunctionBenchmark-**results.txt".
 */
// spotless:on
object CometHigherOrderFunctionBenchmark extends CometBenchmarkBase {

  /**
   * The arrays hold ten elements. Every fifth element is NULL and every seventh is an empty
   * string.
   */
  private def prepareFilterTable(f: => Unit): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val strElems = (0 until 10).map { i =>
          s"""CASE WHEN (value + $i) % 5 = 0 THEN NULL
             |     WHEN (value + $i) % 7 = 0 THEN ''
             |     ELSE cast((value + $i) % 100 as string) END""".stripMargin
        }
        val intElems = (0 until 10).map { i =>
          s"CASE WHEN (value + $i) % 5 = 0 THEN NULL ELSE cast((value + $i) % 100 as int) END"
        }
        prepareTable(
          dir,
          spark.sql(s"""SELECT
               |  array(${strElems.mkString(",\n")}) AS str_arr,
               |  array(${intElems.mkString(",\n")}) AS int_arr,
               |  cast(value % 3 as int) AS threshold
               |FROM $tbl""".stripMargin))
        f
      }
    }
  }

  def filterLambdaBenchmark(values: Int): Unit = {
    prepareFilterTable {
      Seq(
        "string: filter(x -> nvl(x, '') != '')" ->
          "SELECT filter(str_arr, x -> nvl(x, '') != '') FROM parquetV1Table",
        "int: filter(x -> nvl(x, '') != '')" ->
          "SELECT filter(int_arr, x -> nvl(x, '') != '') FROM parquetV1Table",
        "int: filter(x -> x IS NOT NULL)" ->
          "SELECT filter(int_arr, x -> x IS NOT NULL) FROM parquetV1Table",
        "int: filter(x -> x > threshold)" ->
          "SELECT filter(int_arr, x -> x > threshold) FROM parquetV1Table").foreach {
        case (name, query) =>
          runExpressionBenchmark(name, values.toLong, query)
          // The same lambda through the JVM codegen dispatcher, which is what Comet did before it
          // evaluated lambdas natively.
          runExpressionBenchmark(
            s"$name [dispatcher]",
            values.toLong,
            query,
            Map(CometConf.COMET_NATIVE_LAMBDA_ENABLED.key -> "false"))
      }
      // The built-in functions that replace a lambda have no native-lambda variant to compare.
      runExpressionBenchmark(
        "string: array_remove(array_compact(arr), '')",
        values.toLong,
        "SELECT array_remove(array_compact(str_arr), '') FROM parquetV1Table")
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("FilterLambda", 1024 * 1024)(filterLambdaBenchmark)
  }
}
