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

// spotless:off
/**
 * Benchmark to measure performance of Comet array expressions. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometArrayExpressionBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometArrayExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometArrayExpressionBenchmark extends CometBenchmarkBase {

  def arrayPositionBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Create a table with int arrays of size 10 and a search value
        prepareTable(
          dir,
          spark.sql(s"""SELECT
               |  array(
               |    cast(value % 100 as int),
               |    cast((value + 1) % 100 as int),
               |    cast((value + 2) % 100 as int),
               |    cast((value + 3) % 100 as int),
               |    cast((value + 4) % 100 as int),
               |    cast((value + 5) % 100 as int),
               |    cast((value + 6) % 100 as int),
               |    cast((value + 7) % 100 as int),
               |    cast((value + 8) % 100 as int),
               |    cast((value + 9) % 100 as int)
               |  ) as int_arr,
               |  cast((value + 5) % 100 as int) as search_val
               |FROM $tbl""".stripMargin))

        val extraConfigs =
          Map("spark.comet.expression.ArrayPosition.allowIncompatible" -> "true")

        runExpressionBenchmark(
          "array_position - int array",
          values,
          "SELECT array_position(int_arr, search_val) FROM parquetV1Table",
          extraConfigs)
      }
    }

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Create a table with string arrays of size 10 and a search value
        prepareTable(
          dir,
          spark.sql(s"""SELECT
               |  array(
               |    cast(value % 100 as string),
               |    cast((value + 1) % 100 as string),
               |    cast((value + 2) % 100 as string),
               |    cast((value + 3) % 100 as string),
               |    cast((value + 4) % 100 as string),
               |    cast((value + 5) % 100 as string),
               |    cast((value + 6) % 100 as string),
               |    cast((value + 7) % 100 as string),
               |    cast((value + 8) % 100 as string),
               |    cast((value + 9) % 100 as string)
               |  ) as str_arr,
               |  cast((value + 5) % 100 as string) as search_val
               |FROM $tbl""".stripMargin))

        val extraConfigs =
          Map("spark.comet.expression.ArrayPosition.allowIncompatible" -> "true")

        runExpressionBenchmark(
          "array_position - string array",
          values,
          "SELECT array_position(str_arr, search_val) FROM parquetV1Table",
          extraConfigs)
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024

    runBenchmarkWithTable("ArrayPosition", values) { v =>
      arrayPositionBenchmark(v)
    }
  }
}
