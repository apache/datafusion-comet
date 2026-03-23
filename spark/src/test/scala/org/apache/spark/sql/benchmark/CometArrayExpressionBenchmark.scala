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
 * Benchmark to measure performance of Comet array expressions. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometArrayExpressionBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometArrayExpressionBenchmark-**results.txt".
 */
object CometArrayExpressionBenchmark extends CometBenchmarkBase {

  private def buildWideIntArrayExpr(width: Int, modulus: Int): String = {
    require(width > 0, "width must be positive")

    (0 until width)
      .map { i =>
        val seed = 13 + i * 17
        if (i % 11 == 0) {
          s"CASE WHEN value % 32 = 0 THEN NULL ELSE CAST((value * $seed + $i) % $modulus AS INT) END"
        } else {
          s"CAST((value * $seed + $i) % $modulus AS INT)"
        }
      }
      .mkString("array(", ",\n                ", ")")
  }

  private def prepareSortArrayTable(width: Int)(f: => Unit): Unit = {
    val intArrayExpr = buildWideIntArrayExpr(width, modulus = width * 32)
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"""
            SELECT
              $intArrayExpr AS int_arr
            FROM $tbl
          """))
        f
      }
    }
  }

  def sortArrayIntAscBenchmark(values: Int, width: Int): Unit = {
    prepareSortArrayTable(width) {
      runExpressionBenchmark(
        s"sort_array int ascending (width=$width)",
        values,
        "SELECT sort_array(int_arr) FROM parquetV1Table")
    }
  }

  def sortArrayIntDescBenchmark(values: Int, width: Int): Unit = {
    prepareSortArrayTable(width) {
      runExpressionBenchmark(
        s"sort_array int descending (width=$width)",
        values,
        "SELECT sort_array(int_arr, false) FROM parquetV1Table")
    }
  }

  def sortArrayIntAscFirstElementBenchmark(values: Int, width: Int): Unit = {
    prepareSortArrayTable(width) {
      runExpressionBenchmark(
        s"element_at(sort_array(int_arr), 1) (width=$width)",
        values,
        "SELECT element_at(sort_array(int_arr), 1) FROM parquetV1Table")
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 4 * 1024 * 1024

    runBenchmarkWithTable("sortArrayIntAsc", values) { v =>
      sortArrayIntAscBenchmark(v, width = 16)
    }

    runBenchmarkWithTable("sortArrayIntDesc", values) { v =>
      sortArrayIntDescBenchmark(v, width = 16)
    }

    runBenchmarkWithTable("sortArrayIntAscWide", values) { v =>
      sortArrayIntAscBenchmark(v, width = 32)
    }

    runBenchmarkWithTable("sortArrayIntAscFirstElement", values) { v =>
      sortArrayIntAscFirstElementBenchmark(v, width = 32)
    }
  }
}
