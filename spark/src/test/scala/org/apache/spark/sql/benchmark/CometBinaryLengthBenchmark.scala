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
 * Benchmark for `length` over binary and string columns. Payloads are the decimal text of the
 * generated Long, so widths vary per row: `b_short` is that text (up to 20 bytes), `b_long`
 * repeats it 128 times, `b_null` blanks one row in five, and `b_repeated` cycles through the nine
 * values of `value % 5` over a signed generator. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometBinaryLengthBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometBinaryLengthBenchmark-**results.txt".
 */
object CometBinaryLengthBenchmark extends CometBenchmarkBase {

  private val cases = List(
    ("length_string", "select length(c1) from parquetV1Table"),
    ("length_binary_short", "select length(b_short) from parquetV1Table"),
    ("length_binary_long", "select length(b_long) from parquetV1Table"),
    ("length_binary_nulls", "select length(b_null) from parquetV1Table"),
    ("length_binary_repeated", "select length(b_repeated) from parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("length on binary and string", 1024 * 1024) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // c1 is the row's decimal text repeated ten times; the binary columns cover the same
          // text as bytes, 128 repeats of it, a fifth of the rows null, and the nine values of
          // value % 5 that the writer dictionary-encodes.
          prepareTable(
            dir,
            spark.sql("SELECT REPEAT(CAST(value AS STRING), 10) AS c1," +
              " CAST(CAST(value AS STRING) AS BINARY) AS b_short," +
              " CAST(REPEAT(CAST(value AS STRING), 128) AS BINARY) AS b_long," +
              " IF(value % 5 = 0, CAST(NULL AS BINARY), CAST(CAST(value AS STRING) AS BINARY))" +
              " AS b_null," +
              " CAST(CAST(value % 5 AS STRING) AS BINARY) AS b_repeated" +
              s" FROM $tbl"))

          cases.foreach { case (name, query) =>
            runBenchmark(name) {
              runExpressionBenchmark(name, v.toLong, query)
            }
          }
        }
      }
    }
  }
}
