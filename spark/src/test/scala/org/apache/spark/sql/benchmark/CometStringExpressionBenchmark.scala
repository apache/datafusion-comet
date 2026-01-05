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
 * Configuration for a string expression benchmark.
 * @param name
 *   Name for the benchmark
 * @param query
 *   SQL query to benchmark
 * @param extraCometConfigs
 *   Additional Comet configurations for the scan+exec case
 */
case class StringExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

/**
 * Benchmark to measure performance of Comet string expressions. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometStringExpressionBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometStringExpressionBenchmark-**results.txt".
 */
object CometStringExpressionBenchmark extends CometBenchmarkBase {

  // Configuration for all string expression benchmarks
  private val stringExpressions = List(
    StringExprConfig("ascii", "select ascii(c1) from parquetV1Table"),
    StringExprConfig("bit_length", "select bit_length(c1) from parquetV1Table"),
    StringExprConfig("btrim", "select btrim(c1) from parquetV1Table"),
    StringExprConfig("chr", "select chr(c1) from parquetV1Table"),
    StringExprConfig("concat", "select concat(c1, c1) from parquetV1Table"),
    StringExprConfig("concat_ws", "select concat_ws(' ', c1, c1) from parquetV1Table"),
    StringExprConfig("contains", "select contains(c1, '123') from parquetV1Table"),
    StringExprConfig("endswith", "select endswith(c1, '9') from parquetV1Table"),
    StringExprConfig("initCap", "select initCap(c1) from parquetV1Table"),
    StringExprConfig("instr", "select instr(c1, '123') from parquetV1Table"),
    StringExprConfig("length", "select length(c1) from parquetV1Table"),
    StringExprConfig("like", "select c1 like '%123%' from parquetV1Table"),
    StringExprConfig("lower", "select lower(c1) from parquetV1Table"),
    StringExprConfig("lpad", "select lpad(c1, 150, 'x') from parquetV1Table"),
    StringExprConfig("ltrim", "select ltrim(c1) from parquetV1Table"),
    StringExprConfig("octet_length", "select octet_length(c1) from parquetV1Table"),
    StringExprConfig(
      "regexp_replace",
      "select regexp_replace(c1, '[0-9]', 'X') from parquetV1Table"),
    StringExprConfig("repeat", "select repeat(c1, 3) from parquetV1Table"),
    StringExprConfig("replace", "select replace(c1, '123', 'ab') from parquetV1Table"),
    StringExprConfig("reverse", "select reverse(c1) from parquetV1Table"),
    StringExprConfig("rlike", "select c1 rlike '[0-9]+' from parquetV1Table"),
    StringExprConfig("rpad", "select rpad(c1, 150, 'x') from parquetV1Table"),
    StringExprConfig("rtrim", "select rtrim(c1) from parquetV1Table"),
    StringExprConfig("space", "select space(2) from parquetV1Table"),
    StringExprConfig("startswith", "select startswith(c1, '1') from parquetV1Table"),
    StringExprConfig("substring", "select substring(c1, 1, 100) from parquetV1Table"),
    StringExprConfig("translate", "select translate(c1, '123456', 'aBcDeF') from parquetV1Table"),
    StringExprConfig("trim", "select trim(c1) from parquetV1Table"),
    StringExprConfig("upper", "select upper(c1) from parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("String expressions", 1024) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 10) AS c1 FROM $tbl"))

          val extraConfigs = Map(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true")

          stringExpressions.foreach { config =>
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
