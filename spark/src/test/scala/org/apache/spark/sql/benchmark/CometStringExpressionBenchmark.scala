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
import org.apache.spark.sql.DataFrame

import org.apache.comet.CometConf

/**
 * Configuration for a string expression benchmark.
 * @param name
 *   Display name for the benchmark
 * @param shortName
 *   Short name for runBenchmarkWithTable
 * @param query
 *   SQL query to benchmark
 * @param extraCometConfigs
 *   Additional Comet configurations for the scan+exec case
 */
case class StringExprConfig(
    name: String,
    shortName: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

// spotless:off
/**
 * Benchmark to measure performance of Comet string expressions. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometStringExpressionBenchmark`
 * Results will be written to "spark/benchmarks/CometStringExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometStringExpressionBenchmark extends CometBenchmarkBase {

  /**
   * Generic method to run a string expression benchmark with the given configuration.
   */
  def runStringExprBenchmark(config: StringExprConfig, values: Int): Unit = {
    val benchmark = new Benchmark(config.name, values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql(config.query).noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql(config.query).noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          val baseConfigs =
            Map(CometConf.COMET_ENABLED.key -> "true", CometConf.COMET_EXEC_ENABLED.key -> "true")
          val allConfigs = baseConfigs ++ config.extraCometConfigs

          withSQLConf(allConfigs.toSeq: _*) {
            spark.sql(config.query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  // Configuration for all string expression benchmarks
  private val stringExpressions = List(
    StringExprConfig(
      "Substring Expr",
      "Substring",
      "select substring(c1, 1, 100) from parquetV1Table"),
    StringExprConfig("Expr ascii", "ascii", "select ascii(c1) from parquetV1Table"),
    StringExprConfig("Expr bit_length", "bitLength", "select bit_length(c1) from parquetV1Table"),
    StringExprConfig(
      "Expr octet_length",
      "octet_length",
      "select octet_length(c1) from parquetV1Table"),
    StringExprConfig(
      "Expr upper",
      "upper",
      "select upper(c1) from parquetV1Table",
      extraCometConfigs = Map(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true")),
    StringExprConfig("Expr lower", "lower", "select lower(c1) from parquetV1Table"),
    StringExprConfig("Expr chr", "chr", "select chr(c1) from parquetV1Table"),
    StringExprConfig("Expr initCap", "initCap", "select initCap(c1) from parquetV1Table"),
    StringExprConfig("Expr trim", "trim", "select trim(c1) from parquetV1Table"),
    StringExprConfig(
      "Expr concatws",
      "concatws",
      "select concat_ws(' ', c1, c1) from parquetV1Table"),
    StringExprConfig("Expr length", "length", "select length(c1) from parquetV1Table"),
    StringExprConfig("Expr repeat", "repeat", "select repeat(c1, 3) from parquetV1Table"),
    StringExprConfig("Expr reverse", "reverse", "select reverse(c1) from parquetV1Table"),
    StringExprConfig("Expr instr", "instr", "select instr(c1, '123') from parquetV1Table"),
    StringExprConfig(
      "Expr replace",
      "replace",
      "select replace(c1, '123', 'abc') from parquetV1Table"),
    StringExprConfig(
      "Expr translate",
      "translate",
      "select translate(c1, '123456', 'aBcDeF') from parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024;

    stringExpressions.foreach { config =>
      runBenchmarkWithTable(config.shortName, values) { v =>
        runStringExprBenchmark(config, v)
      }
    }
  }
}
