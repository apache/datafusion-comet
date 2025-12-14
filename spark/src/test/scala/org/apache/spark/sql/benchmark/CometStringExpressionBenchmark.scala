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
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.comet.CometConf

/**
 * Configuration for a string expression benchmark.
 * @param name
 *   Display name for the benchmark
 * @param query
 *   SQL query to benchmark
 * @param dataPreparation
 *   Function to prepare test data (defaults to repeated string)
 * @param extraCometConfigs
 *   Additional Comet configurations for the scan+exec case
 */
case class StringExprConfig(
    name: String,
    query: String,
    dataPreparation: (String, SparkSession) => DataFrame = (tbl, spark) =>
      spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"),
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
        prepareTable(dir, config.dataPreparation(tbl, spark))

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
    StringExprConfig("Substring Expr", "select substring(c1, 1, 100) from parquetV1Table"),
    StringExprConfig(
      "StringSpace Expr",
      "select space(c1) from parquetV1Table",
      (tbl, spark) => spark.sql(s"SELECT CAST(RAND(1) * 100 AS INTEGER) AS c1 FROM $tbl")),
    StringExprConfig("Expr ascii", "select ascii(c1) from parquetV1Table"),
    StringExprConfig("Expr bit_length", "select bit_length(c1) from parquetV1Table"),
    StringExprConfig("Expr octet_length", "select octet_length(c1) from parquetV1Table"),
    StringExprConfig(
      "Expr upper",
      "select upper(c1) from parquetV1Table",
      extraCometConfigs = Map(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true")),
    StringExprConfig("Expr lower", "select lower(c1) from parquetV1Table"),
    StringExprConfig("Expr chr", "select chr(c1) from parquetV1Table"),
    StringExprConfig("Expr initCap", "select initCap(c1) from parquetV1Table"),
    StringExprConfig("Expr trim", "select trim(c1) from parquetV1Table"),
    StringExprConfig("Expr concatws", "select concat_ws(' ', c1, c1) from parquetV1Table"),
    StringExprConfig("Expr length", "select length(c1) from parquetV1Table"),
    StringExprConfig("Expr repeat", "select repeat(c1, 3) from parquetV1Table"),
    StringExprConfig("Expr reverse", "select reverse(c1) from parquetV1Table"),
    StringExprConfig("Expr instr", "select instr(c1, '123') from parquetV1Table"),
    StringExprConfig("Expr replace", "select replace(c1, '123', 'abc') from parquetV1Table"),
    StringExprConfig(
      "Expr translate",
      "select translate(c1, '123456', 'aBcDeF') from parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024;

    // Map each config to a short name for runBenchmarkWithTable
    val benchmarkNames = List(
      "Substring",
      "StringSpace",
      "ascii",
      "bitLength",
      "octet_length",
      "upper",
      "lower",
      "chr",
      "initCap",
      "trim",
      "concatws",
      "repeat",
      "length",
      "reverse",
      "instr",
      "replace",
      "translate")

    stringExpressions.zip(benchmarkNames).foreach { case (config, name) =>
      runBenchmarkWithTable(name, values) { v =>
        runStringExprBenchmark(config, v)
      }
    }
  }
}
