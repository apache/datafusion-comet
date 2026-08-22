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

/**
 * Configuration for a single rlike pattern under benchmark.
 *
 * @param name
 *   short label for the pattern
 * @param pattern
 *   the regex literal supplied to rlike
 */
case class RegExpPattern(name: String, pattern: String)

/**
 * Benchmark `rlike` across execution modes.
 *
 * In-subset patterns (proved Java-equivalent by [[org.apache.comet.expressions.CometRegex]]) take
 * the native path by default, so they are measured as Spark / Scan / Native. Out-of-subset
 * patterns that Rust can still compile (`\d+`) keep a four-way comparison: default is the JVM
 * dispatcher, and `allowIncompatible` selects native.
 *
 * To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 \
 *     make benchmark-org.apache.spark.sql.benchmark.CometRegExpBenchmark
 * }}}
 *
 * Results land in `spark/benchmarks/CometRegExpBenchmark-**results.txt`.
 */
object CometRegExpBenchmark extends CometBenchmarkBase {

  // Analyzer-admitted patterns. Default Comet exec is already native, so do not add a
  // "JVM regex" case: it would silently measure the native path.
  private val inSubsetPatterns = List(
    RegExpPattern("character_class", "[0-9]+"),
    RegExpPattern("alternation", "abc|def|ghi"),
    RegExpPattern("multi_class", "[a-zA-Z][0-9]+"),
    RegExpPattern("repetition", "(ab){2,}"))

  // Analyzer-rejected, Rust-accepted. Default exec is the JVM dispatcher; opt-in is native.
  // Input data is ASCII (REPEAT of numeric strings) so `\d` vs `[0-9]` does not change hits.
  private val outOfSubsetPatterns = List(RegExpPattern("digit_class_shorthand", "\\d+"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("rlike modes", 1024 * 1024) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 10) AS c1 FROM $tbl"))

          inSubsetPatterns.foreach { p =>
            val query = s"select c1 rlike '${p.pattern}' from parquetV1Table"
            runBenchmark(p.name) {
              runInSubsetModes(p.name, v, query)
            }
          }
          outOfSubsetPatterns.foreach { p =>
            val query = s"select c1 rlike '${p.pattern}' from parquetV1Table"
            runBenchmark(p.name) {
              runOutOfSubsetModes(p.name, v, query)
            }
          }
        }
      }
    }
  }

  private val baseExec: Map[String, String] = Map(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    "spark.sql.optimizer.excludedRules" ->
      "org.apache.spark.sql.catalyst.optimizer.ConstantFolding")

  private def addSparkAndScan(benchmark: Benchmark, query: String): Unit = {
    benchmark.addCase("Spark") { _ =>
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.sql(query).noop()
      }
    }
    benchmark.addCase("Comet (Scan)") { _ =>
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "false") {
        spark.sql(query).noop()
      }
    }
  }

  /** Spark / Scan / Native (the new default for in-subset patterns). */
  private def runInSubsetModes(name: String, cardinality: Long, query: String): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)
    addSparkAndScan(benchmark, query)
    benchmark.addCase("Comet (Exec, native Rust regex)") { _ =>
      withSQLConf(baseExec.toSeq: _*) {
        spark.sql(query).noop()
      }
    }
    benchmark.run()
  }

  /** Spark / Scan / Native-on-opt-in / JVM-dispatcher-by-default. */
  private def runOutOfSubsetModes(name: String, cardinality: Long, query: String): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)
    addSparkAndScan(benchmark, query)
    benchmark.addCase("Comet (Exec, native Rust regex)") { _ =>
      val configs = baseExec ++ Map(CometConf.getExprAllowIncompatConfigKey("RLike") -> "true")
      withSQLConf(configs.toSeq: _*) {
        spark.sql(query).noop()
      }
    }
    benchmark.addCase("Comet (Exec, JVM regex)") { _ =>
      withSQLConf(baseExec.toSeq: _*) {
        spark.sql(query).noop()
      }
    }
    benchmark.run()
  }
}
