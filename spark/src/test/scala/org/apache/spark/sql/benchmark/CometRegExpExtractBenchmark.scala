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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Configuration for a single regexp_extract pattern under benchmark.
 *
 * @param name
 *   short label for the pattern
 * @param pattern
 *   the regex literal supplied to regexp_extract / regexp_extract_all
 * @param idx
 *   capture-group index (0 for the whole match, 1 for the first group, ...)
 */
case class RegExpExtractPattern(name: String, pattern: String, idx: Int)

/**
 * Benchmark `regexp_extract` and `regexp_extract_all` across all execution modes:
 *   - Spark
 *   - Comet (Scan only)
 *   - Comet (Scan + Exec, native Rust regex)
 *   - Comet (Scan + Exec, JVM-side java.util.regex via codegen dispatcher)
 *
 * To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 \
 *     make benchmark-org.apache.spark.sql.benchmark.CometRegExpExtractBenchmark
 * }}}
 *
 * Results land in `spark/benchmarks/CometRegExpExtractBenchmark-**results.txt`.
 */
object CometRegExpExtractBenchmark extends CometBenchmarkBase {

  // CometBenchmarkBase wires `CometSparkSessionExtensions` via `withExtensions`, but that call
  // is silently dropped when `SparkSession.builder.getOrCreate()` returns an existing session
  // (the `SqlBasedBenchmark.spark` field can construct one before the override runs). Setting
  // `spark.sql.extensions` on the SparkConf forces extension registration regardless. The
  // off-heap and shuffle-manager configs match what CometTestBase sets so Comet's planning
  // rules don't bail out early.
  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometRegExpExtractBenchmark")
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")
    sparkSession
  }

  // Patterns chosen to span common shapes that both engines accept. Avoid Java-only constructs
  // (backreferences, lookaround, possessive quantifiers, embedded flags) so the native (Rust)
  // path is actually exercised rather than falling through to the codegen dispatcher.
  private val patterns = List(
    RegExpExtractPattern("single_group", "([0-9]+)", 1),
    RegExpExtractPattern("two_groups_first", "([a-z]+)([0-9]+)", 1),
    RegExpExtractPattern("two_groups_second", "([a-z]+)([0-9]+)", 2),
    RegExpExtractPattern("whole_match", "[a-z]+[0-9]+", 0),
    RegExpExtractPattern("anchored", "^([0-9]+)", 1),
    RegExpExtractPattern("alternation", "(abc|def|ghi)", 1))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("regexp_extract modes", 1024 * 1024) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // Build a realistic alphanumeric subject so the patterns actually match. The
          // underlying `tbl` view holds Long values (some negative, some positive); we
          // sandwich them between two short letter runs so every pattern in `patterns`
          // (single digit group, `[a-z]+[0-9]+` whole match, alternation against `abc`,
          // etc.) finds a match.
          prepareTable(
            dir,
            spark.sql(s"SELECT CONCAT('abc', CAST(ABS(value) AS STRING), 'def') AS c1 FROM $tbl"))

          patterns.foreach { p =>
            val extractName = s"regexp_extract / ${p.name}"
            val extractQuery =
              s"select regexp_extract(c1, '${p.pattern}', ${p.idx}) from parquetV1Table"
            runBenchmark(extractName) {
              runModes("RegExpExtract", extractName, v, extractQuery)
            }

            val extractAllName = s"regexp_extract_all / ${p.name}"
            val extractAllQuery =
              s"select regexp_extract_all(c1, '${p.pattern}', ${p.idx}) from parquetV1Table"
            runBenchmark(extractAllName) {
              runModes("RegExpExtractAll", extractAllName, v, extractAllQuery)
            }
          }
        }
      }
    }
  }

  /** Runs all four modes for a single regexp_extract / regexp_extract_all query. */
  private def runModes(
      exprClassName: String,
      name: String,
      cardinality: Long,
      query: String): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)

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

    val baseExec = Map(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding")

    benchmark.addCase("Comet (Exec, native Rust regex)") { _ =>
      val configs =
        baseExec ++ Map(CometConf.getExprAllowIncompatConfigKey(exprClassName) -> "true")
      withSQLConf(configs.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.addCase("Comet (Exec, JVM regex)") { _ =>
      // The codegen dispatcher is enabled by default, so no extra config is needed.
      withSQLConf(baseExec.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.run()
  }
}
