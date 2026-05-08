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
 * Benchmark regex expressions across execution modes:
 *
 *   - Spark
 *   - Comet (Scan only)
 *   - Comet (Scan + Exec, native Rust regex), where applicable
 *   - Comet (Scan + Exec, JVM hand-coded UDF; codegen dispatch explicitly disabled)
 *   - Comet (Scan + Exec, JVM codegen dispatch forced)
 *
 * Plus a composed-expression block that exercises the codegen dispatcher's headline advantage:
 * fusing nested expression trees into one Janino-compiled kernel rather than running each
 * sub-expression as its own native operator with intermediate column materialization.
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

  // Patterns chosen to span common rlike shapes. Avoid Java-only constructs
  // that the native (Rust) path cannot accept, since those would be skipped
  // rather than benchmarked in the native case.
  private val patterns = List(
    RegExpPattern("character_class", "[0-9]+"),
    RegExpPattern("anchored", "^[0-9]"),
    RegExpPattern("alternation", "abc|def|ghi"),
    RegExpPattern("multi_class", "[a-zA-Z][0-9]+"),
    RegExpPattern("repetition", "(ab){2,}"))

  // regexp_replace cases. Returns StringType, so the codegen dispatcher exercises the
  // variable-length output write path. No_match keeps the input intact (upper bound on copy
  // cost); small_match replaces a narrow span; wide_match replaces most of each row.
  private val replacePatterns = List(
    (RegExpPattern("replace_no_match", "xyzzy"), "<none>"),
    (RegExpPattern("replace_small_match", "\\d+"), "N"),
    (RegExpPattern("replace_wide_match", "[a-zA-Z0-9]"), "*"))

  // regexp_extract cases. Returns StringType. Audit question: does the default codegen path
  // (Spark's `RegExpExtract.doGenCode` plus our wrapper) pay a measurable penalty vs the
  // hand-coded `RegExpExtractUDF`? If yes, that justifies a specialized emitter analogous to
  // the `RegExpReplace` one.
  private val extractPatterns = List(
    RegExpPattern("extract_alpha", "([a-z]+)"),
    RegExpPattern("extract_digit_run", "([0-9]+)"),
    RegExpPattern("extract_two_groups", "([a-z]+)([0-9]+)"))

  // regexp_instr cases. Returns IntegerType. Same hand-coded vs codegen comparison; also
  // exercises the IntVector output writer path end to end.
  private val instrPatterns = List(
    RegExpPattern("instr_digit", "[0-9]+"),
    RegExpPattern("instr_alpha", "[a-z]+"),
    RegExpPattern("instr_no_match", "xyzzy"))

  // Composed-expression cases. The interesting comparison is "one fused codegen kernel" vs
  // "Comet runs the inner expression as a native operator, materializes the intermediate
  // string column, hands it to the JVM UDF". The codegen-dispatch column should win the wider
  // the gap as the inner expression count grows.
  private val composedPatterns: List[(String, String)] = List(
    ("composed_upper_rlike", "upper(c1) rlike '[A-Z0-9]+'"),
    ("composed_regexp_replace_upper", "regexp_replace(upper(c1), '[0-9]+', 'N')"),
    ("composed_substr_upper_rlike", "substring(upper(c1), 1, 5) rlike '^[A-Z]+$'"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("rlike modes", 1024) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 10) AS c1 FROM $tbl"))

          patterns.foreach { p =>
            val query = s"select c1 rlike '${p.pattern}' from parquetV1Table"
            runBenchmark(p.name) {
              runRegexModes(p.name, v, query, hasNativeRustPath = true)
            }
          }

          replacePatterns.foreach { case (p, replacement) =>
            val query =
              s"select regexp_replace(c1, '${p.pattern}', '$replacement') from parquetV1Table"
            runBenchmark(p.name) {
              runRegexModes(p.name, v, query, hasNativeRustPath = true)
            }
          }

          extractPatterns.foreach { p =>
            val query =
              s"select regexp_extract(c1, '${p.pattern}', 1) from parquetV1Table"
            runBenchmark(p.name) {
              runRegexModes(p.name, v, query, hasNativeRustPath = false)
            }
          }

          instrPatterns.foreach { p =>
            val query =
              s"select regexp_instr(c1, '${p.pattern}', 0) from parquetV1Table"
            runBenchmark(p.name) {
              runRegexModes(p.name, v, query, hasNativeRustPath = false)
            }
          }

          composedPatterns.foreach { case (name, exprSql) =>
            val query = s"select $exprSql from parquetV1Table"
            runBenchmark(name) {
              // Composed cases must enable case conversion so upper() doesn't fall back at
              // plan time; we want to compare with that path engaged.
              withSQLConf(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true") {
                runRegexModes(name, v, query, hasNativeRustPath = false)
              }
            }
          }
        }
      }
    }
  }

  /**
   * Runs the standard set of execution modes for a single regex query. `hasNativeRustPath`
   * controls whether the "native Rust regex" case is included; expressions like regexp_extract /
   * regexp_instr have no Comet-native implementation so the column would just duplicate the Spark
   * fallback row.
   */
  private def runRegexModes(
      name: String,
      cardinality: Long,
      query: String,
      hasNativeRustPath: Boolean): Unit = {
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
      "spark.sql.optimizer.constantFolding.enabled" -> "false")

    if (hasNativeRustPath) {
      benchmark.addCase("Comet (Exec, native Rust regex)") { _ =>
        val configs =
          baseExec ++ Map(CometConf.getExprAllowIncompatConfigKey("regexp") -> "true")
        withSQLConf(configs.toSeq: _*) {
          spark.sql(query).noop()
        }
      }
    }

    // Hand-coded JVM UDF path. Explicitly disable codegen dispatch; the default `auto` mode
    // would otherwise prefer codegen when engine=java and we'd be measuring the same path
    // twice.
    benchmark.addCase("Comet (Exec, JVM regex hand-coded)") { _ =>
      val configs =
        baseExec ++ Map(
          CometConf.COMET_REGEXP_ENGINE.key -> CometConf.REGEXP_ENGINE_JAVA,
          CometConf.COMET_CODEGEN_DISPATCH_MODE.key -> CometConf.CODEGEN_DISPATCH_DISABLED)
      withSQLConf(configs.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.addCase("Comet (Exec, JVM codegen dispatch)") { _ =>
      val configs =
        baseExec ++ Map(
          CometConf.COMET_REGEXP_ENGINE.key -> CometConf.REGEXP_ENGINE_JAVA,
          CometConf.COMET_CODEGEN_DISPATCH_MODE.key -> CometConf.CODEGEN_DISPATCH_FORCE)
      withSQLConf(configs.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.run()
  }
}
