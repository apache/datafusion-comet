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
 * Benchmark `rlike` across all execution modes:
 *   - Spark
 *   - Comet (Scan only)
 *   - Comet (Scan + Exec, native Rust regex)
 *   - Comet (Scan + Exec, JVM-side java.util.regex)
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
              runRLikeModes(p.name, v, query)
            }
          }
        }
      }
    }
  }

  /** Runs all four modes for a single rlike query. */
  private def runRLikeModes(name: String, cardinality: Long, query: String): Unit = {
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

    benchmark.addCase("Comet (Exec, native Rust regex)") { _ =>
      val configs = baseExec ++ Map(CometConf.getExprAllowIncompatConfigKey("regexp") -> "true")
      withSQLConf(configs.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.addCase("Comet (Exec, JVM regex)") { _ =>
      val configs = baseExec ++ Map(CometConf.COMET_REGEXP_ENGINE.key -> "java")
      withSQLConf(configs.toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    benchmark.run()
  }
}
