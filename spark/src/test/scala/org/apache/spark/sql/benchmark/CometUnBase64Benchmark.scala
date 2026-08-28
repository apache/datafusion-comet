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
 * Configuration for a single unbase64 input shape under benchmark. Each shape is materialized as
 * its own column in the source table, so the same benchmark harness that measures the other
 * string expressions can address it by name.
 *
 * @param name
 *   short label for the shape
 * @param column
 *   name of the pre-encoded source column to feed into unbase64
 */
case class UnBase64Shape(name: String, column: String)

/**
 * Benchmarks `unbase64` end-to-end against Spark's JVM codegen path. The native kernel lives in
 * `native/spark-expr/src/string_funcs/unbase64.rs` and has its own criterion bench for the decode
 * loop; this file measures the full plan (scan + project + shuffle-free) so the reported number
 * captures the same overhead a real query pays.
 *
 * Shapes match the criterion bench so the two views are directly comparable: short single-line
 * values, long single-line values, long CRLF-wrapped values (the default output of Spark's
 * `base64` when `spark.sql.chunkBase64String.enabled = true`), and tiny values (one base64 char
 * per row, the worst case for per-row overhead).
 *
 * To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 \
 *     make benchmark-org.apache.spark.sql.benchmark.CometUnBase64Benchmark
 * }}}
 *
 * Results land in `spark/benchmarks/CometUnBase64Benchmark-**results.txt`.
 */
object CometUnBase64Benchmark extends CometBenchmarkBase {

  private val shapes = List(
    UnBase64Shape("short", "b64_short"),
    UnBase64Shape("long_single_line", "b64_long"),
    UnBase64Shape("long_crlf_wrapped", "b64_long_wrapped"),
    UnBase64Shape("tiny", "b64_tiny"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("unbase64", 8192) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          // Pre-encode each shape at write time so unbase64 is the only decoder in the plan.
          // `base64` on a 16-byte payload fits in one line; `base64` on a 200-byte payload with
          // Spark's default chunking wraps at 76 chars with CRLF, which is the round-trip shape
          // real workloads see. The tiny column encodes a single byte per row, isolating per-row
          // overhead (offset walk, capacity guard, null-check) from decode throughput.
          // `translate(x, concat(chr(13), chr(10)), '')` deletes both CR and LF from the base64
          // output, giving a long single-line variant of the same payload the wrapped column
          // encodes. Avoid `'\r\n'` in a SQL string literal because Spark's parser does not
          // interpret backslash escapes there.
          prepareTable(
            dir,
            spark.sql(s"""
              SELECT
                base64(cast(repeat('z', 16) AS binary)) AS b64_short,
                base64(cast(repeat('q', 200) AS binary)) AS b64_long_wrapped,
                translate(
                  base64(cast(repeat('q', 200) AS binary)),
                  concat(chr(13), chr(10)),
                  '') AS b64_long,
                base64(cast('a' AS binary)) AS b64_tiny
              FROM $tbl
            """))

          shapes.foreach { s =>
            val query = s"select unbase64(${s.column}) from parquetV1Table"
            runBenchmark(s.name) {
              runUnBase64Modes(s.name, v, query)
            }
          }
        }
      }
    }
  }

  /** Runs Spark vs Comet-native for a single unbase64 shape. */
  private def runUnBase64Modes(name: String, cardinality: Long, query: String): Unit = {
    // CometUnBase64 is `Compatible`, so no allowIncompatible flag is required for the native
    // arm. There is no meaningful JVM-fallback intermediate case here (unlike RLike), so this is
    // a straight Spark-vs-Comet comparison.
    runExpressionBenchmark(name, cardinality, query)
  }
}
