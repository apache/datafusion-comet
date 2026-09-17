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
 * Benchmark to measure performance of Comet's native `sequence` kernel against Spark's codegen
 * (issue #5349). Integral shapes run natively under Comet; the date case stays on the JVM codegen
 * dispatcher in both arms and is included to show that path is unchanged. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometSequenceBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometSequenceBenchmark-**results.txt".
 *
 * Endpoints are materialized as columns in the prepared Parquet table. The native path is only
 * eligible when every argument to `sequence` is a literal or a column reference (see
 * `CometSequence.argsAreLiteralsOrRefs`), so an arithmetic argument such as `c_start + 4` would
 * silently route the whole expression through the JVM codegen dispatcher and defeat the point of
 * the benchmark.
 */
object CometSequenceBenchmark extends CometBenchmarkBase {

  private val sequenceQueries = List(
    ("seq_short_5_elems", "SELECT sequence(c_start, c_stop_5) FROM parquetV1Table"),
    ("seq_spine_365_elems", "SELECT sequence(c_start, c_stop_365) FROM parquetV1Table"),
    ("seq_long_10000_elems", "SELECT sequence(c_start, c_stop_10000) FROM parquetV1Table"),
    ("seq_descending_default_step", "SELECT sequence(c_stop_365, c_start) FROM parquetV1Table"),
    ("seq_explicit_step_7", "SELECT sequence(c_start, c_stop_365, 7L) FROM parquetV1Table"),
    (
      "seq_sparse_nulls_365_elems",
      "SELECT sequence(c_null_start, c_null_stop_365) FROM parquetV1Table"),
    // Date/timestamp element types always stay on the JVM codegen dispatcher regardless of
    // argument shape, so the arithmetic form here is intentional — this case is the control that
    // shows the dispatcher path is unchanged.
    (
      "seq_date_spine_dispatcher",
      "SELECT sequence(c_date, c_date + INTERVAL 364 DAYS) FROM parquetV1Table"))

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("sequence", 8192) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql("SELECT CAST(PMOD(value, 100000) AS BIGINT) AS c_start," +
              " CAST(PMOD(value, 100000) AS BIGINT) + 4 AS c_stop_5," +
              " CAST(PMOD(value, 100000) AS BIGINT) + 364 AS c_stop_365," +
              " CAST(PMOD(value, 100000) AS BIGINT) + 9999 AS c_stop_10000," +
              " CASE WHEN PMOD(value, 10) = 0 THEN CAST(NULL AS BIGINT)" +
              " ELSE CAST(PMOD(value, 100000) AS BIGINT) END AS c_null_start," +
              " CASE WHEN PMOD(value, 10) = 0 THEN CAST(NULL AS BIGINT)" +
              " ELSE CAST(PMOD(value, 100000) AS BIGINT) + 364 END AS c_null_stop_365," +
              s" DATE_ADD(DATE'2020-01-01', CAST(PMOD(value, 3650) AS INT)) AS c_date FROM $tbl"))

          sequenceQueries.foreach { case (name, query) =>
            runBenchmark(name) {
              runExpressionBenchmark(name, v, query)
            }
          }
        }
      }
    }
  }
}
