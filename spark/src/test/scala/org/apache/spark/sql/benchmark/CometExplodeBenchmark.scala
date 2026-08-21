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

import java.io.File

/**
 * Benchmark to measure performance of Comet's explode operator (`CometExplodeExec`) against
 * Spark's `GenerateExec`, across the dimensions that drive generator cost: fan-out, generator
 * variant, element type, and the number of columns replicated alongside the generated one. To
 * run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometExplodeBenchmark
 * }}}
 *
 * `runExpressionBenchmark` reports whole-query totals, so the times below also include the
 * Parquet scan, the result transfer, and the per-iteration query planning. That fixed cost is a
 * large share of the total at fan-out 2, where it compresses the ratio between the two engines,
 * and a small one at fan-out 100. Issue #5363 tracks reporting operator cost against a scan
 * baseline instead; when that lands, this paragraph should go.
 */
object CometExplodeBenchmark extends CometBenchmarkBase {

  private val numRows = 256 * 1024

  /**
   * A SQL expression for an array column of `len` elements of `elementExpr`, where `elementExpr`
   * may reference the row's `id` and the element's one-based position `x`.
   *
   * One in ten rows holds a null array and another one in ten holds an empty array, so that
   * `explode` and `explode_outer` are a real comparison rather than the same query twice: the
   * outer variants emit a null row for those 20% of rows where the plain variants emit nothing.
   *
   * The empty array is built with `slice`, not `array()`, because `array()` types as
   * `array<null>` and would give that row's column a different element type.
   */
  private def arrayColumn(elementExpr: String, len: Int): String = {
    val full = s"transform(sequence(1, $len), x -> $elementExpr)"
    s"""CASE
       |  WHEN id % 10 = 0 THEN NULL
       |  WHEN id % 10 = 1 THEN slice($full, 1, 0)
       |  ELSE $full
       |END AS arr""".stripMargin
  }

  /**
   * The temp views the benchmark reads, each with the expressions that build it.
   *
   * Each array column gets its own view rather than sharing one wide table, so that a case is
   * never charged for scanning an array column it does not read.
   */
  private val datasets: Seq[(String, Seq[String])] = Seq(
    "arr_len2" -> Seq(arrayColumn("id + x", 2)),
    "arr_len10" -> Seq(arrayColumn("id + x", 10)),
    "arr_len100" -> Seq(arrayColumn("id + x", 100)),
    "arr_str10" -> Seq(arrayColumn("concat('str_', CAST(id + x AS STRING))", 10)),
    "arr_struct10" -> Seq(
      arrayColumn("struct(id + x AS a, concat('s', CAST(x AS STRING)) AS b)", 10)),
    "arr_carry" -> Seq(
      arrayColumn("id + x", 10),
      "id AS k",
      "CAST(id AS STRING) AS s",
      "id * 2 AS v"))

  /** Writes `selectExprs` over `numRows` rows to Parquet and registers it as a temp view. */
  private def createView(dir: File, name: String, selectExprs: Seq[String]): Unit = {
    val path = s"${dir.getAbsolutePath}/$name"
    spark.range(numRows).selectExpr(selectExprs: _*).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(name)
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    withTempPath { dir =>
      withTempTable(datasets.map(_._1): _*) {
        datasets.foreach { case (name, selectExprs) => createView(dir, name, selectExprs) }

        // Cardinality is input rows for every case, so the numbers are per scanned row rather
        // than per generated row. Fan-out is named in the case title: the 100-element case emits
        // roughly 50 times as many rows as the 2-element case from the same 256K inputs.
        runBenchmark("Explode - fan-out") {
          Seq(2, 10, 100).foreach { len =>
            runExpressionBenchmark(
              s"explode array<bigint>[$len]",
              numRows,
              s"SELECT explode(arr) FROM arr_len$len")
          }
        }

        runBenchmark("Explode - generator variants") {
          Seq("explode", "posexplode", "explode_outer", "posexplode_outer").foreach { generator =>
            runExpressionBenchmark(
              s"$generator array<bigint>[10]",
              numRows,
              s"SELECT $generator(arr) FROM arr_len10")
          }
        }

        runBenchmark("Explode - element type") {
          Seq("bigint" -> "arr_len10", "string" -> "arr_str10", "struct" -> "arr_struct10")
            .foreach { case (elementType, view) =>
              runExpressionBenchmark(
                s"explode array<$elementType>[10]",
                numRows,
                s"SELECT explode(arr) FROM $view")
            }
        }

        runBenchmark("Explode - carried columns") {
          runExpressionBenchmark("explode alone", numRows, "SELECT explode(arr) FROM arr_carry")
          runExpressionBenchmark(
            "explode plus 3 carried columns",
            numRows,
            "SELECT k, s, v, explode(arr) FROM arr_carry")
        }
      }
    }
  }
}
