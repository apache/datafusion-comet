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

import org.apache.spark.sql.catalyst.optimizer.InferFiltersFromGenerate
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure performance of Comet's explode operator (`CometExplodeExec`) against
 * Spark's `GenerateExec`, across the dimensions that drive generator cost: fan-out, generator
 * variant, element type, and the number of columns replicated alongside the generated one. To
 * run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometExplodeBenchmark
 * }}}
 *
 * Every case counts the generated columns rather than writing them, so the only row boundary is
 * one row per partition. Writing them with `.noop()` would put a columnar-to-row conversion of
 * every generated row inside the Comet arm and none inside the Spark arm, whose `GenerateExec`
 * already emits rows: 419K conversions at fan-out 2 and 21M at fan-out 100, scaling with the very
 * dimension the case is meant to isolate. `CometColumnarToRowBenchmark` measures that conversion
 * on its own.
 *
 * Times are still whole-query totals and include the Parquet scan, the counting aggregate and its
 * exchange, and per-iteration planning. The scan is a large share of the total at fan-out 2,
 * where it compresses the ratio between the two engines, and a small one at fan-out 100. Issue
 * #5363 tracks reporting operator cost against a scan baseline instead; when that lands, this
 * paragraph should go.
 *
 * `InferFiltersFromGenerate` is excluded, for both engines. It infers `size(arr) > 0 AND arr IS
 * NOT NULL` below a generator but matches only on `outer = false`, so leaving it enabled hands
 * `explode` and `posexplode` 209,714 rows and an extra filter while their outer variants get all
 * 262,144 — a comparison of two different plans, reported against a row count neither arm
 * processes. Worth knowing when reading these numbers: real queries do get that filter, so a
 * plain `explode` in production usually sees an array column with no nulls and no empty rows.
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
   *
   * Elements are wrapped in a never-taken null branch so the array types as `containsNull`; see
   * [[nullableExpr]].
   */
  private def arrayColumn(elementExpr: String, len: Int): String = {
    val full = s"transform(sequence(1, $len), x -> ${nullableExpr(elementExpr, "x = 0")})"
    s"""CASE
       |  WHEN id % 10 = 0 THEN NULL
       |  WHEN id % 10 = 1 THEN slice($full, 1, 0)
       |  ELSE $full
       |END AS arr""".stripMargin
  }

  /**
   * Types `expr` as nullable without ever evaluating to null, `guard` being a predicate that is
   * never true.
   *
   * Every column these queries count has to be nullable: `NullPropagation` rewrites `count(c)` to
   * `count(1)` when `c` is not, and the counted column then has no reader at all. For the
   * carried-column case that is fatal, because column pruning goes on to drop `k`, `s` and `v`
   * from the generator's input, which is the entire dimension being measured. It would also split
   * the variant group, since `outer` forces the generated column nullable and the plain variants
   * do not. Parquet columns are usually optional in practice anyway.
   */
  private def nullableExpr(expr: String, guard: String): String = s"IF($guard, NULL, $expr)"

  /**
   * The string element, shared by the string and struct datasets so that the element-type cases
   * differ in element type alone. A struct field of `s1` through `s10` would hold 10 distinct
   * values against this column's 260,000-odd, which stays under the writer's 1 MiB dictionary
   * page threshold where this one does not, so the comparison would also be measuring the
   * difference between a dictionary-encoded column and a plain one.
   */
  private val stringElement = "concat('str_', CAST(id + x AS STRING))"

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
    "arr_str10" -> Seq(arrayColumn(stringElement, 10)),
    "arr_struct10" -> Seq(arrayColumn(s"struct(id + x AS a, $stringElement AS b)", 10)),
    "arr_carry" -> Seq(
      arrayColumn("id + x", 10),
      s"${nullableExpr("id", "id < 0")} AS k",
      s"${nullableExpr("CAST(id AS STRING)", "id < 0")} AS s",
      s"${nullableExpr("id * 2", "id < 0")} AS v"))

  /** Writes `selectExprs` over `numRows` rows to Parquet and registers it as a temp view. */
  private def createView(dir: File, name: String, selectExprs: Seq[String]): Unit = {
    val path = s"${dir.getAbsolutePath}/$name"
    spark.range(numRows).selectExpr(selectExprs: _*).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(name)
  }

  /**
   * A query that applies `generator` to `view`'s `arr` column, carries `carried` through the
   * generator alongside it, and counts every column that comes out.
   *
   * The position column of the `posexplode` variants is counted too, because a generator whose
   * second output nothing reads is not the generator being named.
   */
  private def countGenerated(
      generator: String,
      view: String,
      carried: Seq[String] = Nil,
      where: Option[String] = None): String = {
    val generated = if (generator.startsWith("posexplode")) Seq("pos", "col") else Seq("col")
    val alias =
      if (generated.length == 1) s"AS ${generated.head}"
      else generated.mkString("AS (", ", ", ")")
    val projectList = (carried :+ s"$generator(arr) $alias").mkString(", ")
    val filter = where.map(w => s" WHERE $w").getOrElse("")
    val counts = (carried ++ generated).map(c => s"count($c)").mkString(", ")
    s"SELECT $counts FROM (SELECT $projectList FROM $view$filter)"
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    withTempPath { dir =>
      withTempTable(datasets.map(_._1): _*) {
        datasets.foreach { case (name, selectExprs) => createView(dir, name, selectExprs) }

        // `runExpressionBenchmark` appends ConstantFolding to whatever the caller has already
        // excluded, and applies the result to both arms, so setting this here excludes both.
        withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromGenerate.ruleName) {

          // Cardinality is input rows for every case, so the numbers are per scanned row rather
          // than per generated row. Fan-out is named in the case title: the 100-element case
          // emits roughly 50 times as many rows as the 2-element case from the same 256K inputs.
          runBenchmark("Explode - fan-out") {
            Seq(2, 10, 100).foreach { len =>
              runExpressionBenchmark(
                s"explode array<bigint>[$len]",
                numRows,
                countGenerated("explode", s"arr_len$len"))
            }
          }

          runBenchmark("Explode - generator variants") {
            Seq("explode", "posexplode", "explode_outer", "posexplode_outer").foreach {
              generator =>
                runExpressionBenchmark(
                  s"$generator array<bigint>[10]",
                  numRows,
                  countGenerated(generator, "arr_len10"))
            }
          }

          runBenchmark("Explode - element type") {
            Seq("bigint" -> "arr_len10", "string" -> "arr_str10", "struct" -> "arr_struct10")
              .foreach { case (elementType, view) =>
                runExpressionBenchmark(
                  s"explode array<$elementType>[10]",
                  numRows,
                  countGenerated("explode", view))
              }
          }

          // Both cases read all four columns. The filter is always true and references k, s and
          // v below the generator, where column pruning then drops them from the generator's
          // input rather than replicating them; without it, `explode alone` would prune three
          // columns from the Parquet scan and the difference between the two cases would be
          // scan and string-decode work as much as replication. What the filter does not
          // equalize is the three extra counts the carried case runs over the generated rows.
          // Those are cheaper than the three gathers they exist to measure, but not free.
          runBenchmark("Explode - carried columns") {
            val readAllColumns = Some("k >= 0 AND v >= 0 AND length(s) > 0")
            runExpressionBenchmark(
              "explode alone",
              numRows,
              countGenerated("explode", "arr_carry", where = readAllColumns))
            runExpressionBenchmark(
              "explode plus 3 carried columns",
              numRows,
              countGenerated(
                "explode",
                "arr_carry",
                carried = Seq("k", "s", "v"),
                where = readAllColumns))
          }
        }
      }
    }
  }
}
