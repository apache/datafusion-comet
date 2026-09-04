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

import org.apache.comet.CometConf

/**
 * Benchmark to measure performance of Comet's explode operator (`CometExplodeExec`) against
 * Spark's `GenerateExec`, across the dimensions that drive generator cost: fan-out, generator
 * variant, element type, how deeply the exploded array is nested in its input row, and the number
 * of columns replicated alongside the generated one. To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometExplodeBenchmark
 * }}}
 *
 * Every case aggregates the generated columns rather than writing them, so the only row boundary
 * is one row per partition. Writing them with `.noop()` would put a columnar-to-row conversion of
 * every generated row inside the Comet arm and none inside the Spark arm, whose `GenerateExec`
 * already emits rows: 419K conversions at fan-out 2 and 21M at fan-out 100, scaling with the very
 * dimension the case is meant to isolate. `CometColumnarToRowBenchmark` measures that conversion
 * on its own.
 *
 * Every case's aggregate is checked against the row it must produce, under both engines, before
 * it is timed; see [[verifySink]]. A sink that quietly stops reading what it names still returns
 * a number, and the results table cannot tell the difference.
 *
 * Times are still whole-query totals and include the Parquet scan, the aggregate and its
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
 *
 * Only array inputs are covered. Comet declines to convert a generator over a map
 * (https://github.com/apache/datafusion-comet/issues/2837), so the Comet arm of such a case would
 * be Spark's `GenerateExec` behind a columnar-to-row transition and its timing would say nothing
 * about `CometExplodeExec`. The nesting group below therefore reaches its event list through an
 * `array<struct<...>>` where a map would be the more natural modeling choice.
 */
object CometExplodeBenchmark extends CometBenchmarkBase {

  private val numRows = 256 * 1024

  /** One benchmark case: the query to time, and the single row it must produce. */
  private case class Case(query: String, expected: Seq[Long])

  /** A temp view the benchmark reads: its row count, and the expressions that build it. */
  private case class TempView(name: String, rows: Int, columns: Seq[String])

  /**
   * Rows of an `rows`-row dataset whose [[arrayColumn]] is neither NULL nor empty, which is how
   * many rows reach a non-outer generator with something to emit.
   */
  private def nonEmptyRows(rows: Int): Long = rows - (rows + 9) / 10 - (rows + 8) / 10

  /**
   * A SQL expression for an array of `len` elements of `elementExpr`, where `elementExpr` may
   * reference the row's `id` and the element's one-based position `v`.
   *
   * Elements are wrapped in a never-taken null branch so the array types as `containsNull`; see
   * [[nullableExpr]].
   */
  private def fullArray(elementExpr: String, len: Int, v: String = "x"): String =
    s"transform(sequence(1, $len), $v -> ${nullableExpr(elementExpr, s"$v = 0")})"

  /**
   * [[fullArray]], but one row in ten holds a null array and another one in ten holds an empty
   * array, so that `explode` and `explode_outer` are a real comparison rather than the same query
   * twice: the outer variants emit a null row for those 20% of rows where the plain variants emit
   * nothing.
   *
   * The empty array is built with `slice`, not `array()`, because `array()` types as
   * `array<null>` and would give that row's column a different element type.
   */
  private def arrayColumn(elementExpr: String, len: Int, v: String = "x"): String = {
    val full = fullArray(elementExpr, len, v)
    s"""CASE
       |  WHEN id % 10 = 0 THEN NULL
       |  WHEN id % 10 = 1 THEN slice($full, 1, 0)
       |  ELSE $full
       |END""".stripMargin
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

  /** Rows in the nesting group's dataset. Its rows are far larger than the other datasets'. */
  private val nestedRows = 100 * 1024

  /** Event lists per row, and events per list, in the nesting group's dataset. */
  private val eventsLen = 4
  private val entriesLen = 5

  /** The struct fields between `profile.account` and `sessions`, outermost first. */
  private val accountPath = Seq("settings", "preferences", "notifications", "activity")

  /** `profile.account.settings.preferences.notifications.activity.sessions.events`. */
  private val deepEvents =
    (Seq("profile", "account") ++ accountPath ++ Seq("sessions", "events")).mkString(".")

  /** One event: four scalar fields, so that carrying it through a generator is not free. */
  private val entryStruct =
    """named_struct(
      |  'type', concat('t_', CAST(y AS STRING)),
      |  'ts', id * 1000 + y,
      |  'page', concat('page_', CAST(id + y AS STRING)),
      |  'source', concat('src_', CAST(id % 4 AS STRING)))""".stripMargin

  /** One platform's event list, the element of the array the nesting group explodes. */
  private val eventStruct =
    s"""named_struct(
       |  'platform', concat('p_', CAST(x AS STRING)),
       |  'entries', ${fullArray(entryStruct, entriesLen, "y")})""".stripMargin

  private val eventsArray = arrayColumn(eventStruct, eventsLen)

  /** [[eventsArray]] wrapped in the struct chain that puts it eight accessors down. */
  private val profileColumn = {
    val sessions =
      s"""named_struct(
         |  'device', named_struct('type', 'mobile', 'os', 'iOS'),
         |  'events', $eventsArray)""".stripMargin
    val account = accountPath.foldRight(s"named_struct('sessions', $sessions)") {
      (field, inner) => s"named_struct('$field', $inner)"
    }
    s"""named_struct(
       |  'account', $account,
       |  'billing', named_struct('currency', 'USD', 'country', 'US'),
       |  'addresses', array(named_struct('city', 'San Jose', 'country', 'US')))""".stripMargin
  }

  /**
   * The temp views the benchmark reads.
   *
   * Each array column gets its own view rather than sharing one wide table, so that a case is
   * never charged for scanning an array column it does not read. The nesting group is the
   * exception: `events` and `profile` hold the same array at two different depths, and keeping
   * them in one view is what makes them the same array. Nested schema pruning stops either case
   * from reading the other's copy.
   */
  private val views: Seq[TempView] = Seq(
    TempView("arr_len2", numRows, Seq(s"${arrayColumn("id + x", 2)} AS arr")),
    TempView("arr_len10", numRows, Seq(s"${arrayColumn("id + x", 10)} AS arr")),
    TempView("arr_len100", numRows, Seq(s"${arrayColumn("id + x", 100)} AS arr")),
    TempView("arr_str10", numRows, Seq(s"${arrayColumn(stringElement, 10)} AS arr")),
    TempView(
      "arr_struct10",
      numRows,
      Seq(s"${arrayColumn(s"struct(id + x AS a, $stringElement AS b)", 10)} AS arr")),
    TempView(
      "arr_carry",
      numRows,
      Seq(
        s"${arrayColumn("id + x", 10)} AS arr",
        s"${nullableExpr("id", "id < 0")} AS k",
        s"${nullableExpr("CAST(id AS STRING)", "id < 0")} AS s",
        s"${nullableExpr("id * 2", "id < 0")} AS v")),
    TempView(
      "nested",
      nestedRows,
      Seq(
        s"${nullableExpr("id", "id < 0")} AS k",
        s"${nullableExpr("concat('r_', CAST(id % 8 AS STRING))", "id < 0")} AS region",
        s"$eventsArray AS events",
        s"$profileColumn AS profile")))

  /** Writes a view's rows to Parquet and registers it. */
  private def createView(dir: File, view: TempView): Unit = {
    val path = s"${dir.getAbsolutePath}/${view.name}"
    spark.range(view.rows).selectExpr(view.columns: _*).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(view.name)
  }

  /**
   * A case that applies `generator` to `arrayExpr` over `view`, carries `carried` through the
   * generator alongside it, and aggregates every column that comes out.
   *
   * The position column of the `posexplode` variants is summed rather than counted, because
   * counting it does not read a position. `pos` is declared non-null, so `NullPropagation`
   * rewrites `count(pos)` to `count(1)`; even without that rewrite `Count` never reads a non-null
   * argument's value. The two engines produce those values very differently — Spark hands over
   * the loop index it already has, while Comet materializes a parallel `List<Int32>` through
   * `ListPositionsExpr` and unnests it alongside the values — so a sink that ignores them is not
   * measuring the generator it names. `sum` over the same column is value-dependent.
   *
   * Everything else is counted rather than summed: the element type varies across these cases and
   * most of the element types cannot be summed, and unlike `pos` the generated column is nullable
   * everywhere, so the count is not rewritten away.
   */
  private def generatorCase(
      generator: String,
      view: String,
      len: Int,
      rows: Int = numRows,
      arrayExpr: String = "arr",
      carried: Seq[String] = Nil,
      where: Option[String] = None): Case = {
    val position = generator.startsWith("posexplode")
    val generated = if (position) Seq("pos", "col") else Seq("col")
    val alias =
      if (generated.length == 1) s"AS ${generated.head}"
      else generated.mkString("AS (", ", ", ")")
    val projectList = (carried :+ s"$generator($arrayExpr) $alias").mkString(", ")
    val filter = where.map(w => s" WHERE $w").getOrElse("")
    val aggregates = (carried ++ generated).map(aggregate)
    val query =
      s"SELECT ${aggregates.mkString(", ")} FROM (SELECT $projectList FROM $view$filter)"

    // The outer variants add one all-null row for each row the plain variants drop, which the
    // carried columns count and the generated columns do not.
    val elements = nonEmptyRows(rows) * len
    val outputRows =
      if (generator.endsWith("_outer")) elements + (rows - nonEmptyRows(rows)) else elements
    val expected = carried.map(_ => outputRows) ++ generated.map {
      // Positions run 0 until len on every row that emits, and are null on the rows only an
      // outer variant emits, which `sum` skips.
      case "pos" => elements * (len - 1) / 2
      case _ => elements
    }
    Case(query, expected)
  }

  /** See [[generatorCase]] for why `pos` alone is summed. */
  private def aggregate(column: String): String =
    if (column == "pos") s"sum($column)" else s"count($column)"

  /**
   * Runs `query` under both engines, untimed, and fails unless each produces exactly `expected`.
   *
   * This guards the sinks. Every timing here is only worth reading if the case's aggregate
   * actually consumes the generated columns, and an aggregate that has stopped consuming them is
   * invisible in the results table: it still reports a rate, just a better one. `count(pos)` was
   * such a sink until this benchmark switched to `sum(pos)`. Pinning the expected values makes
   * the next one loud.
   *
   * Constant folding is left enabled, unlike in the timed runs. Excluding it there keeps per-row
   * work per-row; it cannot change a result, and these queries hold no constant subexpression for
   * it to fold in any case.
   */
  private def verifySink(name: String, query: String, expected: Seq[Long]): Unit =
    Seq(false, true).foreach { cometEnabled =>
      val engine = if (cometEnabled) "Comet" else "Spark"
      withSQLConf(
        CometConf.COMET_ENABLED.key -> cometEnabled.toString,
        CometConf.COMET_EXEC_ENABLED.key -> cometEnabled.toString) {
        val actual = spark.sql(query).collect().head.toSeq.map {
          case null => null
          case value: Number => value.longValue()
          case other => other
        }
        if (actual != expected) {
          throw new AssertionError(
            s"$name: $engine produced $actual, expected $expected. The case is not aggregating " +
              s"what it names.\n$query")
        }
      }
    }

  /** Verifies a case's aggregate under both engines, then times it. */
  private def runCase(name: String, benchmarkCase: Case, rows: Int = numRows): Unit = {
    verifySink(name, benchmarkCase.query, benchmarkCase.expected)
    runExpressionBenchmark(name, rows, benchmarkCase.query)
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    withTempPath { dir =>
      withTempTable(views.map(_.name): _*) {
        views.foreach(view => createView(dir, view))

        // `runExpressionBenchmark` appends ConstantFolding to whatever the caller has already
        // excluded, and applies the result to both arms, so setting this here excludes both.
        withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromGenerate.ruleName) {

          // Cardinality is input rows for every case, so the numbers are per scanned row rather
          // than per generated row. Fan-out is named in the case title: the 100-element case
          // emits roughly 50 times as many rows as the 2-element case from the same 256K inputs.
          runBenchmark("Explode - fan-out") {
            Seq(2, 10, 100).foreach { len =>
              runCase(
                s"explode array<bigint>[$len]",
                generatorCase("explode", s"arr_len$len", len))
            }
          }

          runBenchmark("Explode - generator variants") {
            Seq("explode", "posexplode", "explode_outer", "posexplode_outer").foreach {
              generator =>
                runCase(
                  s"$generator array<bigint>[10]",
                  generatorCase(generator, "arr_len10", 10))
            }
          }

          runBenchmark("Explode - element type") {
            Seq("bigint" -> "arr_len10", "string" -> "arr_str10", "struct" -> "arr_struct10")
              .foreach { case (elementType, view) =>
                runCase(s"explode array<$elementType>[10]", generatorCase("explode", view, 10))
              }
          }

          runBenchmark("Explode - nested input") {
            nestedInputCases.foreach { case (name, benchmarkCase) =>
              runCase(name, benchmarkCase, nestedRows)
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
            runCase(
              "explode alone",
              generatorCase("explode", "arr_carry", 10, where = readAllColumns))
            runCase(
              "explode plus 3 carried columns",
              generatorCase(
                "explode",
                "arr_carry",
                10,
                carried = Seq("k", "s", "v"),
                where = readAllColumns))
          }
        }
      }
    }
  }

  /**
   * The nesting group, over 100K rows rather than 256K because each row carries 20 events.
   *
   * The shape is the one asked for in review: a customer profile whose event list sits eight
   * struct accessors down, holding a second array of four-field structs inside each element. The
   * requested outer container was a map keyed by platform, which is where a real schema would put
   * it; that is an `array<struct<platform, entries>>` here because Comet has no native generator
   * over maps yet (#2837) and the Comet arm would silently be Spark.
   *
   * The whole event struct is counted rather than one of its fields, so nested schema pruning
   * cannot narrow the exploded element and leave the case measuring a two-column gather. It does
   * prune the siblings no case reads: on the executed plan the `depth 8` scan's `profile` column
   * is `struct<account:struct<...sessions:struct<events:array<...>>>>` with `billing`,
   * `addresses` and `device` gone, and the `depth 1` scan does not project `profile` at all.
   *
   * `depth 1` and `depth 8` explode the same array, written twice into the same file, so the pair
   * measures what the struct chain costs: extra definition levels in the Parquet column and a
   * chain of `GetStructField` above the scan. `then its inner array` chains a second generator
   * onto the first, which is the shape the review asked about; its fan-out is 20 against the
   * other two's 4, so read it on its own rather than against them.
   */
  private def nestedInputCases: Seq[(String, Case)] = {
    val carried = Seq("k", "region")
    val single = Seq("depth 1" -> "events", "depth 8" -> deepEvents).map { case (label, path) =>
      s"explode array<struct>[$eventsLen] at $label" ->
        generatorCase(
          "explode",
          "nested",
          eventsLen,
          rows = nestedRows,
          arrayExpr = path,
          carried = carried)
    }

    // The inner arrays are always full, so the second generator drops nothing and the case's
    // fan-out is the product of the two lengths.
    val events = nonEmptyRows(nestedRows) * eventsLen
    val entries = events * entriesLen
    val outer =
      s"SELECT k, region, ev.platform AS platform, ev.entries AS entries " +
        s"FROM (SELECT k, region, explode($deepEvents) AS ev FROM nested)"
    val chained =
      s"SELECT ${Seq("k", "region", "platform", "entry").map(aggregate).mkString(", ")} " +
        s"FROM (SELECT k, region, platform, explode(entries) AS entry FROM ($outer))"

    single :+ (s"explode array<struct>[$eventsLen] at depth 8, then its inner array" ->
      Case(chained, Seq.fill(4)(entries)))
  }
}
