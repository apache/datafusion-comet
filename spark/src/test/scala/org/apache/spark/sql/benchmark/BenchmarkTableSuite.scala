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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.benchmark.Benchmark

class BenchmarkTableSuite extends AnyFunSuite {

  /** `Result(avgMs, bestRate, bestMs, stdevMs)`. Only bestMs and stdevMs are rendered. */
  private def result(bestMs: Double, stdevMs: Double = 0.1): Benchmark.Result =
    Benchmark.Result(bestMs, 0.0, bestMs, stdevMs)

  private def row(name: String, bestMs: Double, baselineMs: Double, stdevMs: Double = 0.1) =
    BenchmarkRow(name, result(bestMs, stdevMs), Some(result(baselineMs, stdevMs)))

  /** The measured rows: everything after the rule, excluding the trailing footnote. */
  private def dataLines(table: String): Seq[String] =
    table.linesIterator.toSeq
      .dropWhile(!_.startsWith("-"))
      .drop(1)
      .filter(line => line.trim.nonEmpty && !line.startsWith("[-]"))

  private def assertClose(actual: Option[Double], expected: Double): Unit =
    assert(
      actual.exists(v => math.abs(v - expected) < 1e-9),
      s"$actual was not close to $expected")

  test("expression cost is the total minus the baseline") {
    assertClose(BenchmarkTable.exprMs(row("Spark", bestMs = 9.0, baselineMs = 8.1)), 0.9)
  }

  test("relative is computed on expression cost when every arm has one") {
    // Spark spends 4.0ms on the expression, Comet 2.0ms, so Comet is 2x on the expression even
    // though the totals are only 1.2x apart.
    val table = BenchmarkTable.render(
      "space",
      1000,
      Seq(row("Spark", bestMs = 24.0, baselineMs = 20.0), row("Comet", bestMs = 20.0, 18.0)))

    assert(table.contains("Relative(expr)"))
    val Seq(spark, comet) = dataLines(table)
    assert(spark.contains("4.0") && spark.endsWith("1.0X"))
    assert(comet.contains("2.0") && comet.endsWith("2.0X"))
  }

  test("a missing baseline blanks the derived columns and falls back to relative on total") {
    val table = BenchmarkTable.render(
      "inner join",
      1000,
      Seq(BenchmarkRow("Spark", result(20.0), None), BenchmarkRow("Comet", result(10.0), None)))

    assert(table.contains("Relative(total)"))
    val Seq(spark, comet) = dataLines(table)
    // Baseline and Expr blank; relative still reports the 2x on the total.
    assert(spark.contains("-") && spark.endsWith("1.0X"))
    assert(comet.endsWith("2.0X"))
    assert(table.contains("no single-table baseline could be derived"))
  }

  test("when only one arm has an expression cost, every ratio is over the total") {
    // Regression: an earlier version used the row's own expression cost when it had one, so a
    // table headed Relative(total) divided one arm's total by the other arm's expression cost.
    val table = BenchmarkTable.render(
      "levenshtein",
      1024,
      Seq(
        // Spark's 2.5ms difference is lost in a combined stdev of 2.6ms.
        row("Spark", bestMs = 15.1, baselineMs = 12.6, stdevMs = 1.3),
        row("Comet", bestMs = 35.4, baselineMs = 8.6, stdevMs = 0.9)))

    assert(table.contains("Relative(total)"))
    val Seq(spark, comet) = dataLines(table)
    assert(spark.endsWith("1.0X"))
    // 15.1 / 35.4 = 0.43, not 15.1 / 26.8 = 0.56.
    assert(comet.endsWith("0.4X"), comet)
  }

  test("a negative expression cost is blanked rather than reported") {
    // The baseline ran slower than the query it is a floor for, which is noise, not a negative cost.
    assert(BenchmarkTable.exprMs(row("Spark", bestMs = 8.0, baselineMs = 9.0)).isEmpty)

    val table = BenchmarkTable.render(
      "length",
      1000,
      Seq(row("Spark", bestMs = 8.0, baselineMs = 9.0), row("Comet", bestMs = 7.0, 6.0)))
    assert(table.contains("the expression cost is below the measurement floor"))
    assert(table.contains("Relative(total)"))
  }

  test("an expression cost within combined stdev of zero is blanked") {
    // diff of 0.5ms against stdevs summing to 0.6ms is not evidence of anything.
    assert(
      BenchmarkTable.exprMs(row("Spark", bestMs = 8.5, baselineMs = 8.0, stdevMs = 0.3)).isEmpty)
    // The same 0.5ms diff is meaningful once the measurement is tighter.
    assertClose(
      BenchmarkTable.exprMs(row("Spark", bestMs = 8.5, baselineMs = 8.0, stdevMs = 0.1)),
      0.5)
  }

  test("the baseline query is shown so a reader can audit what the floor measured") {
    val table = BenchmarkTable.render(
      "abs",
      1000,
      Seq(row("Spark", bestMs = 9.0, baselineMs = 8.0)),
      baselineQuery = Some("SELECT c1 FROM parquetV1Table"))
    assert(table.contains("baseline: SELECT c1 FROM parquetV1Table"))
  }

  test("the row count is carried in the title") {
    val table =
      BenchmarkTable.render("abs", 1024 * 1024, Seq(row("Spark", bestMs = 9.0, baselineMs = 8.0)))
    assert(table.contains("abs (1048576 rows):"))
  }
}
