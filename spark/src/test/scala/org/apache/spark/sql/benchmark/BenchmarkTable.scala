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

/**
 * One measured arm of a benchmark, with the scan-plus-row-conversion floor it should be read
 * against.
 *
 * @param name
 *   the arm, e.g. "Spark" or "Comet"
 * @param total
 *   the whole query, including scan and result transfer
 * @param baseline
 *   the same scan projecting its raw columns, or None when no baseline could be derived
 */
case class BenchmarkRow(name: String, total: Benchmark.Result, baseline: Option[Benchmark.Result])

/**
 * Renders benchmark results as a table that separates expression cost from the scan and result
 * transfer surrounding it.
 *
 * Spark's `Benchmark.run` cannot express this: it computes results from its own case list,
 * accepts no precomputed `Result`, and has nowhere to put a derived column. This renderer
 * consumes the `Result` values that `Benchmark.measure` returns, so measurement still uses
 * Spark's warmup, iteration and stdev logic.
 *
 * `render` is pure, which is what makes the column rules testable without a `SparkSession`.
 */
object BenchmarkTable {

  /**
   * The cost attributable to the expression, or None when it cannot be told apart from
   * measurement noise.
   *
   * A difference smaller than the combined standard deviations is not evidence of anything, and a
   * negative difference means the baseline happened to run slower than the query it is a floor
   * for. Both are reported as "below the measurement floor" rather than as a number, because a
   * number invites a conclusion the data does not support.
   */
  def exprMs(row: BenchmarkRow): Option[Double] = row.baseline.flatMap { baseline =>
    val diff = row.total.bestMs - baseline.bestMs
    if (diff > row.total.stdevMs + baseline.stdevMs) Some(diff) else None
  }

  def render(
      title: String,
      cardinality: Long,
      rows: Seq[BenchmarkRow],
      baselineQuery: Option[String] = None): String = {
    require(rows.nonEmpty, "a benchmark table needs at least one row")

    val exprs = rows.map(exprMs)
    // Ratios are only comparable when every arm has an expression cost. Otherwise fall back to the
    // contaminated total, and say so in the header so nobody reads it as an expression ratio.
    val onExpr = exprs.forall(_.isDefined)
    val reference = if (onExpr) exprs.head.get else rows.head.total.bestMs
    val relativeHeader = if (onExpr) "Relative(expr)" else "Relative(total)"

    val nameWidth = math.max(40, rows.map(_.name.length).max)
    val format = s"%-${nameWidth}s %12s %11s %14s %11s %16s"

    val out = new StringBuilder
    out.append(Benchmark.getJVMOSInfo()).append('\n')
    out.append(Benchmark.getProcessorName()).append('\n')
    baselineQuery.foreach(query => out.append(s"baseline: $query\n"))
    out.append(
      format.format(
        s"$title ($cardinality rows):",
        "Best(ms)",
        "Stdev(ms)",
        "Baseline(ms)",
        "Expr(ms)",
        relativeHeader))
    out.append('\n')
    out.append("-" * (nameWidth + 70)).append('\n')

    rows.zip(exprs).foreach { case (row, expr) =>
      // Every row's ratio must be over the same quantity as `reference`. Using a row's expression
      // cost while the reference is a total would divide two different things.
      val relative = if (onExpr) expr.get else row.total.bestMs
      out.append(
        format.format(
          row.name,
          "%.1f".format(row.total.bestMs),
          "%.1f".format(row.total.stdevMs),
          row.baseline.map(b => "%.1f".format(b.bestMs)).getOrElse("-"),
          expr.map("%.1f".format(_)).getOrElse("-"),
          "%.1fX".format(reference / relative)))
      out.append('\n')
    }

    if (exprs.exists(_.isEmpty)) {
      out.append(footnote(rows, exprs)).append('\n')
    }
    out.toString
  }

  private def footnote(rows: Seq[BenchmarkRow], exprs: Seq[Option[Double]]): String = {
    val noBaseline = rows.zip(exprs).exists { case (row, _) => row.baseline.isEmpty }
    val belowFloor = rows.zip(exprs).exists { case (row, expr) =>
      row.baseline.isDefined && expr.isEmpty
    }
    val reasons = Seq(
      if (noBaseline) Some("no single-table baseline could be derived for this query") else None,
      if (belowFloor) Some("the expression cost is below the measurement floor")
      else None).flatten
    s"[-] ${reasons.mkString("; ")}"
  }
}
