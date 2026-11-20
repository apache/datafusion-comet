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
import org.apache.spark.sql.Row
import org.apache.spark.sql.comet.CometWindowExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Compare DataFusion's legacy SUM, checked native SUM/TRY_SUM, and Spark windows. Arguments: row
 * count (default 65536), frames (all, bounded, or suffix). For example, `4000000 suffix` measures
 * a whole-partition initial frame.
 */
object CometSlidingSumBenchmark extends CometBenchmarkBase {
  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = mainArgs.headOption.map(_.toLong).getOrElse(65536L)
    val frames = mainArgs.lift(1).getOrElse("all")
    require(rows > 0 && Set("all", "bounded", "suffix").contains(frames))
    val boundedFrames =
      Seq(16, 1024).map(width => (s"rows=$width", s"${width - 1} PRECEDING AND CURRENT ROW"))
    val suffixFrame = Seq(("suffix", "CURRENT ROW AND UNBOUNDED FOLLOWING"))
    val selectedFrames = frames match {
      case "bounded" => boundedFrames
      case "suffix" => suffixFrame
      case _ => boundedFrames ++ suffixFrame
    }
    val nativeCases = Seq(
      ("DataFusion SUM (legacy)", "sum", false, true),
      ("Comet SUM (ANSI)", "sum", true, true),
      ("Comet TRY_SUM", "try_sum", true, true))
    val sparkCases =
      Seq(("Spark SUM (ANSI)", "sum", true, false), ("Spark TRY_SUM", "try_sum", true, false))
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      "spark.comet.operator.WindowExec.allowIncompatible" -> "true") {
      for ((shape, value) <- Seq(
          "cancelling" -> "id % 17 - 8",
          "positive" -> "id % 17",
          // Partial sums fit in i64, but both sign totals cross the bounds.
          "near-limit" ->
            "CASE WHEN id % 2 = 0 THEN 4611686018427387904L ELSE -4611686018427387903L END");
        nullPercent <- (if (shape == "cancelling") Seq(0, 50, 100) else Seq(0, 50))) {
        withTempPath { dir =>
          spark
            .range(rows)
            .selectExpr(
              "id",
              s"CASE WHEN pmod(id, 100) < $nullPercent THEN NULL ELSE $value END AS v")
            .write
            .parquet(dir.getCanonicalPath)
          withTempTable("sliding_sum_benchmark") {
            spark.read
              .parquet(dir.getCanonicalPath)
              .createOrReplaceTempView("sliding_sum_benchmark")
            for ((frameName, bounds) <- selectedFrames) {
              def run(
                  function: String,
                  ansi: Boolean,
                  native: Boolean,
                  inputRows: Long,
                  verify: Boolean): Seq[Row] = {
                // Spark 3.5's withSQLConf returns Unit.
                var result = Seq.empty[Row]
                withSQLConf(
                  SQLConf.ANSI_ENABLED.key -> ansi.toString,
                  CometConf.COMET_EXEC_WINDOW_ENABLED.key -> native.toString) {
                  // Consume the window values with a sink unaffected by ANSI mode.
                  val filter = if (inputRows < rows) s" WHERE id < $inputRows" else ""
                  val query = s"SELECT bit_xor(s), count(s) FROM (SELECT $function(v) OVER " +
                    s"(ORDER BY id ROWS BETWEEN $bounds) AS s " +
                    s"FROM sliding_sum_benchmark$filter)"
                  val df = spark.sql(query)
                  if (verify) {
                    assert(
                      df.queryExecution.executedPlan.exists(
                        _.isInstanceOf[CometWindowExec]) == native,
                      df.queryExecution.executedPlan.toString)
                  }
                  result = df.collect().toSeq
                }
                result
              }
              // Spark recomputes suffix frames in O(rows^2). Check a small partition
              // against Spark, then compare all native cases on the full partition.
              val verificationRows = if (frameName == "suffix") math.min(rows, 1024L) else rows
              val expected = run(
                "sum",
                ansi = true,
                native = false,
                inputRows = verificationRows,
                verify = true)
              for ((_, function, ansi, native) <- nativeCases ++ sparkCases.tail) {
                assert(run(function, ansi, native, verificationRows, verify = true) == expected)
              }
              if (verificationRows != rows) {
                val legacy =
                  run("sum", ansi = false, native = true, inputRows = rows, verify = true)
                for ((_, function, ansi, native) <- nativeCases.tail) {
                  assert(run(function, ansi, native, rows, verify = true) == legacy)
                }
              }
              val cases = if (frameName == "suffix") nativeCases else nativeCases ++ sparkCases
              val benchmark = new Benchmark(
                s"$shape: frame=$frameName, NULL=$nullPercent%",
                rows,
                output = output)
              for ((name, function, ansi, native) <- cases) {
                benchmark.addCase(name) { _ =>
                  val _ = run(function, ansi, native, rows, verify = false)
                }
              }
              benchmark.run()
            }
          }
        }
      }
    }
  }
}
