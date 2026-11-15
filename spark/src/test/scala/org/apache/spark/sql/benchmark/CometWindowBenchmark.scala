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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Row
import org.apache.spark.sql.comet.CometWindowExec

import org.apache.comet.CometConf

/**
 * Benchmark to measure Comet's window operator (`CometWindowExec`) against Spark's `WindowExec`
 * on the dimensions that drive its cost: how many rows each PARTITION BY group holds, which
 * window function runs, the type of its argument, and how many columns ride through the window
 * next to the keys. To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometWindowBenchmark
 * }}}
 *
 * Every case runs three arms: Spark, Comet, and Comet with `spark.comet.exec.window.enabled` off,
 * which keeps the scan, shuffle and sort in Comet but runs the window in Spark, the way a user
 * would work around a slow native window.
 *
 * Every case aggregates the window's output rather than writing it, so the only row boundary is
 * one row per partition. The aggregate reads the window column, and reads the carried columns in
 * the cases that carry them, since Spark would otherwise prune them below the window. Before a
 * case is timed, every arm must return the same row.
 *
 * Times are whole-query totals: they include the Parquet scan, the shuffle on the partition keys,
 * the sort the window needs, and the aggregate. `row_number` in the function group is the nearest
 * thing to a floor, since it is the cheapest window function each engine has. The native operator
 * alone is measured by `native/operators/benches/window.rs`.
 */
object CometWindowBenchmark extends CometBenchmarkBase {

  private val numRows = 512 * 1024

  /** Columns that ride through the window unread by it, as in plans that window wide rows. */
  private val carriedColumns = 18

  /** The PARTITION BY group sizes the datasets are built for. */
  private val groupSizes = Seq(1, 4, 64, 4096, 16384)

  private def table(rowsPerPartition: Int): String = s"wnd_rpp$rowsPerPartition"

  /**
   * Writes a dataset whose rows fall in PARTITION BY (visit, tab, app) groups of
   * `rowsPerPartition` rows. `visit` changes every ten groups, like a session holding several.
   * `v` and the struct `s` are NULL on every other row, so IGNORE NULLS has values to skip.
   */
  private def createTable(dir: File, rowsPerPartition: Int): Unit = {
    val g = s"(id div $rowsPerPartition)"
    val carried = (0 until carriedColumns).map { c =>
      s"concat('carried-$c-', CAST(id % 1000000 AS STRING)) AS c$c"
    }
    val columns = Seq(
      s"concat(lpad(hex($g div 10), 16, '0'), '-', lpad(CAST($g div 10 AS STRING), 19, '0'))" +
        " AS visit",
      s"element_at(array('Today', 'Games', 'Apps'), CAST(($g div 10) % 3 AS INT) + 1) AS tab",
      s"CAST(1000000000 + $g AS STRING) AS app",
      "id AS ts",
      "IF(id % 2 = 1, NULL, id) AS v",
      """IF(id % 2 = 1, NULL, named_struct(
        |  's', concat('algo-', CAST(id % 100000 AS STRING)),
        |  'b', id % 7 = 0,
        |  'i', CAST(id % 40 AS INT),
        |  't', concat('sub-', CAST(id % 100000 AS STRING)),
        |  'u', concat('container-', CAST(id % 100000 AS STRING)))) AS s""".stripMargin) ++
      carried
    val path = s"${dir.getAbsolutePath}/${table(rowsPerPartition)}"
    spark.range(numRows.toLong).selectExpr(columns: _*).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(table(rowsPerPartition))
  }

  /**
   * A query that computes `window` over the dataset with `rowsPerPartition`-row groups and
   * aggregates its output. `valueSink` reads the window column's value, and the carried columns
   * are counted when `carry` is set.
   */
  private def windowQuery(
      window: String,
      rowsPerPartition: Int,
      valueSink: String = "sum(w)",
      partitionBy: String = "visit, tab, app",
      carry: Boolean = false): String = {
    val carried = if (carry) (0 until carriedColumns).map(c => s"c$c") else Nil
    val sinks = Seq("count(w)", valueSink) ++ carried.map(c => s"count($c)")
    val inner = (carried :+ s"$window OVER (PARTITION BY $partitionBy ORDER BY ts) AS w")
      .mkString(", ")
    s"SELECT ${sinks.mkString(", ")} FROM (SELECT $inner FROM ${table(rowsPerPartition)})"
  }

  private val sparkArm = Seq(CometConf.COMET_ENABLED.key -> "false")

  private val cometArm =
    Seq(CometConf.COMET_ENABLED.key -> "true", CometConf.COMET_EXEC_ENABLED.key -> "true")

  private val sparkWindowArm = cometArm :+ (CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "false")

  private val arms =
    Seq("Spark" -> sparkArm, "Comet" -> cometArm, "Comet, Spark window" -> sparkWindowArm)

  /**
   * Checks that every arm returns the same row and that the Comet arm runs the window natively,
   * then times the arms.
   */
  private def runWindowCase(name: String, query: String): Unit = {
    val results = arms.map { case (arm, confs) =>
      // `withSQLConf` returns `Unit` before Spark 4.0, so the rows leave through a variable.
      var rows = Seq.empty[Row]
      withSQLConf(confs: _*) {
        rows = spark.sql(query).collect().toSeq
      }
      arm -> rows
    }
    results.foreach { case (arm, rows) =>
      if (rows != results.head._2) {
        throw new AssertionError(
          s"$name: $arm returned $rows, Spark returned ${results.head._2}.\n$query")
      }
    }
    withSQLConf(cometArm: _*) {
      val df = spark.sql(query)
      df.noop()
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      if (plan.collectFirst { case w: CometWindowExec => w }.isEmpty) {
        throw new AssertionError(
          s"$name: the Comet arm does not run CometWindowExec.\n${plan.treeString}")
      }
    }

    val benchmark = new Benchmark(name, numRows.toLong, output = output)
    arms.foreach { case (arm, confs) =>
      benchmark.addCase(arm) { _ =>
        withSQLConf(confs: _*)(spark.sql(query).noop())
      }
    }
    benchmark.run()
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    withTempPath { dir =>
      withTempTable(groupSizes.map(table): _*) {
        groupSizes.foreach(createTable(dir, _))

        // The two windows of a plan that takes `last(struct(...), true)` and then `max(int)`
        // over clickstream rows sorted by session, with the other columns that plan carries.
        // The group size separates the fixed cost per PARTITION BY group from the cost that
        // grows with the frame.
        runBenchmark("Window - rows per partition, wide rows") {
          groupSizes.foreach { rowsPerPartition =>
            runWindowCase(
              s"last(struct, true), $rowsPerPartition rows per partition, " +
                s"$carriedColumns carried columns",
              windowQuery("last(s, true)", rowsPerPartition, "sum(w.i)", carry = true))
          }
          groupSizes.foreach { rowsPerPartition =>
            runWindowCase(
              s"max(bigint), $rowsPerPartition rows per partition, " +
                s"$carriedColumns carried columns",
              windowQuery("max(v)", rowsPerPartition, partitionBy = "visit, app", carry = true))
          }
        }

        // Every function over the same bigint argument, at a small and a large group size.
        runBenchmark("Window - function") {
          Seq(4, 16384).foreach { rowsPerPartition =>
            Seq("row_number()", "max(v)", "last(v)", "first(v, true)", "last(v, true)").foreach {
              function =>
                runWindowCase(
                  s"$function, $rowsPerPartition rows per partition",
                  windowQuery(function, rowsPerPartition))
            }
          }
        }

        // The argument type alone, at a group size where per-row work dominates.
        runBenchmark("Window - argument type") {
          runWindowCase(
            "last(bigint, true), 4 rows per partition",
            windowQuery("last(v, true)", 4))
          runWindowCase(
            "last(struct, true), 4 rows per partition",
            windowQuery("last(s, true)", 4, "sum(w.i)"))
        }
      }
    }
  }
}
