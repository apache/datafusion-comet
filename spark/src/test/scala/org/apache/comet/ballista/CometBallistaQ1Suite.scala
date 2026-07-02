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

package org.apache.comet.ballista

import java.math.{BigDecimal => JBigDecimal}
import java.sql.Date
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.CometListenerBusUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.CometNativeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf

/**
 * The milestone demonstration for the R1 driver-side Ballista offload, using TPC-H Q1's
 * `lineitem` data and per-row semantics: run the query with
 * `spark.comet.exec.ballista.enabled=true` and prove the collected rows are identical to the
 * flag-off (Spark/Comet-on-executors) baseline, while launching ZERO Spark executor tasks.
 *
 * Scope note — why this offloads the pre-aggregation slice of Q1, not the full aggregate:
 *
 * The R1 offload path only accepts a plan with exactly ONE serialized `CometNativeExec` block (a
 * single native leaf reading Parquet directly). Full Q1's `GROUP BY` cannot be squeezed into one
 * such block under this machinery:
 *   - Plain read: Spark plans partial-agg -> `CometExchange` -> final-agg, i.e. a shuffle
 *     boundary \=> two serialized blocks (the guard rejects it as multi-stage). The Parquet scan
 *     reports `UnknownPartitioning`, which never satisfies the aggregate's
 *     `ClusteredDistribution`, so the exchange is unavoidable regardless of how many
 *     files/partitions the input has.
 *   - `.coalesce(1)`: this removes the exchange (the single-partition child satisfies the
 *     distribution) but inserts a `CometCoalesce` *sink*, which is itself a native-block boundary
 *     \=> still two serialized blocks, still rejected. So no arrangement of full Q1 is a single
 *     exchange-free `CometNativeExec` with a Parquet leaf in R1; the multi-block/aggregate case
 *     is R2 and explicitly out of scope here (see task brief).
 *
 * We therefore offload the largest single-block subset of Q1: its scan + `WHERE` date filter +
 * the exact decimal arithmetic projections that feed the aggregate (`disc_price`, `charge`). This
 * exercises the parts that matter for offload correctness — Parquet native scan, date filtering
 * against the Q1 cutoff, and Q1's decimal multiplications — as ONE exchange-free native block.
 * The test asserts the plan really is single-block before offloading, and compares full result
 * rows flag-on vs flag-off using the exact decimal types Spark produces.
 */
class CometBallistaQ1Suite extends CometTestBase with AdaptiveSparkPlanHelper {

  /**
   * The single-block, pre-aggregation slice of TPC-H Q1: the scan, the Q1 `WHERE` date filter,
   * and Q1's per-row decimal projections (`l_extendedprice * (1 - l_discount)` and `... * (1 +
   * l_tax)`) — everything up to, but not including, the `GROUP BY` (which would force a shuffle
   * boundary, see the class doc). No `ORDER BY` (a global sort would also need a range-partition
   * exchange); the test sorts the collected rows itself.
   */
  private val q1 =
    """
      |SELECT l_returnflag, l_linestatus,
      |  l_quantity,
      |  l_extendedprice,
      |  l_extendedprice * (1 - l_discount) AS disc_price,
      |  l_extendedprice * (1 - l_discount) * (1 + l_tax) AS charge
      |FROM lineitem
      |WHERE l_shipdate <= date '1998-12-01' - interval '90' day
      |""".stripMargin

  /**
   * TPC-H `lineitem`, restricted to the columns Q1 touches, with the correct Spark types.
   * Decimals use the classic TPC-H `decimal(12,2)`; `l_shipdate` is a real `date`.
   */
  private val lineitemSchema: StructType = StructType(
    Seq(
      StructField("l_quantity", DecimalType(12, 2), nullable = false),
      StructField("l_extendedprice", DecimalType(12, 2), nullable = false),
      StructField("l_discount", DecimalType(12, 2), nullable = false),
      StructField("l_tax", DecimalType(12, 2), nullable = false),
      StructField("l_returnflag", StringType, nullable = false),
      StructField("l_linestatus", StringType, nullable = false),
      StructField("l_shipdate", DateType, nullable = false)))

  private def dec(v: String): JBigDecimal = new JBigDecimal(v).setScale(2)

  /**
   * A small synthetic `lineitem`: a handful of rows spanning three `(returnflag, linestatus)`
   * groups, with shipdates straddling the Q1 cutoff (`1998-12-01 - 90 days = 1998-09-02`) so the
   * `WHERE` filter actually removes rows (the two past-cutoff rows). A range of discount/tax
   * values gives the decimal projections non-trivial products.
   */
  private def lineitemRows: Seq[Row] = Seq(
    // group (A, F) -- all kept
    Row(
      dec("17.00"),
      dec("21168.23"),
      dec("0.04"),
      dec("0.02"),
      "A",
      "F",
      Date.valueOf("1998-08-01")),
    Row(
      dec("36.00"),
      dec("45983.16"),
      dec("0.09"),
      dec("0.06"),
      "A",
      "F",
      Date.valueOf("1998-07-15")),
    Row(
      dec("8.00"),
      dec("13309.60"),
      dec("0.10"),
      dec("0.02"),
      "A",
      "F",
      Date.valueOf("1998-09-01")),
    // group (N, O) -- all kept
    Row(
      dec("28.00"),
      dec("28955.64"),
      dec("0.05"),
      dec("0.08"),
      "N",
      "O",
      Date.valueOf("1998-06-10")),
    Row(
      dec("24.00"),
      dec("32000.00"),
      dec("0.00"),
      dec("0.00"),
      "N",
      "O",
      Date.valueOf("1998-08-20")),
    Row(
      dec("2.00"),
      dec("2600.00"),
      dec("0.06"),
      dec("0.03"),
      "N",
      "O",
      Date.valueOf("1998-09-02")),
    // group (R, F) -- all kept
    Row(
      dec("32.00"),
      dec("41000.50"),
      dec("0.07"),
      dec("0.05"),
      "R",
      "F",
      Date.valueOf("1998-05-05")),
    Row(
      dec("45.00"),
      dec("60000.00"),
      dec("0.02"),
      dec("0.01"),
      "R",
      "F",
      Date.valueOf("1998-08-31")),
    // rows PAST the cutoff -- must be filtered out (would form a (N, F) group if kept)
    Row(
      dec("50.00"),
      dec("70000.00"),
      dec("0.03"),
      dec("0.04"),
      "N",
      "F",
      Date.valueOf("1998-09-03")),
    Row(
      dec("99.00"),
      dec("99999.99"),
      dec("0.05"),
      dec("0.05"),
      "N",
      "F",
      Date.valueOf("1998-12-01")))

  /**
   * Runs `f`, counting Spark executor task starts during it. Drains the listener bus before
   * attaching and after running so asynchronous task-start events are flushed. (Same apparatus as
   * `CometBallistaOffloadSuite`.)
   */
  private def countTaskStarts(f: => Unit): Int = {
    val taskStarts = new AtomicInteger(0)
    val listener = new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        taskStarts.incrementAndGet()
      }
    }
    CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    spark.sparkContext.addSparkListener(listener)
    try {
      f
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    taskStarts.get()
  }

  test(
    "TPC-H Q1 (pre-aggregation slice) offloads to Ballista single-block with identical results " +
      "and no executor tasks") {
    assume(
      NativeBallista.isAvailable,
      s"native ballista library not available: ${NativeBallista.loadFailure.map(_.getMessage)}")

    withTempPath { dir =>
      // Single Parquet file (coalesce(1)) so the offloaded plan reads one native scan leaf.
      spark
        .createDataFrame(spark.sparkContext.parallelize(lineitemRows), lineitemSchema)
        .coalesce(1)
        .write
        .parquet(dir.getCanonicalPath)

      // AQE off so the physical plan is stable (no AdaptiveSparkPlanExec collect root wrapping the
      // Comet columnar-to-row node that carries our executeCollect offload hook).
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("lineitem")

        // Confirm the plan is offloadable BEFORE running it: no exchange, and exactly one
        // CometNativeExec block carrying a serialized plan (the R1 single-block requirement).
        val executed = withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
          spark.sql(q1).queryExecution.executedPlan
        }
        val exchanges = executed.collect { case e: Exchange => e }
        assert(
          exchanges.isEmpty,
          s"expected no exchange (single-stage) in the plan, found ${exchanges.size}:\n$executed")
        val nativeBlocks = executed.collect {
          case n: CometNativeExec if n.serializedPlanOpt.isDefined => n
        }
        assert(
          nativeBlocks.size == 1,
          s"expected exactly one serialized CometNativeExec block, found ${nativeBlocks.size}:\n" +
            s"$executed")

        // Baseline: normal Comet execution (offload off), run through the same listener apparatus.
        // This is a positive control proving the listener actually observes executor task starts,
        // so the `== 0` assertion for the offloaded run is meaningful.
        var baseline: Seq[Seq[Any]] = null
        val baselineTaskStarts = countTaskStarts {
          baseline = withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
            spark.sql(q1).collect().map(_.toSeq.toIndexedSeq).toIndexedSeq
          }
        }
        assert(
          baselineTaskStarts > 0,
          "expected the flag-off baseline collect to launch at least one Spark executor task " +
            s"(sanity check for the listener apparatus); got $baselineTaskStarts")

        // Ballista offload: run the same query with the flag on, counting executor task starts.
        var offloaded: Seq[Seq[Any]] = null
        val offloadedTaskStarts = countTaskStarts {
          offloaded = withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "true") {
            spark.sql(q1).collect().map(_.toSeq.toIndexedSeq).toIndexedSeq
          }
        }

        // Compare full rows (each sorted into a stable total order by its string form) using the
        // exact values/types Spark produced -- decimals stay decimals with their computed scale.
        def sortKey(r: Seq[Any]): String = r.map(v => s"$v").mkString("")
        val baselineSorted = baseline.sortBy(sortKey)
        val offloadedSorted = offloaded.sortBy(sortKey)
        assert(
          offloadedSorted == baselineSorted,
          "offloaded rows do not match baseline\n" +
            s"  baseline:  $baselineSorted\n  offloaded: $offloadedSorted")

        // The 8 rows on/before the Q1 cutoff are kept; the two past-cutoff rows are filtered out.
        assert(
          baselineSorted.size == 8,
          s"expected 8 rows after the Q1 date filter, got ${baselineSorted.size}: $baselineSorted")

        // Crucially, NO Spark executor tasks ran for the offloaded collect.
        assert(
          offloadedTaskStarts == 0,
          s"expected 0 Spark executor tasks for the Ballista-offloaded collect, " +
            s"but $offloadedTaskStarts started")
      }
    }
  }
}
