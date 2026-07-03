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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.CometListenerBusUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometNativeExec
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * The R2 milestone: a Spark `GROUP BY` runs DISTRIBUTED on an in-process Apache DataFusion
 * Ballista engine on the Spark driver, across a hash shuffle, with Comet native fragments on both
 * sides (partial aggregate -> Ballista hash shuffle -> final aggregate). The collected rows must
 * be identical to the flag-off (Spark/Comet-on-executors) baseline, launching ZERO Spark executor
 * tasks.
 *
 * Plan shape offloaded (with `spark.comet.exec.shuffle.directRead.enabled=false` so the final
 * aggregate's input leaf serializes as a plain `Scan` fed by the Ballista shuffle):
 * {{{
 *   CometNativeExec[block2]  (final HashAggregate over a Scan leaf)
 *     CometShuffleExchangeExec (HashPartitioning(k), N)
 *       CometNativeExec[block1]  (partial HashAggregate over a NativeScan)
 *         CometNativeScanExec
 * }}}
 *
 * No `ORDER BY` in the offloaded query: a global sort would add a range-partition exchange (a
 * third stage), which is out of the 2-block scope. The test sorts the collected rows itself.
 */
class CometBallistaDistributedSuite extends CometTestBase with AdaptiveSparkPlanHelper {

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

  test("two-stage GROUP BY is offloaded to distributed Ballista with no Spark executor tasks") {
    assume(
      NativeBallista.isAvailable,
      s"native ballista library not available: ${NativeBallista.loadFailure.map(_.getMessage)}")

    withTempPath { dir =>
      import testImplicits._

      // A few `k` values with distinct row counts (k=1 x3, k=2 x2, k=3 x4), spread across several
      // input files so a GROUP BY needs a shuffle to aggregate across partitions.
      Seq((1, 10), (1, 11), (1, 12), (2, 20), (2, 21), (3, 30), (3, 31), (3, 32), (3, 33))
        .toDF("k", "v")
        .repartition(4)
        .write
        .parquet(dir.getCanonicalPath)

      // AQE off so the collect root is the Comet columnar-to-row node carrying our executeCollect
      // override (not an AdaptiveSparkPlanExec wrapper), and the shuffle boundary is deterministic.
      // Small shuffle-partition count keeps the in-process distributed run fast.
      // directRead off so block2's input leaf serializes as a plain Scan (#100) fed by the Ballista
      // shuffle, not a native ShuffleScan (#116).
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "false") {
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("t")
        val query = "SELECT k, count(*) AS c FROM t GROUP BY k"

        // Confirm the plan is the offloadable R2 shape BEFORE running it: exactly one Comet hash
        // exchange and exactly two serialized CometNativeExec blocks (partial + final aggregate).
        val executed = withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
          spark.sql(query).queryExecution.executedPlan
        }
        val exchanges = executed.collect { case e: CometShuffleExchangeExec => e }
        assert(
          exchanges.size == 1,
          s"expected exactly one Comet hash exchange (two stages), found ${exchanges.size}:\n" +
            s"$executed")
        val nativeBlocks = executed.collect {
          case n: CometNativeExec if n.serializedPlanOpt.isDefined => n
        }
        assert(
          nativeBlocks.size == 2,
          s"expected exactly two serialized CometNativeExec blocks, found ${nativeBlocks.size}:\n" +
            s"$executed")

        // Baseline: normal Comet execution (offload off) through the same listener apparatus. This
        // positive control proves the listener observes executor task starts, so the `== 0`
        // assertion for the offloaded run is meaningful.
        var baseline: Seq[Seq[Any]] = null
        val baselineTaskStarts = countTaskStarts {
          baseline = withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
            spark.sql(query).collect().map(_.toSeq.toIndexedSeq).toIndexedSeq
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
            spark.sql(query).collect().map(_.toSeq.toIndexedSeq).toIndexedSeq
          }
        }

        def sortKey(r: Seq[Any]): String = r.map(v => s"$v").mkString(",")
        val baselineSorted = baseline.sortBy(sortKey)
        val offloadedSorted = offloaded.sortBy(sortKey)

        // The distributed aggregate must compose across the shuffle: partial counts written to
        // Ballista's IPC shuffle, read back, and merged by the final aggregate into correct totals.
        assert(
          offloadedSorted == baselineSorted,
          "offloaded (distributed) rows do not match baseline\n" +
            s"  baseline:  $baselineSorted\n  offloaded: $offloadedSorted")
        assert(
          baselineSorted == Seq(Seq(1, 3L), Seq(2, 2L), Seq(3, 4L)),
          s"unexpected group counts: $baselineSorted")

        // Crucially, NO Spark executor tasks ran for the offloaded (driver-side, distributed)
        // collect.
        assert(
          offloadedTaskStarts == 0,
          s"expected 0 Spark executor tasks for the Ballista-offloaded distributed collect, " +
            s"but $offloadedTaskStarts started")
      }
    }
  }
}
