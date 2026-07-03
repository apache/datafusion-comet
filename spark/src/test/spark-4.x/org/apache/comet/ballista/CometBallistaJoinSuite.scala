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
import org.apache.spark.sql.comet.{CometHashJoinExec, CometNativeExec, CometSortMergeJoinExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Task 8 (proof milestone): a distributed shuffle-hash/sort-merge JOIN and an N-stage aggregate
 * (>2 native blocks / 2 hash exchanges) both run DISTRIBUTED on the in-process Ballista engine,
 * producing results identical to the flag-off (Spark/Comet-on-executors) baseline while launching
 * ZERO Spark executor tasks.
 *
 * The join fragment shape (see [[BallistaOffloadPlanner]]): a single co-partitioned join block
 * fed by exactly two Comet hash exchanges, one per side.
 * `spark.sql.autoBroadcastJoinThreshold=-1` forces a shuffle-hash / sort-merge join instead of a
 * broadcast join, since a broadcast join side is not yet a supported offload shape (the walker
 * rejects it).
 */
class CometBallistaJoinSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  /**
   * Runs `f`, counting Spark executor task starts during it. Drains the listener bus before
   * attaching and after running so asynchronous task-start events are flushed. (Same apparatus as
   * `CometBallistaDistributedSuite` / `CometBallistaOffloadSuite`.)
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

  /**
   * Runs `query` with the Ballista offload flag set to `ballista`, counting Spark executor task
   * starts. AQE is off, shuffle partitions are pinned small, shuffle direct-read is off (so a
   * downstream fragment's input leaf serializes as a plain `Scan` fed by the Ballista shuffle),
   * and the broadcast-join threshold is disabled so joins plan as shuffle-hash / sort-merge
   * rather than broadcast.
   */
  private def runWith(ballista: Boolean, query: String): (Seq[Seq[Any]], Int) = {
    var rows: Seq[Seq[Any]] = null
    val taskStarts = countTaskStarts {
      rows = withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> ballista.toString) {
        spark.sql(query).collect().map(_.toSeq.toIndexedSeq).toIndexedSeq
      }
    }
    (rows, taskStarts)
  }

  private def sortKey(r: Seq[Any]): String = r.map(v => s"$v").mkString(",")

  test("distributed shuffle-hash join offloads with zero Spark tasks") {
    assume(
      NativeBallista.isAvailable,
      s"native ballista library not available: ${NativeBallista.loadFailure.map(_.getMessage)}")

    withParquetTable((0 until 200).map(i => (i, i * 10)), "l") {
      withParquetTable((0 until 200).map(i => (i % 50, i * 100)), "r") {
        val query = "SELECT l._1, l._2, r._2 FROM l JOIN r ON l._1 = r._1"

        // Pre-flight: confirm the plan is the co-partitioned join shape (two Comet hash
        // exchanges feeding a shuffle-hash/sort-merge join) BEFORE running it.
        val executed = withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.SHUFFLE_PARTITIONS.key -> "4",
          CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
          spark.sql(query).queryExecution.executedPlan
        }
        val exchanges = executed.collect { case e: CometShuffleExchangeExec => e }
        assert(
          exchanges.size == 2,
          s"expected exactly two Comet hash exchanges (one per join side), found " +
            s"${exchanges.size}:\n$executed")
        val joins = executed.collect {
          case j: CometHashJoinExec => j
          case j: CometSortMergeJoinExec => j
        }
        assert(
          joins.size == 1,
          s"expected exactly one shuffle-hash/sort-merge join, found ${joins.size}:\n$executed")

        // Baseline: normal Comet execution (offload off), a positive control proving the
        // listener observes executor task starts.
        val (baseline, baselineTaskStarts) = runWith(ballista = false, query)
        assert(
          baselineTaskStarts > 0,
          "expected the flag-off baseline collect to launch at least one Spark executor task " +
            s"(sanity check for the listener apparatus); got $baselineTaskStarts")

        // Ballista offload: same query, flag on.
        val (offloaded, offloadedTaskStarts) = runWith(ballista = true, query)

        val baselineSorted = baseline.sortBy(sortKey)
        val offloadedSorted = offloaded.sortBy(sortKey)
        assert(
          offloadedSorted == baselineSorted,
          "offloaded (distributed) rows do not match baseline\n" +
            s"  baseline:  $baselineSorted\n  offloaded: $offloadedSorted")

        assert(
          offloadedTaskStarts == 0,
          "expected 0 Spark executor tasks for the Ballista-offloaded distributed join, " +
            s"but $offloadedTaskStarts started")
      }
    }
  }

  test("three-stage aggregate (two hash exchanges) offloads with zero Spark tasks") {
    assume(
      NativeBallista.isAvailable,
      s"native ballista library not available: ${NativeBallista.loadFailure.map(_.getMessage)}")

    // _1 ranges over 200 distinct values (0..199), each appearing twice, so the inner
    // `GROUP BY _1` needs a real shuffle to de-duplicate across partitions. k = _1 % 10 then
    // buckets those 200 distinct values into 10 groups of 20, and the outer `GROUP BY k` needs a
    // second shuffle to merge partial counts. Two shuffles => three native blocks (partial
    // dedupe -> exchange -> final dedupe + partial count -> exchange -> final count).
    withParquetTable((0 until 400).map(i => (i % 200, i)), "t") {
      val query = "SELECT k, count(*) AS c FROM (SELECT _1 % 10 AS k FROM t GROUP BY _1) " +
        "GROUP BY k"

      // Pre-flight: confirm the plan is the >=3-block / 2-exchange shape BEFORE running it.
      val executed = withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "false",
        CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
        spark.sql(query).queryExecution.executedPlan
      }
      val exchanges = executed.collect { case e: CometShuffleExchangeExec => e }
      assert(
        exchanges.size == 2,
        s"expected exactly two Comet hash exchanges (three stages), found ${exchanges.size}:\n" +
          s"$executed")
      val nativeBlocks = executed.collect {
        case n: CometNativeExec if n.serializedPlanOpt.isDefined => n
      }
      assert(
        nativeBlocks.size == 3,
        s"expected exactly three serialized CometNativeExec blocks, found " +
          s"${nativeBlocks.size}:\n$executed")

      // Baseline: normal Comet execution (offload off), a positive control proving the listener
      // observes executor task starts.
      val (baseline, baselineTaskStarts) = runWith(ballista = false, query)
      assert(
        baselineTaskStarts > 0,
        "expected the flag-off baseline collect to launch at least one Spark executor task " +
          s"(sanity check for the listener apparatus); got $baselineTaskStarts")
      assert(
        baseline.sortBy(sortKey) == (0 until 10).map(k => Seq(k, 20L)),
        s"unexpected group counts: ${baseline.sortBy(sortKey)}")

      // Ballista offload: same query, flag on.
      val (offloaded, offloadedTaskStarts) = runWith(ballista = true, query)

      val baselineSorted = baseline.sortBy(sortKey)
      val offloadedSorted = offloaded.sortBy(sortKey)
      assert(
        offloadedSorted == baselineSorted,
        "offloaded (distributed) rows do not match baseline\n" +
          s"  baseline:  $baselineSorted\n  offloaded: $offloadedSorted")

      assert(
        offloadedTaskStarts == 0,
        "expected 0 Spark executor tasks for the Ballista-offloaded distributed aggregate, " +
          s"but $offloadedTaskStarts started")
    }
  }
}
