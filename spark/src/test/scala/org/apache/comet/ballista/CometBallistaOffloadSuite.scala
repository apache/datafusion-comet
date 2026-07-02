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
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Proves the driver-side "offload to Ballista" collect path (R1): when
 * `spark.comet.exec.ballista.enabled=true`, a `collect()` on a single-stage Comet query runs on
 * the Spark driver via an in-process Ballista engine and returns the same rows as the normal
 * path, launching NO Spark executor tasks.
 */
class CometBallistaOffloadSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  /**
   * Runs `f`, counting Spark executor task starts that occur during it. Drains the listener bus
   * before attaching (so events from prior setup don't leak in) and after running `f` (so
   * asynchronously-dispatched task-start events are flushed before we read the counter).
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

  test("single-stage collect is offloaded to Ballista with no Spark executor tasks") {
    assume(
      NativeBallista.isAvailable,
      s"native ballista library not available: ${NativeBallista.loadFailure.map(_.getMessage)}")

    withTempPath { dir =>
      import testImplicits._

      // A single Parquet file (coalesce(1)) with two int columns, so the offloaded plan is a
      // clean single-stage scan with no exchange.
      Seq((1, 10), (2, 20), (3, 30), (4, 40), (5, 50))
        .toDF("a", "b")
        .coalesce(1)
        .write
        .parquet(dir.getCanonicalPath)

      // Disable AQE so the collect root is the Comet columnar-to-row node (which carries our
      // executeCollect override) rather than an AdaptiveSparkPlanExec wrapper.
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("t")
        val query = "SELECT a, b FROM t WHERE a > 2"

        // Baseline: normal Comet execution (offload off), run through the same
        // listener/waitUntilEmpty apparatus used for the offloaded case below. This is a
        // positive control: it proves the listener actually observes executor task starts (i.e.
        // it isn't a broken apparatus that would report 0 regardless), so the `== 0` assertion
        // for the offloaded collect is meaningful.
        var baseline: Set[Seq[Any]] = null
        val baselineTaskStarts = countTaskStarts {
          baseline = withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
            spark.sql(query).collect().map(_.toSeq).toSet
          }
        }
        assert(
          baseline == Set(Seq(3, 30), Seq(4, 40), Seq(5, 50)),
          s"unexpected baseline: $baseline")
        assert(
          baselineTaskStarts > 0,
          "expected the flag-off baseline collect to launch at least one Spark executor task " +
            "(sanity check that the listener/waitUntilEmpty apparatus catches task starts); " +
            s"got $baselineTaskStarts")

        // Ballista offload: count executor task starts around the collect.
        var offloaded: Set[Seq[Any]] = null
        val offloadedTaskStarts = countTaskStarts {
          offloaded = withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "true") {
            spark.sql(query).collect().map(_.toSeq).toSet
          }
        }

        // Same rows via the offloaded path...
        assert(offloaded == baseline, s"offloaded rows $offloaded != baseline $baseline")
        // ...and crucially, NO Spark executor tasks ran for the offloaded collect.
        assert(
          offloadedTaskStarts == 0,
          s"expected 0 Spark executor tasks for the Ballista-offloaded collect, " +
            s"but $offloadedTaskStarts started")
      }
    }
  }

  test("multi-stage collect (exchange present) throws under Ballista offload") {
    assume(
      NativeBallista.isAvailable,
      s"native ballista library not available: ${NativeBallista.loadFailure.map(_.getMessage)}")

    withTempPath { dir =>
      import testImplicits._

      // Several partition files with repeated keys, so a `GROUP BY` requires a shuffle
      // (exchange) to aggregate across partitions -> more than one CometNativeExec boundary.
      Seq((1, 10), (1, 20), (2, 30), (2, 40), (3, 50), (3, 60), (4, 70), (4, 80))
        .toDF("k", "v")
        .repartition(4)
        .write
        .parquet(dir.getCanonicalPath)

      // Disable AQE so the shuffle boundary/plan shape is deterministic (no runtime coalescing
      // of the exchange back down to a single stage).
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("t2")
        val query = "SELECT k, count(*) FROM t2 GROUP BY k"

        withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "true") {
          val df = spark.sql(query)
          // Sanity check: the plan does contain an exchange, i.e. the single-stage guard is
          // actually exercised and not vacuously satisfied.
          val hasExchange = df.queryExecution.executedPlan.collect {
            case e: org.apache.spark.sql.execution.exchange.Exchange => e
          }.nonEmpty
          assert(
            hasExchange,
            s"expected an exchange in the plan:\n${df.queryExecution.executedPlan}")

          val ex = intercept[UnsupportedOperationException] {
            df.collect()
          }
          assert(
            ex.getMessage.contains("single-stage plans only"),
            s"unexpected exception message: ${ex.getMessage}")
        }
      }
    }
  }
}
