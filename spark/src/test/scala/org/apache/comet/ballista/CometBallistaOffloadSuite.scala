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

        // Baseline: normal Comet execution (offload off).
        val baseline = withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "false") {
          spark.sql(query).collect().map(_.toSeq).toSet
        }
        assert(
          baseline == Set(Seq(3, 30), Seq(4, 40), Seq(5, 50)),
          s"unexpected baseline: $baseline")

        // Ballista offload: count executor task starts around the collect.
        val taskStarts = new AtomicInteger(0)
        val listener = new SparkListener {
          override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
            taskStarts.incrementAndGet()
          }
        }
        // Drain any events from setup/baseline before attaching, so the counter only sees the
        // offloaded collect.
        CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
        spark.sparkContext.addSparkListener(listener)

        val offloaded =
          try {
            withSQLConf(CometConf.COMET_EXEC_BALLISTA_ENABLED.key -> "true") {
              val df = spark.sql(query)
              val rows = df.collect().map(_.toSeq).toSet
              CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
              rows
            }
          } finally {
            spark.sparkContext.removeSparkListener(listener)
          }

        // Same rows via the offloaded path...
        assert(offloaded == baseline, s"offloaded rows $offloaded != baseline $baseline")
        // ...and crucially, NO Spark executor tasks ran for the offloaded collect.
        assert(
          taskStarts.get() == 0,
          s"expected 0 Spark executor tasks for the Ballista-offloaded collect, " +
            s"but ${taskStarts.get()} started")
      }
    }
  }
}
