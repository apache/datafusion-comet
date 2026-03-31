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

package org.apache.spark.sql.comet

import scala.collection.mutable

import org.apache.spark.executor.InputMetrics
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.execution.shuffle.CometNativeShuffle
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.CometConf

class CometTaskMetricsSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  import testImplicits._

  test("per-task native shuffle metrics") {
    withParquetTable((0 until 10000).map(i => (i, (i + 1).toLong)), "tbl") {
      val df = sql("SELECT * FROM tbl").sortWithinPartitions($"_1".desc)
      val shuffled = df.repartition(1, $"_1")

      val cometShuffle = find(shuffled.queryExecution.executedPlan) {
        case _: CometShuffleExchangeExec => true
        case _ => false
      }
      assert(cometShuffle.isDefined, "CometShuffleExchangeExec not found in the plan")
      assert(
        cometShuffle.get.asInstanceOf[CometShuffleExchangeExec].shuffleType == CometNativeShuffle)

      val shuffleWriteMetricsList = mutable.ArrayBuffer.empty[ShuffleWriteMetrics]
      val shuffleReadMetricsList = mutable.ArrayBuffer.empty[ShuffleReadMetrics]

      spark.sparkContext.addSparkListener(new SparkListener {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          val taskMetrics = taskEnd.taskMetrics

          if (taskEnd.taskType.contains("ShuffleMapTask")) {
            val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
            shuffleWriteMetricsList.synchronized {
              shuffleWriteMetricsList += shuffleWriteMetrics
            }
          } else {
            val shuffleReadMetrics = taskMetrics.shuffleReadMetrics
            shuffleReadMetricsList.synchronized {
              shuffleReadMetricsList += shuffleReadMetrics
            }
          }
        }
      })

      // Avoid receiving earlier taskEnd events
      spark.sparkContext.listenerBus.waitUntilEmpty()

      // Run the action to trigger the shuffle
      shuffled.collect()

      spark.sparkContext.listenerBus.waitUntilEmpty()

      // Check the shuffle write and read metrics
      assert(shuffleWriteMetricsList.nonEmpty, "No shuffle write metrics found")
      shuffleWriteMetricsList.foreach { metrics =>
        assert(metrics.writeTime > 0)
        assert(metrics.bytesWritten > 0)
        assert(metrics.recordsWritten > 0)
      }

      assert(shuffleReadMetricsList.nonEmpty, "No shuffle read metrics found")
      shuffleReadMetricsList.foreach { metrics =>
        assert(metrics.recordsRead > 0)
        assert(metrics.totalBytesRead > 0)
      }
    }
  }

  test("native_datafusion scan reports task-level input metrics matching Spark") {
    withParquetTable((0 until 10000).map(i => (i, (i + 1).toLong)), "tbl") {
      // Collect baseline input metrics from vanilla Spark (Comet disabled)
      val (sparkBytes, sparkRecords, _) =
        collectInputMetrics(CometConf.COMET_ENABLED.key -> "false")

      // Collect input metrics from Comet native_datafusion scan
      val (cometBytes, cometRecords, cometPlan) = collectInputMetrics(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION)

      // Verify the plan actually used CometNativeScanExec
      assert(
        find(cometPlan)(_.isInstanceOf[CometNativeScanExec]).isDefined,
        s"Expected CometNativeScanExec in plan:\n${cometPlan.treeString}")

      // Records must match exactly
      assert(
        cometRecords == sparkRecords,
        s"recordsRead mismatch: comet=$cometRecords, spark=$sparkRecords")

      // Bytes should be in the same ballpark -- both read the same Parquet file(s),
      // but the exact byte count can differ due to reader implementation details
      // (e.g. footer reads, page headers, buffering granularity).
      assert(sparkBytes > 0, s"Spark bytesRead should be > 0, got $sparkBytes")
      assert(cometBytes > 0, s"Comet bytesRead should be > 0, got $cometBytes")
      val ratio = cometBytes.toDouble / sparkBytes.toDouble
      assert(
        ratio >= 0.8 && ratio <= 1.2,
        s"bytesRead ratio out of range: comet=$cometBytes, spark=$sparkBytes, ratio=$ratio")
    }
  }

  /**
   * Runs `SELECT * FROM tbl` with the given SQL config overrides and returns the aggregated
   * (bytesRead, recordsRead) across all tasks, along with the executed plan.
   */
  private def collectInputMetrics(confs: (String, String)*): (Long, Long, SparkPlan) = {
    val inputMetricsList = mutable.ArrayBuffer.empty[InputMetrics]

    val listener = new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val im = taskEnd.taskMetrics.inputMetrics
        inputMetricsList.synchronized {
          inputMetricsList += im
        }
      }
    }

    spark.sparkContext.addSparkListener(listener)
    try {
      // Drain any earlier events
      spark.sparkContext.listenerBus.waitUntilEmpty()

      var plan: SparkPlan = null
      withSQLConf(confs: _*) {
        val df = sql("SELECT * FROM tbl where _1 > 2000")
        df.collect()
        plan = stripAQEPlan(df.queryExecution.executedPlan)
      }

      spark.sparkContext.listenerBus.waitUntilEmpty()

      assert(inputMetricsList.nonEmpty, s"No input metrics found for confs=$confs")
      val totalBytes = inputMetricsList.map(_.bytesRead).sum
      val totalRecords = inputMetricsList.map(_.recordsRead).sum
      (totalBytes, totalRecords, plan)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}
