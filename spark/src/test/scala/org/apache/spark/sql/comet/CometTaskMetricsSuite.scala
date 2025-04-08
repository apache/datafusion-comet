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

import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.execution.shuffle.CometNativeShuffle
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

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
}
