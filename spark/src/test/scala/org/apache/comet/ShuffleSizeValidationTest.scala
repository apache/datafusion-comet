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

import org.apache.spark.SparkConf
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.CometConf

/**
 * Quick validation test to verify shuffle size comparison works. This runs a single simple test
 * to validate the testing infrastructure.
 */
class ShuffleSizeValidationTest extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
  }

  test("validate shuffle size measurement - simple case") {
    val numRows = 10000

    var sparkBytes = 0L
    var sparkRecords = 0L
    var cometBytes = 0L
    var cometRecords = 0L

    // Test Spark shuffle
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {

      val shuffleMetrics = mutable.ArrayBuffer.empty[ShuffleWriteMetrics]
      val listener = new SparkListener {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          if (taskEnd.taskType.contains("ShuffleMapTask")) {
            shuffleMetrics.synchronized {
              shuffleMetrics += taskEnd.taskMetrics.shuffleWriteMetrics
            }
          }
        }
      }

      spark.sparkContext.addSparkListener(listener)
      spark.sparkContext.listenerBus.waitUntilEmpty()

      try {
        withParquetTable((0 until numRows).map(i => (i % 100, i)), "test_data") {
          sql("SELECT _1, COUNT(*), SUM(_2) FROM test_data GROUP BY _1").collect()
        }
        spark.sparkContext.listenerBus.waitUntilEmpty()

        sparkBytes = shuffleMetrics.map(_.bytesWritten).sum
        sparkRecords = shuffleMetrics.map(_.recordsWritten).sum
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }

    // Test Comet JVM shuffle
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {

      val shuffleMetrics = mutable.ArrayBuffer.empty[ShuffleWriteMetrics]
      val listener = new SparkListener {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          if (taskEnd.taskType.contains("ShuffleMapTask")) {
            shuffleMetrics.synchronized {
              shuffleMetrics += taskEnd.taskMetrics.shuffleWriteMetrics
            }
          }
        }
      }

      spark.sparkContext.addSparkListener(listener)
      spark.sparkContext.listenerBus.waitUntilEmpty()

      try {
        withParquetTable((0 until numRows).map(i => (i % 100, i)), "test_data") {
          sql("SELECT _1, COUNT(*), SUM(_2) FROM test_data GROUP BY _1").collect()
        }
        spark.sparkContext.listenerBus.waitUntilEmpty()

        cometBytes = shuffleMetrics.map(_.bytesWritten).sum
        cometRecords = shuffleMetrics.map(_.recordsWritten).sum
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }

    println("\n=== Shuffle Size Validation ===")
    println(f"Spark shuffle:  $sparkBytes%,12d bytes  ($sparkRecords%,8d records)")
    println(f"Comet shuffle:  $cometBytes%,12d bytes  ($cometRecords%,8d records)")
    println(f"Size ratio:     ${cometBytes.toDouble / sparkBytes}%.2fx")
    println(f"Overhead:       ${((cometBytes.toDouble / sparkBytes) - 1) * 100}%+.2f%%")

    // Verify we got meaningful results
    assert(sparkBytes > 0, "Spark shuffle bytes should be > 0")
    assert(cometBytes > 0, "Comet shuffle bytes should be > 0")
    assert(
      sparkRecords == cometRecords,
      s"Record counts should match: $sparkRecords vs $cometRecords")
    assert(sparkRecords == 100, s"Expected 100 groups, got $sparkRecords")
  }
}
