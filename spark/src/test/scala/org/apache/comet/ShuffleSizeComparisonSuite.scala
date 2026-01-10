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
 * Test suite to compare shuffle file sizes between Spark, Comet JVM, and Comet Native shuffle.
 *
 * This suite measures the actual bytes written to disk during shuffle operations to understand
 * the size overhead of different shuffle implementations.
 */
class ShuffleSizeComparisonSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
  }

  private def formatBytes(bytes: Long): String = {
    if (bytes < 1024) {
      s"${bytes} B"
    } else if (bytes < 1024 * 1024) {
      f"${bytes / 1024.0}%.2f KB"
    } else if (bytes < 1024 * 1024 * 1024) {
      f"${bytes / (1024.0 * 1024)}%.2f MB"
    } else {
      f"${bytes / (1024.0 * 1024 * 1024)}%.2f GB"
    }
  }

  test("compare shuffle sizes - simple integer groupBy") {
    val numRows = 100000

    println("\n" + "=" * 80)
    println("Shuffle Size Comparison: Simple Integer GroupBy")
    println(s"Number of rows: ${numRows.toDouble / 1000}K")
    println("=" * 80)

    var sparkBytes = 0L
    var sparkRecords = 0L

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

    var cometJvmBytes = 0L
    var cometJvmRecords = 0L

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

        cometJvmBytes = shuffleMetrics.map(_.bytesWritten).sum
        cometJvmRecords = shuffleMetrics.map(_.recordsWritten).sum
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }

    var cometNativeBytes = 0L
    var cometNativeRecords = 0L

    // Test Comet Native shuffle
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native") {

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

        cometNativeBytes = shuffleMetrics.map(_.bytesWritten).sum
        cometNativeRecords = shuffleMetrics.map(_.recordsWritten).sum
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }

    val jvmOverhead = ((cometJvmBytes.toDouble / sparkBytes) - 1) * 100
    val nativeOverhead = ((cometNativeBytes.toDouble / sparkBytes) - 1) * 100

    println("\nResults:")
    println(
      f"  Spark Shuffle:        ${formatBytes(sparkBytes)}%15s  (${sparkRecords}%,12d records)")
    println(
      f"  Comet JVM Shuffle:    ${formatBytes(cometJvmBytes)}%15s  (${cometJvmRecords}%,12d records)  [${jvmOverhead}%+6.2f%% vs Spark]")
    println(
      f"  Comet Native Shuffle: ${formatBytes(cometNativeBytes)}%15s  (${cometNativeRecords}%,12d records)  [${nativeOverhead}%+6.2f%% vs Spark]")

    println("\nBytes per record:")
    println(f"  Spark:        ${sparkBytes.toDouble / sparkRecords}%.2f bytes/record")
    println(f"  Comet JVM:    ${cometJvmBytes.toDouble / cometJvmRecords}%.2f bytes/record")
    println(f"  Comet Native: ${cometNativeBytes.toDouble / cometNativeRecords}%.2f bytes/record")

    assert(
      sparkRecords == cometJvmRecords && sparkRecords == cometNativeRecords,
      s"Record counts don't match: Spark=$sparkRecords, JVM=$cometJvmRecords, Native=$cometNativeRecords")
  }
}
