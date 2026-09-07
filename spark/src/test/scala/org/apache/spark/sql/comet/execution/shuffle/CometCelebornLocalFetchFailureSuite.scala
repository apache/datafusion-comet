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

package org.apache.spark.sql.comet.execution.shuffle

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleHandle, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.sql.CometTestBase

import org.apache.comet.CometConf

class CometCelebornLocalFetchFailureSuite extends CometTestBase {

  import testImplicits._

  override protected val shuffleManager: String =
    classOf[CometCelebornLocalFetchFailureTestManager].getName

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SHUFFLE_MODE.key, "native")
      .set(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key, "false")
      .set(CometConf.COMET_SHUFFLE_RSS_MAX_FRAME_BYTES.key, "32k")
      .set("spark.shuffle.compress", "false")
      .set("spark.io.encryption.enabled", "false")
      .set("spark.celeborn.client.spark.stageRerun.enabled", "true")
      .set("spark.stage.maxConsecutiveAttempts", "4")

  test("local fetch failure after fallback recovers a partially completed result stage") {
    val manager = SparkEnv.get.shuffleManager
      .asInstanceOf[CometCelebornLocalFetchFailureTestManager]
    val listener = new SparkListener {
      override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
        if (event.taskType == "ResultTask" && event.taskInfo.successful) {
          manager.completedResultTasks.incrementAndGet()
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      withSQLConf("spark.sql.adaptive.enabled" -> "false") {
        // Both deterministic input partitions exceed the remote frame limit, forcing fresh
        // local materialization. Keys 1 and 2 reach different reducers, so the result partition
        // that completes before the fetch failure contains an actual row.
        val query = spark
          .range(1, 3, 1, 2)
          .selectExpr("CAST(id AS INT) AS key", "repeat('x', 1048576) AS payload")
          .repartition(2, $"key")

        val actual = query.collect().map(row => (row.getInt(0), row.getString(1))).toSeq

        assert(actual.sortBy(_._1) == Seq((1, "x" * (1024 * 1024)), (2, "x" * (1024 * 1024))))
        assert(manager.injectedFetchFailures.get() == 1)
        assert(manager.completedResultsBeforeFailure.get() > 0)
        assert(!manager.remoteAttempts.isEmpty)
        val failedShuffleId = manager.failedLocalShuffleId.get()
        assert(manager.remoteAttempts.asScala.forall(_._1.shuffleId != failedShuffleId))
        assert(manager.localAttempts.asScala.exists { attempt =>
          attempt.shuffleId == failedShuffleId && attempt.stageAttempt > 0
        })
      }
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}

/** Injects one local block loss only after Spark accepts another result partition. */
class CometCelebornLocalFetchFailureTestManager(conf: SparkConf, isDriver: Boolean)
    extends CometCelebornFallbackTestShuffleManager(conf, isDriver) {

  // This local-mode fixture keeps state in its SparkContext-owned shuffle manager.
  val completedResultTasks = new AtomicInteger()
  val completedResultsBeforeFailure = new AtomicInteger()
  val injectedFetchFailures = new AtomicInteger()
  val failedLocalShuffleId = new AtomicInteger(-1)
  private val failNextLocalFetch = new AtomicBoolean(true)

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    if (handle.isInstanceOf[CometNativeShuffleHandle[_, _]] &&
      context.partitionId() == 1 &&
      failNextLocalFetch.compareAndSet(true, false)) {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
      while (completedResultTasks.get() == 0 && System.nanoTime() < deadline) {
        Thread.sleep(10)
      }
      val completed = completedResultTasks.get()
      require(completed > 0, "Another result partition must finish before the local fetch fails")
      completedResultsBeforeFailure.set(completed)
      failedLocalShuffleId.set(handle.shuffleId)
      injectedFetchFailures.incrementAndGet()
      throw new FetchFailedException(
        SparkEnv.get.blockManager.shuffleServerId,
        handle.shuffleId,
        -1L,
        0,
        startPartition,
        "A local shuffle block was lost after another result partition completed",
        null)
    }
    super.getReader(
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics)
  }
}
