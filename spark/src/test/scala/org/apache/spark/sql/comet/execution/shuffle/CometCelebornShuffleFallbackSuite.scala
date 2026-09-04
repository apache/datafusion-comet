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

import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.shuffle.{ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.CometTestBase

import org.apache.comet.CometConf
import org.apache.comet.shuffle.{CelebornShufflePusherFactory, RecordingCelebornPushClient, ResolvedCelebornShufflePusher}

/**
 * Real Spark stage recovery and local Comet I/O; only the optional Celeborn client is replaced.
 */
class CometCelebornShuffleFallbackSuite extends CometTestBase {

  import testImplicits._

  override protected val shuffleManager: String =
    classOf[CometCelebornFallbackTestShuffleManager].getName

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SHUFFLE_MODE.key, "native")
      .set(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key, "false")
      .set(CometConf.COMET_SHUFFLE_RSS_MAX_FRAME_BYTES.key, "32k")
      .set("spark.shuffle.compress", "false")
      .set("spark.io.encryption.enabled", "false")
      .set("spark.celeborn.client.spark.stageRerun.enabled", "true")
      .set("spark.stage.maxConsecutiveAttempts", "3")

  private def manager: CometCelebornFallbackTestShuffleManager =
    SparkEnv.get.shuffleManager.asInstanceOf[CometCelebornFallbackTestShuffleManager]

  for (adaptive <- Seq(false, true)) {
    test(s"an oversized row retries its complete map stage locally and succeeds: AQE=$adaptive") {
      if (!CometCelebornShuffleManager.supportsNativeShuffleStageRecovery(
          org.apache.spark.SPARK_VERSION)) {
        cancel("Native Celeborn stage recovery requires Spark 3.5.1 or newer")
      }
      withSQLConf("spark.sql.adaptive.enabled" -> adaptive.toString) {
        val rows = Seq((0, "small"), (1, "x" * (1024 * 1024)))
        withTempPath { path =>
          rows
            .toDF("key", "payload")
            .coalesce(1)
            .write
            .option("parquet.enable.dictionary", "false")
            .parquet(path.getCanonicalPath)
          val remoteBefore = manager.remoteAttempts.size()
          val writersBefore = manager.writerStages.size()
          val readsBefore = manager.nativeReads.get()
          val shuffled = spark.read.parquet(path.getCanonicalPath).repartition(3, $"key")

          val actual = shuffled.collect().map(row => (row.getInt(0), row.getString(1))).toSeq

          assert(actual.sortBy(_._1) == rows)
          val remote = manager.remoteAttempts.asScala.drop(remoteBefore).toSeq
          assert(remote.size == 1, s"expected one failed remote map attempt, got $remote")
          assert(remote.head._2 == 0)
          val stages = manager.writerStages.asScala.drop(writersBefore).toSeq
          assert(stages.sorted == Seq(0, 1), s"expected one remote and one local stage: $stages")
          assert(manager.nativeReads.get() > readsBefore)
          val client = remote.head._3
          assert(client.cleanupCalls.get() == 1)
          assert(client.mapperEndCalls.get() == 0)
          assert(client.fetchFailureReports.get() == 1)

          // Shuffle removal must clean both destinations after the successful replacement.
          assert(manager.unregisterShuffle(remote.head._1))
          assert(client.shuffleCleanupCalls.get() == 1)
        }
      }
    }

    test(s"fallback recomputes a previously completed remote map: AQE=$adaptive") {
      if (!CometCelebornShuffleManager.supportsNativeShuffleStageRecovery(
          org.apache.spark.SPARK_VERSION)) {
        cancel("Native Celeborn stage recovery requires Spark 3.5.1 or newer")
      }
      withSQLConf("spark.sql.adaptive.enabled" -> adaptive.toString) {
        val remoteBefore = manager.remoteAttempts.size()
        val writersBefore = manager.writerAttempts.size()
        val readsBefore = manager.nativeReads.get()
        manager.remoteMapsBeforeFailure.set(0)
        manager.waitForFirstRemoteMap = true
        try {
          // Range partition zero produces the small row; partition one produces the oversized
          // row. Delay the second writer until Spark has registered the first remote map output.
          val shuffled = spark
            .range(0, 2, 1, 2)
            .selectExpr(
              "CAST(id AS INT) AS key",
              "CASE WHEN id = 0 THEN 'small' ELSE repeat('x', 1048576) END AS payload")
            .repartition(3, $"key")
          val actual = shuffled.collect().map(row => (row.getInt(0), row.getString(1))).toSeq

          assert(actual.sortBy(_._1) == Seq((0, "small"), (1, "x" * (1024 * 1024))))
          assert(manager.remoteMapsBeforeFailure.get() == 1)
          val remote = manager.remoteAttempts.asScala.drop(remoteBefore).toSeq
          assert(remote.size == 2)
          assert(remote.map(_._1).distinct.size == 1)
          assert(remote.forall(_._2 == 0))
          assert(remote.count(_._3.mapperEndCalls.get() == 1) == 1)
          assert(remote.map(_._3.fetchFailureReports.get()).sum == 1)
          val attempts = manager.writerAttempts.asScala.drop(writersBefore).toSeq
          assert(
            attempts.sorted == Seq((0, 0), (0, 1), (1, 0), (1, 1)),
            s"both maps must be recomputed exactly once after fallback: $attempts")
          assert(manager.nativeReads.get() > readsBefore)
          assert(manager.unregisterShuffle(remote.head._1))
        } finally {
          manager.waitForFirstRemoteMap = false
        }
      }
    }
  }
}

class CometCelebornFallbackTestShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends CometCelebornShuffleManager(
      conf,
      isDriver,
      (configuration, _) => new CometCelebornFallbackTestBackend(configuration),
      planningSupportFactory = _ => CelebornNativeShufflePlanningSupport()) {

  val remoteAttempts =
    new ConcurrentLinkedQueue[(Int, Int, CometCelebornFallbackTestClient)]()
  val writerStages = new ConcurrentLinkedQueue[Int]()
  val nativeReads = new AtomicInteger()
  val writerAttempts = new ConcurrentLinkedQueue[(Int, Int)]()
  val remoteMapsBeforeFailure = new AtomicInteger()
  @volatile var waitForFirstRemoteMap = false

  override protected[shuffle] def shouldReportShuffleFetchFailure(taskAttemptId: Long): Boolean =
    true

  override protected[shuffle] def createRemotePusher(
      handle: ShuffleHandle,
      context: TaskContext,
      onGenerationResolved: (Int, Int) => Unit,
      onGenerationInvalidated: (Int, Int) => Unit,
      onInvalidationUnsafe: (Int, Int) => Boolean): ResolvedCelebornShufflePusher = {
    val remoteHandle = handle.asInstanceOf[CelebornShuffleHandle[_, _, _]]
    val dependency = remoteHandle.dependency
    val client = new CometCelebornFallbackTestClient
    val generation = handle.shuffleId + 1000
    val numMappers = remoteHandle.numMappers
    onGenerationResolved(generation, numMappers)
    remoteAttempts.add((handle.shuffleId, context.stageAttemptNumber(), client))
    ResolvedCelebornShufflePusher(
      CelebornShufflePusherFactory.create(
        conf,
        client,
        generation,
        numMappers,
        dependency.partitioner.numPartitions,
        context),
      client,
      generation)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    if (waitForFirstRemoteMap && handle.isInstanceOf[CelebornShuffleHandle[_, _, _]] &&
      context.stageAttemptNumber() == 0 && context.partitionId() == 1) {
      // This fixture runs Spark locally, so the executor can observe the driver's tracker.
      // Waiting for a recorded MapStatus proves the retry discards already published output.
      val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
      while (tracker.getNumAvailableOutputs(handle.shuffleId) == 0 &&
        System.nanoTime() < deadline) {
        Thread.sleep(10)
      }
      val completed = tracker.getNumAvailableOutputs(handle.shuffleId)
      require(
        completed == 1,
        s"Expected one completed remote map before size failure: $completed")
      remoteMapsBeforeFailure.set(completed)
    }
    val writer = super.getWriter[K, V](handle, mapId, context, metrics)
    if (handle.isInstanceOf[CelebornShuffleHandle[_, _, _]]) {
      writerStages.add(context.stageAttemptNumber())
      writerAttempts.add((context.stageAttemptNumber(), context.partitionId()))
    }
    writer
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val reader = super.getReader[K, C](
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics)
    if (handle.isInstanceOf[CelebornShuffleHandle[_, _, _]]) {
      nativeReads.incrementAndGet()
    }
    reader
  }
}

class CometCelebornFallbackTestClient extends RecordingCelebornPushClient {
  val fetchFailureReports = new AtomicInteger()
  val shuffleCleanupCalls = new AtomicInteger()

  def reportShuffleFetchFailure(
      shuffleId: Int,
      celebornShuffleId: Int,
      taskAttemptId: Long): Boolean = {
    fetchFailureReports.incrementAndGet()
    true
  }

  def cleanupShuffle(shuffleId: Int): Unit = {
    shuffleCleanupCalls.incrementAndGet()
  }
}

private[shuffle] class CometCelebornFallbackTestBackend(conf: SparkConf) extends ShuffleManager {
  private val ordinaryShuffle = new CometShuffleManager(conf)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = dependency match {
    case native: CometShuffleDependency[_, _, _] if native.shuffleType == CometNativeShuffle =>
      require(
        dependency.rdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE,
        "Remote shuffle input must invalidate every previous-stage map result during fallback")
      new CelebornShuffleHandle(shuffleId, dependency)
    case _ => ordinaryShuffle.registerShuffle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    require(
      !handle.isInstanceOf[CelebornShuffleHandle[_, _, _]],
      "Unexpected ordinary native writer")
    ordinaryShuffle.getWriter(handle, mapId, context, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    require(
      !handle.isInstanceOf[CelebornShuffleHandle[_, _, _]],
      "Unexpected remote native reader")
    ordinaryShuffle.getReader(
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = ordinaryShuffle.shuffleBlockResolver

  override def unregisterShuffle(shuffleId: Int): Boolean =
    ordinaryShuffle.unregisterShuffle(shuffleId)

  override def stop(): Unit = ordinaryShuffle.stop()
}
