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

import java.util.Properties
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.language.existentials

import org.apache.spark.{FutureAction, MapOutputStatistics, MapOutputTrackerMaster, ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.scheduler.{JobFailed, MapStatus, SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.shuffle.{ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.CometTestBase

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
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
    test(s"an oversized row materializes a fresh local shuffle and succeeds: AQE=$adaptive") {
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
          val localBefore = manager.localAttempts.size()
          val readsBefore = manager.nativeReads.get()
          val shuffled = spark.read.parquet(path.getCanonicalPath).repartition(3, $"key")

          val actual = shuffled.collect().map(row => (row.getInt(0), row.getString(1))).toSeq

          assert(actual.sortBy(_._1) == rows)
          val remote = manager.remoteAttempts.asScala.drop(remoteBefore).toSeq
          assert(remote.size == 1, s"expected one failed remote map attempt, got $remote")
          val local = manager.localAttempts.asScala.drop(localBefore).toSeq
          assert(local.size == 1, s"expected a complete local map stage: $local")
          assert(local.head.shuffleId != remote.head._1.shuffleId)
          assert(local.head.stageId != remote.head._1.stageId)
          assert(local.head.stageAttempt == 0)
          assert(manager.nativeReads.get() > readsBefore)
          val client = remote.head._2
          assert(client.cleanupCalls.get() == 1)
          assert(client.mapperEndCalls.get() == 0)
          assert(client.fetchFailureReports.get() == 1)

          // Each destination has its own shuffle lifecycle after the successful replacement.
          assert(manager.unregisterShuffle(remote.head._1.shuffleId))
          assert(manager.unregisterShuffle(local.head.shuffleId))
          assert(client.shuffleCleanupCalls.get() == 1)
        }
      }
    }

    test(s"fallback recomputes a previously completed remote map: AQE=$adaptive") {
      withSQLConf("spark.sql.adaptive.enabled" -> adaptive.toString) {
        val remoteBefore = manager.remoteAttempts.size()
        val localBefore = manager.localAttempts.size()
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
          assert(remote.map(_._1.shuffleId).distinct.size == 1)
          assert(remote.forall(_._1.stageAttempt == 0))
          assert(remote.count(_._2.mapperEndCalls.get() == 1) == 1)
          assert(remote.map(_._2.fetchFailureReports.get()).sum == 1)
          val local = manager.localAttempts.asScala.drop(localBefore).toSeq
          assert(local.map(_.partitionId).sorted == Seq(0, 1))
          assert(local.map(_.shuffleId).distinct.size == 1)
          assert(local.map(_.stageId).distinct.size == 1)
          assert(local.forall(_.shuffleId != remote.head._1.shuffleId))
          assert(local.forall(_.stageId != remote.head._1.stageId))
          assert(local.forall(_.stageAttempt == 0))
          assert(manager.nativeReads.get() > readsBefore)
          assert(manager.unregisterShuffle(remote.head._1.shuffleId))
          assert(manager.unregisterShuffle(local.head.shuffleId))
        } finally {
          manager.waitForFirstRemoteMap = false
        }
      }
    }

    test(s"delayed remote map completion cannot replace fresh local output: AQE=$adaptive") {
      withSQLConf("spark.sql.adaptive.enabled" -> adaptive.toString) {
        val remoteBefore = manager.remoteAttempts.size()
        val localBefore = manager.localAttempts.size()
        val delayed = new DelayedRemoteMapCompletion
        manager.delayedRemoteCompletion = Some(delayed)
        try {
          val shuffled = spark
            .range(0, 2, 1, 2)
            .selectExpr(
              "CAST(id AS INT) AS key",
              "CASE WHEN id = 0 THEN 'small' ELSE repeat('x', 1048576) END AS payload")
            .repartition(3, $"key")

          val actual = shuffled.collect().map(row => (row.getInt(0), row.getString(1))).toSeq

          assert(actual.sortBy(_._1) == Seq((0, "small"), (1, "x" * (1024 * 1024))))
          assert(delayed.remoteReturned.await(20, TimeUnit.SECONDS))
          val remote = manager.remoteAttempts.asScala.drop(remoteBefore).toSeq
          val local = manager.localAttempts.asScala.drop(localBefore).toSeq
          assert(remote.size == 2)
          assert(remote.count(_._2.mapperEndCalls.get() == 1) == 1)
          assert(local.map(_.partitionId).sorted == Seq(0, 1))
          assert(local.map(_.shuffleId).distinct.size == 1)
          assert(local.map(_.stageId).distinct.size == 1)
          assert(local.forall(_.shuffleId != remote.head._1.shuffleId))
          assert(local.forall(_.stageId != remote.head._1.stageId))
          assert(local.forall(_.stageAttempt == 0))
          assert(manager.unregisterShuffle(remote.head._1.shuffleId))
          assert(manager.unregisterShuffle(local.head.shuffleId))
        } finally {
          // Unblock an old writer even if the query fails before the local stage starts.
          delayed.localStageStarted.countDown()
          manager.delayedRemoteCompletion = None
        }
      }
    }
  }

  test("successful remote materialization publishes the original shuffle identity") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      val remoteBefore = manager.remoteAttempts.size()
      val localBefore = manager.localAttempts.size()
      val readsBefore = manager.nativeReads.get()
      val query = spark.range(0, 2, 1, 2).repartition(2, $"id")
      val exchange = collect(query.queryExecution.executedPlan) {
        case value: CometShuffleExchangeExec => value
      }.headOption.getOrElse(fail("Expected a native Comet shuffle exchange"))
      val original = exchange.shuffleDependency.asInstanceOf[CometShuffleDependency[_, _, _]]
      val action = exchange.mapOutputStatisticsFuture
        .asInstanceOf[FutureAction[MapOutputStatistics]]
      try {
        val statistics = Await.result(action, 20.seconds)

        assert(statistics.shuffleId == original.shuffleId)
        assert(statistics.bytesByPartitionId.sum > 0L)
        assert(original.currentShuffleDependency eq original)
        // Spark 4 exposes this ID to AQE; the Spark 3 shim supplies an unused placeholder.
        if (isSpark40Plus) assert(exchange.shuffleId == original.shuffleId)
        assert(action.jobIds.size == 1)
        val remote = manager.remoteAttempts.asScala.drop(remoteBefore).toSeq
        assert(remote.map(_._1.partitionId).sorted == Seq(0, 1))
        assert(remote.forall(_._1.shuffleId == original.shuffleId))
        assert(remote.forall(_._2.mapperEndCalls.get() == 1))
        assert(manager.localAttempts.size() == localBefore)
        assert(manager.nativeReads.get() == readsBefore)
      } finally {
        action.cancel()
        manager.unregisterShuffle(original.shuffleId)
      }
    }
  }

  test("a remote stage failure cannot abort an active local replacement") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      val sc = spark.sparkContext
      val previousProperties = sc.getLocalProperties.clone().asInstanceOf[Properties]
      val groupId = "comet-remote-failure-isolation"
      val remoteBefore = manager.remoteAttempts.size()
      val localBefore = manager.localAttempts.size()
      val paused = new PausedShuffleMapCompletion(local = true)
      val jobStart = new PausedReplacementJobStart(groupId)
      val completedJobs = new CompletedMaterializationJobs
      manager.pausedMapCompletion = Some(paused)
      manager.remoteFailureAfterLocalMap = Some(paused)
      // Delay the materialization's JobStart listener, so the remote size failure reaches
      // Spark before Comet can cancel that job. Observe job failures on an independent queue.
      sc.addSparkListener(jobStart)
      sc.listenerBus.addToQueue(completedJobs, groupId)
      var active: Option[FutureAction[MapOutputStatistics]] = None
      try {
        sc.setJobGroup(groupId, "Isolate the local replacement from remote failure", true)
        val query = spark
          .range(0, 1, 1, 1)
          .selectExpr("CAST(id AS INT) AS key", "repeat('x', 1048576) AS payload")
          .repartition(2, $"key")
        val exchange = collect(query.queryExecution.executedPlan) {
          case value: CometShuffleExchangeExec => value
        }.head
        val original = exchange.shuffleDependency.asInstanceOf[CometShuffleDependency[_, _, _]]
        val action = exchange.mapOutputStatisticsFuture
          .asInstanceOf[FutureAction[MapOutputStatistics]]
        active = Some(action)
        assert(jobStart.entered.await(20, TimeUnit.SECONDS))
        assert(paused.stopped.await(20, TimeUnit.SECONDS))
        assert(action.jobIds.size == 2)
        val remoteJobId = action.jobIds.head
        completedJobs.awaitCompletion(Seq(remoteJobId))
        assert(completedJobs.hasFailed(remoteJobId))

        jobStart.release.countDown()
        paused.release.countDown()
        val statistics = Await.result(action, 20.seconds)

        val selected = original.currentShuffleDependency
        assert(selected.useLocalShuffle)
        assert(selected.rdd ne original.rdd)
        assert(statistics.shuffleId == selected.shuffleId)
        completedJobs.awaitCompletion(action.jobIds)
        assert(!completedJobs.hasFailed(action.jobIds.last))
        assert(query.collect().length == 1)
      } finally {
        jobStart.release.countDown()
        paused.release.countDown()
        active.foreach(_.cancel())
        sc.setLocalProperties(previousProperties)
        manager.pausedMapCompletion = None
        manager.remoteFailureAfterLocalMap = None
        sc.removeSparkListener(jobStart)
        sc.removeSparkListener(completedJobs)
        manager.remoteAttempts.asScala.drop(remoteBefore).foreach { case (attempt, _) =>
          manager.unregisterShuffle(attempt.shuffleId)
        }
        manager.localAttempts.asScala.drop(localBefore).foreach { attempt =>
          manager.unregisterShuffle(attempt.shuffleId)
        }
      }
    }
  }

  test("job-group cancellation while registering fallback does not submit a replacement job") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      val sc = spark.sparkContext
      val previousProperties = sc.getLocalProperties.clone().asInstanceOf[Properties]
      val groupId = "comet-materialization-cancel-during-local-registration"
      val remoteBefore = manager.remoteAttempts.size()
      val localBefore = manager.localAttempts.size()
      val registration = new PausedLocalShuffleRegistration
      val completedJobs = new CompletedMaterializationJobs
      manager.pausedLocalRegistration = Some(registration)
      // The materialization holds its lock during registration, so its own listener can block.
      // An independent queue lets the test observe Spark's cancellation before releasing it.
      sc.listenerBus.addToQueue(completedJobs, groupId)
      var active: Option[FutureAction[MapOutputStatistics]] = None
      try {
        sc.setJobGroup(groupId, "Cancel while local shuffle registration is paused", true)
        val query = spark
          .range(0, 1, 1, 1)
          .selectExpr("CAST(id AS INT) AS key", "repeat('x', 1048576) AS payload")
          .repartition(2, $"key")
        val exchange = collect(query.queryExecution.executedPlan) {
          case value: CometShuffleExchangeExec => value
        }.headOption.getOrElse(fail("Expected a native Comet shuffle exchange"))
        val original = exchange.shuffleDependency.asInstanceOf[CometShuffleDependency[_, _, _]]
        val action = exchange.mapOutputStatisticsFuture
          .asInstanceOf[FutureAction[MapOutputStatistics]]
        active = Some(action)
        assert(registration.entered.await(20, TimeUnit.SECONDS))
        val remoteJobId = completedJobs.awaitJobInGroup(groupId)

        // Fallback has won, but submitMapStage(local) has not happened. Spark's ordinary
        // cancelJobGroup only cancels current jobs, so a later replacement must be fenced.
        sc.cancelJobGroup(groupId)
        completedJobs.awaitCompletion(Seq(remoteJobId))
        assert(completedJobs.hasFailed(remoteJobId))
        registration.release.countDown()
        assert(registration.returned.await(20, TimeUnit.SECONDS))
        Await.ready(action, 20.seconds)

        assert(action.value.exists(_.isFailure))
        assert(original.materialization.get.completedDependency.isEmpty)
        assert(action.jobIds == Seq(remoteJobId))
        assert(manager.localAttempts.size() == localBefore)
        assert(manager.remoteAttempts.size() == remoteBefore + 1)
      } finally {
        registration.release.countDown()
        active.foreach(_.cancel())
        sc.setLocalProperties(previousProperties)
        manager.pausedLocalRegistration = None
        sc.removeSparkListener(completedJobs)
        manager.remoteAttempts.asScala.drop(remoteBefore).foreach { case (attempt, _) =>
          manager.unregisterShuffle(attempt.shuffleId)
        }
        if (registration.shuffleId.get() >= 0) {
          manager.unregisterShuffle(registration.shuffleId.get())
        }
      }
    }
  }

  for (cancelLocal <- Seq(false, true)) {
    test(s"cancelling materialization ends every submitted job: local=$cancelLocal") {
      withSQLConf("spark.sql.adaptive.enabled" -> "false") {
        val remoteBefore = manager.remoteAttempts.size()
        val localBefore = manager.localAttempts.size()
        val paused = new PausedShuffleMapCompletion(cancelLocal)
        val completedJobs = new CompletedMaterializationJobs
        manager.pausedMapCompletion = Some(paused)
        spark.sparkContext.addSparkListener(completedJobs)
        var active: Option[FutureAction[MapOutputStatistics]] = None
        try {
          val payload = if (cancelLocal) "repeat('x', 1048576)" else "'small'"
          val query = spark
            .range(0, 1, 1, 1)
            .selectExpr("CAST(id AS INT) AS key", s"$payload AS payload")
            .repartition(2, $"key")
          val exchange = collect(query.queryExecution.executedPlan) {
            case value: CometShuffleExchangeExec => value
          }.headOption.getOrElse(fail("Expected a native Comet shuffle exchange"))
          val original = exchange.shuffleDependency.asInstanceOf[CometShuffleDependency[_, _, _]]
          val action = exchange.mapOutputStatisticsFuture
            .asInstanceOf[FutureAction[MapOutputStatistics]]
          active = Some(action)
          assert(paused.stopped.await(20, TimeUnit.SECONDS))

          // stop(true) has already produced a real MapStatus, but Spark has not received it.
          // Cancelling here must prevent that delayed completion from publishing a dependency.
          if (cancelLocal) {
            original.materialization.get.cancel(Some("Cancelled during local materialization"))
          } else {
            action.cancel()
          }
          paused.release.countDown()
          assert(paused.returned.await(20, TimeUnit.SECONDS))
          Await.ready(action, 20.seconds)
          completedJobs.awaitCompletion(action.jobIds)

          assert(action.isCancelled)
          assert(action.value.exists(_.isFailure))
          if (cancelLocal) {
            assert(
              action.value.get.failed.get.getMessage == "Cancelled during local materialization")
          }
          assert(original.materialization.get.completedDependency.isEmpty)
          assert(action.jobIds.size == (if (cancelLocal) 2 else 1))
          val local = manager.localAttempts.asScala.drop(localBefore).toSeq
          assert(local.size == (if (cancelLocal) 1 else 0))
          assert(local.forall(_.shuffleId != original.shuffleId))
          val remote = manager.remoteAttempts.asScala.drop(remoteBefore).toSeq
          assert(remote.size == 1)
        } finally {
          paused.release.countDown()
          active.foreach(_.cancel())
          manager.pausedMapCompletion = None
          spark.sparkContext.removeSparkListener(completedJobs)
          manager.remoteAttempts.asScala.drop(remoteBefore).foreach { case (attempt, _) =>
            manager.unregisterShuffle(attempt.shuffleId)
          }
          manager.localAttempts.asScala.drop(localBefore).foreach { attempt =>
            manager.unregisterShuffle(attempt.shuffleId)
          }
        }
      }
    }
  }

}

private[shuffle] case class CometShuffleFallbackAttempt(
    shuffleId: Int,
    stageId: Int,
    stageAttempt: Int,
    partitionId: Int)

/** Synchronizes one old remote map's validated completion with the new local stage. */
private[shuffle] class DelayedRemoteMapCompletion {
  val remoteCommitted = new CountDownLatch(1)
  val localStageStarted = new CountDownLatch(1)
  val remoteReturned = new CountDownLatch(1)

  def awaitRemoteCommit(): Unit =
    require(
      remoteCommitted.await(20, TimeUnit.SECONDS),
      "The small remote map must commit before the oversized map starts")

  def awaitLocalStage(): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
    var interrupted = false
    var started = false
    try {
      while (!started && System.nanoTime() < deadline) {
        try {
          started = localStageStarted.await(
            math.max(1L, deadline - System.nanoTime()),
            TimeUnit.NANOSECONDS)
        } catch {
          case _: InterruptedException => interrupted = true
        }
      }
      require(started, "A fresh local stage must start before the old map result is returned")
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt()
      }
    }
  }
}

/** Holds one already computed map result so cancellation races with a concrete completion. */
private[shuffle] class PausedShuffleMapCompletion(val local: Boolean) {
  val stopped = new CountDownLatch(1)
  val release = new CountDownLatch(1)
  val returned = new CountDownLatch(1)

  def awaitRelease(): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
    var interrupted = false
    var released = false
    try {
      while (!released && System.nanoTime() < deadline) {
        try {
          released =
            release.await(math.max(1L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
        } catch {
          case _: InterruptedException => interrupted = true
        }
      }
      require(released, "The test must release the completed map")
    } finally {
      if (interrupted) Thread.currentThread().interrupt()
    }
  }
}

/** Holds listener delivery while Spark processes the old remote stage's failure. */
private[shuffle] class PausedReplacementJobStart(groupId: String) extends SparkListener {
  val entered = new CountDownLatch(1)
  val release = new CountDownLatch(1)
  private var startedJobs = 0

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    if (Option(event.properties).exists(_.getProperty("spark.jobGroup.id") == groupId)) {
      startedJobs += 1
      if (startedJobs == 2) {
        entered.countDown()
        require(release.await(20, TimeUnit.SECONDS), "The test must release local JobStart")
      }
    }
  }
}

/** Holds fallback registration before the materialization can submit its local map stage. */
private[shuffle] class PausedLocalShuffleRegistration {
  val shuffleId = new AtomicInteger(-1)
  val entered = new CountDownLatch(1)
  val release = new CountDownLatch(1)
  val returned = new CountDownLatch(1)
}

/** Waits for scheduler-confirmed job completion, including jobs cancelled during fallback. */
private[shuffle] class CompletedMaterializationJobs extends SparkListener {
  private val completed = mutable.Set.empty[Int]
  private val failed = mutable.Set.empty[Int]
  private val jobsByGroup = mutable.Map.empty[String, Int]

  override def onJobStart(event: SparkListenerJobStart): Unit = synchronized {
    Option(event.properties)
      .flatMap(properties => Option(properties.getProperty("spark.jobGroup.id")))
      .foreach(groupId => jobsByGroup(groupId) = event.jobId)
    notifyAll()
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = synchronized {
    completed += event.jobId
    if (event.jobResult.isInstanceOf[JobFailed]) failed += event.jobId
    notifyAll()
  }

  def hasFailed(jobId: Int): Boolean = synchronized { failed.contains(jobId) }

  def awaitJobInGroup(groupId: String): Int = synchronized {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
    while (!jobsByGroup.contains(groupId) && System.nanoTime() < deadline) {
      wait(math.max(1L, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())))
    }
    require(jobsByGroup.contains(groupId), s"No materialization job started in group $groupId")
    jobsByGroup(groupId)
  }

  def awaitCompletion(jobIds: Seq[Int]): Unit = synchronized {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
    while (!jobIds.forall(completed.contains) && System.nanoTime() < deadline) {
      wait(math.max(1L, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())))
    }
    require(jobIds.forall(completed.contains), s"Materialization jobs still running: $jobIds")
  }
}

class CometCelebornFallbackTestShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends CometCelebornShuffleManager(
      conf,
      isDriver,
      (configuration, _) => new CometCelebornFallbackTestBackend(configuration),
      planningSupportFactory = _ => CelebornNativeShufflePlanningSupport()) {

  private[shuffle] val remoteAttempts =
    new ConcurrentLinkedQueue[(CometShuffleFallbackAttempt, CometCelebornFallbackTestClient)]()
  private[shuffle] val localAttempts = new ConcurrentLinkedQueue[CometShuffleFallbackAttempt]()
  val nativeReads = new AtomicInteger()
  val remoteMapsBeforeFailure = new AtomicInteger()
  @volatile var waitForFirstRemoteMap = false
  @volatile private[shuffle] var delayedRemoteCompletion: Option[DelayedRemoteMapCompletion] =
    None
  @volatile private[shuffle] var pausedMapCompletion: Option[PausedShuffleMapCompletion] = None
  @volatile private[shuffle] var remoteFailureAfterLocalMap: Option[PausedShuffleMapCompletion] =
    None
  @volatile private[shuffle] var pausedLocalRegistration: Option[PausedLocalShuffleRegistration] =
    None

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val registration = dependency match {
      case native: CometShuffleDependency[_, _, _] if native.useLocalShuffle =>
        pausedLocalRegistration
      case _ => None
    }
    registration.foreach { paused =>
      paused.shuffleId.set(shuffleId)
      paused.entered.countDown()
      require(
        paused.release.await(20, TimeUnit.SECONDS),
        "The test must release local shuffle registration")
    }
    try super.registerShuffle(shuffleId, dependency)
    finally registration.foreach(_.returned.countDown())
  }

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
    remoteAttempts.add((recordAttempt(handle, context), client))
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
    val delayed = delayedRemoteCompletion
    if (handle.isInstanceOf[CelebornShuffleHandle[_, _, _]] && context.partitionId() == 1) {
      delayed.foreach(_.awaitRemoteCommit())
    }
    val underlying = super.getWriter[K, V](handle, mapId, context, metrics)
    val failureGate = remoteFailureAfterLocalMap.filter { _ =>
      handle.isInstanceOf[CelebornShuffleHandle[_, _, _]]
    }
    val writer = if (failureGate.nonEmpty) {
      new ShuffleWriter[K, V] {
        override def write(records: Iterator[Product2[K, V]]): Unit = {
          try underlying.write(records)
          catch {
            case failure if CometNativeShuffleWriter.isSizeLimitFailure(failure) =>
              // The replacement must be active when Spark processes the abandoned stage's
              // exception; otherwise the shared-RDD bug depends on scheduler timing.
              require(failureGate.get.stopped.await(20, TimeUnit.SECONDS))
              throw failure
          }
        }

        override def getPartitionLengths(): Array[Long] = underlying.getPartitionLengths()

        override def stop(success: Boolean): Option[MapStatus] = underlying.stop(success)
      }
    } else {
      underlying
    }
    if (handle.isInstanceOf[CometNativeShuffleHandle[_, _]]) {
      localAttempts.add(recordAttempt(handle, context))
      delayed.foreach(_.localStageStarted.countDown())
    }
    val paused = pausedMapCompletion.filter { completion =>
      completion.local == handle.isInstanceOf[CometNativeShuffleHandle[_, _]]
    }
    if (paused.nonEmpty) {
      new ShuffleWriter[K, V] {
        override def write(records: Iterator[Product2[K, V]]): Unit = writer.write(records)

        override def getPartitionLengths(): Array[Long] = writer.getPartitionLengths()

        override def stop(success: Boolean): Option[MapStatus] = {
          val result = writer.stop(success)
          if (success && result.nonEmpty) {
            paused.get.stopped.countDown()
            paused.get.awaitRelease()
            paused.get.returned.countDown()
          }
          result
        }
      }
    } else if (handle
        .isInstanceOf[CelebornShuffleHandle[_, _, _]] && context.partitionId() == 0 &&
      delayed.nonEmpty) {
      new ShuffleWriter[K, V] {
        override def write(records: Iterator[Product2[K, V]]): Unit = writer.write(records)

        override def getPartitionLengths(): Array[Long] = writer.getPartitionLengths()

        override def stop(success: Boolean): Option[MapStatus] = {
          val result = writer.stop(success)
          if (success && result.nonEmpty) {
            delayed.get.remoteCommitted.countDown()
            // Keep the already validated result until local writers have started. Cancellation
            // may interrupt this worker, but must not let this old result replace local output.
            delayed.get.awaitLocalStage()
            delayed.get.remoteReturned.countDown()
          }
          result
        }
      }
    } else {
      writer
    }
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
    if (handle.isInstanceOf[CometNativeShuffleHandle[_, _]]) {
      nativeReads.incrementAndGet()
    }
    reader
  }

  private def recordAttempt(
      handle: ShuffleHandle,
      context: TaskContext): CometShuffleFallbackAttempt =
    CometShuffleFallbackAttempt(
      handle.shuffleId,
      context.stageId(),
      context.stageAttemptNumber(),
      context.partitionId())
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
