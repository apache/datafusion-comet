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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.{CometTestBase, DataFrame, SparkSession}
import org.apache.spark.sql.comet.shims.ShimCometShuffleMaterialization
import org.apache.spark.util.ThreadUtils

import org.apache.comet.CometConf

class CometCelebornConcurrentMaterializationSuite extends CometTestBase {

  import testImplicits._

  override protected val shuffleManager: String =
    classOf[CometCelebornConcurrentMaterializationTestManager].getName

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SHUFFLE_MODE.key, "native")
      .set(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key, "false")
      .set(CometConf.COMET_SHUFFLE_RSS_MAX_FRAME_BYTES.key, "32k")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.shuffle.compress", "false")
      .set("spark.io.encryption.enabled", "false")
      .set("spark.celeborn.client.spark.stageRerun.enabled", "true")

  private def manager: CometCelebornConcurrentMaterializationTestManager =
    SparkEnv.get.shuffleManager.asInstanceOf[CometCelebornConcurrentMaterializationTestManager]

  private def branch(start: Long, oversized: Boolean): DataFrame = {
    // Keep the large value dependent on the input, so Spark cannot replace the inner shuffle's
    // payload with a small key and reconstruct a folded string above that exchange.
    val payload = if (oversized) "concat(CAST(id AS STRING), repeat('x', 1048576))" else "'small'"
    spark
      .range(start, start + 1, 1, 1)
      .selectExpr("id", s"$payload AS payload")
      .repartition(2, $"id")
  }

  test("materialization restores the captured session without changing the worker's session") {
    class CapturedSession extends ShimCometShuffleMaterialization {
      def current: Option[SparkSession] = withCapturedSession(SparkSession.getActiveSession)
    }
    val captured = spark.withActive { new CapturedSession }
    val worker = Future {
      val previous = SparkSession.getActiveSession
      SparkSession.clearActiveSession()
      try {
        assert(captured.current.contains(spark))
        assert(SparkSession.getActiveSession.isEmpty)
      } finally {
        previous.foreach(SparkSession.setActiveSession)
      }
    }(ExecutionContext.global)
    Await.result(worker, 20.seconds)
  }

  test("non-AQE sibling shuffles start before either remote materialization finishes") {
    val gate = new ConcurrentRemoteMapStarts
    manager.remoteMapStarts = Some(gate)
    val query = branch(0, oversized = false).union(branch(100, oversized = false))
    val plan = query.queryExecution.executedPlan
    val exchanges = collect(plan) { case exchange: CometShuffleExchangeExec => exchange }
    assert(exchanges.size == 2)
    val localBefore = manager.localAttempts.size()
    val construction = Future(exchanges.map(_.executeColumnar()))(ExecutionContext.global)
    try {
      // Both maps remain blocked. Reaching this point proves independent branches overlap,
      // without relying on elapsed task timings or allowing one branch to finish first.
      assert(gate.bothStarted.await(20, TimeUnit.SECONDS))
      Await.result(construction, 20.seconds)
      assert(gate.shuffleCount == 2)
      gate.release.countDown()
      exchanges.foreach { exchange =>
        val original = exchange.shuffleDependency.asInstanceOf[CometShuffleDependency[_, _, _]]
        val materialization = original.materialization.get
        val statistics = Await.result(materialization, 20.seconds)
        assert(statistics.shuffleId == original.shuffleId)
        assert(materialization.completedDependency.exists(_ eq original))
      }
      assert(manager.localAttempts.size() == localBefore)
    } finally {
      gate.release.countDown()
      Await.ready(construction, 20.seconds)
      manager.remoteMapStarts = None
      exchanges.foreach { exchange =>
        val dependency = exchange.shuffleDependency.asInstanceOf[CometShuffleDependency[_, _, _]]
        dependency.materialization.foreach(_.cancel())
        manager.unregisterShuffle(dependency.shuffleId)
      }
    }
  }

  test("cancelling while an upstream shuffle materializes prevents downstream job admission") {
    val gate = new ConcurrentRemoteMapStarts
    manager.remoteMapStarts = Some(gate)
    val remoteBefore = manager.remoteAttempts.size()
    val localBefore = manager.localAttempts.size()
    val query = branch(0, oversized = true)
      .selectExpr("id + 10 AS id", "payload")
      .repartition(3, $"id")
    val exchanges = collect(query.queryExecution.executedPlan) {
      case exchange: CometShuffleExchangeExec => exchange
    }
    assert(exchanges.size == 2)
    val outer = exchanges.find(_.outputPartitioning.numPartitions == 3).get
    val construction = Future(outer.executeColumnar())(ExecutionContext.global)
    try {
      assert(gate.firstStarted.await(20, TimeUnit.SECONDS))
      Await.result(construction, 20.seconds)
      val dependency = outer.shuffleDependency.asInstanceOf[CometShuffleDependency[_, _, _]]
      val materialization = dependency.materialization.get
      assert(!materialization.isCompleted)
      assert(materialization.jobIds.isEmpty)

      // The outer worker is waiting for the blocked inner shuffle. It must not hold the state
      // lock during that wait, and cancellation must fence its not-yet-submitted map-stage job.
      val cancellation = Future(materialization.cancel(Some("Cancelled before job admission")))(
        ExecutionContext.global)
      Await.result(cancellation, 5.seconds)
      assert(materialization.isCancelled)
      assert(
        materialization.value.exists(_.failed.get.getMessage ==
          "Cancelled before job admission"))
      gate.release.countDown()
      exchanges.filterNot(_ eq outer).foreach { inner =>
        val original = inner.shuffleDependency.asInstanceOf[CometShuffleDependency[_, _, _]]
        Await.result(original.materialization.get, 20.seconds)
      }
      assert(materialization.jobIds.isEmpty)
    } finally {
      gate.release.countDown()
      Await.ready(construction, 20.seconds)
      manager.remoteMapStarts = None
      exchanges.foreach { exchange =>
        exchange.shuffleDependency
          .asInstanceOf[CometShuffleDependency[_, _, _]]
          .materialization
          .foreach(_.cancel())
      }
      manager.remoteAttempts.asScala.drop(remoteBefore).foreach { case (attempt, _) =>
        manager.unregisterShuffle(attempt.shuffleId)
      }
      manager.localAttempts.asScala.drop(localBefore).foreach { attempt =>
        manager.unregisterShuffle(attempt.shuffleId)
      }
    }
  }

  test("shared upstream materialization does not exhaust a bounded execution context") {
    val workers = ThreadUtils.newDaemonFixedThreadPool(2, "comet-materialization-test")
    val executionContext = ExecutionContext.fromExecutorService(workers)
    val gate = new ConcurrentRemoteMapStarts
    val remoteBefore = manager.remoteAttempts.size()
    val localBefore = manager.localAttempts.size()
    val branchCount = 6
    val shared = branch(0, oversized = true)
    val query = (1 to branchCount)
      .map { index =>
        shared.selectExpr(s"id + $index AS id", "payload").repartition(3, $"id")
      }
      .reduce(_.union(_))
    val exchanges = collect(query.queryExecution.executedPlan) {
      case exchange: CometShuffleExchangeExec => exchange
    }
    val materializations =
      mutable.ArrayBuffer.empty[CometCelebornShuffleMaterialization[_, _, _]]
    val completedJobs = new CompletedMaterializationJobs
    spark.sparkContext.addSparkListener(completedJobs)
    manager.testMaterializationExecutionContext = Some(executionContext)
    manager.remoteMapStarts = Some(gate)
    try {
      // Exchange reuse gives every sibling the same unfinished upstream destination. There are
      // more dependents than workers, so blocking even a small bounded pool would starve the
      // upstream completion callback as well as unrelated work scheduled on that pool.
      assert(exchanges.size == branchCount + 1)
      exchanges.foreach { exchange =>
        exchange.executeColumnar()
        materializations += exchange.shuffleDependency
          .asInstanceOf[CometShuffleDependency[_, _, _]]
          .materialization
          .get
      }
      assert(gate.firstStarted.await(20, TimeUnit.SECONDS))
      assert(gate.shuffleCount == 1)
      assert(materializations.forall(!_.isCompleted))
      Await.result(Future(())(executionContext), 5.seconds)

      gate.release.countDown()
      materializations.foreach { materialization =>
        Await.result(materialization, 30.seconds)
        assert(materialization.completedDependency.exists(_.useLocalShuffle))
        assert(materialization.jobIds.size == 2)
      }
      val actual = query.collect().map(row => (row.getLong(0), row.getString(1))).toSeq
      val expected = (1 to branchCount).map(index => (index.toLong, "0" + "x" * 1048576))
      assert(actual.sortBy(_._1) == expected)
    } finally {
      gate.release.countDown()
      materializations.foreach(_.cancel())
      completedJobs.awaitCompletion(materializations.flatMap(_.jobIds).toSeq)
      Await.result(Future(())(executionContext), 20.seconds)
      spark.sparkContext.removeSparkListener(completedJobs)
      manager.remoteMapStarts = None
      manager.testMaterializationExecutionContext = None
      executionContext.shutdown()
      assert(executionContext.awaitTermination(20, TimeUnit.SECONDS))
      manager.remoteAttempts.asScala.drop(remoteBefore).foreach { case (attempt, _) =>
        manager.unregisterShuffle(attempt.shuffleId)
      }
      manager.localAttempts.asScala.drop(localBefore).foreach { attempt =>
        manager.unregisterShuffle(attempt.shuffleId)
      }
    }
  }

  for (nested <- Seq(false, true)) {
    test(s"non-AQE fallback preserves concurrent branches and downstream reads: nested=$nested") {
      val gate = new ConcurrentRemoteMapStarts
      manager.remoteMapStarts = Some(gate)
      val remoteBefore = manager.remoteAttempts.size()
      val localBefore = manager.localAttempts.size()
      val left = if (nested) {
        // The changed partition key keeps a second shuffle above the first one. Submitting its
        // materialization must wait for the first destination without blocking the right branch.
        branch(0, oversized = true)
          .selectExpr("id + 10 AS id", "payload")
          .repartition(3, $"id")
      } else {
        branch(0, oversized = true)
      }
      val query = left.union(branch(100, oversized = true))
      val exchanges = collect(query.queryExecution.executedPlan) {
        case exchange: CometShuffleExchangeExec => exchange
      }
      assert(exchanges.size == (if (nested) 3 else 2))
      assert(exchanges.forall(_.output.exists(_.name == "payload")))
      val execution = Future(query.collect())(ExecutionContext.global)
      try {
        assert(gate.bothStarted.await(20, TimeUnit.SECONDS))
        assert(gate.shuffleCount == 2)
        gate.release.countDown()
        val actual =
          Await.result(execution, 30.seconds).map(row => (row.getLong(0), row.getString(1))).toSeq
        val leftId = if (nested) 10L else 0L
        assert(
          actual
            .sortBy(_._1) == Seq((leftId, "0" + "x" * 1048576), (100L, "100" + "x" * 1048576)))
        assert(manager.localAttempts.size() > localBefore)
        exchanges.foreach { exchange =>
          val dependency =
            exchange.shuffleDependency.asInstanceOf[CometShuffleDependency[_, _, _]]
          assert(dependency.materialization.get.completedDependency.exists(_.useLocalShuffle))
        }
      } finally {
        gate.release.countDown()
        Await.ready(execution, 30.seconds)
        manager.remoteMapStarts = None
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

/** Holds two distinct remote map stages so the tests can prove their execution overlaps. */
private[shuffle] class ConcurrentRemoteMapStarts {
  val firstStarted = new CountDownLatch(1)
  val bothStarted = new CountDownLatch(2)
  val release = new CountDownLatch(1)
  private val shuffleIds = mutable.Set.empty[Int]

  def shuffleCount: Int = synchronized { shuffleIds.size }

  def mapStarted(shuffleId: Int): Unit = {
    synchronized {
      if (shuffleIds.add(shuffleId)) {
        firstStarted.countDown()
        bothStarted.countDown()
      }
    }
    require(release.await(20, TimeUnit.SECONDS), "The test must release concurrent remote maps")
  }
}

class CometCelebornConcurrentMaterializationTestManager(conf: SparkConf, isDriver: Boolean)
    extends CometCelebornFallbackTestShuffleManager(conf, isDriver) {

  @volatile private[shuffle] var remoteMapStarts: Option[ConcurrentRemoteMapStarts] = None
  @volatile private[shuffle] var testMaterializationExecutionContext: Option[ExecutionContext] =
    None

  override protected[shuffle] def materializationExecutionContext: ExecutionContext =
    testMaterializationExecutionContext.getOrElse(super.materializationExecutionContext)

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    if (handle.isInstanceOf[CelebornShuffleHandle[_, _, _]]) {
      remoteMapStarts.foreach(_.mapStarted(handle.shuffleId))
    }
    super.getWriter(handle, mapId, context, metrics)
  }
}
