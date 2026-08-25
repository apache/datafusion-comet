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

package org.apache.comet.shuffle

import java.io.IOException
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference, LongAdder}

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.shuffle.ShuffleHandle

import org.apache.comet.{CometConf, CometExecIterator}

/** Matches the pinned Celeborn completion hook without adding its optional client dependency. */
trait RecordingCelebornPushMetricsCallback {
  def incPushDataCount(count: Long): Unit
  def incPushDataRetryCount(count: Long): Unit
  def incPushDataTime(time: Long): Unit
  def incInFlightWaitTime(time: Long): Unit
}

/** Public so the production adapter can invoke its method through ordinary Java reflection. */
final class RecordingCelebornShuffleClient {

  @volatile var acceptedBytes: Option[Int] = None
  @volatile var failure: Throwable = _
  @volatile var mapperEndFailure: Throwable = _
  @volatile var cleanupFailure: Throwable = _
  @volatile var observedTaskContext: TaskContext = _
  @volatile var lastPush: RecordedCelebornPush = _
  @volatile var lastMapperEnd: (Int, Int, Int, Int) = _
  @volatile var lastCleanup: (Int, Int, Int) = _
  @volatile var pushStarted: CountDownLatch = _
  @volatile var allowPush: CountDownLatch = _
  @volatile var pushStateCaptured: CountDownLatch = _
  @volatile var allowPushStateCapture: CountDownLatch = _
  @volatile var pushRegistered: CountDownLatch = _
  @volatile var allowPushRegistration: CountDownLatch = _
  @volatile var pushCompletedBeforeReturn: CountDownLatch = _
  @volatile var allowPushReturn: CountDownLatch = _
  @volatile var mapperEndStarted: CountDownLatch = _
  @volatile var allowMapperEnd: CountDownLatch = _
  @volatile var drainStarted: CountDownLatch = _
  @volatile var allowDrain: CountDownLatch = _
  @volatile var drainFailure: Throwable = _
  @volatile var drainTimedOut = false
  @volatile var automaticallyCompletePushes = true
  @volatile var automaticallyCompleteLatestPush = false
  @volatile var silentlyCompletePushes = false
  @volatile var metricsFailureWithoutBatchRemoval = false
  @volatile var completionRemoved: CountDownLatch = _
  @volatile var allowCompletionMetrics: CountDownLatch = _
  @volatile var cleanupUnblocksPush = false
  @volatile var recreatePushStateAfterCleanup = false
  @volatile var cleanupUnblocksMapperEnd = false
  @volatile var cleanupUnblocksDrain = false
  @volatile var retainedPushState = false
  @volatile var observedMapKey: String = _
  @volatile var generationInvalidated = true
  @volatile var lastInvalidatedShuffle: (Int, Int, Long) = _
  val pushCalls = new AtomicInteger()
  val pushStateCreations = new AtomicInteger()
  val pushCompletionCalls = new AtomicInteger()
  val mapperEndCalls = new AtomicInteger()
  val cleanupCalls = new AtomicInteger()
  private val pushStates = new ConcurrentHashMap[String, RecordingCelebornPushState]()
  private val pendingPushCompletions = new ConcurrentLinkedDeque[RecordingCelebornPushState]()

  def getPushState(mapKey: String): RecordingCelebornPushState = {
    observedMapKey = mapKey
    retainedPushState = true
    pushStates.computeIfAbsent(
      mapKey,
      _ => {
        pushStateCreations.incrementAndGet()
        new RecordingCelebornPushState(this)
      })
  }

  def completeNextPush(): Boolean = {
    completePush(pendingPushCompletions.pollFirst())
  }

  def completeLatestPush(): Boolean = {
    completePush(pendingPushCompletions.pollLast())
  }

  private def completePush(pushState: RecordingCelebornPushState): Boolean = {
    if (pushState == null) {
      false
    } else {
      pushCompletionCalls.incrementAndGet()
      if (metricsFailureWithoutBatchRemoval) {
        pushState.failPushWithoutRemovingBatch()
      } else {
        pushState.completePush()
      }
      true
    }
  }

  def reportShuffleFetchFailure(
      sparkShuffleId: Int,
      celebornShuffleId: Int,
      taskAttemptId: Long): Boolean = {
    lastInvalidatedShuffle = (sparkShuffleId, celebornShuffleId, taskAttemptId)
    generationInvalidated
  }

  @throws[IOException]
  def pushOrMergeData(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int,
      doPush: Boolean,
      skipCompress: Boolean): Int = {
    pushCalls.incrementAndGet()
    observedTaskContext = TaskContext.get()
    lastPush = RecordedCelebornPush(
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      bytes,
      offset,
      length,
      numMappers,
      numPartitions,
      doPush,
      skipCompress)

    if (pushStarted != null) {
      pushStarted.countDown()
      if (!allowPush.await(5, TimeUnit.SECONDS)) {
        throw new IOException("timed out waiting for the test push")
      }
    }

    if (failure != null) {
      throw failure
    }

    if (recreatePushStateAfterCleanup && cleanupCalls.get() > 0) {
      retainedPushState = true
    }

    val pushState = getPushState(s"$shuffleId-$mapId-$attemptId")
    pushState.addPush()
    if (pushStateCaptured != null) {
      pushStateCaptured.countDown()
      if (!allowPushStateCapture.await(5, TimeUnit.SECONDS)) {
        throw new IOException("timed out waiting for the captured test push state")
      }
    }
    pendingPushCompletions.add(pushState)
    if (pushRegistered != null) {
      pushRegistered.countDown()
      if (!allowPushRegistration.await(5, TimeUnit.SECONDS)) {
        throw new IOException("timed out waiting for the registered test push")
      }
    }
    if (automaticallyCompleteLatestPush) {
      completeLatestPush()
    } else if (automaticallyCompletePushes) {
      completeNextPush()
    }

    if (pushCompletedBeforeReturn != null) {
      pushCompletedBeforeReturn.countDown()
      if (!allowPushReturn.await(5, TimeUnit.SECONDS)) {
        throw new IOException("timed out waiting for the completed test push to return")
      }
    }

    acceptedBytes.getOrElse(length + 16)
  }

  @throws[IOException]
  def mapperEnd(shuffleId: Int, mapId: Int, attemptId: Int, numMappers: Int): Unit = {
    lastMapperEnd = (shuffleId, mapId, attemptId, numMappers)
    mapperEndCalls.incrementAndGet()
    if (getPushState(s"$shuffleId-$mapId-$attemptId").limitZeroInFlight()) {
      throw new IOException("Timed out waiting for the accepted Celeborn shuffle push")
    }
    if (mapperEndStarted != null) {
      mapperEndStarted.countDown()
      if (!allowMapperEnd.await(5, TimeUnit.SECONDS)) {
        throw new IOException("timed out waiting for test map completion")
      }
    }
    if (mapperEndFailure != null) {
      throw mapperEndFailure
    }
  }

  def cleanup(shuffleId: Int, mapId: Int, attemptId: Int): Unit = {
    lastCleanup = (shuffleId, mapId, attemptId)
    cleanupCalls.incrementAndGet()
    retainedPushState = false
    pushStates.remove(s"$shuffleId-$mapId-$attemptId")
    if (cleanupUnblocksPush) {
      failure = new IOException("Celeborn map attempt was cleaned up")
      allowPush.countDown()
    }
    if (recreatePushStateAfterCleanup) {
      allowPush.countDown()
    }
    if (cleanupUnblocksMapperEnd) {
      mapperEndFailure = new IOException("Celeborn map completion was cleaned up")
      allowMapperEnd.countDown()
    }
    if (cleanupUnblocksDrain) {
      drainFailure = new IOException("Celeborn pending push was cleaned up")
      allowDrain.countDown()
    }
    if (cleanupFailure != null) {
      throw cleanupFailure
    }
  }
}

/** Public so production reflection can wait for the fake client's accepted network request. */
final class RecordingCelebornInFlightRequestTracker {
  private val totalInflightReqs = new LongAdder()

  def addBatch(): Unit = totalInflightReqs.increment()

  def removeBatch(): Unit = totalInflightReqs.decrement()
}

final class RecordingCelebornPushState(client: RecordingCelebornShuffleClient) {
  private val inFlightRequestTracker = new RecordingCelebornInFlightRequestTracker()
  private val exception = new AtomicReference[IOException]()
  @volatile private var metricsCallback: RecordingCelebornPushMetricsCallback = _

  def setMetricsCallback(callback: RecordingCelebornPushMetricsCallback): Unit = {
    metricsCallback = callback
  }

  def addPush(): Unit = inFlightRequestTracker.addBatch()

  def completePush(): Unit = {
    inFlightRequestTracker.removeBatch()
    if (client.completionRemoved != null) {
      client.completionRemoved.countDown()
      if (!client.allowCompletionMetrics.await(5, TimeUnit.SECONDS)) {
        throw new IOException("timed out waiting to report the completed test push")
      }
    }
    if (!client.silentlyCompletePushes) {
      onSuccess()
    }
  }

  def failPushWithoutRemovingBatch(): Unit = onFailure()

  private def onSuccess(): Unit = {
    Option(metricsCallback).foreach(_.incPushDataCount(1))
  }

  private def onFailure(): Unit = {
    Option(metricsCallback).foreach(_.incPushDataCount(1))
    exception.compareAndSet(null, new IOException("the test Celeborn push failed"))
  }

  @throws[IOException]
  def limitZeroInFlight(): Boolean = {
    if (client.drainStarted != null) {
      client.drainStarted.countDown()
      if (!client.allowDrain.await(5, TimeUnit.SECONDS)) {
        throw new IOException("timed out waiting for the test request to complete")
      }
    }
    if (client.drainFailure != null) {
      throw client.drainFailure
    }
    client.drainTimedOut
  }
}

/** Mirrors Apache Celeborn's public client without a push-completion metrics callback. */
final class StockCelebornShuffleClient {
  @volatile var automaticallyCompletePushes = true
  @volatile var lastMapperEnd: (Int, Int, Int, Int, Int) = _
  val pushCalls = new AtomicInteger()
  private val pushStates = new ConcurrentHashMap[String, StockCelebornPushState]()
  private val pendingPushCompletions = new ConcurrentLinkedDeque[StockCelebornPushState]()

  def getPushState(mapKey: String): StockCelebornPushState =
    pushStates.computeIfAbsent(mapKey, _ => new StockCelebornPushState)

  def pushOrMergeData(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int,
      doPush: Boolean,
      skipCompress: Boolean): Int = {
    pushCalls.incrementAndGet()
    val pushState = getPushState(s"$shuffleId-$mapId-$attemptId")
    pushState.addPush()
    pendingPushCompletions.add(pushState)
    if (automaticallyCompletePushes) completeNextPush()
    length + 16
  }

  def completeNextPush(): Boolean = {
    val pushState = pendingPushCompletions.pollFirst()
    if (pushState == null) false
    else {
      pushState.completePush()
      true
    }
  }

  def failNextPush(): Boolean = {
    val pushState = pendingPushCompletions.pollFirst()
    if (pushState == null) false
    else {
      pushState.failPushWithoutRemovingBatch()
      true
    }
  }

  def mapperEnd(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      numPartitions: Int): Unit = {
    Option(pushStates.get(s"$shuffleId-$mapId-$attemptId")).foreach(_.checkFailure())
    lastMapperEnd = (shuffleId, mapId, attemptId, numMappers, numPartitions)
  }

  def cleanup(shuffleId: Int, mapId: Int, attemptId: Int): Unit = {
    Option(pushStates.remove(s"$shuffleId-$mapId-$attemptId")).foreach(_.cleanup())
  }
}

final class StockCelebornPushState {
  private val inFlightRequestTracker = new RecordingCelebornInFlightRequestTracker()
  private val exception = new AtomicReference[IOException]()

  def addPush(): Unit = inFlightRequestTracker.addBatch()

  def completePush(): Unit = inFlightRequestTracker.removeBatch()

  def failPushWithoutRemovingBatch(): Unit = {
    exception.compareAndSet(null, new IOException("the stock Celeborn push failed"))
  }

  def cleanup(): Unit = {
    exception.compareAndSet(null, new IOException("Cleaned Up"))
  }

  def checkFailure(): Unit = Option(exception.get()).foreach(throw _)
}

final case class RecordedCelebornPush(
    shuffleId: Int,
    mapId: Int,
    attemptId: Int,
    partitionId: Int,
    bytes: Array[Byte],
    offset: Int,
    length: Int,
    numMappers: Int,
    numPartitions: Int,
    doPush: Boolean,
    skipCompress: Boolean)

/** The method exists, but its return type is incompatible with Celeborn's raw-push contract. */
final class WrongReturnTypeCelebornShuffleClient {
  def pushOrMergeData(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int,
      doPush: Boolean,
      skipCompress: Boolean): Long = length.toLong
}

class CelebornShufflePartitionPusherSuite extends AnyFunSuite {

  private val managerKey = "spark.shuffle.manager"
  private val managerClass = "org.apache.spark.shuffle.celeborn.SparkShuffleManager"
  private val compositeManagerClass =
    "org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager"
  private val pluginKey = "spark.shuffle.sort.io.plugin.class"
  private val pluginClass = "org.apache.spark.shuffle.celeborn.CelebornShuffleDataIO"
  private val endpointsKey = "spark.celeborn.master.endpoints"
  private val enabledKey = CometConf.COMET_SHUFFLE_CELEBORN_ENABLED.key

  private def enabledConf: SparkConf =
    new SparkConf(false).set(endpointsKey, "celeborn-master:9097")

  /** Spark scopes empty() to private[spark] in Scala, but its JVM companion method is public. */
  private def emptyTaskContext(): TaskContext =
    TaskContext.getClass.getMethod("empty").invoke(TaskContext).asInstanceOf[TaskContext]

  private def pusher(client: AnyRef): CelebornShufflePartitionPusher =
    new CelebornShufflePartitionPusher(client, 19, 3, (4 << 16) | 7, 12, 9)

  test("raw Celeborn push forwards captured task metadata and preserves Comet frame bytes") {
    val client = new RecordingCelebornShuffleClient
    val bytes = Array[Byte](1, 2, 3, 4)

    assert(pusher(client).pushPartitionData(6, bytes, 3) == 3)

    val push = client.lastPush
    assert(push.shuffleId == 19)
    assert(push.mapId == 3)
    assert(push.attemptId == ((4 << 16) | 7))
    assert(push.partitionId == 6)
    assert(push.bytes eq bytes)
    assert(push.offset == 0)
    assert(push.length == 3)
    assert(push.numMappers == 12)
    assert(push.numPartitions == 9)
    assert(push.doPush)
    assert(push.skipCompress)
    assert(client.observedMapKey == s"19-3-${(4 << 16) | 7}")
  }

  test("stock Apache Celeborn commits with its reducer-aware mapperEnd API") {
    val client = new StockCelebornShuffleClient
    val adapter = new CelebornShufflePartitionPusher(client, 19, 3, 7, 12, 9)

    assert(adapter.pushPartitionData(2, Array[Byte](1, 2, 3), 3) == 3)
    assert(adapter.finish().sameElements(Array[Long](0, 0, 3, 0, 0, 0, 0, 0, 0)))
    assert(client.lastMapperEnd == ((19, 3, 7, 12, 9)))
  }

  test("stock Apache Celeborn restores byte admission without a push metrics callback") {
    val client = new StockCelebornShuffleClient
    client.automaticallyCompletePushes = false
    val first = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 48)
    val second = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 48)
    val failure = new AtomicReference[Throwable]()
    val bytes = Array.fill[Byte](32)(1)

    assert(first.pushPartitionData(0, bytes, bytes.length) == bytes.length)
    val worker = new Thread(() => {
      try second.pushPartitionData(0, bytes, bytes.length)
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()

    Thread.sleep(100)
    assert(worker.isAlive)
    assert(client.pushCalls.get() == 1)

    assert(client.completeNextPush())
    worker.join(5000)

    if (worker.isAlive) {
      second.abort()
      worker.join(5000)
      fail("stock Celeborn transport completion did not restore executor-wide admission")
    }
    assert(failure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.completeNextPush())
  }

  test("stock Apache Celeborn terminal push failures restore shared byte admission once") {
    val client = new StockCelebornShuffleClient
    client.automaticallyCompletePushes = false
    val failed = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 48)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 48)
    val replacementFailure = new AtomicReference[Throwable]()
    val frame = Array.fill[Byte](32)(1)

    assert(failed.pushPartitionData(0, frame, frame.length) == frame.length)
    val worker = new Thread(() => {
      try replacement.pushPartitionData(0, frame, frame.length)
      catch { case failure: Throwable => replacementFailure.set(failure) }
    })
    worker.start()

    Thread.sleep(100)
    assert(worker.isAlive)
    assert(client.failNextPush())
    val failure = intercept[IOException](failed.finish())
    assert(failure.getMessage.contains("stock Celeborn push failed"))
    worker.join(5000)

    if (worker.isAlive) {
      replacement.abort()
      worker.join(5000)
      fail("a stock Celeborn terminal push failure permanently retained byte admission")
    }
    assert(replacementFailure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.completeNextPush())
  }

  test("stock Apache Celeborn cancellation does not release a live transport request") {
    val client = new StockCelebornShuffleClient
    client.automaticallyCompletePushes = false
    val cancelled = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 48)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 48)
    val replacementFailure = new AtomicReference[Throwable]()
    val frame = Array.fill[Byte](32)(1)

    assert(cancelled.pushPartitionData(0, frame, frame.length) == frame.length)
    cancelled.abort()

    val worker = new Thread(() => {
      try replacement.pushPartitionData(0, frame, frame.length)
      catch { case failure: Throwable => replacementFailure.set(failure) }
    })
    worker.start()

    Thread.sleep(100)
    assert(worker.isAlive)
    assert(client.pushCalls.get() == 1)
    assert(client.completeNextPush())
    worker.join(5000)

    if (worker.isAlive) {
      replacement.abort()
      worker.join(5000)
      fail("a completed stock Celeborn request permanently retained byte admission")
    }
    assert(replacementFailure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.completeNextPush())
  }

  test("raw Celeborn push requires exactly the payload plus its transport header") {
    val bytes = Array[Byte](1, 2, 3)

    Seq(0, bytes.length, bytes.length + 15, bytes.length + 17, -1).foreach { accepted =>
      val client = new RecordingCelebornShuffleClient
      client.acceptedBytes = Some(accepted)

      val error = intercept[IOException] {
        pusher(client).pushPartitionData(0, bytes, bytes.length)
      }

      assert(error.getMessage.contains(accepted.toString))
      assert(error.getMessage.contains((bytes.length + 16).toString))
    }
  }

  test("raw Celeborn push preserves the exact client IOException") {
    val client = new RecordingCelebornShuffleClient
    val expected = new IOException("worker rejected the shuffle frame")
    client.failure = expected

    val actual = intercept[IOException] {
      pusher(client).pushPartitionData(0, Array[Byte](7), 1)
    }

    assert(actual eq expected)
    assert(client.cleanupCalls.get() == 1)
    assert(client.lastCleanup == ((19, 3, (4 << 16) | 7)))
  }

  test("raw Celeborn push preserves unchecked client failures") {
    val client = new RecordingCelebornShuffleClient
    val expected = new IllegalStateException("client was closed")
    client.failure = expected

    val actual = intercept[IllegalStateException] {
      pusher(client).pushPartitionData(0, Array[Byte](7), 1)
    }

    assert(actual eq expected)
    assert(client.cleanupCalls.get() == 1)
  }

  test("executor-wide byte admission stays reserved until the accepted push completes") {
    val client = new RecordingCelebornShuffleClient
    client.automaticallyCompletePushes = false
    val first = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 48)
    val second = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 48)
    val secondFailure = new AtomicReference[Throwable]()
    val bytes = Array.fill[Byte](32)(1)

    assert(first.pushPartitionData(0, bytes, bytes.length) == bytes.length)
    assert(client.pushCompletionCalls.get() == 0)

    val secondThread = new Thread(() => {
      try second.pushPartitionData(0, bytes, bytes.length)
      catch { case failure: Throwable => secondFailure.set(failure) }
    })
    secondThread.start()

    Thread.sleep(100)
    assert(client.pushCalls.get() == 1)

    assert(client.completeNextPush())
    secondThread.join(5000)

    assert(!secondThread.isAlive)
    assert(secondFailure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.completeNextPush())
  }

  test("accepted frames pipeline without synchronously draining Celeborn requests") {
    val client = new RecordingCelebornShuffleClient
    client.automaticallyCompletePushes = false
    val adapter = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 96)

    (0 until 3).foreach { partition =>
      assert(adapter.pushPartitionData(partition, Array.fill[Byte](8)(1), 8) == 8)
    }

    assert(client.pushCalls.get() == 3)
    assert(client.pushCompletionCalls.get() == 0)
    assert(client.completeNextPush())
    assert(client.completeNextPush())
    assert(client.completeNextPush())
  }

  test("native encoding reservations shrink to actual frame bytes before transport completes") {
    val client = new RecordingCelebornShuffleClient
    client.automaticallyCompletePushes = false
    val adapter = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 80)
    val failure = new AtomicReference[Throwable]()
    val frame = Array.fill[Byte](8)(1)

    adapter.reservePartitionData(32)
    assert(adapter.pushPartitionData(0, frame, frame.length) == frame.length)

    val worker = new Thread(() => {
      try {
        adapter.reservePartitionData(32)
        adapter.pushPartitionData(1, frame, frame.length)
      } catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    worker.join(5000)

    if (worker.isAlive) {
      assert(client.completeNextPush())
      worker.join(5000)
      fail("the second small native frame waited for the first transport to complete")
    }

    assert(failure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.pushCompletionCalls.get() == 0)
    assert(client.completeNextPush())
    assert(client.completeNextPush())
  }

  test("native push admission covers every overlapping copy until the raw push returns") {
    val client = new RecordingCelebornShuffleClient
    client.automaticallyCompletePushes = false
    client.pushStarted = new CountDownLatch(1)
    client.allowPush = new CountDownLatch(1)
    val original = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 112)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 112)
    val originalFailure = new AtomicReference[Throwable]()
    val replacementFailure = new AtomicReference[Throwable]()
    val originalFrame = Array.fill[Byte](32)(1)
    val replacementFrame = Array.fill[Byte](8)(1)

    val originalWorker = new Thread(() => {
      try {
        original.reservePartitionData(96)
        original.pushPartitionData(0, originalFrame, originalFrame.length)
      } catch { case failure: Throwable => originalFailure.set(failure) }
    })
    originalWorker.start()
    assert(client.pushStarted.await(5, TimeUnit.SECONDS))

    val replacementWorker = new Thread(() => {
      try {
        replacement.reservePartitionData(24)
        replacement.pushPartitionData(0, replacementFrame, replacementFrame.length)
      } catch { case failure: Throwable => replacementFailure.set(failure) }
    })
    replacementWorker.start()

    Thread.sleep(100)
    assert(replacementWorker.isAlive)
    assert(client.pushCalls.get() == 1)

    client.allowPush.countDown()
    originalWorker.join(5000)
    replacementWorker.join(5000)

    if (replacementWorker.isAlive) {
      assert(client.completeNextPush())
      replacementWorker.join(5000)
      fail("native-copy admission was retained after the raw push returned")
    }

    assert(!originalWorker.isAlive)
    assert(originalFailure.get() == null)
    assert(replacementFailure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.pushCompletionCalls.get() == 0)
    assert(client.completeNextPush())
    assert(client.completeNextPush())
  }

  test("inline transport completion cannot release native copies before the raw push returns") {
    val client = new RecordingCelebornShuffleClient
    client.pushCompletedBeforeReturn = new CountDownLatch(1)
    client.allowPushReturn = new CountDownLatch(1)
    val original = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 112)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 112)
    val originalFailure = new AtomicReference[Throwable]()
    val replacementFailure = new AtomicReference[Throwable]()
    val originalFrame = Array.fill[Byte](32)(1)
    val replacementFrame = Array.fill[Byte](8)(1)

    val originalWorker = new Thread(() => {
      try {
        original.reservePartitionData(96)
        original.pushPartitionData(0, originalFrame, originalFrame.length)
      } catch { case failure: Throwable => originalFailure.set(failure) }
    })
    originalWorker.start()
    assert(client.pushCompletedBeforeReturn.await(5, TimeUnit.SECONDS))
    assert(client.pushCompletionCalls.get() == 1)

    val replacementWorker = new Thread(() => {
      try {
        replacement.reservePartitionData(24)
        replacement.pushPartitionData(0, replacementFrame, replacementFrame.length)
      } catch { case failure: Throwable => replacementFailure.set(failure) }
    })
    replacementWorker.start()

    Thread.sleep(100)
    assert(replacementWorker.isAlive)
    assert(client.pushCalls.get() == 1)

    client.allowPushReturn.countDown()
    originalWorker.join(5000)
    replacementWorker.join(5000)

    assert(!originalWorker.isAlive)
    assert(!replacementWorker.isAlive)
    assert(originalFailure.get() == null)
    assert(replacementFailure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.pushCompletionCalls.get() == 2)
  }

  test("undersized native-copy reservations fail before the Celeborn client is invoked") {
    val client = new RecordingCelebornShuffleClient
    val rejected = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 112)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 112)
    val frame = Array.fill[Byte](32)(1)

    rejected.reservePartitionData(32)
    val failure = intercept[IOException] {
      rejected.pushPartitionData(0, frame, frame.length)
    }
    rejected.releasePartitionDataReservation()

    assert(failure.getMessage.contains("native encoding reservation"))
    assert(client.pushCalls.get() == 0)

    replacement.reservePartitionData(96)
    assert(replacement.pushPartitionData(0, frame, frame.length) == frame.length)
    assert(client.pushCalls.get() == 1)
  }

  test("native scratch reservations can exceed the frame cap but not executor admission") {
    val client = new RecordingCelebornShuffleClient
    val adapter = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 112)

    assert(adapter.maxFrameBytes() == 32)
    adapter.reservePartitionData(96)
    adapter.releasePartitionDataReservation()

    val failure = intercept[IOException] {
      adapter.reservePartitionData(97)
    }
    assert(failure.getMessage.contains("reservation exceeds its byte limit"))
    assert(client.pushCalls.get() == 0)
  }

  test("silent mapper-ended transport completion restores executor-wide byte admission") {
    val client = new RecordingCelebornShuffleClient
    client.automaticallyCompletePushes = false
    client.silentlyCompletePushes = true
    val ended = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 48)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 48)
    val failure = new AtomicReference[Throwable]()
    val frame = Array.fill[Byte](32)(1)

    assert(ended.pushPartitionData(0, frame, frame.length) == frame.length)
    val worker = new Thread(() => {
      try replacement.pushPartitionData(0, frame, frame.length)
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()

    Thread.sleep(100)
    assert(client.pushCalls.get() == 1)
    assert(client.completeNextPush())
    client.silentlyCompletePushes = false

    worker.join(5000)
    if (worker.isAlive) {
      replacement.abort()
      worker.join(5000)
      fail("silent transport completion did not restore executor-wide admission")
    }

    assert(failure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.completeNextPush())
  }

  test("metrics-only failures and silent completions each restore their own admission") {
    val client = new RecordingCelebornShuffleClient
    client.automaticallyCompletePushes = false
    val original = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 96)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 96)
    val failure = new AtomicReference[Throwable]()
    val originalFrame = Array.fill[Byte](32)(1)
    val replacementFrame = Array.fill[Byte](80)(1)

    assert(
      original.pushPartitionData(0, originalFrame, originalFrame.length) == originalFrame.length)
    assert(
      original.pushPartitionData(1, originalFrame, originalFrame.length) == originalFrame.length)

    client.metricsFailureWithoutBatchRemoval = true
    assert(client.completeNextPush())
    client.metricsFailureWithoutBatchRemoval = false
    client.silentlyCompletePushes = true
    assert(client.completeNextPush())
    client.silentlyCompletePushes = false

    val worker = new Thread(() => {
      try replacement.pushPartitionData(0, replacementFrame, replacementFrame.length)
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    worker.join(5000)

    if (worker.isAlive) {
      replacement.abort()
      worker.join(5000)
      fail("a metrics-only failure masked a separate silently completed request")
    }

    assert(failure.get() == null)
    assert(client.pushCalls.get() == 3)
    assert(client.completeNextPush())
  }

  test("tracker reconciliation and delayed completion metrics do not release a live request") {
    val client = new RecordingCelebornShuffleClient
    client.automaticallyCompletePushes = false
    val original = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 72)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 72)
    val later = new CelebornShufflePartitionPusher(client, 19, 5, 1, 12, 9, 72)
    val replacementFailure = new AtomicReference[Throwable]()
    val laterFailure = new AtomicReference[Throwable]()
    val smallFrame = Array.fill[Byte](8)(1)
    val largeFrame = Array.fill[Byte](32)(1)

    assert(original.pushPartitionData(0, smallFrame, smallFrame.length) == smallFrame.length)
    assert(original.pushPartitionData(1, smallFrame, smallFrame.length) == smallFrame.length)

    client.completionRemoved = new CountDownLatch(1)
    client.allowCompletionMetrics = new CountDownLatch(1)
    val completion = new Thread(() => client.completeNextPush())
    completion.start()
    assert(client.completionRemoved.await(5, TimeUnit.SECONDS))

    val replacementWorker = new Thread(() => {
      try replacement.pushPartitionData(0, largeFrame, largeFrame.length)
      catch { case error: Throwable => replacementFailure.set(error) }
    })
    replacementWorker.start()
    replacementWorker.join(5000)

    if (replacementWorker.isAlive) {
      client.allowCompletionMetrics.countDown()
      replacement.abort()
      replacementWorker.join(5000)
      fail("tracker reconciliation did not release the first completed request")
    }

    assert(replacementFailure.get() == null)
    assert(client.pushCalls.get() == 3)
    client.completionRemoved = null
    client.allowCompletionMetrics.countDown()
    completion.join(5000)
    assert(!completion.isAlive)

    val laterWorker = new Thread(() => {
      try later.pushPartitionData(0, smallFrame, smallFrame.length)
      catch { case error: Throwable => laterFailure.set(error) }
    })
    laterWorker.start()

    Thread.sleep(100)
    assert(laterWorker.isAlive)
    assert(client.pushCalls.get() == 3)

    assert(client.completeNextPush())
    laterWorker.join(5000)
    assert(!laterWorker.isAlive)
    assert(laterFailure.get() == null)
    assert(client.pushCalls.get() == 4)
    assert(client.completeNextPush())
    assert(client.completeNextPush())
  }

  test("completed requests do not claim an overlapping push's unsubmitted reservation") {
    val client = new RecordingCelebornShuffleClient
    client.automaticallyCompletePushes = false
    val original = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 72)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 72)
    val overlappingFailure = new AtomicReference[Throwable]()
    val replacementFailure = new AtomicReference[Throwable]()
    val smallFrame = Array.fill[Byte](8)(1)
    val largeFrame = Array.fill[Byte](32)(1)

    assert(original.pushPartitionData(0, largeFrame, largeFrame.length) == largeFrame.length)

    client.pushStarted = new CountDownLatch(1)
    client.allowPush = new CountDownLatch(1)
    val overlapping = new Thread(() => {
      try original.pushPartitionData(1, smallFrame, smallFrame.length)
      catch { case error: Throwable => overlappingFailure.set(error) }
    })
    overlapping.start()
    assert(client.pushStarted.await(5, TimeUnit.SECONDS))

    client.pushStarted = null
    assert(client.completeNextPush())

    val replacementWorker = new Thread(() => {
      try replacement.pushPartitionData(0, largeFrame, largeFrame.length)
      catch { case error: Throwable => replacementFailure.set(error) }
    })
    replacementWorker.start()
    replacementWorker.join(5000)

    if (replacementWorker.isAlive) {
      client.allowPush.countDown()
      overlapping.join(5000)
      assert(client.completeNextPush())
      replacementWorker.join(5000)
      fail("a completed request consumed the smaller unsubmitted reservation's credit")
    }

    assert(replacementFailure.get() == null)
    assert(client.pushCalls.get() == 3)
    client.allowPush.countDown()
    overlapping.join(5000)
    assert(!overlapping.isAlive)
    assert(overlappingFailure.get() == null)
    assert(client.completeNextPush())
    assert(client.completeNextPush())
  }

  test("inline completion cannot release an older live request before submission is accepted") {
    val client = new RecordingCelebornShuffleClient
    client.automaticallyCompletePushes = false
    val original = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 72)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 72)
    val replacementFailure = new AtomicReference[Throwable]()
    val smallFrame = Array.fill[Byte](8)(1)
    val largeFrame = Array.fill[Byte](32)(1)

    assert(original.pushPartitionData(0, largeFrame, largeFrame.length) == largeFrame.length)
    client.automaticallyCompleteLatestPush = true
    assert(original.pushPartitionData(1, smallFrame, smallFrame.length) == smallFrame.length)
    client.automaticallyCompleteLatestPush = false
    assert(client.pushCompletionCalls.get() == 1)

    val replacementWorker = new Thread(() => {
      try replacement.pushPartitionData(0, largeFrame, largeFrame.length)
      catch { case error: Throwable => replacementFailure.set(error) }
    })
    replacementWorker.start()

    Thread.sleep(100)
    assert(replacementWorker.isAlive)
    assert(client.pushCalls.get() == 2)

    assert(client.completeNextPush())
    replacementWorker.join(5000)
    assert(!replacementWorker.isAlive)
    assert(replacementFailure.get() == null)
    assert(client.pushCalls.get() == 3)
    assert(client.completeNextPush())
  }

  test(
    "native frame admission is reserved before encoding and released when encoding is skipped") {
    val client = new RecordingCelebornShuffleClient
    val first = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 48)
    val second = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 48)
    val secondFailure = new AtomicReference[Throwable]()

    first.reservePartitionData(32)
    val blocked = new Thread(() => {
      try {
        second.reservePartitionData(32)
        second.releasePartitionDataReservation()
      } catch { case failure: Throwable => secondFailure.set(failure) }
    })
    blocked.start()

    Thread.sleep(100)
    assert(blocked.isAlive)
    assert(client.pushCalls.get() == 0)

    first.releasePartitionDataReservation()
    blocked.join(5000)
    assert(!blocked.isAlive)
    assert(secondFailure.get() == null)
  }

  test("cancelled accepted requests retain executor admission until their transport callback") {
    val client = new RecordingCelebornShuffleClient
    client.automaticallyCompletePushes = false
    val cancelled = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 48)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 48)
    val replacementFailure = new AtomicReference[Throwable]()
    val frame = Array.fill[Byte](32)(1)

    assert(cancelled.pushPartitionData(0, frame, frame.length) == frame.length)
    cancelled.abort()

    val worker = new Thread(() => {
      try replacement.pushPartitionData(0, frame, frame.length)
      catch { case failure: Throwable => replacementFailure.set(failure) }
    })
    worker.start()

    Thread.sleep(100)
    assert(worker.isAlive)
    assert(client.pushCalls.get() == 1)

    assert(client.completeNextPush())
    worker.join(5000)
    assert(!worker.isAlive)
    assert(replacementFailure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.completeNextPush())
  }

  test("map completion fails and cleans up when accepted Celeborn pushes time out") {
    val client = new RecordingCelebornShuffleClient
    client.drainTimedOut = true
    val adapter = pusher(client)

    assert(adapter.pushPartitionData(0, Array[Byte](1), 1) == 1)

    val failure = intercept[IOException] {
      adapter.finish()
    }

    assert(failure.getMessage.contains("Timed out"))
    assert(client.cleanupCalls.get() == 1)
    assert(client.mapperEndCalls.get() == 1)
  }

  test("executor-wide admission rejects a frame larger than its configured budget") {
    val client = new RecordingCelebornShuffleClient
    val adapter = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 20)

    val failure = intercept[IOException] {
      adapter.pushPartitionData(0, Array.fill[Byte](8)(1), 8)
    }

    assert(failure.getMessage.contains("in-flight byte limit"))
    assert(client.pushCalls.get() == 0)
  }

  test("successful map completion drains pending pushes and reports per-reducer frame bytes") {
    val client = new RecordingCelebornShuffleClient
    val adapter = pusher(client)

    adapter.pushPartitionData(2, Array[Byte](1, 2, 3), 3)
    adapter.pushPartitionData(5, Array[Byte](4, 5), 2)
    adapter.pushPartitionData(2, Array[Byte](6), 1)

    val partitionLengths = adapter.finish()
    assert(partitionLengths.sameElements(Array[Long](0, 0, 4, 0, 0, 2, 0, 0, 0)))
    assert(client.mapperEndCalls.get() == 1)
    assert(client.lastMapperEnd == ((19, 3, (4 << 16) | 7, 12)))
    assert(client.cleanupCalls.get() == 0)

    assert(adapter.finish().sameElements(partitionLengths))
    assert(client.mapperEndCalls.get() == 1)

    adapter.abort()
    adapter.abort()
    assert(client.cleanupCalls.get() == 1)
  }

  test("an empty map still commits all reducer partitions to Celeborn") {
    val client = new RecordingCelebornShuffleClient

    assert(pusher(client).finish().sameElements(Array.fill[Long](9)(0L)))
    assert(client.mapperEndCalls.get() == 1)
  }

  test("asynchronous push failures observed by mapperEnd fail the task and clean up") {
    val client = new RecordingCelebornShuffleClient
    val expected = new IOException("a pending Celeborn push failed")
    client.mapperEndFailure = expected
    val adapter = pusher(client)

    adapter.pushPartitionData(0, Array[Byte](1), 1)
    val actual = intercept[IOException] {
      adapter.finish()
    }

    assert(actual eq expected)
    assert(client.mapperEndCalls.get() == 1)
    assert(client.cleanupCalls.get() == 1)
  }

  test("cleanup errors are suppressed without replacing the original push failure") {
    val client = new RecordingCelebornShuffleClient
    val expected = new IOException("the shuffle worker rejected the frame")
    val cleanupFailure = new IllegalStateException("attempt cleanup also failed")
    client.failure = expected
    client.cleanupFailure = cleanupFailure

    val actual = intercept[IOException] {
      pusher(client).pushPartitionData(0, Array[Byte](1), 1)
    }

    assert(actual eq expected)
    assert(actual.getSuppressed.sameElements(Array[Throwable](cleanupFailure)))
    assert(client.cleanupCalls.get() == 1)
  }

  test("completed and aborted map attempts reject subsequent native pushes") {
    val completedClient = new RecordingCelebornShuffleClient
    val completed = pusher(completedClient)
    completed.finish()

    intercept[IOException] {
      completed.pushPartitionData(0, Array[Byte](1), 1)
    }

    val abortedClient = new RecordingCelebornShuffleClient
    val aborted = pusher(abortedClient)
    aborted.abort()

    intercept[IOException] {
      aborted.pushPartitionData(0, Array[Byte](1), 1)
    }

    assert(completedClient.lastPush == null)
    assert(abortedClient.lastPush == null)
    assert(abortedClient.cleanupCalls.get() == 1)
  }

  test("map completion waits for an active native callback before invoking mapperEnd") {
    val client = new RecordingCelebornShuffleClient
    client.pushStarted = new CountDownLatch(1)
    client.allowPush = new CountDownLatch(1)
    val adapter = pusher(client)
    val pushFailure = new AtomicReference[Throwable]()
    val finishFailure = new AtomicReference[Throwable]()
    val finished = new CountDownLatch(1)

    val pushThread = new Thread(() => {
      try adapter.pushPartitionData(4, Array[Byte](1, 2), 2)
      catch { case error: Throwable => pushFailure.set(error) }
    })
    pushThread.start()
    assert(client.pushStarted.await(5, TimeUnit.SECONDS))

    val finishThread = new Thread(() => {
      try adapter.finish()
      catch { case error: Throwable => finishFailure.set(error) }
      finally finished.countDown()
    })
    finishThread.start()

    assert(!finished.await(100, TimeUnit.MILLISECONDS))
    assert(client.mapperEndCalls.get() == 0)

    client.allowPush.countDown()
    pushThread.join(5000)
    finishThread.join(5000)

    assert(!pushThread.isAlive)
    assert(!finishThread.isAlive)
    assert(pushFailure.get() == null)
    assert(finishFailure.get() == null)
    assert(client.mapperEndCalls.get() == 1)
  }

  test("failed task cleanup wakes a native push blocked in Celeborn backpressure") {
    val client = new RecordingCelebornShuffleClient
    client.pushStarted = new CountDownLatch(1)
    client.allowPush = new CountDownLatch(1)
    client.cleanupUnblocksPush = true
    val adapter = pusher(client)
    val failure = new AtomicReference[Throwable]()

    val worker = new Thread(() => {
      try adapter.pushPartitionData(0, Array[Byte](1), 1)
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    assert(client.pushStarted.await(5, TimeUnit.SECONDS))

    adapter.abort()
    worker.join(5000)

    assert(!worker.isAlive)
    assert(failure.get().isInstanceOf[IOException])
    assert(client.cleanupCalls.get() == 2)
    assert(client.mapperEndCalls.get() == 0)
  }

  test("an aborted client push cleans up Celeborn state recreated after initial cleanup") {
    val client = new RecordingCelebornShuffleClient
    client.pushStarted = new CountDownLatch(1)
    client.allowPush = new CountDownLatch(1)
    client.recreatePushStateAfterCleanup = true
    val adapter = pusher(client)
    val failure = new AtomicReference[Throwable]()

    val worker = new Thread(() => {
      try adapter.pushPartitionData(0, Array[Byte](1), 1)
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    assert(client.pushStarted.await(5, TimeUnit.SECONDS))
    assert(client.retainedPushState)

    adapter.abort()
    worker.join(5000)

    assert(!worker.isAlive)
    assert(failure.get().isInstanceOf[IOException])
    assert(failure.get().getMessage.contains("aborted during its push"))
    assert(client.cleanupCalls.get() == 2)
    assert(!client.retainedPushState)

    adapter.abort()
    assert(client.cleanupCalls.get() == 2)
  }

  Seq("connection creation", "transport registration").foreach { cancellationWindow =>
    test(
      s"cancellation during $cancellationWindow retains the captured state's transport admission") {
      val client = new RecordingCelebornShuffleClient
      client.automaticallyCompletePushes = false
      val pausedPush = new CountDownLatch(1)
      val allowPush = new CountDownLatch(1)
      if (cancellationWindow == "connection creation") {
        client.pushStateCaptured = pausedPush
        client.allowPushStateCapture = allowPush
      } else {
        client.pushRegistered = pausedPush
        client.allowPushRegistration = allowPush
      }

      val cancelled = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 112)
      val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 112)
      val cancelledFailure = new AtomicReference[Throwable]()
      val replacementFailure = new AtomicReference[Throwable]()
      val frame = Array.fill[Byte](32)(1)

      val cancelledWorker = new Thread(() => {
        try {
          cancelled.reservePartitionData(96)
          cancelled.pushPartitionData(0, frame, frame.length)
        } catch { case failure: Throwable => cancelledFailure.set(failure) }
      })
      cancelledWorker.start()
      assert(pausedPush.await(5, TimeUnit.SECONDS))

      cancelled.abort()
      allowPush.countDown()
      cancelledWorker.join(5000)
      assert(!cancelledWorker.isAlive)
      assert(cancelledFailure.get().isInstanceOf[IOException])
      assert(client.cleanupCalls.get() == 2)

      client.pushStateCaptured = null
      client.pushRegistered = null
      val replacementWorker = new Thread(() => {
        try {
          replacement.reservePartitionData(96)
          replacement.pushPartitionData(0, frame, frame.length)
        } catch { case failure: Throwable => replacementFailure.set(failure) }
      })
      replacementWorker.start()

      Thread.sleep(100)
      assert(replacementWorker.isAlive)
      assert(client.pushCalls.get() == 1)
      assert(client.pushStateCreations.get() == 1)
      assert(client.pushCompletionCalls.get() == 0)

      assert(client.completeNextPush())
      replacementWorker.join(5000)
      assert(!replacementWorker.isAlive)
      assert(replacementFailure.get() == null)
      assert(client.pushCalls.get() == 2)
      assert(client.completeNextPush())
    }
  }

  test("a recreated cancelled push retains byte admission until its own transport completes") {
    val client = new RecordingCelebornShuffleClient
    client.automaticallyCompletePushes = false
    client.pushStarted = new CountDownLatch(1)
    client.allowPush = new CountDownLatch(1)
    client.recreatePushStateAfterCleanup = true
    val cancelled = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 48)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 48)
    val cancelledFailure = new AtomicReference[Throwable]()
    val replacementFailure = new AtomicReference[Throwable]()
    val frame = Array.fill[Byte](32)(1)

    val cancelledWorker = new Thread(() => {
      try cancelled.pushPartitionData(0, frame, frame.length)
      catch { case failure: Throwable => cancelledFailure.set(failure) }
    })
    cancelledWorker.start()
    assert(client.pushStarted.await(5, TimeUnit.SECONDS))

    cancelled.abort()
    cancelledWorker.join(5000)
    assert(!cancelledWorker.isAlive)
    assert(cancelledFailure.get().isInstanceOf[IOException])
    assert(client.cleanupCalls.get() == 2)

    client.pushStarted = null
    client.allowPush = null
    val replacementWorker = new Thread(() => {
      try replacement.pushPartitionData(0, frame, frame.length)
      catch { case failure: Throwable => replacementFailure.set(failure) }
    })
    replacementWorker.start()

    Thread.sleep(100)
    assert(replacementWorker.isAlive)
    assert(client.pushCalls.get() == 1)

    assert(client.completeNextPush())
    replacementWorker.join(5000)
    assert(!replacementWorker.isAlive)
    assert(replacementFailure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.completeNextPush())
  }

  test("inline completion on a recreated cancelled state does not strand byte admission") {
    val client = new RecordingCelebornShuffleClient
    client.pushStarted = new CountDownLatch(1)
    client.allowPush = new CountDownLatch(1)
    client.recreatePushStateAfterCleanup = true
    val cancelled = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 48)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 48)
    val cancelledFailure = new AtomicReference[Throwable]()
    val replacementFailure = new AtomicReference[Throwable]()
    val frame = Array.fill[Byte](32)(1)

    val cancelledWorker = new Thread(() => {
      try cancelled.pushPartitionData(0, frame, frame.length)
      catch { case error: Throwable => cancelledFailure.set(error) }
    })
    cancelledWorker.start()
    assert(client.pushStarted.await(5, TimeUnit.SECONDS))

    cancelled.abort()
    cancelledWorker.join(5000)
    assert(!cancelledWorker.isAlive)
    assert(cancelledFailure.get().isInstanceOf[IOException])
    assert(client.pushCompletionCalls.get() == 1)

    client.pushStarted = null
    client.allowPush = null
    val replacementWorker = new Thread(() => {
      try replacement.pushPartitionData(0, frame, frame.length)
      catch { case error: Throwable => replacementFailure.set(error) }
    })
    replacementWorker.start()
    replacementWorker.join(5000)

    if (replacementWorker.isAlive) {
      replacement.abort()
      replacementWorker.join(5000)
      fail("inline completion on the recreated state permanently retained byte admission")
    }

    assert(replacementFailure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.pushCompletionCalls.get() == 2)
  }

  test("inline failure on a recreated cancelled state does not strand byte admission") {
    val client = new RecordingCelebornShuffleClient
    client.pushStarted = new CountDownLatch(1)
    client.allowPush = new CountDownLatch(1)
    client.recreatePushStateAfterCleanup = true
    client.metricsFailureWithoutBatchRemoval = true
    val cancelled = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 48)
    val replacement = new CelebornShufflePartitionPusher(client, 19, 4, 1, 12, 9, 48)
    val cancelledFailure = new AtomicReference[Throwable]()
    val replacementFailure = new AtomicReference[Throwable]()
    val frame = Array.fill[Byte](32)(1)

    val cancelledWorker = new Thread(() => {
      try cancelled.pushPartitionData(0, frame, frame.length)
      catch { case error: Throwable => cancelledFailure.set(error) }
    })
    cancelledWorker.start()
    assert(client.pushStarted.await(5, TimeUnit.SECONDS))

    cancelled.abort()
    cancelledWorker.join(5000)
    assert(!cancelledWorker.isAlive)
    assert(cancelledFailure.get().isInstanceOf[IOException])
    assert(client.pushCompletionCalls.get() == 1)

    client.pushStarted = null
    client.allowPush = null
    client.metricsFailureWithoutBatchRemoval = false
    val replacementWorker = new Thread(() => {
      try replacement.pushPartitionData(0, frame, frame.length)
      catch { case error: Throwable => replacementFailure.set(error) }
    })
    replacementWorker.start()
    replacementWorker.join(5000)

    if (replacementWorker.isAlive) {
      replacement.abort()
      replacementWorker.join(5000)
      fail("inline failure on the recreated state permanently retained byte admission")
    }

    assert(replacementFailure.get() == null)
    assert(client.pushCalls.get() == 2)
    assert(client.pushCompletionCalls.get() == 2)
  }

  test("failed task cleanup wakes map completion while it drains pending Celeborn pushes") {
    val client = new RecordingCelebornShuffleClient
    client.mapperEndStarted = new CountDownLatch(1)
    client.allowMapperEnd = new CountDownLatch(1)
    client.cleanupUnblocksMapperEnd = true
    val adapter = pusher(client)
    val failure = new AtomicReference[Throwable]()

    adapter.pushPartitionData(0, Array[Byte](1), 1)
    val worker = new Thread(() => {
      try adapter.finish()
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    assert(client.mapperEndStarted.await(5, TimeUnit.SECONDS))

    adapter.abort()
    worker.join(5000)

    assert(!worker.isAlive)
    assert(failure.get().isInstanceOf[IOException])
    assert(client.mapperEndCalls.get() == 1)
    assert(client.cleanupCalls.get() == 1)
  }

  test("failed task cleanup wakes mapper-end completion drain for an accepted push") {
    val client = new RecordingCelebornShuffleClient
    client.drainStarted = new CountDownLatch(1)
    client.allowDrain = new CountDownLatch(1)
    client.cleanupUnblocksDrain = true
    val adapter = pusher(client)
    val failure = new AtomicReference[Throwable]()

    assert(adapter.pushPartitionData(0, Array[Byte](1), 1) == 1)
    val worker = new Thread(() => {
      try adapter.finish()
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    assert(client.drainStarted.await(5, TimeUnit.SECONDS))

    adapter.abort()
    worker.join(5000)

    assert(!worker.isAlive)
    assert(failure.get().isInstanceOf[IOException])
    assert(client.cleanupCalls.get() == 1)
  }

  test("cancellation interrupts mapperEnd RPC even when client cleanup cannot wake it") {
    val client = new RecordingCelebornShuffleClient
    client.mapperEndStarted = new CountDownLatch(1)
    client.allowMapperEnd = new CountDownLatch(1)
    val adapter = pusher(client)
    val failure = new AtomicReference[Throwable]()

    val worker = new Thread(() => {
      try adapter.finish()
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    assert(client.mapperEndStarted.await(5, TimeUnit.SECONDS))

    adapter.abort()
    worker.join(5000)

    assert(!worker.isAlive)
    assert(failure.get().isInstanceOf[IOException])
    assert(client.cleanupCalls.get() == 1)
    assert(client.allowMapperEnd.getCount == 1)
  }

  test("adapter rejects a missing client or incompatible Celeborn raw-push API") {
    intercept[IllegalArgumentException] {
      new CelebornShufflePartitionPusher(null, 0, 0, 0, 1, 1)
    }

    val missing = intercept[IllegalArgumentException] {
      new CelebornShufflePartitionPusher(new Object, 0, 0, 0, 1, 1)
    }
    assert(missing.getMessage.contains("raw-push"))

    intercept[IllegalArgumentException] {
      new CelebornShufflePartitionPusher(new WrongReturnTypeCelebornShuffleClient, 0, 0, 0, 1, 1)
    }
  }

  test("adapter rejects invalid shuffle identity, mapper identity, and partition counts") {
    val client = new RecordingCelebornShuffleClient

    Seq(
      (-1, 0, 0, 1, 1),
      (0, -1, 0, 1, 1),
      (0, 0, -1, 1, 1),
      (0, 0, 0, 0, 1),
      (0, 1, 0, 1, 1),
      (0, 0, 0, 1, 0)).foreach { case (shuffleId, mapId, attemptId, numMappers, numPartitions) =>
      intercept[IllegalArgumentException] {
        new CelebornShufflePartitionPusher(
          client,
          shuffleId,
          mapId,
          attemptId,
          numMappers,
          numPartitions)
      }
    }
  }

  test("adapter rejects invalid output partitions and incomplete Comet frames") {
    val client = new RecordingCelebornShuffleClient
    val adapter = pusher(client)

    Seq(-1, 9).foreach { partitionId =>
      intercept[IOException] {
        adapter.pushPartitionData(partitionId, Array[Byte](1), 1)
      }
    }

    intercept[IOException] {
      adapter.pushPartitionData(0, null, 1)
    }

    Seq(-1, 0, 2).foreach { length =>
      intercept[IOException] {
        adapter.pushPartitionData(0, Array[Byte](1), length)
      }
    }

    assert(client.lastPush == null)
  }

  test("task-scoped native registration validates its handle, callback, reducers, and frame") {
    val adapter = pusher(new RecordingCelebornShuffleClient)

    val registration = CometExecIterator.RssPartitionPusherRegistration(1L, adapter, 9, 20)
    assert(registration.handle == 1L)
    assert(registration.pusher eq adapter)
    assert(registration.numPartitions == 9)
    assert(registration.maxFrameBytes == 20)

    Seq(
      (0L, adapter, 9, 20),
      (-1L, adapter, 9, 20),
      (1L, null, 9, 20),
      (1L, adapter, 0, 20),
      (1L, adapter, 9, 19)).foreach { case (handle, callback, partitions, maxFrame) =>
      intercept[IllegalArgumentException] {
        CometExecIterator.RssPartitionPusherRegistration(handle, callback, partitions, maxFrame)
      }
    }
  }

  test("factory recognizes the existing Celeborn shuffle manager") {
    val conf = new SparkConf(false).set(managerKey, managerClass)

    assert(CelebornShufflePusherFactory.isEnabled(conf))

    conf.set(managerKey, "org.apache.spark.shuffle.sort.SortShuffleManager")
    assert(!CelebornShufflePusherFactory.isEnabled(conf))
  }

  test("factory disables Celeborn native shuffle while Spark I/O encryption is enabled") {
    val conf = enabledConf.set("spark.io.encryption.enabled", "true")

    assert(!CelebornShufflePusherFactory.isEnabled(conf))

    conf.set("spark.io.encryption.enabled", "false")
    assert(CelebornShufflePusherFactory.isEnabled(conf))
  }

  test("an acquired client remains owned when Celeborn shuffle-generation resolution fails") {
    val client = new RecordingCelebornShuffleClient
    val expected = new IOException("Celeborn shuffle-generation lookup failed")
    var ownedClient: AnyRef = null

    val actual = intercept[IOException] {
      val acquired = CelebornShufflePusherFactory.acquireClient(client, ownedClient = _)
      assert(ownedClient eq acquired)
      throw expected
    }

    assert(actual eq expected)
    assert(ownedClient eq client)
    assert(client.cleanupCalls.get() == 0)
  }

  test("failed Celeborn client acquisition does not record application ownership") {
    val expected = new IOException("Celeborn client acquisition failed")
    var ownedClient: AnyRef = null

    val actual = intercept[IOException] {
      CelebornShufflePusherFactory.acquireClient(throw expected, ownedClient = _)
    }

    assert(actual eq expected)
    assert(ownedClient == null)
  }

  test("an authorized retried map invalidates its ambiguous Celeborn shuffle generation") {
    val client = new RecordingCelebornShuffleClient
    val context = emptyTaskContext()
    var driverInvalidated = false

    val failure = intercept[Exception] {
      CelebornShufflePusherFactory.rejectRetriedAttempt(
        client,
        7,
        91,
        context,
        true,
        () => driverInvalidated = true)
    }

    assert(failure.getClass.getName == "org.apache.spark.shuffle.FetchFailedException")
    assert(failure.getMessage.contains("new shuffle generation"))
    assert(client.lastInvalidatedShuffle == ((7, 91, context.taskAttemptId())))
    assert(driverInvalidated)
  }

  test("factory invokes Celeborn's barrier failure-listener hook with the task handle") {
    val client = new Object
    val context = emptyTaskContext()
    val handle = new ShuffleHandle(7) {}

    CelebornShufflePusherFactory.registerBarrierFailureListener(
      classOf[RecordingCelebornSparkUtils],
      classOf[Object],
      classOf[ShuffleHandle],
      client,
      context,
      handle)

    assert(RecordingCelebornSparkUtils.client eq client)
    assert(RecordingCelebornSparkUtils.taskContext eq context)
    assert(RecordingCelebornSparkUtils.shuffleHandle eq handle)
  }

  test("a losing speculative map attempt never invalidates the winning shuffle generation") {
    val client = new RecordingCelebornShuffleClient
    val taskContext = emptyTaskContext()

    val failure = intercept[Exception] {
      CelebornShufflePusherFactory.rejectRetriedAttempt(client, 7, 91, taskContext, false)
    }

    assert(failure.getClass.getName == "org.apache.spark.executor.CommitDeniedException")
    assert(failure.getMessage.contains("already owns"))
    val reason = failure.getClass.getMethod("toTaskCommitDeniedReason").invoke(failure)
    assert(reason.getClass.getName == "org.apache.spark.TaskCommitDenied")
    assert(
      !reason.getClass.getMethod("countTowardsTaskFailures").invoke(reason).asInstanceOf[Boolean])
    assert(client.lastInvalidatedShuffle == null)
  }

  test("an authorized retry cannot proceed when Celeborn rejects generation invalidation") {
    val client = new RecordingCelebornShuffleClient
    client.generationInvalidated = false
    var driverInvalidated = false

    val failure = intercept[IOException] {
      CelebornShufflePusherFactory.rejectRetriedAttempt(
        client,
        7,
        91,
        emptyTaskContext(),
        true,
        () => driverInvalidated = true)
    }

    assert(failure.getMessage.contains("Could not invalidate"))
    assert(client.lastInvalidatedShuffle._1 == 7)
    assert(!driverInvalidated)
  }

  test("a speculative owner abandons its commit when Celeborn preserves the live original") {
    val client = new RecordingCelebornShuffleClient
    client.generationInvalidated = false
    var driverAbandoned = false

    val failure = intercept[Exception] {
      CelebornShufflePusherFactory.rejectRetriedAttempt(
        client,
        7,
        91,
        emptyTaskContext(),
        true,
        () => fail("a rejected generation must not be marked invalid"),
        () => {
          driverAbandoned = true
          true
        })
    }

    assert(failure.getClass.getName == "org.apache.spark.executor.CommitDeniedException")
    assert(driverAbandoned)
    assert(client.lastInvalidatedShuffle == null)
  }

  test("factory recognizes the composite Comet and Celeborn shuffle manager") {
    val conf = new SparkConf(false).set(managerKey, compositeManagerClass)

    assert(CelebornShufflePusherFactory.isEnabled(conf))

    conf.set(enabledKey, "false")
    assert(!CelebornShufflePusherFactory.isEnabled(conf))
  }

  test("factory recognizes the existing Celeborn shuffle data IO plugin") {
    val conf = new SparkConf(false).set(pluginKey, pluginClass)

    assert(CelebornShufflePusherFactory.isEnabled(conf))

    conf.set(pluginKey, "org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO")
    assert(!CelebornShufflePusherFactory.isEnabled(conf))
  }

  test("factory recognizes nonblank existing Celeborn master endpoints") {
    assert(CelebornShufflePusherFactory.isEnabled(enabledConf))

    val conf = new SparkConf(false).set(endpointsKey, "   ")
    assert(!CelebornShufflePusherFactory.isEnabled(conf))
  }

  test("application enable flag alone does not invent an unconfigured Celeborn backend") {
    val conf = new SparkConf(false).set(enabledKey, "true")

    assert(!CelebornShufflePusherFactory.isEnabled(conf))
  }

  test("explicit application opt-out overrides every existing Celeborn selection") {
    Seq(
      managerKey -> managerClass,
      managerKey -> compositeManagerClass,
      pluginKey -> pluginClass,
      endpointsKey -> "celeborn-master:9097").foreach { case (key, value) =>
      val conf = new SparkConf(false).set(key, value).set(enabledKey, "false")

      assert(!CelebornShufflePusherFactory.isEnabled(conf))
    }
  }

  test("disabled factory does not inspect a client, task context, or task metadata") {
    val conf = new SparkConf(false)

    assert(
      CelebornShufflePusherFactory
        .create(
          conf,
          null,
          celebornShuffleId = -1,
          numMappers = 0,
          numPartitions = 0,
          taskContext = null)
        .isEmpty)
  }

  test("enabled factory binds an existing client to captured Spark task metadata") {
    val client = new RecordingCelebornShuffleClient
    val taskContext = emptyTaskContext()
    val adapter = CelebornShufflePusherFactory
      .create(
        enabledConf,
        client,
        celebornShuffleId = 27,
        numMappers = 8,
        numPartitions = 4,
        taskContext = taskContext)
      .get

    assert(adapter.pushPartitionData(2, Array[Byte](3, 4), 2) == 2)
    assert(client.lastPush.shuffleId == 27)
    assert(client.lastPush.mapId == taskContext.partitionId())
    assert(client.lastPush.attemptId == 0)
    assert(client.lastPush.numMappers == 8)
    assert(client.lastPush.numPartitions == 4)
  }

  test("enabled factory requires admission for all three copies of a minimum native frame") {
    val admissionKey = "spark.comet.shuffle.rss.maxInFlightBytes"
    val taskContext = emptyTaskContext()

    val rejected = intercept[IllegalArgumentException] {
      CelebornShufflePusherFactory.create(
        enabledConf.set(admissionKey, "75"),
        new RecordingCelebornShuffleClient,
        celebornShuffleId = 27,
        numMappers = 8,
        numPartitions = 4,
        taskContext = taskContext)
    }
    assert(rejected.getMessage.contains("in-flight byte limit"))

    val minimum = CelebornShufflePusherFactory
      .create(
        enabledConf.set(admissionKey, "76"),
        new RecordingCelebornShuffleClient,
        celebornShuffleId = 27,
        numMappers = 8,
        numPartitions = 4,
        taskContext = taskContext)
      .get
    assert(minimum.maxFrameBytes() == 20)
  }

  test("factory encodes the stage and task attempt without loading the Celeborn client jar") {
    assert(CelebornShufflePusherFactory.encodeAttemptNumber(4, 7) == ((4 << 16) | 7))
    assert(CelebornShufflePusherFactory.encodeAttemptNumber(0, 0) == 0)
    assert(CelebornShufflePusherFactory.encodeAttemptNumber(32767, 65535) == Int.MaxValue)
  }

  test("factory rejects attempts that cannot be represented as a nonnegative Celeborn ID") {
    Seq((-1, 0), (32768, 0), (0, -1), (0, 65536)).foreach { case (stageAttempt, taskAttempt) =>
      intercept[IllegalArgumentException] {
        CelebornShufflePusherFactory.encodeAttemptNumber(stageAttempt, taskAttempt)
      }
    }
  }

  test("a captured task-owned pusher works on a worker without Spark's thread-local context") {
    val client = new RecordingCelebornShuffleClient
    val adapter = CelebornShufflePusherFactory
      .create(enabledConf, client, 11, 3, 2, emptyTaskContext())
      .get
    val failure = new AtomicReference[Throwable]()

    val worker = new Thread(() => {
      try {
        assert(TaskContext.get() == null)
        assert(adapter.pushPartitionData(1, Array[Byte](9), 1) == 1)
      } catch {
        case error: Throwable => failure.set(error)
      }
    })

    worker.start()
    worker.join(5000)

    assert(!worker.isAlive, "worker thread did not finish")
    assert(failure.get() == null)
    assert(client.observedTaskContext == null)
    assert(client.lastPush.partitionId == 1)
  }
}
