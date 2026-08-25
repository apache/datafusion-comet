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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext, TaskEndReason, UnknownReason}
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}

import org.apache.comet.shuffle.{CelebornShufflePartitionPusher, RecordingCelebornShuffleClient}

class CometCelebornShuffleManagerSuite extends AnyFunSuite {

  private class RecordingShuffleManager extends ShuffleManager {

    val returnedHandle: ShuffleHandle =
      new BaseShuffleHandle[Any, Any, Any](31, null)

    var registration: Option[(Int, ShuffleDependency[_, _, _])] = None
    var writerCall: Option[(ShuffleHandle, Long)] = None
    var readerCall: Option[(ShuffleHandle, Int, Int, Int, Int)] = None
    var unregisteredShuffleId: Option[Int] = None
    var unregisterResult = true
    var resolverReads = 0
    var stopped = false
    var registrationFailure: RuntimeException = _

    override def registerShuffle[K, V, C](
        shuffleId: Int,
        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
      registration = Some((shuffleId, dependency))
      if (registrationFailure != null) {
        throw registrationFailure
      }
      returnedHandle
    }

    override def getWriter[K, V](
        handle: ShuffleHandle,
        mapId: Long,
        context: TaskContext,
        metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
      writerCall = Some((handle, mapId))
      null
    }

    override def getReader[K, C](
        handle: ShuffleHandle,
        startMapIndex: Int,
        endMapIndex: Int,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext,
        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
      readerCall = Some((handle, startMapIndex, endMapIndex, startPartition, endPartition))
      null
    }

    override def shuffleBlockResolver: ShuffleBlockResolver = {
      resolverReads += 1
      null
    }

    override def unregisterShuffle(shuffleId: Int): Boolean = {
      unregisteredShuffleId = Some(shuffleId)
      unregisterResult
    }

    override def stop(): Unit = stopped = true
  }

  private def manager(
      backend: ShuffleManager,
      conf: SparkConf = new SparkConf(false),
      isDriver: Boolean = true): CometCelebornShuffleManager = {
    new CometCelebornShuffleManager(conf, isDriver, (_, _) => backend)
  }

  test("manager forwards the existing Spark configuration and driver identity") {
    val conf = new SparkConf(false)
      .set("spark.celeborn.master.endpoints", "existing-master:9097")
    val backend = new RecordingShuffleManager
    var observedConf: SparkConf = null
    var observedIsDriver = true

    new CometCelebornShuffleManager(
      conf,
      false,
      (actualConf, actualIsDriver) => {
        observedConf = actualConf
        observedIsDriver = actualIsDriver
        backend
      })

    assert(observedConf eq conf)
    assert(!observedIsDriver)
    assert(conf.get("spark.celeborn.master.endpoints") == "existing-master:9097")
  }

  test("native frame limits reserve all submission copies within executor admission") {
    val client = new RecordingCelebornShuffleClient
    val pusher = new CelebornShufflePartitionPusher(client, 19, 3, 1, 12, 9, 112)

    assert(CometCelebornShuffleManager.maxNativeFrameBytes(64, pusher) == 32)
    assert(CometCelebornShuffleManager.maxNativeFrameBytes(32, pusher) == 32)
    assert(CometCelebornShuffleManager.maxNativeFrameBytes(24, pusher) == 24)
    pusher.reservePartitionData(96)
    pusher.releasePartitionDataReservation()
    assert(pusher.pushPartitionData(0, Array.fill[Byte](32)(1), 32) == 32)
    assert(client.lastPush.length == 32)

    val minimum =
      new CelebornShufflePartitionPusher(new RecordingCelebornShuffleClient, 19, 3, 1, 12, 9, 76)
    assert(CometCelebornShuffleManager.maxNativeFrameBytes(64, minimum) == 20)
  }

  test("a new Celeborn generation resets successful map commit owners on the Spark driver") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val firstGeneration = PrepareCelebornShuffleGeneration(7, 91, 12, 0, 2)
    val replacementGeneration = PrepareCelebornShuffleGeneration(7, 92, 12, 1, 2)
    val stageStart = sparkCoordinator.getClass
      .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
    val canCommit = sparkCoordinator.getClass
      .getMethod(
        "handleAskPermissionToCommit",
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE)

    def authorize(stageAttempt: Int, partition: Int, taskAttempt: Int): Boolean =
      canCommit
        .invoke(
          sparkCoordinator,
          Int.box(12),
          Int.box(stageAttempt),
          Int.box(partition),
          Int.box(taskAttempt))
        .asInstanceOf[Boolean]

    stageStart.invoke(sparkCoordinator, Int.box(12), Int.box(1))
    assert(coordinator.prepareGeneration(firstGeneration))
    assert(authorize(0, 0, 0))
    assert(authorize(0, 1, 1))
    assert(!coordinator.prepareGeneration(firstGeneration.copy(celebornShuffleId = 92)))
    assert(!authorize(1, 0, 0))

    assert(coordinator.prepareGeneration(replacementGeneration))
    assert(authorize(1, 0, 0))
    assert(coordinator.prepareGeneration(replacementGeneration))
    assert(!authorize(1, 0, 1))
    assert(authorize(1, 1, 0))
    assert(!coordinator.prepareGeneration(firstGeneration))
  }

  test("early map ownership denies speculation and releases only after genuine task failure") {
    val coordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val stageId = 74
    coordinator.getClass
      .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
      .invoke(coordinator, Int.box(stageId), Int.box(0))
    val canCommit = coordinator.getClass
      .getMethod(
        "handleAskPermissionToCommit",
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE)

    def authorize(attempt: Int): Boolean =
      canCommit
        .invoke(coordinator, Int.box(stageId), Int.box(0), Int.box(0), Int.box(attempt))
        .asInstanceOf[Boolean]

    assert(authorize(0))
    assert(!authorize(1))
    // Spark's coordinator also rejects the exact same owner when asked twice.
    assert(!authorize(0))

    coordinator.getClass
      .getMethod(
        "taskCompleted",
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        classOf[TaskEndReason])
      .invoke(coordinator, Int.box(stageId), Int.box(0), Int.box(0), Int.box(0), UnknownReason)

    assert(authorize(1))
  }

  test("driver claims survive concurrent generation resets and reject stale map commits") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val stageId = 75
    sparkCoordinator.getClass
      .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
      .invoke(sparkCoordinator, Int.box(stageId), Int.box(1))

    val firstGeneration = PrepareCelebornShuffleGeneration(7, 91, stageId, 0, 2)
    assert(coordinator.prepareGeneration(firstGeneration))
    val originalClaim = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(7, stageId, 0, 0, 0))
    assert(originalClaim.authorized)
    assert(!coordinator.claimMapAttempt(ClaimCelebornMapAttempt(7, stageId, 0, 0, 1)).authorized)
    val originalPrepared = coordinator.prepareGenerationAndClaim(firstGeneration
      .copy(mapId = 0, taskAttempt = 0, claimEpoch = originalClaim.epoch, claimAuthorized = true))
    assert(originalPrepared == originalClaim)
    val originalValidation =
      ValidateCelebornMapAttempt(7, 91, stageId, 0, 0, 0, originalPrepared.epoch)
    assert(coordinator.validateMapAttempt(originalValidation))

    // Another partition can claim before its peer prepares and resets the next generation.
    val concurrentClaim =
      coordinator.claimMapAttempt(ClaimCelebornMapAttempt(7, stageId, 1, 1, 0))
    assert(concurrentClaim.authorized)
    val blockedClaim = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(7, stageId, 1, 0, 0))
    assert(!blockedClaim.authorized)
    val replacementGeneration = PrepareCelebornShuffleGeneration(7, 92, stageId, 1, 2)
    val replacementClaim = coordinator.prepareGenerationAndClaim(
      replacementGeneration.copy(
        mapId = 0,
        taskAttempt = 0,
        claimEpoch = blockedClaim.epoch,
        claimAuthorized = blockedClaim.authorized))
    assert(replacementClaim.authorized)
    assert(replacementClaim.epoch > originalPrepared.epoch)

    // The reset erased the other partition's otherwise successful early authorization.
    val recoveredConcurrentClaim = coordinator.prepareGenerationAndClaim(
      replacementGeneration.copy(
        mapId = 1,
        taskAttempt = 0,
        claimEpoch = concurrentClaim.epoch,
        claimAuthorized = true))
    assert(recoveredConcurrentClaim.authorized)
    assert(recoveredConcurrentClaim.epoch == replacementClaim.epoch)
    assert(!coordinator.validateMapAttempt(originalValidation))
    assert(!coordinator.claimMapAttempt(ClaimCelebornMapAttempt(7, stageId, 0, 1, 2)).authorized)

    val replacementValidation =
      ValidateCelebornMapAttempt(7, 92, stageId, 1, 0, 0, replacementClaim.epoch)
    assert(coordinator.validateMapAttempt(replacementValidation))
    assert(
      coordinator.invalidateGeneration(
        InvalidateCelebornShuffleGeneration(7, 92, stageId, 1, replacementClaim.epoch)))
    assert(!coordinator.validateMapAttempt(replacementValidation))
    val rejectedOldClaim =
      coordinator.claimMapAttempt(ClaimCelebornMapAttempt(7, stageId, 1, 1, 1))
    assert(!rejectedOldClaim.authorized)
    assert(
      !coordinator
        .prepareGenerationAndClaim(
          replacementGeneration.copy(
            mapId = 1,
            taskAttempt = 1,
            claimEpoch = rejectedOldClaim.epoch,
            claimAuthorized = false))
        .authorized)

    val nextGeneration = PrepareCelebornShuffleGeneration(7, 93, stageId, 2, 2)
    val nextEarlyClaim =
      coordinator.claimMapAttempt(ClaimCelebornMapAttempt(7, stageId, 2, 0, 0))
    val nextPrepared = coordinator.prepareGenerationAndClaim(
      nextGeneration.copy(
        mapId = 0,
        taskAttempt = 0,
        claimEpoch = nextEarlyClaim.epoch,
        claimAuthorized = nextEarlyClaim.authorized))
    assert(nextPrepared.authorized)
    assert(
      coordinator.validateMapAttempt(
        ValidateCelebornMapAttempt(7, 93, stageId, 2, 0, 0, nextPrepared.epoch)))
  }

  test("speculation arriving first reserves its original without creating phantom owners") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val stageId = 76
    sparkCoordinator.getClass
      .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
      .invoke(sparkCoordinator, Int.box(stageId), Int.box(0))

    val speculative = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(8, stageId, 0, 0, 1))
    assert(!speculative.authorized)
    assert(!speculative.requiresGenerationResolution)
    val original = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(8, stageId, 0, 0, 0))
    assert(original.authorized)
    assert(original.epoch == speculative.epoch)

    sparkCoordinator.getClass
      .getMethod(
        "taskCompleted",
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        classOf[TaskEndReason])
      .invoke(
        sparkCoordinator,
        Int.box(stageId),
        Int.box(0),
        Int.box(0),
        Int.box(0),
        UnknownReason)

    // Spark deliberately does not mark CommitDenied attempt 1 as failed; Comet must skip it.
    val genuineRetry = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(8, stageId, 0, 0, 2))
    assert(genuineRetry.authorized)
  }

  test("a first retry can resolve a replacement generation after its original failed in input") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val stageId = 77
    sparkCoordinator.getClass
      .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
      .invoke(sparkCoordinator, Int.box(stageId), Int.box(0))

    val oldGeneration = PrepareCelebornShuffleGeneration(9, 91, stageId, 0, 1)
    assert(coordinator.prepareGeneration(oldGeneration))
    assert(coordinator.claimMapAttempt(ClaimCelebornMapAttempt(9, stageId, 0, 0, 0)).authorized)
    sparkCoordinator.getClass
      .getMethod(
        "taskCompleted",
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        classOf[TaskEndReason])
      .invoke(
        sparkCoordinator,
        Int.box(stageId),
        Int.box(1),
        Int.box(0),
        Int.box(0),
        UnknownReason)

    val retry = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(9, stageId, 1, 0, 1))
    assert(!retry.authorized)
    assert(retry.requiresGenerationResolution)

    val prepared = coordinator.prepareGenerationAndClaim(
      PrepareCelebornShuffleGeneration(
        9,
        92,
        stageId,
        1,
        1,
        mapId = 0,
        taskAttempt = 1,
        claimEpoch = retry.epoch,
        claimAuthorized = retry.authorized))
    assert(prepared.authorized)
    assert(!prepared.requiresGenerationResolution)
  }

  test("a replacement generation preserves another partition's previously reported failure") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val stageId = 80
    sparkCoordinator.getClass
      .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
      .invoke(sparkCoordinator, Int.box(stageId), Int.box(1))

    val originalGeneration = PrepareCelebornShuffleGeneration(12, 91, stageId, 0, 2)
    assert(coordinator.prepareGeneration(originalGeneration))
    assert(coordinator.claimMapAttempt(ClaimCelebornMapAttempt(12, stageId, 0, 0, 0)).authorized)

    // Partition zero fails while resolving its input, before it can ask Comet for a writer.
    sparkCoordinator.getClass
      .getMethod(
        "taskCompleted",
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        classOf[TaskEndReason])
      .invoke(
        sparkCoordinator,
        Int.box(stageId),
        Int.box(1),
        Int.box(0),
        Int.box(0),
        UnknownReason)

    // Partition one is the first replacement-stage mapper to prepare the new generation.
    val firstReplacementClaim =
      coordinator.claimMapAttempt(ClaimCelebornMapAttempt(12, stageId, 1, 1, 0))
    assert(firstReplacementClaim.authorized)
    val replacementGeneration = PrepareCelebornShuffleGeneration(12, 92, stageId, 1, 2)
    val preparedReplacement = coordinator.prepareGenerationAndClaim(
      replacementGeneration.copy(
        mapId = 1,
        taskAttempt = 0,
        claimEpoch = firstReplacementClaim.epoch,
        claimAuthorized = firstReplacementClaim.authorized))
    assert(preparedReplacement.authorized)

    // Its peer's already-failed attempt must not become a phantom commit owner after the reset.
    val retry = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(12, stageId, 1, 0, 1))
    assert(retry.authorized)
    val preparedRetry = coordinator.prepareGenerationAndClaim(
      replacementGeneration.copy(
        mapId = 0,
        taskAttempt = 1,
        claimEpoch = retry.epoch,
        claimAuthorized = retry.authorized))
    assert(preparedRetry.authorized)
    assert(preparedRetry.epoch == preparedReplacement.epoch)
  }

  test("a replacement generation cannot recreate a completed Spark stage") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val stageId = 81
    sparkCoordinator.getClass
      .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
      .invoke(sparkCoordinator, Int.box(stageId), Int.box(0))

    val originalGeneration = PrepareCelebornShuffleGeneration(13, 91, stageId, 0, 1)
    assert(coordinator.prepareGeneration(originalGeneration))
    sparkCoordinator.getClass
      .getMethod("stageEnd", java.lang.Integer.TYPE)
      .invoke(sparkCoordinator, Int.box(stageId))

    val failure = intercept[IllegalStateException] {
      coordinator.prepareGeneration(
        originalGeneration.copy(celebornShuffleId = 92, stageAttempt = 1))
    }
    assert(failure.getMessage.contains(s"stage $stageId"))
    assert(sparkCoordinator.isEmpty)
  }

  test("a replacement-stage original blocks speculation before its generation is resolved") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val stageId = 78
    sparkCoordinator.getClass
      .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
      .invoke(sparkCoordinator, Int.box(stageId), Int.box(0))

    assert(coordinator.prepareGeneration(PrepareCelebornShuffleGeneration(10, 91, stageId, 0, 1)))
    val original = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(10, stageId, 1, 0, 0))
    assert(original.authorized)

    val speculative = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(10, stageId, 1, 0, 1))
    assert(!speculative.authorized)
    assert(!speculative.requiresGenerationResolution)
  }

  test("a speculative replacement owner yields its commit to an original blocked in input") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator, _ => false)
    val stageId = 79
    sparkCoordinator.getClass
      .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
      .invoke(sparkCoordinator, Int.box(stageId), Int.box(0))

    val oldGeneration = PrepareCelebornShuffleGeneration(11, 91, stageId, 0, 1)
    assert(coordinator.prepareGeneration(oldGeneration))
    assert(coordinator.claimMapAttempt(ClaimCelebornMapAttempt(11, stageId, 0, 0, 0)).authorized)

    val speculative = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(11, stageId, 1, 0, 1))
    assert(!speculative.authorized)
    assert(speculative.requiresGenerationResolution)
    val replacementGeneration = PrepareCelebornShuffleGeneration(11, 92, stageId, 1, 1)
    val speculativeOwner = coordinator.prepareGenerationAndClaim(
      replacementGeneration.copy(
        mapId = 0,
        taskAttempt = 1,
        claimEpoch = speculative.epoch,
        claimAuthorized = speculative.authorized))
    assert(speculativeOwner.authorized)
    assert(
      coordinator.abandonMapAttempt(
        AbandonCelebornMapAttempt(11, 92, stageId, 1, 0, 1, speculativeOwner.epoch, 101L)))

    val original = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(11, stageId, 1, 0, 0))
    assert(original.authorized)
    val preparedOriginal = coordinator.prepareGenerationAndClaim(
      replacementGeneration.copy(
        mapId = 0,
        taskAttempt = 0,
        claimEpoch = original.epoch,
        claimAuthorized = original.authorized))
    assert(preparedOriginal.authorized)
    assert(
      coordinator.validateMapAttempt(
        ValidateCelebornMapAttempt(11, 92, stageId, 1, 0, 0, preparedOriginal.epoch)))
  }

  test(
    "a later stage attempt preserves commit owners when the Celeborn generation is unchanged") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val generation = PrepareCelebornShuffleGeneration(7, 91, 12, 0, 1)
    sparkCoordinator.getClass
      .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
      .invoke(sparkCoordinator, Int.box(12), Int.box(0))
    val canCommit = sparkCoordinator.getClass
      .getMethod(
        "handleAskPermissionToCommit",
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE)

    assert(coordinator.prepareGeneration(generation))
    assert(
      canCommit
        .invoke(sparkCoordinator, Int.box(12), Int.box(0), Int.box(0), Int.box(0))
        .asInstanceOf[Boolean])

    assert(coordinator.prepareGeneration(generation.copy(stageAttempt = 1)))
    assert(
      !canCommit
        .invoke(sparkCoordinator, Int.box(12), Int.box(1), Int.box(0), Int.box(1))
        .asInstanceOf[Boolean])
  }

  test("ordinary shuffle registration preserves the existing Celeborn handle and fallback") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val dependency = null.asInstanceOf[ShuffleDependency[Any, Any, Any]]

    val handle = composite.registerShuffle(31, dependency)

    assert(handle eq backend.returnedHandle)
    assert(backend.registration.contains((31, dependency)))
  }

  test("ordinary map writers and reduce readers are owned by the existing Celeborn manager") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle

    assert(composite.getWriter[Any, Any](handle, 93L, null, null) == null)
    assert(backend.writerCall.contains((handle, 93L)))

    assert(composite.getReader[Any, Any](handle, 2, 8, 3, 7, null, null) == null)
    assert(backend.readerCall.contains((handle, 2, 8, 3, 7)))
  }

  test("the inherited all-mapper reader also delegates to the existing Celeborn manager") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle

    assert(composite.getReader[Any, Any](handle, 4, 9, null, null) == null)
    assert(backend.readerCall.contains((handle, 0, Int.MaxValue, 4, 9)))
  }

  test("resolver, shuffle cleanup, and shutdown preserve existing Celeborn behavior") {
    val backend = new RecordingShuffleManager
    backend.unregisterResult = false
    val composite = manager(backend)

    assert(composite.shuffleBlockResolver == null)
    assert(backend.resolverReads == 1)

    assert(!composite.unregisterShuffle(17))
    assert(backend.unregisteredShuffleId.contains(17))

    composite.stop()
    assert(backend.stopped)
  }

  test("backend registration failures propagate without local Comet fallback") {
    val backend = new RecordingShuffleManager
    val expected = new IllegalStateException("remote shuffle registration failed")
    backend.registrationFailure = expected
    val composite = manager(backend)

    val actual = intercept[IllegalStateException] {
      composite.registerShuffle[Any, Any, Any](31, null)
    }

    assert(actual eq expected)
    assert(backend.writerCall.isEmpty)
  }

  test("native and JVM Comet handles never reach the stock Celeborn row path") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val unsupportedHandles = Seq[ShuffleHandle](
      new CometNativeShuffleHandle[Any, Any](41, null),
      new CometBypassMergeSortShuffleHandle[Any, Any](42, null),
      new CometSerializedShuffleHandle[Any, Any](43, null))

    unsupportedHandles.foreach { handle =>
      val writerFailure = intercept[UnsupportedOperationException] {
        composite.getWriter[Any, Any](handle, 0L, null, null)
      }
      assert(writerFailure.getMessage.contains("Comet shuffle over Celeborn is not supported"))

      val readerFailure = intercept[UnsupportedOperationException] {
        composite.getReader[Any, Any](handle, 0, 1, 0, 1, null, null)
      }
      assert(readerFailure.getMessage.contains("Comet shuffle over Celeborn is not supported"))
    }

    assert(backend.writerCall.isEmpty)
    assert(backend.readerCall.isEmpty)
  }

  test("a missing delegated backend fails closed without selecting local shuffle") {
    val error = intercept[IllegalStateException] {
      new CometCelebornShuffleManager(new SparkConf(false), true, (_, _) => null)
    }

    assert(error.getMessage.contains("factory returned null"))
  }

  test("the public constructor rejects an application without the Celeborn client") {
    val celebornManagerAvailable =
      try {
        getClass.getClassLoader.loadClass("org.apache.spark.shuffle.celeborn.SparkShuffleManager")
        true
      } catch {
        case _: ClassNotFoundException => false
      }

    if (celebornManagerAvailable) {
      cancel("The optional Celeborn Spark client is present on this test classpath")
    }

    val error = intercept[IllegalStateException] {
      new CometCelebornShuffleManager(new SparkConf(false), true)
    }

    assert(error.getMessage.contains("Celeborn Spark shuffle manager is not available"))
  }
}
