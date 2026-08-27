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

import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{ExecutorLostFailure, ShuffleDependency, SparkConf, TaskContext, TaskEndReason, UnknownReason}
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}

class CometCelebornShuffleManagerSuite extends AnyFunSuite {

  private class RecordingShuffleManager extends ShuffleManager {

    val returnedHandle: ShuffleHandle = new BaseShuffleHandle[Any, Any, Any](31, null)

    var registration: Option[(Int, ShuffleDependency[_, _, _])] = None
    var writerCall: Option[(ShuffleHandle, Long)] = None
    var rangedReaderCall: Option[(ShuffleHandle, Int, Int, Int, Int)] = None
    var unregisteredShuffleId: Option[Int] = None
    var unregisterResult = true
    var resolverReads = 0
    var stopped = false
    var registrationFailure: RuntimeException = _
    var writerFailure: RuntimeException = _
    var readerFailure: RuntimeException = _

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
      if (writerFailure != null) {
        throw writerFailure
      }
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
      rangedReaderCall = Some((handle, startMapIndex, endMapIndex, startPartition, endPartition))
      if (readerFailure != null) {
        throw readerFailure
      }
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

  private def startStage(
      coordinator: OutputCommitCoordinator,
      stageId: Int,
      numMappers: Int): Unit = {
    coordinator.getClass
      .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
      .invoke(coordinator, Int.box(stageId), Int.box(numMappers - 1))
  }

  private def completeFailedAttempt(
      coordinator: OutputCommitCoordinator,
      stageId: Int,
      stageAttempt: Int,
      mapId: Int,
      taskAttempt: Int,
      reason: TaskEndReason = UnknownReason): Unit = {
    coordinator.getClass
      .getMethod(
        "taskCompleted",
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        classOf[TaskEndReason])
      .invoke(
        coordinator,
        Int.box(stageId),
        Int.box(stageAttempt),
        Int.box(mapId),
        Int.box(taskAttempt),
        reason)
  }

  private def sparkCommitOwners(
      coordinator: OutputCommitCoordinator,
      stageId: Int): Array[AnyRef] = coordinator.synchronized {
    val statesField = classOf[OutputCommitCoordinator].getDeclaredField("stageStates")
    statesField.setAccessible(true)
    val state = statesField.get(coordinator).asInstanceOf[mutable.Map[Int, AnyRef]](stageId)
    state.getClass.getMethod("authorizedCommitters").invoke(state).asInstanceOf[Array[AnyRef]]
  }

  test("replacement shuffle generations invalidate stale Comet map ownership") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val stageId = 71
    startStage(sparkCoordinator, stageId, numMappers = 2)

    val first = PrepareCelebornShuffleGeneration(7, 91, stageId, 0, 2)
    assert(coordinator.prepareGeneration(first))
    val owner = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(7, stageId, 0, 0, 0))
    assert(owner.authorized)
    assert(sparkCommitOwners(sparkCoordinator, stageId).forall(_ == null))
    assert(
      coordinator.validateMapAttempt(
        ValidateCelebornMapAttempt(7, 91, stageId, 0, 0, 0, owner.epoch)))
    assert(!coordinator.claimMapAttempt(ClaimCelebornMapAttempt(7, stageId, 0, 0, 1)).authorized)

    val replacement = first.copy(celebornShuffleId = 92, stageAttempt = 1)
    assert(coordinator.prepareGeneration(replacement))
    val replacementOwner =
      coordinator.claimMapAttempt(ClaimCelebornMapAttempt(7, stageId, 1, 0, 0))
    assert(replacementOwner.authorized)
    assert(replacementOwner.epoch > owner.epoch)
    assert(
      !coordinator.validateMapAttempt(
        ValidateCelebornMapAttempt(7, 91, stageId, 0, 0, 0, owner.epoch)))
    assert(
      coordinator.validateMapAttempt(
        ValidateCelebornMapAttempt(7, 92, stageId, 1, 0, 0, replacementOwner.epoch)))
  }

  test("speculation arriving first reserves the original Spark map attempt") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val stageId = 72
    startStage(sparkCoordinator, stageId, numMappers = 1)

    val speculative = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(8, stageId, 0, 0, 1))
    assert(!speculative.authorized)
    assert(!speculative.requiresGenerationResolution)

    val original = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(8, stageId, 0, 0, 0))
    assert(original.authorized)
    assert(original.epoch == speculative.epoch)
  }

  test("abandoning an unsafe owner releases Comet admission without taking Spark commit locks") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator, _ => false)
    val stageId = 73
    startStage(sparkCoordinator, stageId, numMappers = 1)

    val generation = PrepareCelebornShuffleGeneration(9, 93, stageId, 0, 1)
    assert(coordinator.prepareGeneration(generation))
    val original = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(9, stageId, 0, 0, 0))
    assert(original.authorized)
    val prepared = coordinator.prepareGenerationAndClaim(
      generation
        .copy(mapId = 0, taskAttempt = 0, claimEpoch = original.epoch, claimAuthorized = true))
    assert(prepared.authorized)
    assert(sparkCommitOwners(sparkCoordinator, stageId).forall(_ == null))

    assert(
      coordinator.abandonMapAttempt(
        AbandonCelebornMapAttempt(9, 93, stageId, 0, 0, 0, prepared.epoch, 101L)))
    val retry = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(9, stageId, 0, 0, 1))
    assert(retry.authorized)
    assert(sparkCommitOwners(sparkCoordinator, stageId).forall(_ == null))
    assert(
      coordinator.abandonMapAttempt(
        AbandonCelebornMapAttempt(9, 93, stageId, 0, 0, 0, prepared.epoch, 101L)))
    assert(coordinator.claimMapAttempt(ClaimCelebornMapAttempt(9, stageId, 0, 0, 1)).authorized)
  }

  test("stale and recorded-failed claims cannot pass the generation invalidation gate") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    var livenessChecks = 0
    val coordinator = new CelebornShuffleGenerationCoordinator(
      sparkCoordinator,
      _ => {
        livenessChecks += 1
        true
      })
    val stageId = 77
    startStage(sparkCoordinator, stageId, numMappers = 1)
    val firstGeneration = PrepareCelebornShuffleGeneration(13, 100, stageId, 0, 1)
    assert(coordinator.prepareGeneration(firstGeneration))
    val first = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(13, stageId, 0, 0, 0))
    assert(first.authorized)

    completeFailedAttempt(sparkCoordinator, stageId, 0, 0, 0)
    assert(
      coordinator.abandonMapAttempt(
        AbandonCelebornMapAttempt(13, 100, stageId, 0, 0, 0, first.epoch, 201L)))
    assert(livenessChecks == 0)

    val replacement = firstGeneration.copy(celebornShuffleId = 101, stageAttempt = 1)
    assert(coordinator.prepareGeneration(replacement))
    val current = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(13, stageId, 1, 0, 0))
    assert(current.authorized)
    assert(
      coordinator.abandonMapAttempt(
        AbandonCelebornMapAttempt(13, 100, stageId, 0, 0, 0, first.epoch, 201L)))
    assert(livenessChecks == 0)
    assert(
      coordinator.validateMapAttempt(
        ValidateCelebornMapAttempt(13, 101, stageId, 1, 0, 0, current.epoch)))
    assert(
      !coordinator.abandonMapAttempt(
        AbandonCelebornMapAttempt(13, 101, stageId, 1, 0, 0, current.epoch, 202L)))
    assert(livenessChecks == 1)
  }

  Seq[(String, TaskEndReason)](
    "original task failure" -> UnknownReason,
    "executor loss" -> ExecutorLostFailure(
      "lost-executor",
      exitCausedByApp = false,
      reason = Some("executor disconnected before map completion"))).foreach {
    case (description, reason) =>
      test(
        s"$description releases Comet admission without becoming a Spark file-commit failure") {
        val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
        val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
        val stageId = 76
        startStage(sparkCoordinator, stageId, numMappers = 1)
        assert(
          coordinator.prepareGeneration(PrepareCelebornShuffleGeneration(12, 98, stageId, 0, 1)))
        val original = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(12, stageId, 0, 0, 0))
        assert(original.authorized)

        // Spark 3.4 calls stageFailed when taskCompleted matches an authorized file committer.
        // Keeping that slot empty also covers executor loss, where no task-side cleanup can run.
        assert(sparkCommitOwners(sparkCoordinator, stageId).forall(_ == null))
        completeFailedAttempt(sparkCoordinator, stageId, 0, 0, 0, reason)
        assert(sparkCommitOwners(sparkCoordinator, stageId).forall(_ == null))
        assert(
          !coordinator.validateMapAttempt(
            ValidateCelebornMapAttempt(12, 98, stageId, 0, 0, 0, original.epoch)))
        assert(
          !coordinator.claimMapAttempt(ClaimCelebornMapAttempt(12, stageId, 0, 0, 0)).authorized)

        val retry = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(12, stageId, 0, 0, 1))
        assert(retry.authorized)
        assert(
          !coordinator.claimMapAttempt(ClaimCelebornMapAttempt(12, stageId, 0, 0, 2)).authorized)
        assert(sparkCommitOwners(sparkCoordinator, stageId).forall(_ == null))
      }
  }

  test("replacement generations preserve another partition's previously recorded failure") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val stageId = 74
    startStage(sparkCoordinator, stageId, numMappers = 2)

    val original = PrepareCelebornShuffleGeneration(10, 94, stageId, 0, 2)
    assert(coordinator.prepareGeneration(original))
    assert(coordinator.claimMapAttempt(ClaimCelebornMapAttempt(10, stageId, 0, 0, 0)).authorized)
    completeFailedAttempt(sparkCoordinator, stageId, 1, mapId = 0, taskAttempt = 0)

    val firstReplacement =
      coordinator.claimMapAttempt(ClaimCelebornMapAttempt(10, stageId, 1, 1, 0))
    assert(firstReplacement.authorized)
    val replacement = original.copy(celebornShuffleId = 95, stageAttempt = 1)
    assert(
      coordinator
        .prepareGenerationAndClaim(
          replacement.copy(
            mapId = 1,
            taskAttempt = 0,
            claimEpoch = firstReplacement.epoch,
            claimAuthorized = true))
        .authorized)

    val failedPartitionRetry =
      coordinator.claimMapAttempt(ClaimCelebornMapAttempt(10, stageId, 1, 0, 1))
    assert(failedPartitionRetry.authorized)
  }

  test("invalidated generations reject stale owners and permit a fresh replacement") {
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val stageId = 75
    startStage(sparkCoordinator, stageId, numMappers = 1)

    val generation = PrepareCelebornShuffleGeneration(11, 96, stageId, 0, 1)
    assert(coordinator.prepareGeneration(generation))
    val owner = coordinator.claimMapAttempt(ClaimCelebornMapAttempt(11, stageId, 0, 0, 0))
    assert(owner.authorized)
    assert(
      coordinator.invalidateGeneration(
        InvalidateCelebornShuffleGeneration(11, 96, stageId, 0, owner.epoch)))
    assert(
      !coordinator.validateMapAttempt(
        ValidateCelebornMapAttempt(11, 96, stageId, 0, 0, 0, owner.epoch)))
    assert(!coordinator.prepareGeneration(generation))

    val replacement = generation.copy(celebornShuffleId = 97, stageAttempt = 1)
    assert(coordinator.prepareGeneration(replacement))
    assert(coordinator.claimMapAttempt(ClaimCelebornMapAttempt(11, stageId, 1, 0, 0)).authorized)
  }

  test("manager preserves the application Spark configuration and driver identity") {
    val conf = new SparkConf(false).set("spark.app.name", "existing-celeborn-application")
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
    assert(conf.get("spark.app.name") == "existing-celeborn-application")
  }

  test("ordinary shuffle registration preserves the delegated manager's handle") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val dependency = null.asInstanceOf[ShuffleDependency[Any, Any, Any]]

    val handle = composite.registerShuffle(31, dependency)

    assert(handle eq backend.returnedHandle)
    assert(backend.registration.contains((31, dependency)))
  }

  test("ordinary map writers are owned by the delegated Celeborn manager") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle

    assert(composite.getWriter[Any, Any](handle, 93L, null, null) == null)
    assert(backend.writerCall.contains((handle, 93L)))
  }

  test("mapper-range reads preserve the delegated Celeborn reader's exact range") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle

    assert(composite.getReader[Any, Any](handle, 2, 8, 3, 7, null, null) == null)
    assert(backend.rangedReaderCall.contains((handle, 2, 8, 3, 7)))
  }

  test("all-mapper reads delegate through Spark's inherited complete mapper range") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle

    assert(composite.getReader[Any, Any](handle, 4, 9, null, null) == null)
    assert(backend.rangedReaderCall.contains((handle, 0, Int.MaxValue, 4, 9)))
  }

  test("resolver, shuffle removal, and shutdown retain the delegated lifecycle") {
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

  test("registration failures retain their original exception without local fallback") {
    val backend = new RecordingShuffleManager
    val expected = new IllegalStateException("remote shuffle registration failed")
    backend.registrationFailure = expected

    val actual = intercept[IllegalStateException] {
      manager(backend).registerShuffle[Any, Any, Any](31, null)
    }

    assert(actual eq expected)
    assert(backend.writerCall.isEmpty)
    assert(backend.rangedReaderCall.isEmpty)
  }

  test("delegated writer and reader failures retain their original exception") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle
    val writerFailure = new IllegalStateException("remote shuffle writer failed")
    val readerFailure = new IllegalStateException("remote shuffle reader failed")
    backend.writerFailure = writerFailure
    backend.readerFailure = readerFailure

    val actualWriterFailure = intercept[IllegalStateException] {
      composite.getWriter[Any, Any](handle, 0L, null, null)
    }
    val actualAllMapperFailure = intercept[IllegalStateException] {
      composite.getReader[Any, Any](handle, 0, 1, null, null)
    }
    val actualRangedFailure = intercept[IllegalStateException] {
      composite.getReader[Any, Any](handle, 0, 1, 0, 1, null, null)
    }

    assert(actualWriterFailure eq writerFailure)
    assert(actualAllMapperFailure eq readerFailure)
    assert(actualRangedFailure eq readerFailure)
  }

  test("Comet native and JVM handles never reach Celeborn's ordinary shuffle paths") {
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
      val allMapperFailure = intercept[UnsupportedOperationException] {
        composite.getReader[Any, Any](handle, 0, 1, null, null)
      }
      val rangedFailure = intercept[UnsupportedOperationException] {
        composite.getReader[Any, Any](handle, 0, 1, 0, 1, null, null)
      }

      assert(writerFailure.getMessage.contains("Comet shuffle over Celeborn is not supported"))
      assert(allMapperFailure.getMessage.contains("Comet shuffle over Celeborn is not supported"))
      assert(rangedFailure.getMessage.contains("Comet shuffle over Celeborn is not supported"))
    }

    assert(backend.writerCall.isEmpty)
    assert(backend.rangedReaderCall.isEmpty)
  }

  test("a null delegated backend fails without selecting a local shuffle manager") {
    val failure = intercept[IllegalStateException] {
      new CometCelebornShuffleManager(new SparkConf(false), true, (_, _) => null)
    }

    assert(failure.getMessage.contains("factory returned null"))
  }

  test("a delegated backend construction failure retains its original exception") {
    val expected = new IllegalStateException("existing Celeborn manager failed to initialize")

    val actual = intercept[IllegalStateException] {
      new CometCelebornShuffleManager(new SparkConf(false), true, (_, _) => throw expected)
    }

    assert(actual eq expected)
  }

  test("the public constructor rejects an application without the optional Celeborn client") {
    val currentThread = Thread.currentThread()
    val originalLoader = currentThread.getContextClassLoader
    val missingCelebornLoader = new ClassLoader(originalLoader) {
      override protected def loadClass(name: String, resolve: Boolean): Class[_] = {
        if (name == "org.apache.spark.shuffle.celeborn.SparkShuffleManager") {
          throw new ClassNotFoundException(name)
        }
        super.loadClass(name, resolve)
      }
    }

    currentThread.setContextClassLoader(missingCelebornLoader)
    try {
      val failure = intercept[IllegalStateException] {
        new CometCelebornShuffleManager(new SparkConf(false), true)
      }

      assert(failure.getMessage.contains("Celeborn Spark shuffle manager is not available"))
      assert(failure.getCause.isInstanceOf[ClassNotFoundException])
    } finally {
      currentThread.setContextClassLoader(originalLoader)
    }
  }
}
