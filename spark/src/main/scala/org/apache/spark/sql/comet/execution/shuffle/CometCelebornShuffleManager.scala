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

import java.lang.reflect.InvocationTargetException
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext, TaskEndReason, UnknownReason}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.util.RpcUtils

import org.apache.comet.CometConf
import org.apache.comet.shuffle.{CelebornShufflePartitionPusher, CelebornShufflePusherFactory}
import org.apache.comet.util.ClassLoaders

/**
 * Lets Comet execution coexist with the application's existing Celeborn shuffle manager.
 *
 * Ordinary Spark shuffle dependencies are owned entirely by Celeborn. Native Comet map tasks can
 * write directly to Celeborn, but query planning keeps native shuffle disabled until the matching
 * remote reader is available. JVM Comet shuffle remains unsupported.
 *
 * Celeborn is loaded reflectively because its client is an optional, application-provided
 * dependency rather than part of Comet's compile-time or runtime distribution.
 */
class CometCelebornShuffleManager private[shuffle] (
    conf: SparkConf,
    isDriver: Boolean,
    backendFactory: (SparkConf, Boolean) => ShuffleManager)
    extends ShuffleManager {

  /** Constructor selected by Spark for driver and executor shuffle managers. */
  def this(conf: SparkConf, isDriver: Boolean) =
    this(conf, isDriver, CometCelebornShuffleManager.createBackend)

  private val celebornManager = Option(backendFactory(conf, isDriver)).getOrElse {
    throw new IllegalStateException("Celeborn Spark shuffle manager factory returned null")
  }
  private val nativeShuffleClients =
    new ConcurrentHashMap[Int, ConcurrentHashMap[Int, AnyRef]]()
  private val ownedNativeClients = new ConcurrentHashMap[AnyRef, java.lang.Boolean]()
  @volatile private var nativeGenerationCoordinator: CelebornShuffleGenerationCoordinator = _
  @volatile private var nativeGenerationEndpoint: RpcEndpointRef = _

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    dependency match {
      case native: CometShuffleDependency[_, _, _] if native.shuffleType == CometNativeShuffle =>
        val handle = celebornManager.registerShuffle(shuffleId, dependency)
        if (!CometCelebornShuffleManager.isCelebornHandle(handle)) {
          try celebornManager.unregisterShuffle(shuffleId)
          catch {
            case cleanupFailure: Throwable =>
              val failure = new UnsupportedOperationException(
                "Native Comet shuffle cannot use Celeborn's local fallback writer")
              failure.addSuppressed(cleanupFailure)
              throw failure
          }
          throw new UnsupportedOperationException(
            "Native Comet shuffle cannot use Celeborn's local fallback writer")
        }
        if (isDriver) {
          initializeNativeGenerationCoordinator()
        }
        handle
      case _: CometShuffleDependency[_, _, _] => rejectCometShuffle()
      case _ => celebornManager.registerShuffle(shuffleId, dependency)
    }
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    nativeDependency(handle) match {
      case Some(dependency) =>
        val earlyClaim = claimNativeShuffleAttempt(handle.shuffleId, context)
        if (!earlyClaim.authorized && context.attemptNumber() > 0 &&
          !earlyClaim.requiresGenerationResolution) {
          throw CelebornShufflePusherFactory.commitDenied(context)
        }
        var preparedClaim = earlyClaim
        val resolved = CelebornShufflePusherFactory.createFromHandle(
          conf,
          handle,
          context,
          client => ownedNativeClients.put(client, java.lang.Boolean.TRUE),
          (celebornShuffleId, numMappers) =>
            preparedClaim = prepareNativeShuffleGeneration(
              handle.shuffleId,
              celebornShuffleId,
              numMappers,
              context,
              earlyClaim),
          (sparkShuffleId, celebornShuffleId) =>
            invalidateNativeShuffleGeneration(
              sparkShuffleId,
              celebornShuffleId,
              context,
              preparedClaim),
          (sparkShuffleId, celebornShuffleId) =>
            abandonNativeShuffleAttempt(
              sparkShuffleId,
              celebornShuffleId,
              context,
              preparedClaim))
        nativeShuffleClients
          .computeIfAbsent(handle.shuffleId, _ => new ConcurrentHashMap[Int, AnyRef]())
          .put(resolved.celebornShuffleId, resolved.client)

        new CometNativeShuffleWriter[K, V](
          dependency.nativeShuffleSpec.getOrElse {
            throw new IllegalStateException("Native Comet shuffle has no execution plan")
          },
          dependency.outputPartitioning.getOrElse {
            throw new IllegalStateException("Native Comet shuffle has no output partitioning")
          },
          dependency.outputAttributes,
          dependency.shuffleWriteMetrics,
          dependency.numParts,
          dependency.shuffleId,
          mapId,
          context,
          metrics,
          dependency.rangePartitionBounds,
          Some(
            CelebornNativeShuffleDestination(
              resolved.pusher,
              CometCelebornShuffleManager.maxNativeFrameBytes(
                CometConf.COMET_SHUFFLE_RSS_MAX_FRAME_BYTES.get().toInt,
                resolved.pusher),
              dependency.partitioner.numPartitions,
              commitAuthorized = true,
              commitValidator = () =>
                validateNativeShuffleAttempt(
                  handle.shuffleId,
                  resolved.celebornShuffleId,
                  context,
                  preparedClaim))))

      case None =>
        rejectCometHandle(handle)
        celebornManager.getWriter(handle, mapId, context, metrics)
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
    if (nativeDependency(handle).nonEmpty) {
      throw new UnsupportedOperationException(
        "Celeborn-backed Comet shuffle cannot be enabled until its native reader is available")
    }
    rejectCometHandle(handle)
    celebornManager.getReader(
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver =
    celebornManager.shuffleBlockResolver

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(nativeShuffleClients.remove(shuffleId)).foreach { generations =>
      generations.forEach { (celebornShuffleId, client) =>
        CelebornShufflePusherFactory.cleanupShuffle(client, celebornShuffleId)
      }
    }
    if (isDriver) {
      Option(nativeGenerationCoordinator).foreach(_.unregisterShuffle(shuffleId))
    }
    celebornManager.unregisterShuffle(shuffleId)
  }

  override def stop(): Unit = {
    try celebornManager.stop()
    finally {
      try {
        if (isDriver) {
          Option(nativeGenerationEndpoint).foreach { endpoint =>
            SparkEnv.get.rpcEnv.stop(endpoint)
          }
        }
      } finally {
        ownedNativeClients.keySet().asScala.foreach(CelebornShufflePusherFactory.releaseClient)
        ownedNativeClients.clear()
        nativeShuffleClients.clear()
      }
    }
  }

  private def initializeNativeGenerationCoordinator(): Unit = synchronized {
    if (nativeGenerationEndpoint == null) {
      val env = Option(SparkEnv.get).getOrElse {
        throw new IllegalStateException("Spark environment is unavailable for native shuffle")
      }
      val coordinator = new CelebornShuffleGenerationCoordinator(
        env.outputCommitCoordinator,
        CelebornShufflePusherFactory.shouldReportShuffleFetchFailure)
      val endpoint = env.rpcEnv.setupEndpoint(
        CometCelebornShuffleManager.GENERATION_COORDINATOR_ENDPOINT,
        new CelebornShuffleGenerationEndpoint(env.rpcEnv, coordinator))
      nativeGenerationCoordinator = coordinator
      nativeGenerationEndpoint = endpoint
    }
  }

  private def generationEndpoint: RpcEndpointRef =
    Option(nativeGenerationEndpoint).getOrElse {
      synchronized {
        if (nativeGenerationEndpoint == null) {
          if (isDriver) {
            initializeNativeGenerationCoordinator()
          } else {
            nativeGenerationEndpoint = RpcUtils.makeDriverRef(
              CometCelebornShuffleManager.GENERATION_COORDINATOR_ENDPOINT,
              conf,
              SparkEnv.get.rpcEnv)
          }
        }
        nativeGenerationEndpoint
      }
    }

  private def claimNativeShuffleAttempt(
      shuffleId: Int,
      taskContext: TaskContext): CelebornMapAttemptClaim =
    generationEndpoint.askSync[CelebornMapAttemptClaim](
      ClaimCelebornMapAttempt(
        shuffleId,
        taskContext.stageId(),
        taskContext.stageAttemptNumber(),
        taskContext.partitionId(),
        taskContext.attemptNumber()))

  private def prepareNativeShuffleGeneration(
      shuffleId: Int,
      celebornShuffleId: Int,
      numMappers: Int,
      taskContext: TaskContext,
      earlyClaim: CelebornMapAttemptClaim): CelebornMapAttemptClaim = {
    val prepared = generationEndpoint.askSync[CelebornMapAttemptClaim](
      PrepareCelebornShuffleGeneration(
        shuffleId,
        celebornShuffleId,
        taskContext.stageId(),
        taskContext.stageAttemptNumber(),
        numMappers,
        taskContext.partitionId(),
        taskContext.attemptNumber(),
        earlyClaim.epoch,
        earlyClaim.authorized))
    if (!prepared.authorized) {
      throw CelebornShufflePusherFactory.commitDenied(taskContext)
    }
    prepared
  }

  private def validateNativeShuffleAttempt(
      shuffleId: Int,
      celebornShuffleId: Int,
      taskContext: TaskContext,
      claim: CelebornMapAttemptClaim): Boolean =
    generationEndpoint.askSync[Boolean](
      ValidateCelebornMapAttempt(
        shuffleId,
        celebornShuffleId,
        taskContext.stageId(),
        taskContext.stageAttemptNumber(),
        taskContext.partitionId(),
        taskContext.attemptNumber(),
        claim.epoch))

  private def invalidateNativeShuffleGeneration(
      shuffleId: Int,
      celebornShuffleId: Int,
      taskContext: TaskContext,
      claim: CelebornMapAttemptClaim): Unit = {
    generationEndpoint.askSync[Boolean](
      InvalidateCelebornShuffleGeneration(
        shuffleId,
        celebornShuffleId,
        taskContext.stageId(),
        taskContext.stageAttemptNumber(),
        claim.epoch))
    ()
  }

  private def abandonNativeShuffleAttempt(
      shuffleId: Int,
      celebornShuffleId: Int,
      taskContext: TaskContext,
      claim: CelebornMapAttemptClaim): Boolean =
    generationEndpoint.askSync[Boolean](
      AbandonCelebornMapAttempt(
        shuffleId,
        celebornShuffleId,
        taskContext.stageId(),
        taskContext.stageAttemptNumber(),
        taskContext.partitionId(),
        taskContext.attemptNumber(),
        claim.epoch,
        taskContext.taskAttemptId()))

  private def nativeDependency(handle: ShuffleHandle): Option[CometShuffleDependency[_, _, _]] = {
    if (!CometCelebornShuffleManager.isCelebornHandle(handle)) {
      None
    } else {
      handle.asInstanceOf[BaseShuffleHandle[_, _, _]].dependency match {
        case dependency: CometShuffleDependency[_, _, _]
            if dependency.shuffleType == CometNativeShuffle =>
          Some(dependency)
        case _ => None
      }
    }
  }

  private def rejectCometHandle(handle: ShuffleHandle): Unit = handle match {
    case _: CometNativeShuffleHandle[_, _] => rejectCometShuffle()
    case _: CometBypassMergeSortShuffleHandle[_, _] => rejectCometShuffle()
    case _: CometSerializedShuffleHandle[_, _] => rejectCometShuffle()
    case _ =>
  }

  private def rejectCometShuffle(): Nothing = {
    throw new UnsupportedOperationException(
      "Comet shuffle over Celeborn is not supported yet; its remote writer, reader, " +
        "and task lifecycle must be integrated before Comet shuffle can be enabled")
  }
}

private[shuffle] object CometCelebornShuffleManager {

  private[shuffle] val GENERATION_COORDINATOR_ENDPOINT =
    "CometCelebornShuffleGenerationCoordinator"
  private val CELEBORN_MANAGER_CLASS = "org.apache.spark.shuffle.celeborn.SparkShuffleManager"
  private val CELEBORN_HANDLE_CLASS = "org.apache.spark.shuffle.celeborn.CelebornShuffleHandle"

  private[shuffle] def maxNativeFrameBytes(
      configuredMaxFrameBytes: Int,
      pusher: CelebornShufflePartitionPusher): Int =
    math.min(configuredMaxFrameBytes, pusher.maxFrameBytes())

  private[shuffle] def isCelebornHandle(handle: ShuffleHandle): Boolean =
    handle != null && handle.getClass.getName == CELEBORN_HANDLE_CLASS

  private[shuffle] def createBackend(conf: SparkConf, isDriver: Boolean): ShuffleManager = {
    try {
      val managerClass = ClassLoaders.loadClass(CELEBORN_MANAGER_CLASS)
      if (!classOf[ShuffleManager].isAssignableFrom(managerClass)) {
        throw new IllegalStateException(
          "Celeborn Spark shuffle manager does not implement ShuffleManager: " +
            CELEBORN_MANAGER_CLASS)
      }

      val constructor = managerClass.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
      constructor.newInstance(conf, Boolean.box(isDriver)).asInstanceOf[ShuffleManager]
    } catch {
      case error: ClassNotFoundException =>
        throw new IllegalStateException(
          s"Celeborn Spark shuffle manager is not available: $CELEBORN_MANAGER_CLASS. " +
            "Ensure the Celeborn Spark client is present on the application classpath",
          error)
      case error: InvocationTargetException =>
        throw new IllegalStateException(
          s"Could not initialize Celeborn Spark shuffle manager: $CELEBORN_MANAGER_CLASS",
          Option(error.getCause).getOrElse(error))
      case error: ReflectiveOperationException =>
        throw new IllegalStateException(
          s"Could not construct Celeborn Spark shuffle manager: $CELEBORN_MANAGER_CLASS",
          error)
      case error: LinkageError =>
        throw new IllegalStateException(
          s"Could not load Celeborn Spark shuffle manager: $CELEBORN_MANAGER_CLASS",
          error)
    }
  }
}

private[shuffle] final case class PrepareCelebornShuffleGeneration(
    shuffleId: Int,
    celebornShuffleId: Int,
    stageId: Int,
    stageAttempt: Int,
    numMappers: Int,
    mapId: Int = -1,
    taskAttempt: Int = -1,
    claimEpoch: Long = -1L,
    claimAuthorized: Boolean = false)
    extends Serializable

private[shuffle] final case class ClaimCelebornMapAttempt(
    shuffleId: Int,
    stageId: Int,
    stageAttempt: Int,
    mapId: Int,
    taskAttempt: Int)
    extends Serializable

private[shuffle] final case class CelebornMapAttemptClaim(
    authorized: Boolean,
    epoch: Long,
    requiresGenerationResolution: Boolean = false)
    extends Serializable

private[shuffle] final case class ValidateCelebornMapAttempt(
    shuffleId: Int,
    celebornShuffleId: Int,
    stageId: Int,
    stageAttempt: Int,
    mapId: Int,
    taskAttempt: Int,
    claimEpoch: Long)
    extends Serializable

private[shuffle] final case class InvalidateCelebornShuffleGeneration(
    shuffleId: Int,
    celebornShuffleId: Int,
    stageId: Int,
    stageAttempt: Int,
    claimEpoch: Long)
    extends Serializable

private[shuffle] final case class AbandonCelebornMapAttempt(
    shuffleId: Int,
    celebornShuffleId: Int,
    stageId: Int,
    stageAttempt: Int,
    mapId: Int,
    taskAttempt: Int,
    claimEpoch: Long,
    taskAttemptId: Long)
    extends Serializable

/** Keeps Spark's driver-owned commit authorization aligned with Celeborn shuffle generations. */
private[shuffle] final class CelebornShuffleGenerationCoordinator(
    outputCommitCoordinator: OutputCommitCoordinator,
    shouldReportShuffleFetchFailure: Long => Boolean = _ => true) {

  private val generations = mutable.HashMap.empty[Int, PrepareCelebornShuffleGeneration]
  private val invalidatedGenerations = mutable.HashSet.empty[Int]
  private val generationEpochs = mutable.HashMap.empty[Int, Long]
  private val claimOwners =
    mutable.HashMap.empty[(Int, Int, Int, Int), (Int, Long)]
  private val deniedAttempts =
    mutable.HashMap.empty[(Int, Int, Int, Int), mutable.HashSet[Int]]
  private val authorizeCommit = outputCommitCoordinator.getClass.getMethod(
    "handleAskPermissionToCommit",
    java.lang.Integer.TYPE,
    java.lang.Integer.TYPE,
    java.lang.Integer.TYPE,
    java.lang.Integer.TYPE)

  private def currentEpoch(shuffleId: Int): Long = generationEpochs.getOrElse(shuffleId, 0L)

  private def ownerKey(
      shuffleId: Int,
      stageId: Int,
      stageAttempt: Int,
      mapId: Int): (Int, Int, Int, Int) =
    (shuffleId, stageId, stageAttempt, mapId)

  private def authorize(
      shuffleId: Int,
      stageId: Int,
      stageAttempt: Int,
      mapId: Int,
      taskAttempt: Int): CelebornMapAttemptClaim = {
    val epoch = currentEpoch(shuffleId)
    val authorized = authorizeCommit
      .invoke(
        outputCommitCoordinator,
        Int.box(stageId),
        Int.box(stageAttempt),
        Int.box(mapId),
        Int.box(taskAttempt))
      .asInstanceOf[Boolean]
    if (authorized) {
      claimOwners.update(ownerKey(shuffleId, stageId, stageAttempt, mapId), (taskAttempt, epoch))
    }
    CelebornMapAttemptClaim(authorized, epoch)
  }

  private def invalidateOwners(shuffleId: Int): Unit = {
    generationEpochs.update(shuffleId, currentEpoch(shuffleId) + 1L)
    claimOwners.filterInPlace { case ((ownerShuffleId, _, _, _), _) =>
      ownerShuffleId != shuffleId
    }
    deniedAttempts.filterInPlace { case ((ownerShuffleId, _, _, _), _) =>
      ownerShuffleId != shuffleId
    }
  }

  def claimMapAttempt(claim: ClaimCelebornMapAttempt): CelebornMapAttemptClaim = synchronized {
    val previousGeneration = generations.get(claim.shuffleId)
    val stale = previousGeneration.exists { generation =>
      generation.stageId == claim.stageId &&
      (generation.stageAttempt > claim.stageAttempt ||
        (generation.stageAttempt == claim.stageAttempt &&
          invalidatedGenerations.contains(claim.shuffleId)))
    }
    if (stale) {
      CelebornMapAttemptClaim(false, currentEpoch(claim.shuffleId))
    } else {
      val key = ownerKey(claim.shuffleId, claim.stageId, claim.stageAttempt, claim.mapId)
      val epoch = currentEpoch(claim.shuffleId)
      if (claimOwners.get(key).contains((claim.taskAttempt, epoch))) {
        return CelebornMapAttemptClaim(true, epoch)
      }

      // ShuffleMapTask resolves its input iterator before asking for a writer. Reserve an
      // unfailed lower-numbered attempt even when a speculative copy reaches the manager first.
      // Spark does not remember TaskCommitDenied attempts as failures, so exclude ones that this
      // coordinator has already rejected; otherwise they become permanent phantom owners.
      val alreadyDenied = deniedAttempts.getOrElse(key, mutable.HashSet.empty[Int])
      var candidate = 0
      while (candidate < claim.taskAttempt) {
        if (!alreadyDenied.contains(candidate) &&
          !claimOwners.get(key).contains((candidate, epoch))) {
          val earlier =
            authorize(claim.shuffleId, claim.stageId, claim.stageAttempt, claim.mapId, candidate)
          if (earlier.authorized) {
            deniedAttempts.getOrElseUpdate(key, mutable.HashSet.empty[Int]).add(claim.taskAttempt)
            return CelebornMapAttemptClaim(false, epoch)
          }
        }
        candidate += 1
      }

      val result = authorize(
        claim.shuffleId,
        claim.stageId,
        claim.stageAttempt,
        claim.mapId,
        claim.taskAttempt)
      if (!result.authorized && claim.taskAttempt > 0) {
        val requiresResolution = previousGeneration.exists { generation =>
          generation.stageId == claim.stageId && generation.stageAttempt < claim.stageAttempt
        } && !claimOwners.get(key).exists { case (_, ownerEpoch) => ownerEpoch == epoch }
        if (!requiresResolution) {
          deniedAttempts.getOrElseUpdate(key, mutable.HashSet.empty[Int]).add(claim.taskAttempt)
        }
        result.copy(requiresGenerationResolution = requiresResolution)
      } else {
        result
      }
    }
  }

  def prepareGeneration(generation: PrepareCelebornShuffleGeneration): Boolean = synchronized {
    require(generation.numMappers > 0, "Celeborn shuffle mapper count must be positive")

    generations.get(generation.shuffleId) match {
      case Some(previous)
          if invalidatedGenerations.contains(generation.shuffleId) &&
            previous.celebornShuffleId == generation.celebornShuffleId =>
        false
      case Some(previous)
          if previous.stageId == generation.stageId &&
            previous.stageAttempt > generation.stageAttempt =>
        false
      case Some(previous)
          if previous.stageId == generation.stageId &&
            previous.stageAttempt == generation.stageAttempt =>
        previous.celebornShuffleId == generation.celebornShuffleId
      case Some(previous) if previous.celebornShuffleId != generation.celebornShuffleId =>
        // Spark scopes these lifecycle methods private[scheduler] despite public JVM methods.
        outputCommitCoordinator.synchronized {
          outputCommitCoordinator.getClass
            .getMethod("stageEnd", java.lang.Integer.TYPE)
            .invoke(outputCommitCoordinator, Int.box(generation.stageId))
          outputCommitCoordinator.getClass
            .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
            .invoke(
              outputCommitCoordinator,
              Int.box(generation.stageId),
              Int.box(generation.numMappers - 1))
        }
        invalidateOwners(generation.shuffleId)
        invalidatedGenerations.remove(generation.shuffleId)
        generations.update(generation.shuffleId, generation)
        true
      case Some(_) =>
        generations.update(generation.shuffleId, generation)
        true
      case None =>
        generations.update(generation.shuffleId, generation)
        true
    }
  }

  def prepareGenerationAndClaim(
      generation: PrepareCelebornShuffleGeneration): CelebornMapAttemptClaim = synchronized {
    if (!prepareGeneration(generation)) {
      deniedAttempts
        .getOrElseUpdate(
          ownerKey(
            generation.shuffleId,
            generation.stageId,
            generation.stageAttempt,
            generation.mapId),
          mutable.HashSet.empty[Int])
        .add(generation.taskAttempt)
      return CelebornMapAttemptClaim(false, currentEpoch(generation.shuffleId))
    }

    val epoch = currentEpoch(generation.shuffleId)
    val expectedOwner = claimOwners.get(
      ownerKey(
        generation.shuffleId,
        generation.stageId,
        generation.stageAttempt,
        generation.mapId))
    if (generation.claimAuthorized && generation.claimEpoch == epoch &&
      expectedOwner.contains((generation.taskAttempt, epoch))) {
      CelebornMapAttemptClaim(true, epoch)
    } else {
      val claim = authorize(
        generation.shuffleId,
        generation.stageId,
        generation.stageAttempt,
        generation.mapId,
        generation.taskAttempt)
      if (!claim.authorized) {
        deniedAttempts
          .getOrElseUpdate(
            ownerKey(
              generation.shuffleId,
              generation.stageId,
              generation.stageAttempt,
              generation.mapId),
            mutable.HashSet.empty[Int])
          .add(generation.taskAttempt)
      }
      claim
    }
  }

  def validateMapAttempt(validation: ValidateCelebornMapAttempt): Boolean = synchronized {
    generations.get(validation.shuffleId).exists { generation =>
      !invalidatedGenerations.contains(validation.shuffleId) &&
      generation.celebornShuffleId == validation.celebornShuffleId &&
      generation.stageId == validation.stageId &&
      generation.stageAttempt == validation.stageAttempt &&
      currentEpoch(validation.shuffleId) == validation.claimEpoch &&
      claimOwners
        .get(
          ownerKey(
            validation.shuffleId,
            validation.stageId,
            validation.stageAttempt,
            validation.mapId))
        .contains((validation.taskAttempt, validation.claimEpoch))
    }
  }

  def invalidateGeneration(invalidation: InvalidateCelebornShuffleGeneration): Boolean =
    synchronized {
      val current = generations.get(invalidation.shuffleId).exists { generation =>
        generation.celebornShuffleId == invalidation.celebornShuffleId &&
        generation.stageId == invalidation.stageId &&
        generation.stageAttempt == invalidation.stageAttempt &&
        currentEpoch(invalidation.shuffleId) == invalidation.claimEpoch
      }
      if (current) {
        invalidatedGenerations.add(invalidation.shuffleId)
        invalidateOwners(invalidation.shuffleId)
      }
      current
    }

  def abandonMapAttempt(abandoned: AbandonCelebornMapAttempt): Boolean = synchronized {
    val current = validateMapAttempt(
      ValidateCelebornMapAttempt(
        abandoned.shuffleId,
        abandoned.celebornShuffleId,
        abandoned.stageId,
        abandoned.stageAttempt,
        abandoned.mapId,
        abandoned.taskAttempt,
        abandoned.claimEpoch))
    val abandon = current && !shouldReportShuffleFetchFailure(abandoned.taskAttemptId)
    if (abandon) {
      outputCommitCoordinator.getClass
        .getMethod(
          "taskCompleted",
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          classOf[TaskEndReason])
        .invoke(
          outputCommitCoordinator,
          Int.box(abandoned.stageId),
          Int.box(abandoned.stageAttempt),
          Int.box(abandoned.mapId),
          Int.box(abandoned.taskAttempt),
          UnknownReason)
      val key =
        ownerKey(abandoned.shuffleId, abandoned.stageId, abandoned.stageAttempt, abandoned.mapId)
      claimOwners.remove(key)
      deniedAttempts.getOrElseUpdate(key, mutable.HashSet.empty[Int]).add(abandoned.taskAttempt)
    }
    abandon
  }

  def unregisterShuffle(shuffleId: Int): Unit = synchronized {
    generations.remove(shuffleId)
    invalidatedGenerations.remove(shuffleId)
    generationEpochs.remove(shuffleId)
    claimOwners.filterInPlace { case ((ownerShuffleId, _, _, _), _) =>
      ownerShuffleId != shuffleId
    }
    deniedAttempts.filterInPlace { case ((ownerShuffleId, _, _, _), _) =>
      ownerShuffleId != shuffleId
    }
  }
}

private[shuffle] final class CelebornShuffleGenerationEndpoint(
    override val rpcEnv: RpcEnv,
    coordinator: CelebornShuffleGenerationCoordinator)
    extends ThreadSafeRpcEndpoint {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case claim: ClaimCelebornMapAttempt =>
      context.reply(coordinator.claimMapAttempt(claim))
    case generation: PrepareCelebornShuffleGeneration if generation.mapId < 0 =>
      context.reply(coordinator.prepareGeneration(generation))
    case generation: PrepareCelebornShuffleGeneration =>
      context.reply(coordinator.prepareGenerationAndClaim(generation))
    case validation: ValidateCelebornMapAttempt =>
      context.reply(coordinator.validateMapAttempt(validation))
    case invalidation: InvalidateCelebornShuffleGeneration =>
      context.reply(coordinator.invalidateGeneration(invalidation))
    case abandoned: AbandonCelebornMapAttempt =>
      context.reply(coordinator.abandonMapAttempt(abandoned))
  }
}
