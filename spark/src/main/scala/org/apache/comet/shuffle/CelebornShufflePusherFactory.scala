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
import java.lang.reflect.InvocationTargetException
import java.util.Optional

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.storage.BlockManagerId

import org.apache.comet.CometConf
import org.apache.comet.util.ClassLoaders

/** Resolves an application's optional Celeborn client without adding a Celeborn dependency. */
object CelebornShufflePusherFactory {

  private val CELEBORN_SHUFFLE_HANDLE = "org.apache.spark.shuffle.celeborn.CelebornShuffleHandle"
  private val CELEBORN_SPARK_UTILS = "org.apache.spark.shuffle.celeborn.SparkUtils"
  private val CELEBORN_SPARK_COMMON_UTILS = "org.apache.spark.shuffle.celeborn.SparkCommonUtils"
  private val CELEBORN_SHUFFLE_CLIENT = "org.apache.celeborn.client.ShuffleClient"
  private val CELEBORN_CONF = "org.apache.celeborn.common.CelebornConf"
  private val CELEBORN_USER_IDENTIFIER = "org.apache.celeborn.common.identity.UserIdentifier"
  private val MAX_STAGE_ATTEMPTS = 1 << 15
  private val MAX_TASK_ATTEMPTS = 1 << 16

  private[shuffle] def encodeAttemptNumber(stageAttempt: Int, taskAttempt: Int): Int = {
    require(
      stageAttempt >= 0 && stageAttempt < MAX_STAGE_ATTEMPTS,
      s"Celeborn stage attempt must be between 0 and ${MAX_STAGE_ATTEMPTS - 1}: $stageAttempt")
    require(
      taskAttempt >= 0 && taskAttempt < MAX_TASK_ATTEMPTS,
      s"Celeborn task attempt must be between 0 and ${MAX_TASK_ATTEMPTS - 1}: $taskAttempt")
    (stageAttempt << 16) | taskAttempt
  }

  /** Bind one already-resolved Celeborn generation to one Spark map attempt. */
  def create(
      conf: SparkConf,
      client: AnyRef,
      celebornShuffleId: Int,
      numMappers: Int,
      numPartitions: Int,
      taskContext: TaskContext): CelebornShufflePartitionPusher = {
    requireUnencryptedNativeShuffle(conf)
    val frameEntry = CometConf.COMET_SHUFFLE_RSS_MAX_FRAME_BYTES
    val maxFrameBytes = conf.getSizeAsBytes(frameEntry.key, frameEntry.defaultValue.get.toString)
    require(
      maxFrameBytes >= 20 && maxFrameBytes <= Int.MaxValue - 16,
      "Celeborn frame bytes must fit a complete Comet frame and a request header")
    val limitEntry = CometConf.COMET_SHUFFLE_RSS_MAX_IN_FLIGHT_BYTES
    val maxInFlightBytes =
      conf.getSizeAsBytes(limitEntry.key, limitEntry.defaultValue.get.toString)
    require(
      maxInFlightBytes >= 76 && maxInFlightBytes <= Int.MaxValue,
      "Celeborn executor in-flight bytes must fit three complete frames and a request header")

    new CelebornShufflePartitionPusher(
      client,
      celebornShuffleId,
      taskContext.partitionId(),
      encodeAttemptNumber(taskContext.stageAttemptNumber(), taskContext.attemptNumber()),
      numMappers,
      numPartitions,
      maxFrameBytes.toInt,
      maxInFlightBytes.toInt)
  }

  /**
   * Reuse the Celeborn client described by Spark's actual Celeborn handle. The acquisition hook
   * runs before generation resolution so manager shutdown can release clients even if setup
   * fails.
   */
  def createFromHandle(
      conf: SparkConf,
      handle: ShuffleHandle,
      taskContext: TaskContext,
      onClientAcquired: AnyRef => Unit,
      onShuffleGenerationResolved: (Int, Int) => Unit,
      onShuffleGenerationInvalidated: (Int, Int) => Unit = (_, _) => (),
      onShuffleGenerationInvalidationUnsafe: (Int, Int) => Boolean = (_, _) => false)
      : ResolvedCelebornShufflePusher = {
    requireUnencryptedNativeShuffle(conf)
    try {
      val handleClass = ClassLoaders.loadClass(CELEBORN_SHUFFLE_HANDLE)
      require(
        handleClass.isInstance(handle),
        "Native Comet shuffle requires an actual Celeborn shuffle handle; " +
          s"received ${handle.getClass.getName}")

      val sparkUtilsClass = ClassLoaders.loadClass(CELEBORN_SPARK_UTILS)
      val shuffleClientClass = ClassLoaders.loadClass(CELEBORN_SHUFFLE_CLIENT)
      val celebornConfClass = ClassLoaders.loadClass(CELEBORN_CONF)
      val userIdentifierClass = ClassLoaders.loadClass(CELEBORN_USER_IDENTIFIER)
      val celebornConf =
        sparkUtilsClass.getMethod("fromSparkConf", classOf[SparkConf]).invoke(null, conf)

      def handleValue(name: String): AnyRef = handleClass.getMethod(name).invoke(handle)

      val client = acquireClient(
        resolveClient(
          conf,
          shuffleClientClass,
          celebornConfClass,
          userIdentifierClass,
          Array[AnyRef](
            handleValue("appUniqueId"),
            handleValue("lifecycleManagerHost"),
            handleValue("lifecycleManagerPort"),
            celebornConf,
            handleValue("userIdentifier"),
            handleValue("extension")),
          sparkConf =>
            ClassLoaders
              .loadClass(CELEBORN_SPARK_COMMON_UTILS)
              .getMethod("getCryptoHandler", classOf[SparkConf])
              .invoke(null, sparkConf)
              .asInstanceOf[Optional[_]]),
        onClientAcquired)

      if (!handleValue("stageRerunEnabled").asInstanceOf[Boolean]) {
        throw new IllegalStateException(
          "Native Celeborn shuffle requires stage reruns to recover ambiguous map attempts")
      }

      registerBarrierFailureListener(
        sparkUtilsClass,
        shuffleClientClass,
        handleClass,
        client,
        taskContext,
        handle)

      val celebornShuffleId = sparkUtilsClass
        .getMethod(
          "celebornShuffleId",
          shuffleClientClass,
          handleClass,
          classOf[TaskContext],
          classOf[java.lang.Boolean])
        .invoke(null, client, handle, taskContext, java.lang.Boolean.TRUE)
        .asInstanceOf[Int]
      val numMappers = handleValue("numMappers").asInstanceOf[Int]
      onShuffleGenerationResolved(celebornShuffleId, numMappers)

      if (taskContext.attemptNumber() > 0) {
        rejectRetriedAttempt(
          client,
          handle.shuffleId,
          celebornShuffleId,
          taskContext,
          authorizedToCommit = true,
          () => onShuffleGenerationInvalidated(handle.shuffleId, celebornShuffleId),
          () => onShuffleGenerationInvalidationUnsafe(handle.shuffleId, celebornShuffleId))
      }

      val numPartitions = handleValue("dependency")
        .asInstanceOf[ShuffleDependency[_, _, _]]
        .partitioner
        .numPartitions
      ResolvedCelebornShufflePusher(
        create(conf, client, celebornShuffleId, numMappers, numPartitions, taskContext),
        client,
        celebornShuffleId)
    } catch {
      case failure: InvocationTargetException =>
        throw new IllegalStateException(
          "Could not resolve the application-owned Celeborn shuffle client",
          Option(failure.getCause).getOrElse(failure))
      case failure: ReflectiveOperationException =>
        throw new IllegalStateException(
          "The Celeborn client does not expose the required native shuffle handle API",
          failure)
    }
  }

  private def requireUnencryptedNativeShuffle(conf: SparkConf): Unit = {
    require(
      !conf.getBoolean("spark.io.encryption.enabled", false),
      "Encrypted native Celeborn shuffle is not supported by bounded push admission; " +
        "use ordinary Spark shuffle when spark.io.encryption.enabled is true")
  }

  /** Preserve Spark's shuffle encryption when Celeborn exposes its crypto-aware client API. */
  private[shuffle] def resolveClient(
      conf: SparkConf,
      shuffleClientClass: Class[_],
      celebornConfClass: Class[_],
      userIdentifierClass: Class[_],
      arguments: Array[AnyRef],
      resolveCryptoHandler: SparkConf => Optional[_]): AnyRef = {
    val argumentTypes: Array[Class[_]] = Array(
      classOf[String],
      classOf[String],
      java.lang.Integer.TYPE,
      celebornConfClass,
      userIdentifierClass,
      classOf[Array[Byte]])

    val cryptoAwareMethod =
      try {
        Some(shuffleClientClass.getMethod("get", (argumentTypes :+ classOf[Optional[_]]): _*))
      } catch {
        case _: NoSuchMethodException => None
      }

    cryptoAwareMethod
      .map { method =>
        method.invoke(null, (arguments :+ resolveCryptoHandler(conf)): _*)
      }
      .getOrElse(
        shuffleClientClass.getMethod("get", argumentTypes: _*).invoke(null, arguments: _*))
      .asInstanceOf[AnyRef]
  }

  private[shuffle] def acquireClient(
      resolveClient: => AnyRef,
      onClientAcquired: AnyRef => Unit): AnyRef = {
    val client = resolveClient
    onClientAcquired(client)
    client
  }

  private[shuffle] def registerBarrierFailureListener(
      sparkUtilsClass: Class[_],
      shuffleClientClass: Class[_],
      handleClass: Class[_],
      client: AnyRef,
      taskContext: TaskContext,
      handle: ShuffleHandle): Unit = {
    sparkUtilsClass
      .getMethod(
        "addFailureListenerIfBarrierTask",
        shuffleClientClass,
        classOf[TaskContext],
        handleClass)
      .invoke(null, client, taskContext, handle)
  }

  /** Check driver-side task liveness before invalidating a Celeborn generation. */
  def shouldReportShuffleFetchFailure(taskAttemptId: Long): Boolean =
    ClassLoaders
      .loadClass(CELEBORN_SPARK_UTILS)
      .getMethod("shouldReportShuffleFetchFailure", java.lang.Long.TYPE)
      .invoke(null, Long.box(taskAttemptId))
      .asInstanceOf[Boolean]

  private[shuffle] def rejectRetriedAttempt(
      client: AnyRef,
      sparkShuffleId: Int,
      celebornShuffleId: Int,
      taskContext: TaskContext,
      authorizedToCommit: Boolean,
      onGenerationInvalidated: () => Unit = () => (),
      onGenerationInvalidationUnsafe: () => Boolean = () => false): Unit = {
    if (!authorizedToCommit || onGenerationInvalidationUnsafe()) {
      throw commitDenied(taskContext)
    }

    val invalidated = client.getClass
      .getMethod(
        "reportShuffleFetchFailure",
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Long.TYPE)
      .invoke(
        client,
        Int.box(sparkShuffleId),
        Int.box(celebornShuffleId),
        Long.box(taskContext.taskAttemptId()))
      .asInstanceOf[Boolean]

    if (!invalidated) {
      throw new IOException(
        "Could not invalidate the Celeborn shuffle generation for a retried map attempt")
    }
    onGenerationInvalidated()

    val failure = ClassLoaders
      .loadClass("org.apache.spark.shuffle.FetchFailedException")
      .getConstructor(
        classOf[BlockManagerId],
        java.lang.Integer.TYPE,
        java.lang.Long.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        classOf[String],
        classOf[Throwable])
      .newInstance(
        null,
        Int.box(sparkShuffleId),
        Long.box(-1L),
        Int.box(-1),
        Int.box(-1),
        "Retried Celeborn map attempt requires a new shuffle generation: " +
          s"$sparkShuffleId/$celebornShuffleId",
        null)
      .asInstanceOf[Throwable]
    throw failure
  }

  /** Preserve Spark's non-counted failure classification for speculative losers. */
  def commitDenied(taskContext: TaskContext): Throwable =
    ClassLoaders
      .loadClass("org.apache.spark.executor.CommitDeniedException")
      .getConstructor(
        classOf[String],
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE,
        java.lang.Integer.TYPE)
      .newInstance(
        "Another Spark map attempt already owns the Celeborn shuffle commit",
        Int.box(taskContext.stageId()),
        Int.box(taskContext.partitionId()),
        Int.box(taskContext.attemptNumber()))
      .asInstanceOf[Throwable]

  /** Remove task-independent state for one unregistered Celeborn generation. */
  def cleanupShuffle(client: AnyRef, celebornShuffleId: Int): Unit = {
    try {
      client.getClass
        .getMethod("cleanupShuffle", java.lang.Integer.TYPE)
        .invoke(client, Int.box(celebornShuffleId))
    } catch {
      case failure: InvocationTargetException =>
        throw new IllegalStateException(
          "Could not clean up the Celeborn shuffle generation",
          Option(failure.getCause).getOrElse(failure))
      case failure: ReflectiveOperationException =>
        throw new IllegalStateException("Celeborn shuffle cleanup is unavailable", failure)
    }
  }

  /** Release only application clients that this composite manager actually acquired. */
  def releaseClient(client: AnyRef): Unit = {
    try {
      val shuffleClientClass = ClassLoaders.loadClass(CELEBORN_SHUFFLE_CLIENT)
      try {
        shuffleClientClass.getMethod("removeInstance", shuffleClientClass).invoke(null, client)
      } catch {
        case _: NoSuchMethodException =>
        // Celeborn 0.6 owns one shared application client and cannot remove a single instance.
        // Its global reset would also invalidate ordinary Spark shuffle, so leave it untouched.
      }
    } catch {
      case failure: InvocationTargetException =>
        throw new IllegalStateException(
          "Could not release the application-owned Celeborn shuffle client",
          Option(failure.getCause).getOrElse(failure))
      case failure: ReflectiveOperationException =>
        throw new IllegalStateException("Celeborn shuffle client cleanup is unavailable", failure)
    } finally {
      ExecutorShufflePushAdmission.releaseClient(client)
    }
  }
}

/** A task-owned pusher, its application-owned client, and the resolved shuffle generation. */
final case class ResolvedCelebornShufflePusher(
    pusher: CelebornShufflePartitionPusher,
    client: AnyRef,
    celebornShuffleId: Int)
