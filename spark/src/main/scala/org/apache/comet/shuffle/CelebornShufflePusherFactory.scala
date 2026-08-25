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

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.storage.BlockManagerId

import org.apache.comet.CometConf
import org.apache.comet.util.ClassLoaders

/** Creates task-owned Celeborn pushers using the application's existing Spark configuration. */
object CelebornShufflePusherFactory {

  private val CELEBORN_ENABLED = CometConf.COMET_SHUFFLE_CELEBORN_ENABLED
  private val IO_ENCRYPTION_ENABLED_KEY = "spark.io.encryption.enabled"
  private val SHUFFLE_MANAGER_KEY = "spark.shuffle.manager"
  private val SHUFFLE_DATA_IO_KEY = "spark.shuffle.sort.io.plugin.class"
  private val CELEBORN_MASTER_ENDPOINTS_KEY = "spark.celeborn.master.endpoints"

  private val CELEBORN_SHUFFLE_MANAGER =
    "org.apache.spark.shuffle.celeborn.SparkShuffleManager"
  private val COMET_CELEBORN_SHUFFLE_MANAGER =
    "org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager"
  private val CELEBORN_SHUFFLE_DATA_IO =
    "org.apache.spark.shuffle.celeborn.CelebornShuffleDataIO"
  private val CELEBORN_SHUFFLE_HANDLE =
    "org.apache.spark.shuffle.celeborn.CelebornShuffleHandle"
  private val CELEBORN_SPARK_UTILS = "org.apache.spark.shuffle.celeborn.SparkUtils"
  private val CELEBORN_SHUFFLE_CLIENT = "org.apache.celeborn.client.ShuffleClient"
  private val CELEBORN_CONF = "org.apache.celeborn.common.CelebornConf"
  private val CELEBORN_USER_IDENTIFIER = "org.apache.celeborn.common.identity.UserIdentifier"

  private val MAX_STAGE_ATTEMPTS = 1 << 15
  private val MAX_TASK_ATTEMPTS = 1 << 16

  /** Detects resolved Celeborn configuration while honoring an explicit application opt-out. */
  def isEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean(CELEBORN_ENABLED.key, CELEBORN_ENABLED.defaultValue.get) &&
    !conf.getBoolean(IO_ENCRYPTION_ENABLED_KEY, false) &&
    (conf
      .getOption(SHUFFLE_MANAGER_KEY)
      .exists(manager =>
        manager == CELEBORN_SHUFFLE_MANAGER || manager == COMET_CELEBORN_SHUFFLE_MANAGER) ||
      conf.getOption(SHUFFLE_DATA_IO_KEY).contains(CELEBORN_SHUFFLE_DATA_IO) ||
      conf.getOption(CELEBORN_MASTER_ENDPOINTS_KEY).exists(_.trim.nonEmpty))
  }

  /** Match Celeborn's stage/task attempt packing without depending on its Spark client jar. */
  private[shuffle] def encodeAttemptNumber(stageAttempt: Int, taskAttempt: Int): Int = {
    require(
      stageAttempt >= 0 && stageAttempt < MAX_STAGE_ATTEMPTS,
      s"Celeborn stage attempt must be between 0 and ${MAX_STAGE_ATTEMPTS - 1}: " +
        stageAttempt)
    require(
      taskAttempt >= 0 && taskAttempt < MAX_TASK_ATTEMPTS,
      s"Celeborn task attempt must be between 0 and ${MAX_TASK_ATTEMPTS - 1}: " +
        taskAttempt)

    (stageAttempt << 16) | taskAttempt
  }

  /**
   * Bind an existing Celeborn client to one Spark map-task attempt.
   *
   * The caller supplies the already-resolved Celeborn shuffle ID; it may differ from Spark's
   * shuffle ID after a stage retry. Task metadata is captured here because native callbacks can
   * execute on threads where Spark's thread-local TaskContext is unavailable.
   */
  def create(
      conf: SparkConf,
      client: AnyRef,
      celebornShuffleId: Int,
      numMappers: Int,
      numPartitions: Int,
      taskContext: TaskContext): Option[CelebornShufflePartitionPusher] = {
    if (!isEnabled(conf)) {
      None
    } else {
      val mapId = taskContext.partitionId()
      val stageAttempt = taskContext.stageAttemptNumber()
      val taskAttempt = taskContext.attemptNumber()
      val encodedAttempt = encodeAttemptNumber(stageAttempt, taskAttempt)
      val maxInFlightBytes = conf.getSizeAsBytes(
        CometConf.COMET_SHUFFLE_RSS_MAX_IN_FLIGHT_BYTES.key,
        CometConf.COMET_SHUFFLE_RSS_MAX_IN_FLIGHT_BYTES.defaultValue.get)
      require(
        maxInFlightBytes >= 76 && maxInFlightBytes <= Int.MaxValue,
        "Celeborn executor in-flight byte limit must fit three frame copies and a request header")
      Some(
        new CelebornShufflePartitionPusher(
          client,
          celebornShuffleId,
          mapId,
          encodedAttempt,
          numMappers,
          numPartitions,
          maxInFlightBytes.toInt))
    }
  }

  /**
   * Resolve the application's existing client and stage-attempt shuffle generation from its
   * handle. Ownership is recorded immediately after client acquisition so application shutdown
   * can release it even when the subsequent shuffle-generation lookup fails.
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
    if (!isEnabled(conf)) {
      throw new IllegalStateException("Celeborn shuffle is not enabled for this application")
    }

    try {
      val handleClass = ClassLoaders.loadClass(CELEBORN_SHUFFLE_HANDLE)
      if (!handleClass.isInstance(handle)) {
        throw new IllegalArgumentException(
          "Native Comet shuffle requires an actual Celeborn shuffle handle; " +
            s"the delegated manager returned ${handle.getClass.getName}")
      }

      val sparkUtilsClass = ClassLoaders.loadClass(CELEBORN_SPARK_UTILS)
      val shuffleClientClass = ClassLoaders.loadClass(CELEBORN_SHUFFLE_CLIENT)
      val celebornConfClass = ClassLoaders.loadClass(CELEBORN_CONF)
      val userIdentifierClass = ClassLoaders.loadClass(CELEBORN_USER_IDENTIFIER)
      val celebornConf =
        sparkUtilsClass.getMethod("fromSparkConf", classOf[SparkConf]).invoke(null, conf)

      def handleValue(name: String): AnyRef = handleClass.getMethod(name).invoke(handle)

      val client = acquireClient(
        shuffleClientClass
          .getMethod(
            "get",
            classOf[String],
            classOf[String],
            java.lang.Integer.TYPE,
            celebornConfClass,
            userIdentifierClass,
            classOf[Array[Byte]])
          .invoke(
            null,
            handleValue("appUniqueId"),
            handleValue("lifecycleManagerHost"),
            handleValue("lifecycleManagerPort"),
            celebornConf,
            handleValue("userIdentifier"),
            handleValue("extension"))
          .asInstanceOf[AnyRef],
        onClientAcquired)

      val stageRerunEnabled = handleValue("stageRerunEnabled").asInstanceOf[Boolean]
      if (!stageRerunEnabled) {
        throw new IllegalStateException(
          "Native Celeborn shuffle requires stage reruns to recover ambiguous map attempts")
      }

      // Celeborn installs the barrier-stage failure hook before allocating a generation.
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
          true,
          () => onShuffleGenerationInvalidated(handle.shuffleId, celebornShuffleId),
          () => onShuffleGenerationInvalidationUnsafe(handle.shuffleId, celebornShuffleId))
      }

      val numPartitions = handleValue("dependency")
        .asInstanceOf[ShuffleDependency[_, _, _]]
        .partitioner
        .numPartitions

      ResolvedCelebornShufflePusher(
        create(conf, client, celebornShuffleId, numMappers, numPartitions, taskContext).get,
        client,
        celebornShuffleId)
    } catch {
      case failure: InvocationTargetException =>
        throw new IllegalStateException(
          "Could not resolve the application-owned Celeborn shuffle client",
          Option(failure.getCause).getOrElse(failure))
      case failure: ReflectiveOperationException =>
        throw new IllegalStateException(
          "The Celeborn client does not expose the required native-shuffle handle API",
          failure)
    }
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

  /** Check task liveness on the driver before mutating Celeborn's shuffle generation. */
  def shouldReportShuffleFetchFailure(taskAttemptId: Long): Boolean =
    ClassLoaders
      .loadClass(CELEBORN_SPARK_UTILS)
      .getMethod("shouldReportShuffleFetchFailure", java.lang.Long.TYPE)
      .invoke(null, Long.box(taskAttemptId))
      .asInstanceOf[Boolean]

  /** A retry cannot safely reconstruct the lengths of an already committed Celeborn attempt. */
  private[shuffle] def rejectRetriedAttempt(
      client: AnyRef,
      sparkShuffleId: Int,
      celebornShuffleId: Int,
      taskContext: TaskContext,
      authorizedToCommit: Boolean,
      onGenerationInvalidated: () => Unit = () => (),
      onGenerationInvalidationUnsafe: () => Boolean = () => false): Unit = {
    if (!authorizedToCommit) {
      throw commitDenied(taskContext)
    }
    if (onGenerationInvalidationUnsafe()) {
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

    // Spark scopes this type private[spark] in Scala even though its JVM constructor is public.
    val fetchFailureClass =
      ClassLoaders.loadClass("org.apache.spark.shuffle.FetchFailedException")
    val fetchFailure = fetchFailureClass
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
        s"Retried Celeborn map attempt requires a new shuffle generation: " +
          s"$sparkShuffleId/$celebornShuffleId",
        null)
      .asInstanceOf[Throwable]
    throw fetchFailure
  }

  /**
   * Preserve Spark's non-counted task-failure classification for a speculative losing attempt.
   */
  def commitDenied(taskContext: TaskContext): Throwable = {
    val deniedClass = ClassLoaders.loadClass("org.apache.spark.executor.CommitDeniedException")
    deniedClass
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
  }

  /** Drop task-independent state when Spark unregisters this particular shuffle generation. */
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

  /** Release only the application-scoped client that this manager actually acquired. */
  def releaseClient(client: AnyRef): Unit = {
    try {
      val shuffleClientClass = ClassLoaders.loadClass(CELEBORN_SHUFFLE_CLIENT)
      shuffleClientClass.getMethod("removeInstance", shuffleClientClass).invoke(null, client)
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

/**
 * A task-owned pusher plus the existing application client and its resolved shuffle generation.
 */
final case class ResolvedCelebornShufflePusher(
    pusher: CelebornShufflePartitionPusher,
    client: AnyRef,
    celebornShuffleId: Int)
