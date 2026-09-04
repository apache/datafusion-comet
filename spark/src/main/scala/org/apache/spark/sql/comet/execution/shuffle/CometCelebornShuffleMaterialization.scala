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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{CanAwait, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.{FutureAction, MapOutputStatistics, ShuffleDependency, SparkException}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.util.ThreadUtils

/**
 * Driver-owned materialization of one native Celeborn shuffle. Storage is chosen before any
 * downstream RDD can depend on its output. A size-limit failure cancels the remote map-stage job
 * and materializes a fresh local dependency; its shuffle and stage IDs isolate all late remote
 * task completions from the replacement. Once output is published, its destination is fixed.
 */
private[shuffle] final class CometCelebornShuffleMaterialization[K, V, C](
    remoteDependency: CometShuffleDependency[K, V, C],
    manager: CometCelebornShuffleManager)
    extends FutureAction[MapOutputStatistics] {

  import CometCelebornShuffleMaterialization._

  private val sparkContext = remoteDependency.rdd.context
  private val localProperties = sparkContext.getLocalProperties.clone().asInstanceOf[Properties]
  private val contextClassLoader = Thread.currentThread().getContextClassLoader
  private val completion = Promise[MapOutputStatistics]()
  private val actions = ArrayBuffer.empty[FutureAction[MapOutputStatistics]]
  private var state: State = RunningRemote
  private var activeAction: Option[FutureAction[MapOutputStatistics]] = None
  private var remoteAction: Option[FutureAction[MapOutputStatistics]] = None
  private var remoteCancellationIssued = false
  private var selected: Option[CometShuffleDependency[K, V, C]] = None

  private val jobListener = new SparkListener {
    override def onJobStart(event: SparkListenerJobStart): Unit = {
      localJobStarted(event.jobId)
    }
  }
  sparkContext.addSparkListener(jobListener)
  try {
    manager.registerSizeLimitFallback(remoteDependency.shuffleId, () => requestLocalShuffle())
    submit(remoteDependency, RunningRemote)
  } catch {
    case NonFatal(failure) =>
      removeCallbacks()
      throw failure
  }

  private def withCapturedProperties[T](body: => T): T = {
    val thread = Thread.currentThread()
    val previousProperties = sparkContext.getLocalProperties
    val previousClassLoader = thread.getContextClassLoader
    sparkContext.setLocalProperties(localProperties.clone().asInstanceOf[Properties])
    thread.setContextClassLoader(contextClassLoader)
    try body
    finally {
      thread.setContextClassLoader(previousClassLoader)
      sparkContext.setLocalProperties(previousProperties)
    }
  }

  private def submit(dependency: CometShuffleDependency[K, V, C], expected: State): Boolean = {
    try {
      val action = synchronized {
        if (state != expected) return false
        // Keep submission and activeAction assignment atomic with cancellation and a size report.
        // submitMapStage queues the job without waiting for executor tasks to finish.
        val submitted = withCapturedProperties(sparkContext.submitMapStage(dependency))
        activeAction = Some(submitted)
        if (expected == RunningRemote) remoteAction = Some(submitted)
        actions += submitted
        submitted
      }
      action.onComplete(result => finish(dependency, expected, result))(ExecutionContext.global)
      true
    } catch {
      case NonFatal(failure) =>
        fail(expected, failure)
        cancelActions(failure)
        false
    }
  }

  private def finish(
      dependency: CometShuffleDependency[K, V, C],
      expected: State,
      result: Try[MapOutputStatistics]): Unit = {
    val finished = synchronized {
      if (state == RunningLocal && expected == RunningRemote &&
        !remoteCancellationIssued && result.failed.toOption.exists(isNonSizeLimitFailure)) {
        // A group cancellation processed before the replacement job starts must not disappear
        // just because fallback has already won the storage decision.
        state = Finished
        completion.tryComplete(result)
        cancelActions(result.failed.get)
        true
      } else if (state != expected) {
        false
      } else {
        // JobStart listeners are asynchronous. If local output finishes before that notification,
        // it still proves the replacement was active and lets us retire the old job here.
        val outcome = if (expected == RunningLocal && result.isSuccess) {
          cancelRemoteAfterLocalStarted().map(Failure(_)).getOrElse(result)
        } else {
          result
        }
        outcome match {
          case Success(_) => selected = Some(dependency)
          case _ =>
        }
        state = Finished
        completion.tryComplete(outcome)
        outcome.failed.toOption.foreach(cancelActions)
        true
      }
    }
    if (finished) removeCallbacks()
  }

  private def fail(expected: State, failure: Throwable): Unit =
    finish(remoteDependency, expected, Failure(failure))

  private def cancelActions(failure: Throwable): Unit = synchronized {
    actions.foreach { action =>
      try action.cancel()
      catch {
        case NonFatal(cancellationFailure) =>
          if (failure ne cancellationFailure) failure.addSuppressed(cancellationFailure)
      }
    }
  }

  private def removeCallbacks(): Unit = {
    try manager.removeSizeLimitFallback(remoteDependency.shuffleId)
    finally sparkContext.removeSparkListener(jobListener)
  }

  private def isNonSizeLimitFailure(failure: Throwable): Boolean =
    !CometNativeShuffleWriter.isSizeLimitFailure(failure)

  private def remoteFailure: Option[Throwable] =
    remoteAction.flatMap(_.value).flatMap(_.failed.toOption).filter(isNonSizeLimitFailure)

  // Called with this materialization's lock held, after Spark has made the local job active.
  private def cancelRemoteAfterLocalStarted(): Option[Throwable] = {
    if (remoteCancellationIssued) return None
    val previousFailure = remoteFailure
    if (previousFailure.nonEmpty) return previousFailure
    remoteCancellationIssued = true
    try {
      remoteAction.foreach(_.cancel())
      None
    } catch {
      case NonFatal(failure) => Some(failure)
    }
  }

  private def localJobStarted(jobId: Int): Unit = synchronized {
    if (state == RunningLocal && activeAction.exists(_.jobIds.contains(jobId))) {
      cancelRemoteAfterLocalStarted().foreach { failure =>
        fail(RunningLocal, failure)
        cancelActions(failure)
      }
    }
  }

  private def requestLocalShuffle(): Boolean = synchronized {
    if (state != RunningRemote || activeAction.exists(_.isCompleted)) {
      false
    } else {
      state = RunningLocal
      try {
        // Register the replacement before cancelling the old job. Its input partitions were
        // already computed for the remote submission, and submitMapStage only queues execution.
        // The JobStart listener retires the remote job only after Spark has made the replacement
        // active. Until then, any external cancellation of the remote job also cancels fallback.
        val localDependency =
          withCapturedProperties(remoteDependency.createLocalShuffleDependency())
        remoteFailure match {
          case Some(failure) =>
            fail(RunningLocal, failure)
            cancelActions(failure)
          case None => submit(localDependency, RunningLocal)
        }
        true
      } catch {
        case NonFatal(failure) =>
          fail(RunningLocal, failure)
          cancelActions(failure)
          // The storage transition won even if creating the replacement failed. The coordinator
          // must still fence the abandoned remote ID; the materialization retains this failure.
          true
      }
    }
  }

  def completedDependency: Option[CometShuffleDependency[K, V, C]] = synchronized { selected }

  def selectedDependency: CometShuffleDependency[K, V, C] = {
    try ThreadUtils.awaitResult(this, Duration.Inf)
    catch {
      case interrupted: InterruptedException =>
        cancel()
        Thread.currentThread().interrupt()
        throw interrupted
    }
    synchronized {
      selected.getOrElse(throw new IllegalStateException("Shuffle materialization has no output"))
    }
  }

  override def cancel(): Unit = cancel(None)

  // Spark 4 calls this overload; Spark 3's FutureAction only declares cancel().
  def cancel(reason: Option[String]): Unit = {
    val cancelled = synchronized {
      if (state == Finished || state == Cancelled) {
        false
      } else {
        state = Cancelled
        val failure =
          new SparkException(reason.getOrElse("Comet shuffle materialization cancelled"))
        cancelActions(failure)
        completion.tryFailure(failure)
        true
      }
    }
    if (cancelled) removeCallbacks()
  }

  override def isCancelled: Boolean = synchronized { state == Cancelled }

  override def jobIds: Seq[Int] = synchronized { actions.iterator.flatMap(_.jobIds).toVector }

  override def isCompleted: Boolean = completion.isCompleted

  override def value: Option[Try[MapOutputStatistics]] = completion.future.value

  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
    completion.future.ready(atMost)
    this
  }

  override def result(atMost: Duration)(implicit permit: CanAwait): MapOutputStatistics =
    completion.future.result(atMost)

  override def onComplete[U](f: Try[MapOutputStatistics] => U)(implicit
      executor: ExecutionContext): Unit = completion.future.onComplete(f)

  override def transform[S](f: Try[MapOutputStatistics] => Try[S])(implicit
      executor: ExecutionContext): Future[S] = completion.future.transform(f)

  override def transformWith[S](f: Try[MapOutputStatistics] => Future[S])(implicit
      executor: ExecutionContext): Future[S] = completion.future.transformWith(f)
}

private[shuffle] object CometCelebornShuffleMaterialization {
  private sealed trait State
  private case object RunningRemote extends State
  private case object RunningLocal extends State
  private case object Finished extends State
  private case object Cancelled extends State

  def forDependency(dependency: ShuffleDependency[_, _, _])
      : Option[CometCelebornShuffleMaterialization[_, _, _]] = dependency match {
    case comet: CometShuffleDependency[_, _, _] => comet.materialization
    case _ => None
  }

  def selectForRead(dependency: ShuffleDependency[Int, _, _]): ShuffleDependency[Int, _, _] =
    forDependency(dependency)
      .map(_.selectedDependency.asInstanceOf[ShuffleDependency[Int, _, _]])
      .getOrElse(dependency)
}
