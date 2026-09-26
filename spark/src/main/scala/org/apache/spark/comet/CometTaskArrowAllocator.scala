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

package org.apache.spark.comet

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.util.control.NonFatal

import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode

import org.apache.comet.{CometArrowAllocator, CometConf}

/**
 * Hands out the Arrow allocator that JVM-owned allocations should use, one per Spark task.
 *
 * Each task gets a child of `CometArrowAllocator` carrying its own
 * [[CometArrowAllocationListener]], so what that allocator owns is reported to the task's
 * `TaskMemoryManager`. The allocator, not the calling thread, is what identifies the owner:
 * Arrow's `AllocationListener` is given only a size, and a buffer is released on whichever thread
 * drops the last reference, which for anything that reaches native over the C Data Interface is a
 * Comet Tokio worker with no task context installed. Child allocators cut from the returned
 * allocator inherit its listener and roll their bytes up into it, so the paths that make their
 * own children are covered too.
 *
 * '''This is for buffers the JVM owns.''' Buffers allocated to be handed straight to native -- by
 * `NativeUtil`, for the JVM UDF result, and in `CometArrowStream.stream` -- come from the
 * listener-less root instead, and imports from native come from `CometArrowImportAllocator`, a
 * listener-less child of it. Native memory is native's to account for: a native operator that
 * retains a batch reserves its buffers through Comet's pool, which charges the same Spark task,
 * so charging them here as well would reserve the same memory twice. A batch the JVM read into
 * this allocator and then streams to native is covered by the same rule without its allocation
 * site having to know: the stream's reader retains each batch into its own root-backed allocator
 * and closes the source, which moves the charge off this one, and then calls [[reconcile]] so
 * that the reservation follows straight away. The root is also what callers with no task get,
 * such as broadcast coalescing on the driver or a reader built while native pulls a stream, and
 * what Comet's on-heap mode gets, where charging an off-heap consumer would be wrong.
 *
 * '''Lifetime.''' The task allocator cannot simply be closed when the task ends. The process-wide
 * allocator exists precisely because Arrow buffers can outlive the task that created them, and
 * Arrow treats closing an allocator that still owns bytes as a leak. So at task completion the
 * Spark reservation is dropped and the allocator is closed only if it has been drained; otherwise
 * it is parked and closed by a later task once the stragglers are released. Leaving it open
 * indefinitely is not an option: `BaseAllocator` keeps every child in a map until it closes.
 */
object CometTaskArrowAllocator extends Logging {

  private class TaskAllocator(
      val allocator: BufferAllocator,
      val listener: CometArrowAllocationListener)

  private val perTask = new ConcurrentHashMap[Long, TaskAllocator]()

  /** Allocators whose task has ended but which still own bytes. Guarded by [[closeLock]]. */
  private val awaitingClose = new ConcurrentLinkedQueue[BufferAllocator]()

  private val closeLock = new Object

  /**
   * Resolved once per JVM. Read from the `SparkConf` rather than `SQLConf`, because this is
   * reached from executor threads where `SQLConf` does not carry Comet's settings. The `Option`
   * guard covers tests that install a task context without a `SparkEnv`.
   */
  private lazy val accountingEnabled: Boolean = Option(SparkEnv.get).forall { env =>
    env.conf.getBoolean(
      CometConf.COMET_MEMORY_JVM_ARROW_ACCOUNTING_ENABLED.key,
      CometConf.COMET_MEMORY_JVM_ARROW_ACCOUNTING_ENABLED.defaultValue.get)
  }

  /**
   * The allocator to use for JVM-owned Arrow buffers on the calling thread. Never null, and never
   * an allocator belonging to a task other than the current one.
   */
  def forCurrentTask(): BufferAllocator = {
    // Cheapest check first, and the one that eliminates the most callers: the driver, and native
    // threads pulling a stream, allocate with no task in scope.
    val taskContext = TaskContext.get()
    if (taskContext == null) {
      CometArrowAllocator
    } else {
      val existing = perTask.get(taskContext.taskAttemptId())
      if (existing != null) existing.allocator else create(taskContext)
    }
  }

  private def create(taskContext: TaskContext): BufferAllocator = {
    val taskMemoryManager = taskContext.taskMemoryManager()
    // Comet's on-heap mode accounts for nothing and exists so the Spark SQL suite can run without
    // off-heap memory configured, so it gets the root: the same switch that
    // `CometShuffleMemoryAllocator.getInstance` makes for shuffle pages.
    if (!accountingEnabled || taskMemoryManager == null ||
      taskMemoryManager.getTungstenMemoryMode != MemoryMode.OFF_HEAP) {
      return CometArrowAllocator
    }

    val taskAttemptId = taskContext.taskAttemptId()
    closeDrained()
    val listener = new CometArrowAllocationListener(taskMemoryManager)
    val allocator = CometArrowAllocator
      .newChildAllocator(s"comet-task-$taskAttemptId", listener, 0L, Long.MaxValue)
    listener.bind(() => allocator.getAllocatedMemory)
    val created = new TaskAllocator(allocator, listener)
    val previous = perTask.putIfAbsent(taskAttemptId, created)
    if (previous != null) {
      // Lost the race with another thread in the same task; the loser's allocator is untouched.
      closeQuietly(allocator)
      previous.allocator
    } else {
      // Deliberately outside the map operation: `addTaskCompletionListener` runs the callback
      // inline if the task has already completed, and that callback removes from this same map,
      // which would be a recursive update inside a mapping function.
      taskContext.addTaskCompletionListener[Unit](_ => taskCompleted(taskAttemptId))
      // ...and if it did run inline, the allocator just created is already closed, so hand back
      // the root rather than something the caller cannot allocate from.
      if (perTask.containsKey(taskAttemptId)) allocator else CometArrowAllocator
    }
  }

  private def taskCompleted(taskAttemptId: Long): Unit = {
    val finished = perTask.remove(taskAttemptId)
    if (finished != null) {
      finished.listener.taskCompleted()
      closeLock.synchronized {
        if (!tryClose(finished.allocator)) {
          awaitingClose.add(finished.allocator)
        }
      }
    }
    closeDrained()
  }

  /** Closes any parked allocator whose stragglers have since been released. */
  private def closeDrained(): Unit = {
    if (!awaitingClose.isEmpty) {
      closeLock.synchronized {
        val parked = awaitingClose.iterator()
        while (parked.hasNext) {
          if (tryClose(parked.next())) {
            parked.remove()
          }
        }
      }
    }
  }

  /** Closes the allocator if it has been drained. Returns false if it must stay open. */
  private def tryClose(allocator: BufferAllocator): Boolean = {
    if (allocator.getAllocatedMemory != 0L) {
      false
    } else {
      closeQuietly(allocator)
      true
    }
  }

  private def closeQuietly(allocator: BufferAllocator): Unit = {
    try {
      allocator.close()
    } catch {
      case NonFatal(e) =>
        // Closing is bookkeeping: the bytes are already gone and the Spark reservation is already
        // released, so a failure here must not propagate into a task completion listener.
        logWarning(s"Failed to close Arrow allocator ${allocator.getName}", e)
    }
  }

  /**
   * Brings a task allocator's reservation up to date with what it owns, if `allocator` is a task
   * allocator or a child of one, and does nothing otherwise. For a caller that has just moved
   * buffers out of it by retaining them in another allocator and closing the source, which Arrow
   * does without calling any listener, so that the task stops paying for them now rather than at
   * its next allocation.
   */
  private[spark] def reconcile(allocator: BufferAllocator): Unit = allocator.getListener match {
    case listener: CometArrowAllocationListener => listener.reconcile()
    case _ =>
  }

  /** Number of tasks currently holding an accounted allocator. Visible for testing. */
  private[comet] def trackedTaskCount: Int = perTask.size()

  /** Number of finished tasks whose allocator is still draining. Visible for testing. */
  private[comet] def awaitingCloseCount: Int = awaitingClose.size()

  /** The listener accounting for the given task, if it has one. Visible for testing. */
  private[comet] def listenerForTask(
      taskAttemptId: Long): Option[CometArrowAllocationListener] = {
    Option(perTask.get(taskAttemptId)).map(_.listener)
  }

  /** Bytes currently reserved with Spark on behalf of the given task. Visible for testing. */
  private[comet] def reservedBytesForTask(taskAttemptId: Long): Long = {
    listenerForTask(taskAttemptId).map(_.reservedBytes).getOrElse(0L)
  }
}
