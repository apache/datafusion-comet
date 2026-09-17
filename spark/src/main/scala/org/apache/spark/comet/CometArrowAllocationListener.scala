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

import java.util.concurrent.ConcurrentHashMap

import org.apache.arrow.memory.AllocationListener
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}

/**
 * Reports JVM-side Arrow allocations to Spark's memory manager.
 *
 * `CometArrowAllocator` is a process-wide `RootAllocator` with no limit, so until now the
 * off-heap bytes it hands out were counted by nobody: not Spark's `TaskMemoryManager`, and not
 * Comet's native memory pool. They are still resident in the container, which makes them a blind
 * spot when an executor is killed for exceeding its memory limit.
 *
 * This listener closes the reporting half of that gap. Every allocation is charged to a
 * [[MemoryConsumer]] belonging to the task that made it, so the bytes appear in
 * `TaskMemoryManager.showMemoryUsage` and are arbitrated against Spark's other off-heap
 * consumers.
 *
 * It deliberately does not enforce. A short grant from Spark is logged and the allocation
 * proceeds, because Arrow allocation on these paths cannot fail today and making it fail is a
 * behavioural change that belongs in its own commit. See
 * [[https://github.com/apache/datafusion-comet/issues/5997]].
 *
 * Three cases are handled by doing nothing, each for a different reason:
 *   - No active task. Broadcast coalescing and the cached batch serializer can allocate from the
 *     driver or a non-task thread, where there is no task to charge.
 *   - On-heap mode. Comet's on-heap mode exists so the Spark SQL suite can run without off-heap
 *     memory configured; charging an off-heap consumer there would be wrong.
 *   - A buffer released after its allocating task has finished. The allocator is process-wide
 *     precisely because buffers can outlive the task that created them, so the task's reservation
 *     is dropped at task end and later releases are ignored rather than double-counted.
 */
class CometArrowAllocationListener extends AllocationListener with Logging {

  import CometArrowAllocationListener._

  private val reservations = new ConcurrentHashMap[Long, TaskReservation]()

  @volatile private var configResolved = false
  @volatile private var accountingEnabled = true
  @volatile private var blockSize = DEFAULT_BLOCK_SIZE

  override def onAllocation(size: Long): Unit = {
    val reservation = reservationForCurrentTask()
    if (reservation != null) {
      reservation.allocated(size)
    }
  }

  override def onRelease(size: Long): Unit = {
    val reservation = reservationForCurrentTask()
    if (reservation != null) {
      reservation.released(size)
    }
  }

  /** Bytes currently reserved with Spark on behalf of the given task. Visible for testing. */
  private[comet] def reservedBytesForTask(taskAttemptId: Long): Long = {
    val reservation = reservations.get(taskAttemptId)
    if (reservation == null) 0L else reservation.reservedBytes
  }

  private[comet] def trackedTaskCount: Int = reservations.size()

  /**
   * Resolves configuration from the `SparkConf` rather than a `SQLConf` entry. This listener is
   * attached to a `val` in a package object, so it is constructed on first touch of
   * `CometArrowAllocator`, which can happen before any `SparkSession` exists and on executors
   * where `SQLConf` does not carry Comet's settings. `SparkEnv` is absent until the executor is
   * up, so the read is retried until it succeeds rather than cached from a null environment.
   */
  private def resolveConfig(): Unit = {
    if (!configResolved) {
      val env = SparkEnv.get
      if (env != null) {
        accountingEnabled = env.conf.getBoolean(ACCOUNTING_ENABLED_KEY, defaultValue = true)
        blockSize = env.conf.getSizeAsBytes(BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE_STRING)
        configResolved = true
      }
    }
  }

  private def reservationForCurrentTask(): TaskReservation = {
    resolveConfig()
    if (!accountingEnabled) return null

    val taskContext = TaskContext.get()
    if (taskContext == null) return null

    val taskMemoryManager = taskContext.taskMemoryManager()
    if (taskMemoryManager == null ||
      taskMemoryManager.getTungstenMemoryMode != MemoryMode.OFF_HEAP) {
      return null
    }

    val taskAttemptId = taskContext.taskAttemptId()
    val existing = reservations.get(taskAttemptId)
    if (existing != null) return existing

    val created = new TaskReservation(taskMemoryManager, blockSize, this)
    val previous = reservations.putIfAbsent(taskAttemptId, created)
    if (previous != null) return previous

    taskContext.addTaskCompletionListener[Unit] { _ =>
      val finished = reservations.remove(taskAttemptId)
      if (finished != null) {
        finished.close()
      }
    }
    created
  }

  private[comet] def warnOnShortGrant(requested: Long, granted: Long): Unit = {
    if (!shortGrantLogged) {
      shortGrantLogged = true
      logWarning(
        s"Spark granted $granted of $requested bytes requested for JVM Arrow allocations. " +
          "The allocation proceeds regardless, so this is a reporting gap rather than a failure. " +
          s"Set $ACCOUNTING_ENABLED_KEY=false to stop reporting these allocations to Spark.")
    }
  }

  @volatile private var shortGrantLogged = false
}

object CometArrowAllocationListener {

  val ACCOUNTING_ENABLED_KEY = "spark.comet.arrowAllocator.accounting.enabled"
  val BLOCK_SIZE_KEY = "spark.comet.arrowAllocator.accounting.blockSize"

  private val DEFAULT_BLOCK_SIZE_STRING = "1m"
  private val DEFAULT_BLOCK_SIZE = 1024L * 1024L

  /**
   * One task's reservation against Spark's off-heap pool.
   *
   * Arrow allocates per buffer, and `acquireExecutionMemory` takes locks, so reserving for every
   * buffer would be needlessly chatty. Instead the reservation is grown and shrunk in whole
   * blocks and only block-crossing changes reach Spark.
   */
  private class TaskReservation(
      taskMemoryManager: TaskMemoryManager,
      blockSize: Long,
      listener: CometArrowAllocationListener)
      extends MemoryConsumer(taskMemoryManager, 0L, MemoryMode.OFF_HEAP) {

    // Named `usedBytes` rather than `used` on purpose: `MemoryConsumer` already declares a
    // `protected long used`, and a private field of that name narrows the inherited member, which
    // the compiler rejects as weaker access privileges in overriding.
    private var usedBytes: Long = 0L
    private var reserved: Long = 0L

    /** Comet's native operators cannot be made to spill from here. See issue #5997. */
    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L

    /**
     * Reports our own tally. The inherited `used` counter stays at zero because this consumer
     * never calls `acquireMemory` or `allocatePage`; Arrow has already obtained the memory and we
     * are only accounting for it.
     */
    override def getUsed: Long = synchronized(usedBytes)

    def reservedBytes: Long = synchronized(reserved)

    def allocated(size: Long): Unit = synchronized {
      usedBytes += size
      while (reserved < usedBytes) {
        val request = math.max(blockSize, usedBytes - reserved)
        val granted = taskMemoryManager.acquireExecutionMemory(request, this)
        if (granted <= 0L) {
          listener.warnOnShortGrant(request, granted)
          return
        }
        reserved += granted
        if (granted < request) {
          listener.warnOnShortGrant(request, granted)
          return
        }
      }
    }

    def released(size: Long): Unit = synchronized {
      usedBytes = math.max(0L, usedBytes - size)
      while (reserved - usedBytes >= blockSize) {
        taskMemoryManager.releaseExecutionMemory(blockSize, this)
        reserved -= blockSize
      }
    }

    def close(): Unit = synchronized {
      if (reserved > 0L) {
        taskMemoryManager.releaseExecutionMemory(reserved, this)
        reserved = 0L
      }
      usedBytes = 0L
    }
  }
}
