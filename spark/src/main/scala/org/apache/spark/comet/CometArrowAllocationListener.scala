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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.util.control.NonFatal

import org.apache.arrow.memory.AllocationListener
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, SparkOutOfMemoryError, TaskMemoryManager}

import org.apache.comet.CometConf

/**
 * Accounts one task's JVM-side Arrow allocations against Spark's off-heap execution pool.
 *
 * `CometArrowAllocator` is a process-wide `RootAllocator` with no limit, so until now the
 * off-heap bytes it hands out were counted by nobody: not Spark's `TaskMemoryManager`, and not
 * Comet's native memory pool. They are still resident in the container, which makes them a blind
 * spot when an executor is killed for exceeding its memory limit. This closes the reporting half
 * of that gap: the bytes appear in `TaskMemoryManager.showMemoryUsage` and are arbitrated against
 * Spark's other off-heap consumers.
 *
 * '''Ownership.''' One instance is created per task and attached to that task's Arrow allocator
 * by [[CometTaskArrowAllocator]]. Arrow reports an allocation and its matching release to the
 * listener of the allocator that '''owns''' the buffer, on whichever thread happens to drop the
 * last reference, and `AllocationListener` is handed nothing but a size. Binding the listener to
 * an allocator is therefore the only way to attribute a release, and reading `TaskContext` inside
 * the callbacks would get it wrong: the JVM UDF path exports a JVM-owned vector to native, which
 * drops it later from a Tokio worker with no task context installed. That release would be lost,
 * leaving the task charged for memory it had already freed, batch after batch.
 *
 * '''Reporting only.''' A short grant is logged and the allocation proceeds, because Arrow
 * allocation on these paths cannot fail today and making it fail is a behavioural change that
 * belongs in its own commit. Enforcement belongs in `onPreAllocation`, the only callback
 * permitted to throw, and in `onFailedAllocation`, not here. See
 * [[https://github.com/apache/datafusion-comet/issues/5997]].
 *
 * '''Neither callback may throw.''' Arrow's `AllocationListener` documents that, and
 * `BaseAllocator.buffer` marks the allocation successful before calling `onAllocation`, so
 * throwing from here loses the buffer Arrow has already created and never hands back. Spark's
 * acquisition is fallible -- it runs other consumers' `spill`, which turns a task interrupt into
 * a `RuntimeException` and an I/O failure into a `SparkOutOfMemoryError` -- so every call into
 * the memory manager is wrapped and reported rather than propagated.
 *
 * '''Lock order.''' [[getUsed]] and [[spill]] must stay lock-free, because Spark calls both while
 * holding the `TaskMemoryManager` monitor, and [[adjust]] holds this listener's monitor across
 * `acquireExecutionMemory`, which takes that monitor. Were the snapshot to take this monitor
 * instead, a native reservation arriving through `CometTaskMemoryManager` on a Comet Tokio thread
 * could hold Spark's monitor and wait for ours while an Arrow allocation on the same task held
 * ours and waited for Spark's.
 */
private[comet] class CometArrowAllocationListener(taskMemoryManager: TaskMemoryManager)
    extends MemoryConsumer(taskMemoryManager, 0L, MemoryMode.OFF_HEAP)
    with AllocationListener {

  import CometArrowAllocationListener._

  /**
   * Bytes Arrow currently holds on this task's behalf. An atomic rather than a guarded field so
   * that [[getUsed]] can read it without taking this listener's monitor; see the lock order note
   * above.
   */
  private val live = new AtomicLong(0L)

  /** Bytes currently reserved with Spark. Guarded by this listener's monitor. */
  private var reserved = 0L

  /** Set once the owning task has finished. Volatile so [[getUsed]] can read it lock-free. */
  @volatile private var completed = false

  override def onAllocation(size: Long): Unit = {
    live.addAndGet(size)
    adjustQuietly()
  }

  override def onRelease(size: Long): Unit = {
    live.addAndGet(-size)
    adjustQuietly()
  }

  /**
   * Reports our own tally. Spark reads this for spill-victim ordering, `showMemoryUsage` and
   * end-of-task leak reporting. The inherited `used` counter stays at zero because this consumer
   * never calls `acquireMemory` or `allocatePage`; Arrow has already obtained the memory and we
   * are only accounting for it.
   *
   * Reports zero once the task has finished, so that buffers deliberately allowed to outlive
   * their task are not reported by `cleanUpAllAllocatedMemory` as a Spark memory leak.
   */
  override def getUsed: Long = if (completed) 0L else math.max(0L, live.get())

  /** Comet's native operators cannot be made to spill from here. See issue #5997. */
  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L

  /**
   * Drops the whole reservation and stops accounting.
   *
   * Called from the owning task's completion listener. Anything still alive afterwards is a
   * buffer that outlives its task, which the process-wide allocator exists to allow; those
   * releases are ignored rather than charged to whichever task happens to be running by then.
   */
  private[comet] def taskCompleted(): Unit = {
    try {
      synchronized {
        completed = true
        if (reserved > 0L) {
          taskMemoryManager.releaseExecutionMemory(reserved, this)
          reserved = 0L
        }
      }
    } catch {
      case NonFatal(e) => warnOnMemoryManagerFailure(e)
      case e: SparkOutOfMemoryError => warnOnMemoryManagerFailure(e)
    }
  }

  /** Bytes Arrow currently holds on this task's behalf. Visible for testing. */
  private[comet] def liveBytes: Long = live.get()

  /** Bytes currently reserved with Spark on this task's behalf. Visible for testing. */
  private[comet] def reservedBytes: Long = synchronized(reserved)

  private def adjustQuietly(): Unit = {
    try {
      adjust()
    } catch {
      // Both of these are reachable: `acquireExecutionMemory` runs other consumers' `spill`, and
      // `TaskMemoryManager` rethrows an interrupt as a RuntimeException and an IOException as a
      // SparkOutOfMemoryError, which is an Error and so slips past NonFatal.
      case NonFatal(e) => warnOnMemoryManagerFailure(e)
      case e: SparkOutOfMemoryError => warnOnMemoryManagerFailure(e)
    }
  }

  private def adjust(): Unit = synchronized {
    if (!completed) {
      val liveBytes = math.max(0L, live.get())
      if (reserved < liveBytes) {
        // Round up so `reserved` stays a block multiple and growth always leaves headroom.
        // Requesting the bare deficit would land exactly on `liveBytes` for any buffer at or above
        // the block size, sending the very next allocation straight back into Spark's lock.
        val request = roundUpToBlock(liveBytes - reserved)
        val granted = taskMemoryManager.acquireExecutionMemory(request, this)
        reserved += granted
        if (granted < request) {
          warnOnShortGrant(request, granted)
        }
      } else {
        // Returned in one call rather than one per block: `releaseExecutionMemory` synchronizes on
        // the executor-wide pool, so a per-block loop would take that lock once per megabyte freed.
        val excess = ((reserved - liveBytes) / BLOCK_SIZE) * BLOCK_SIZE
        if (excess > 0L) {
          taskMemoryManager.releaseExecutionMemory(excess, this)
          reserved -= excess
        }
      }
    }
  }
}

object CometArrowAllocationListener extends Logging {

  /**
   * Batching granularity for reservations. Arrow allocates per buffer and
   * `acquireExecutionMemory` takes an executor-wide lock, so the reservation is grown and shrunk
   * in whole blocks and only block-crossing changes reach Spark. Deliberately not configurable:
   * it trades lock chatter against reservation slack and has no plausible per-workload tuning.
   */
  private[comet] val BLOCK_SIZE = 1024L * 1024L

  private val shortGrantLogged = new AtomicBoolean(false)
  private val memoryManagerFailureLogged = new AtomicBoolean(false)

  private def roundUpToBlock(bytes: Long): Long =
    ((bytes + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE

  private def warnOnShortGrant(requested: Long, granted: Long): Unit = {
    if (shortGrantLogged.compareAndSet(false, true)) {
      logWarning(
        s"Spark granted $granted of $requested bytes requested for JVM Arrow allocations. " +
          "The allocation proceeds regardless, so this is a reporting gap rather than a failure. " +
          s"Set ${CometConf.COMET_ARROW_ALLOCATOR_ACCOUNTING_ENABLED.key}=false to stop " +
          "reporting these allocations to Spark.")
    }
  }

  private def warnOnMemoryManagerFailure(e: Throwable): Unit = {
    if (memoryManagerFailureLogged.compareAndSet(false, true)) {
      logWarning(
        "Failed to report a JVM Arrow allocation to Spark's memory manager. The allocation " +
          "itself is unaffected, so this is a reporting gap rather than a failure, but Spark's " +
          "view of these bytes will be short until the task ends. " +
          s"Set ${CometConf.COMET_ARROW_ALLOCATOR_ACCOUNTING_ENABLED.key}=false to stop " +
          "reporting these allocations to Spark.",
        e)
    }
  }
}
