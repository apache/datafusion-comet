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

package org.apache.spark.shuffle.comet

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LogEvent
import org.apache.spark.{CometTaskMemoryManager, TaskContext, TaskContextImpl, TaskMemoryTestUtils}
import org.apache.spark.memory.SparkOutOfMemoryError

class CometUnifiedShuffleMemoryAllocatorSuite extends TaskMemoryTestUtils {

  private val PageSize = 20L

  test("a page waiting in Spark survives a release that empties the task's balance") {
    checkWaitingAllocationSurvives(20L)(allocatePage)
  }

  test("a pointer array waiting in Spark survives a release that empties the task's balance") {
    checkWaitingAllocationSurvives(24L)(allocatePointerArray)
  }

  test("a NoSuchElementException other than Spark's missing task entry is rethrown") {
    val otherMessage = noSuchElement("no entry for task 0", fromExecutionMemoryPool = true)
    val notFromPool = noSuchElement("key not found: 0", fromExecutionMemoryPool = false)
    for (error <- Seq(otherMessage, notFromPool); allocation <- allocations) {
      val taskMemoryManager = new FailingTaskMemoryManager(() => error, failures = Int.MaxValue)
      val allocator = new CometUnifiedShuffleMemoryAllocator(taskMemoryManager, PageSize)
      val events = logEvents(Level.INFO) {
        val thrown = intercept[NoSuchElementException](allocation(allocator))
        assert(thrown eq error)
      }
      assert(events.isEmpty, messages(events))
      assert(taskMemoryManager.calls.get == 1)
      assert(allocator.getUsed == 0L)
      assert(taskMemoryManager.cleanUpAllAllocatedMemory() == 0L)
    }
  }

  test("an allocation that keeps losing its task entry fails with Spark's out of memory error") {
    for ((allocation, requested) <- allocations.zip(Seq("20", "24"))) {
      // A new exception on every call, so the test can tell which one the allocator keeps.
      val taskMemoryManager = new FailingTaskMemoryManager(
        () => noSuchElement("key not found: 0", fromExecutionMemoryPool = true),
        failures = Int.MaxValue)
      val allocator = new CometUnifiedShuffleMemoryAllocator(taskMemoryManager, PageSize)
      val events = logEvents(Level.INFO) {
        val thrown = intercept[SparkOutOfMemoryError](allocation(allocator))
        assert(thrown.getErrorClass == "UNABLE_TO_ACQUIRE_MEMORY")
        assert(thrown.getMessageParameters.get("requestedBytes") == requested)
        assert(thrown.getMessageParameters.get("receivedBytes") == "0")
        // The last attempt's exception carries the task id and where Spark was waiting.
        assert(thrown.getCause eq taskMemoryManager.errors.last)
      }
      assert(events.map(_.getLevel) == Seq(Level.INFO, Level.INFO, Level.WARN), messages(events))
      assert(events.last.getThrown eq taskMemoryManager.errors.last)
      assert(taskMemoryManager.calls.get == MaxAcquireAttempts)
      assert(taskMemoryManager.errors.distinct.size == MaxAcquireAttempts)
      assert(allocator.getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
      assert(taskMemoryManager.cleanUpAllAllocatedMemory() == 0L)
    }
  }

  test("an allocation that loses its task entry after the task ended is not made again") {
    for ((allocation, requested) <- allocations.zip(Seq("20", "24"))) {
      val taskMemoryManager = new FailingTaskMemoryManager(
        () => noSuchElement("key not found: 0", fromExecutionMemoryPool = true),
        failures = Int.MaxValue)
      withTaskContext(taskMemoryManager) { _ =>
        val allocator = new CometUnifiedShuffleMemoryAllocator(taskMemoryManager, PageSize)
        TaskContext.get().asInstanceOf[TaskContextImpl].markTaskCompleted(None)
        val events = logEvents(Level.INFO) {
          val thrown = intercept[SparkOutOfMemoryError](allocation(allocator))
          assert(thrown.getMessageParameters.get("requestedBytes") == requested)
          assert(thrown.getCause eq taskMemoryManager.errors.last)
        }
        assert(events.map(_.getLevel) == Seq(Level.INFO), messages(events))
        assert(taskMemoryManager.calls.get == 1)
        assert(allocator.getUsed == 0L)
      }
    }
  }

  test("an allocation that loses its task entry twice is granted on the last attempt") {
    val error = noSuchElement("key not found: 0", fromExecutionMemoryPool = true)
    for ((allocation, requested) <- allocations.zip(Seq(20L, 24L))) {
      val taskMemoryManager =
        new FailingTaskMemoryManager(() => error, failures = MaxAcquireAttempts - 1)
      val allocator = new CometUnifiedShuffleMemoryAllocator(taskMemoryManager, PageSize)
      val events = logEvents(Level.INFO) {
        assert(allocation(allocator) == requested)
      }
      assert(events.map(_.getLevel) == Seq(Level.INFO, Level.INFO), messages(events))
      assert(taskMemoryManager.calls.get == MaxAcquireAttempts)
      assert(allocator.getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
      assert(taskMemoryManager.cleanUpAllAllocatedMemory() == 0L)
    }
  }

  /** Allocates a 20 byte page, frees it and returns its size. */
  private def allocatePage(allocator: CometUnifiedShuffleMemoryAllocator): Long = {
    val page = allocator.allocate(20L)
    val size = page.size()
    allocator.free(page)
    size
  }

  /** Allocates a 3 entry (24 byte) pointer array, frees it and returns its size in bytes. */
  private def allocatePointerArray(allocator: CometUnifiedShuffleMemoryAllocator): Long = {
    val array = allocator.allocateArray(3L)
    val size = array.memoryBlock().size()
    allocator.freeArray(array)
    size
  }

  private def allocations: Seq[CometUnifiedShuffleMemoryAllocator => Long] =
    Seq(allocatePage _, allocatePointerArray _)

  /**
   * An allocation of `requested` bytes waits in Spark while a native consumer of the task
   * releases the task's last bytes, and must then be granted in full.
   */
  private def checkWaitingAllocationSurvives(requested: Long)(
      allocate: CometUnifiedShuffleMemoryAllocator => Long): Unit =
    checkRequestWhileBalanceEmpties(Seq(classOf[CometUnifiedShuffleMemoryAllocator].getName)) {
      taskMemoryManager =>
        val native = new CometTaskMemoryManager(1L, 0L)
        val allocator = new CometUnifiedShuffleMemoryAllocator(taskMemoryManager, PageSize)
        WaitingRequest(
          native.acquireMemory(_),
          native.releaseMemory(_),
          () => allocate(allocator),
          granted => {
            assert(granted == requested)
            assert(allocator.getUsed == 0L)
            assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
          })
    }

  /** The events that the allocator logs at `level` or above. */
  private def logEvents(level: Level)(f: => Unit): Seq[LogEvent] =
    logEvents(level, Seq(classOf[CometUnifiedShuffleMemoryAllocator].getName))(f)
}
