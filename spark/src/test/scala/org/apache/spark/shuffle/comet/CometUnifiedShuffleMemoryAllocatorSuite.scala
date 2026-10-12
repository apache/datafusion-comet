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

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable.ArrayBuffer

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.spark.{CometTaskMemoryManager, SparkConf, SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, SparkOutOfMemoryError, TaskMemoryManager, UnifiedMemoryManager}

class CometUnifiedShuffleMemoryAllocatorSuite extends SparkFunSuite {

  private val PageSize = 20L
  private val TimeoutSeconds = 10L
  private val MaxAllocateAttempts = 3

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
      assert(taskMemoryManager.calls.get == MaxAllocateAttempts)
      assert(taskMemoryManager.errors.distinct.size == MaxAllocateAttempts)
      assert(allocator.getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
      assert(taskMemoryManager.cleanUpAllAllocatedMemory() == 0L)
    }
  }

  test("an allocation that loses its task entry twice is granted on the last attempt") {
    val error = noSuchElement("key not found: 0", fromExecutionMemoryPool = true)
    for ((allocation, requested) <- allocations.zip(Seq(20L, 24L))) {
      val taskMemoryManager =
        new FailingTaskMemoryManager(() => error, failures = MaxAllocateAttempts - 1)
      val allocator = new CometUnifiedShuffleMemoryAllocator(taskMemoryManager, PageSize)
      val events = logEvents(Level.INFO) {
        assert(allocation(allocator) == requested)
      }
      assert(events.map(_.getLevel) == Seq(Level.INFO, Level.INFO), messages(events))
      assert(taskMemoryManager.calls.get == MaxAllocateAttempts)
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
   * The task holds 10 bytes of a 100 byte pool through a native consumer and another task holds
   * 90. `allocate` asks for `requested` bytes on another thread and waits in Spark below the
   * task's minimum share of 25 while the native consumer releases the task's last 10 bytes, which
   * removes the task's entry from Spark's pool. Once the other task frees its memory the
   * allocation must be granted in full.
   */
  private def checkWaitingAllocationSurvives(requested: Long)(
      allocate: CometUnifiedShuffleMemoryAllocator => Long): Unit = {
    val memoryManager = offHeapMemoryManager()
    val otherTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 1L))

    withTaskContext(new TaskMemoryManager(memoryManager, 0L)) { taskMemoryManager =>
      val native = new CometTaskMemoryManager(1L, 0L)
      val allocator = new CometUnifiedShuffleMemoryAllocator(taskMemoryManager, PageSize)
      // The other task goes first: acquiring its 90 bytes after the task's 10 would cap it at 50.
      assert(otherTask.acquireMemory(90L) == 90L)
      assert(native.acquireMemory(10L) == 10L)

      val granted = new AtomicReference[java.lang.Long]()
      val failure = new AtomicReference[Throwable]()
      val thread = new Thread(() =>
        try granted.set(allocate(allocator))
        catch { case t: Throwable => failure.set(t) })
      thread.setDaemon(true)

      val events = logEvents(Level.INFO) {
        try {
          thread.start()
          val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TimeoutSeconds)
          while (!waitingInSpark(thread) && System.nanoTime() < deadline) Thread.sleep(10)
          assert(waitingInSpark(thread), s"the allocation is ${thread.getState}")
          native.releaseMemory(10L)
        } finally {
          // Free the other task's memory so that the allocation does not outlive a failed test.
          otherTask.freeMemory(otherTask.getUsed)
          thread.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
        }
      }

      assert(!thread.isAlive, s"the allocation is ${thread.getState}")
      assert(failure.get == null, s"the allocation failed: ${failure.get}")
      assert(granted.get == requested)
      assert(allocator.getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
      // The first attempt lost the task's entry and the allocator tried again.
      assert(events.map(_.getLevel) == Seq(Level.INFO), messages(events))
    }
  }

  private def waitingInSpark(thread: Thread): Boolean =
    thread.getState == Thread.State.WAITING &&
      thread.getStackTrace.exists(_.getClassName == "org.apache.spark.memory.ExecutionMemoryPool")

  /** A 100 byte off-heap execution pool. */
  private def offHeapMemoryManager(): UnifiedMemoryManager = {
    val conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "100")
      .set("spark.memory.storageFraction", "0")
    new UnifiedMemoryManager(conf, 1000L, 500L, 1)
  }

  /** An off-heap consumer that never spills. */
  private class OffHeapConsumer(taskMemoryManager: TaskMemoryManager)
      extends MemoryConsumer(taskMemoryManager, 0L, MemoryMode.OFF_HEAP) {
    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
  }

  private val MaxFailingCalls = 10

  /**
   * A task memory manager over a 100 byte off-heap pool whose first `failures` acquires throw the
   * result of `newError`, recording each one in `errors`.
   */
  private class FailingTaskMemoryManager(newError: () => Throwable, failures: Int)
      extends TaskMemoryManager(offHeapMemoryManager(), 0L) {
    val calls = new AtomicInteger()
    val errors = new ArrayBuffer[Throwable]()

    override def acquireExecutionMemory(required: Long, consumer: MemoryConsumer): Long = {
      val call = calls.incrementAndGet()
      // Past this bound a caller is retrying without a cap; fail the test instead of hanging it.
      if (call > MaxFailingCalls) {
        throw new IllegalStateException(
          s"acquireExecutionMemory called more than $MaxFailingCalls times")
      }
      if (call <= failures) {
        val error = newError()
        errors += error
        throw error
      }
      super.acquireExecutionMemory(required, consumer)
    }
  }

  private def noSuchElement(
      message: String,
      fromExecutionMemoryPool: Boolean): NoSuchElementException = {
    val error = new NoSuchElementException(message)
    if (fromExecutionMemoryPool) {
      val poolFrame = new StackTraceElement(
        "org.apache.spark.memory.ExecutionMemoryPool",
        "acquireMemory",
        "ExecutionMemoryPool.scala",
        115)
      error.setStackTrace(poolFrame +: error.getStackTrace)
    }
    error
  }

  /** The events that the allocator logs at `level` or above. */
  private def logEvents(level: Level)(f: => Unit): Seq[LogEvent] = {
    val appender = new LogAppender("unified shuffle memory allocator")
    appender.setThreshold(level)
    val loggers = Seq(classOf[CometUnifiedShuffleMemoryAllocator].getName)
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val unconfigured = loggers.filterNot(context.getConfiguration.getLoggers.containsKey)
    try withLogAppender(appender, loggers, Some(level))(f)
    finally {
      // For a logger with no config of its own, withLogAppender adds one and never removes it.
      // It copies the root config's additivity, which is off, so left in place it would keep the
      // logger's events out of the test log for the rest of the run.
      unconfigured.foreach(context.getConfiguration.removeLogger)
      context.updateLoggers()
    }
    appender.loggingEvents.toSeq
  }

  private def messages(events: Seq[LogEvent]): String =
    events.map(_.getMessage.getFormattedMessage).mkString("\n")

  private def withTaskContext(taskMemoryManager: TaskMemoryManager)(
      f: TaskMemoryManager => Unit): Unit = {
    val taskContext = new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      numPartitions = 1,
      taskAttemptId = 0L,
      attemptNumber = 0,
      taskMemoryManager = taskMemoryManager,
      localProperties = new Properties,
      metricsSystem = null,
      taskMetrics = TaskMetrics.empty,
      cpus = 1,
      resources = Map.empty)

    TaskContext.setTaskContext(taskContext)
    try {
      f(taskMemoryManager)
    } finally {
      try {
        taskMemoryManager.cleanUpAllAllocatedMemory()
      } finally {
        TaskContext.unset()
      }
    }
  }
}
