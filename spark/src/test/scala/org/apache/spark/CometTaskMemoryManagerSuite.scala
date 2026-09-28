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

package org.apache.spark

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager, TestMemoryManager, UnifiedMemoryManager}

class CometTaskMemoryManagerSuite extends AnyFunSuite {

  test("native memory usage is visible to Spark's memory consumer") {
    withTaskMemoryManager { taskMemoryManager =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val consumer = nativeMemoryConsumer(manager)

      assert(manager.getUsed == 0L)
      assert(consumer.getUsed == 0L)

      assert(manager.acquireMemory(128L) == 128L)
      assert(manager.getUsed == 128L)
      assert(consumer.getUsed == 128L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 128L)

      manager.releaseMemory(128L)
      assert(manager.getUsed == 0L)
      assert(consumer.getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
    }
  }

  test("partial and zero grants account only for acquired memory") {
    withTaskMemoryManager { taskMemoryManager =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val consumer = nativeMemoryConsumer(manager)

      def checkUsage(expected: Long): Unit = {
        assert(manager.getUsed == expected)
        assert(consumer.getUsed == expected)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == expected)
      }

      assert(manager.acquireMemory(768L) == 768L)
      checkUsage(768L)
      assert(manager.acquireMemory(512L) == 256L)
      checkUsage(1024L)
      assert(manager.acquireMemory(128L) == 0L)
      checkUsage(1024L)

      manager.releaseMemory(256L)
      checkUsage(768L)
      assert(manager.acquireMemory(128L) == 128L)
      checkUsage(896L)
      manager.releaseMemory(896L)
      checkUsage(0L)
    }
  }

  test("managers in the same task retain separate memory accounting") {
    withTaskMemoryManager { taskMemoryManager =>
      val first = new CometTaskMemoryManager(1L, 0L)
      val second = new CometTaskMemoryManager(2L, 0L)
      val firstConsumer = nativeMemoryConsumer(first)
      val secondConsumer = nativeMemoryConsumer(second)

      def checkUsage(firstBytes: Long, secondBytes: Long): Unit = {
        assert(first.getUsed == firstBytes)
        assert(firstConsumer.getUsed == firstBytes)
        assert(second.getUsed == secondBytes)
        assert(secondConsumer.getUsed == secondBytes)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == firstBytes + secondBytes)
      }

      checkUsage(0L, 0L)
      assert(first.acquireMemory(128L) == 128L)
      checkUsage(128L, 0L)
      assert(second.acquireMemory(256L) == 256L)
      checkUsage(128L, 256L)
      first.releaseMemory(64L)
      checkUsage(64L, 256L)
      second.releaseMemory(256L)
      checkUsage(64L, 0L)
      first.releaseMemory(64L)
      checkUsage(0L, 0L)
    }
  }

  test("an acquire waiting in Spark survives a release that empties the task's balance") {
    val memoryManager = offHeapMemoryManager()
    val otherTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 1L))

    withTaskContext(new TaskMemoryManager(memoryManager, 0L)) { taskMemoryManager =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      assert(otherTask.acquireMemory(90L) == 90L)
      assert(manager.acquireMemory(10L) == 10L)

      // Another native thread releases the task's last 10 bytes while the acquire waits.
      acquireWhileBalanceEmpties(manager, taskMemoryManager, otherTask) {
        manager.releaseMemory(10L)
      }
    }
  }

  test("an acquire waiting in Spark survives a sibling consumer freeing the task's last bytes") {
    val memoryManager = offHeapMemoryManager()
    val otherTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 1L))

    withTaskContext(new TaskMemoryManager(memoryManager, 0L)) { taskMemoryManager =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val sibling = new OffHeapConsumer(taskMemoryManager)
      assert(otherTask.acquireMemory(90L) == 90L)
      assert(sibling.acquireMemory(10L) == 10L)

      // A JVM consumer of the same task frees with freeMemory, which takes no task monitor.
      acquireWhileBalanceEmpties(manager, taskMemoryManager, otherTask) {
        sibling.freeMemory(10L)
      }
    }
  }

  test("a NoSuchElementException other than Spark's missing task entry is rethrown") {
    val wrongTask = missingEntryError("key not found: 7", fromExecutionMemoryPool = true)
    val notFromPool = missingEntryError("key not found: 0", fromExecutionMemoryPool = false)
    for (error <- Seq(wrongTask, notFromPool)) {
      val taskMemoryManager = new FailingTaskMemoryManager(error)
      withTaskContext(taskMemoryManager) { _ =>
        val manager = new CometTaskMemoryManager(1L, 0L)
        val thrown = intercept[NoSuchElementException](manager.acquireMemory(10L))
        assert(thrown eq error)
        assert(taskMemoryManager.calls.get == 1)
        assert(manager.getUsed == 0L)
      }
    }
  }

  test("an acquire that keeps losing its task entry gives up with a zero grant") {
    val error = missingEntryError("key not found: 0", fromExecutionMemoryPool = true)
    val taskMemoryManager = new FailingTaskMemoryManager(error)
    withTaskContext(taskMemoryManager) { _ =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      assert(manager.acquireMemory(10L) == 0L)
      assert(taskMemoryManager.calls.get == maxAcquireAttempts)
      assert(manager.getUsed == 0L)
      assert(nativeMemoryConsumer(manager).getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
    }
  }

  private val TimeoutSeconds = 10L

  /**
   * The task holds 10 bytes of a 100 byte pool and another task holds 90. An acquire of 20 bytes
   * waits in Spark below its minimum share of 25 while `release` takes the task's balance to
   * zero, which removes the task's entry from Spark's pool. Once the other task frees its memory
   * the acquire must be granted all 20 bytes.
   */
  private def acquireWhileBalanceEmpties(
      manager: CometTaskMemoryManager,
      taskMemoryManager: TaskMemoryManager,
      otherTask: OffHeapConsumer)(release: => Unit): Unit = {
    val granted = new AtomicLong(-1L)
    val failure = new AtomicReference[Throwable]()
    val acquire = new Thread(() =>
      try granted.set(manager.acquireMemory(20L))
      catch { case t: Throwable => failure.set(t) })
    acquire.setDaemon(true)

    try {
      acquire.start()
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TimeoutSeconds)
      while (!waitingInSpark(acquire) && System.nanoTime() < deadline) Thread.sleep(10)
      assert(waitingInSpark(acquire), s"the acquire is ${acquire.getState}")
      release
    } finally {
      // Free the other task's memory so that the acquire does not outlive a failed test.
      otherTask.freeMemory(otherTask.getUsed)
      acquire.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
    }

    assert(!acquire.isAlive, s"the acquire is ${acquire.getState}")
    assert(failure.get == null, s"the acquire failed: ${failure.get}")
    assert(granted.get == 20L)
    assert(manager.getUsed == 20L)
    assert(taskMemoryManager.getMemoryConsumptionForThisTask == 20L)
    manager.releaseMemory(20L)
    assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
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

  /** A task memory manager whose every acquire throws `error`. */
  private class FailingTaskMemoryManager(error: Throwable)
      extends TaskMemoryManager(new TestMemoryManager(new SparkConf()), 0L) {
    val calls = new AtomicInteger()

    override def acquireExecutionMemory(required: Long, consumer: MemoryConsumer): Long = {
      // Past this bound a caller is retrying without a cap; fail the test instead of hanging it.
      if (calls.incrementAndGet() > MaxFailingCalls) {
        throw new IllegalStateException(
          s"acquireExecutionMemory called more than $MaxFailingCalls times")
      }
      throw error
    }
  }

  private def missingEntryError(
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

  private def withTaskMemoryManager(f: TaskMemoryManager => Unit): Unit = {
    val memoryManager = new TestMemoryManager(new SparkConf())
    memoryManager.limit(1024)
    withTaskContext(new TaskMemoryManager(memoryManager, 0L))(f)
  }

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

  private def maxAcquireAttempts: Int = {
    val field = classOf[CometTaskMemoryManager].getDeclaredField("MAX_ACQUIRE_ATTEMPTS")
    field.setAccessible(true)
    field.getInt(null)
  }

  private def nativeMemoryConsumer(manager: CometTaskMemoryManager): MemoryConsumer = {
    val field = classOf[CometTaskMemoryManager].getDeclaredField("nativeMemoryConsumer")
    field.setAccessible(true)
    field.get(manager).asInstanceOf[MemoryConsumer]
  }
}
