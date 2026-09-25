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
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

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

  test("the memory pool's anchor byte counts for the task but not as usage") {
    withTaskMemoryManager { taskMemoryManager =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val consumer = nativeMemoryConsumer(manager)

      assert(manager.acquireAnchor(1L) == 1L)
      assert(manager.getUsed == 0L, "the anchor is not a reservation")
      assert(consumer.getUsed == 1L, "Spark's view of the consumer matches the task's balance")
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 1L)

      assert(manager.acquireMemory(128L) == 128L)
      assert(manager.getUsed == 128L)
      assert(consumer.getUsed == 129L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 129L)

      manager.releaseMemory(128L)
      assert(manager.getUsed == 0L)
      assert(consumer.getUsed == 1L)

      manager.releaseAnchor(1L)
      assert(consumer.getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)

      // A task at its share is declined the anchor with a zero grant, which counts nothing.
      assert(manager.acquireMemory(1024L) == 1024L)
      assert(manager.acquireAnchor(1L) == 0L)
      assert(manager.getUsed == 1024L)
      assert(consumer.getUsed == 1024L)
      manager.releaseMemory(1024L)
      assert(consumer.getUsed == 0L)
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

  test("a short grant is handed back while another acquire of the task waits in Spark") {
    // A 100 byte off-heap execution pool. Another task holds 82 bytes and a third holds 1.
    val conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "100")
      .set("spark.memory.storageFraction", "0")
    val memoryManager = new UnifiedMemoryManager(conf, 1000L, 500L, 1)
    val otherTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 1L))
    val thirdTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 2L))
    assert(otherTask.acquireMemory(82L) == 82L)
    assert(thirdTask.acquireMemory(1L) == 1L)

    val shortGrant = new CountDownLatch(1)
    val secondParked = new CountDownLatch(1)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0L) {
      override def acquireExecutionMemory(required: Long, consumer: MemoryConsumer): Long = {
        val got = super.acquireExecutionMemory(required, consumer)
        // Out of Spark's monitor, the short grant waits until the second acquire has parked.
        if (got < required && shortGrant.getCount > 0) {
          shortGrant.countDown()
          secondParked.await(TimeoutSeconds, TimeUnit.SECONDS)
        }
        got
      }
    }

    withTaskContext(taskMemoryManager) { _ =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      // The fair pool's anchor keeps this task in Spark's active set.
      assert(manager.acquireAnchor(1L) == 1L)

      // Three active tasks and 16 bytes free: a 30 byte request is short granted 16 bytes, which
      // native code then hands back.
      val firstGranted = new AtomicLong(-1L)
      val first = new Thread(() => {
        firstGranted.set(manager.acquireMemory(30L))
        manager.releaseMemory(firstGranted.get)
      })
      val secondGranted = new AtomicLong(-1L)
      val second = new Thread(() => secondGranted.set(manager.acquireMemory(10L)))
      Seq(first, second).foreach(_.setDaemon(true))

      try {
        first.start()
        assert(shortGrant.await(TimeoutSeconds, TimeUnit.SECONDS), "no short grant")
        // The third task leaves. With two active tasks this task's minimum share is 25 bytes and
        // 1 byte is free, so a 10 byte request waits inside Spark holding the task's monitor.
        thirdTask.freeMemory(1L)
        second.start()
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TimeoutSeconds)
        while (!waitingInSpark(second) && System.nanoTime() < deadline) Thread.sleep(10)
        assert(waitingInSpark(second), s"the second acquire is ${second.getState}")
        secondParked.countDown()

        // Handing back the short grant is what lets the second acquire through.
        second.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
        assert(
          !second.isAlive,
          s"the first acquire is ${first.getState} and the second is ${second.getState}")
        assert(firstGranted.get == 16L)
        assert(secondGranted.get == 10L)
      } finally {
        secondParked.countDown()
        // Free the other task's memory so that neither thread outlives a failed test.
        otherTask.freeMemory(otherTask.getUsed)
        Seq(first, second).foreach(_.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds)))
      }
      manager.releaseMemory(secondGranted.get)
      manager.releaseAnchor(1L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
    }
  }

  private val TimeoutSeconds = 10L

  private def waitingInSpark(thread: Thread): Boolean =
    thread.getState == Thread.State.WAITING &&
      thread.getStackTrace.exists(_.getClassName == "org.apache.spark.memory.ExecutionMemoryPool")

  /** An off-heap consumer of another task, which never spills. */
  private class OffHeapConsumer(taskMemoryManager: TaskMemoryManager)
      extends MemoryConsumer(taskMemoryManager, 0L, MemoryMode.OFF_HEAP) {
    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
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

  private def nativeMemoryConsumer(manager: CometTaskMemoryManager): MemoryConsumer = {
    val field = classOf[CometTaskMemoryManager].getDeclaredField("nativeMemoryConsumer")
    field.setAccessible(true)
    field.get(manager).asInstanceOf[MemoryConsumer]
  }
}
