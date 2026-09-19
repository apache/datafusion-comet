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
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryConsumer, MemoryManager, TaskMemoryManager, TestMemoryManager, UnifiedMemoryManager}

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

  private def withTaskMemoryManager(f: TaskMemoryManager => Unit): Unit = {
    val memoryManager = new TestMemoryManager(new SparkConf())
    memoryManager.limit(1024)
    withTaskMemoryManager(memoryManager, 0L)(f)
  }

  private def withTaskMemoryManager(memoryManager: MemoryManager, taskAttemptId: Long)(
      f: TaskMemoryManager => Unit): Unit = {
    val taskMemoryManager = new TaskMemoryManager(memoryManager, taskAttemptId)
    val taskContext = new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      numPartitions = 1,
      taskAttemptId = taskAttemptId,
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

  /** Runs f with Spark off-heap storage and an isolated executor owner. */
  private def withBroadcastMemory(limit: Long)(
      f: (AnyRef, MemoryManager, CometBroadcastMemoryManager) => Unit): Unit = {
    val environment = new Object
    val conf = new SparkConf(false)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "4096")
    val memoryManager = UnifiedMemoryManager(conf, 1)
    val owner =
      CometBroadcastMemoryManager.getOrCreate(environment, memoryManager, true, limit)
    assert(owner != null)
    try {
      f(environment, memoryManager, owner)
    } finally {
      CometBroadcastMemoryManager.shutdown()
    }
  }

  test("prepared broadcast storage survives actual task-memory cleanup") {
    withBroadcastMemory(1024L) { (environment, memoryManager, owner) =>
      withTaskMemoryManager(memoryManager, 4500000L) { _ =>
        val taskOwner = new CometTaskMemoryManager(1L, 4500000L)
        assert(taskOwner.acquireMemory(128L) == 128L)
        assert(owner.acquireMemory(256L) == 256L)
        assert(memoryManager.offHeapExecutionMemoryUsed == 128L)
        assert(memoryManager.offHeapStorageMemoryUsed == 256L)
      }

      assert(TaskContext.get() == null)
      assert(memoryManager.offHeapExecutionMemoryUsed == 0L)
      assert(memoryManager.offHeapStorageMemoryUsed == 256L)
      withTaskMemoryManager(memoryManager, 4500001L) { _ =>
        val reused =
          CometBroadcastMemoryManager.getOrCreate(environment, memoryManager, true, 1024L)
        assert(reused eq owner)
        assert(
          CometBroadcastMemoryManager.getOrCreate(environment, memoryManager, true, 512L) == null)
      }
      owner.releaseMemory(256L)
      assert(memoryManager.offHeapStorageMemoryUsed == 0L)
    }
  }

  test("prepared broadcast admission returns no grant when Spark storage cannot fit it") {
    withBroadcastMemory(8192L) { (_, memoryManager, owner) =>
      // This request fits the native cap but exceeds the entire Spark off-heap pool. Spark's
      // real storage admission rejects it before attempting MemoryStore eviction.
      assert(owner.acquireMemory(4097L) == 0L)
      assert(owner.getUsedMemory == 0L)
      assert(memoryManager.offHeapStorageMemoryUsed == 0L)
    }
  }

  test("prepared broadcast concurrent grants share one executor cap without TaskContext") {
    withBroadcastMemory(256L) { (_, memoryManager, owner) =>
      val executor = Executors.newFixedThreadPool(2)
      val start = new CountDownLatch(1)
      try {
        val requests = (0 until 2).map { _ =>
          executor.submit(new Callable[Long] {
            override def call(): Long = {
              assert(start.await(10L, TimeUnit.SECONDS))
              assert(TaskContext.get() == null)
              owner.acquireMemory(200L)
            }
          })
        }
        start.countDown()
        val grants = requests.map(_.get(10L, TimeUnit.SECONDS))
        assert(grants.sorted == Seq(0L, 200L))
        assert(memoryManager.offHeapStorageMemoryUsed == 200L)
        assert(owner.acquireMemory(56L) == 56L)
        assert(owner.acquireMemory(1L) == 0L)
        assert(owner.getUsedMemory == 256L)
        assert(memoryManager.offHeapStorageMemoryUsed == 256L)
        owner.releaseMemory(256L)
        assert(owner.getUsedMemory == 0L)
        assert(memoryManager.offHeapStorageMemoryUsed == 0L)
      } finally {
        start.countDown()
        executor.shutdownNow()
        assert(executor.awaitTermination(10L, TimeUnit.SECONDS))
      }
    }
  }

  test("prepared broadcast retirement returns storage and isolates late lease releases") {
    withBroadcastMemory(256L) { (_, memoryManager, owner) =>
      assert(owner.acquireMemory(192L) == 192L)
      CometBroadcastMemoryManager.shutdown()
      assert(memoryManager.offHeapStorageMemoryUsed == 0L)

      withBroadcastMemory(256L) { (_, nextMemoryManager, nextOwner) =>
        assert(nextOwner ne owner)
        assert(nextOwner.acquireMemory(128L) == 128L)
        owner.releaseMemory(192L)
        assert(nextOwner.getUsedMemory == 128L)
        assert(nextMemoryManager.offHeapStorageMemoryUsed == 128L)
        nextOwner.releaseMemory(128L)
      }
    }
  }

}
