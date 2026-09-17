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

import java.util.Properties

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}

/**
 * Tests that JVM Arrow allocations are reported to Spark, and, just as importantly, that the
 * paths where they cannot be reported fail quietly rather than throwing. Arrow allocation on
 * these paths cannot fail today and this listener must not change that.
 */
class CometArrowAllocationListenerSuite extends AnyFunSuite {

  private val blockSize = 1024L * 1024L

  test("allocations are charged to the current task in whole blocks") {
    withOffHeapTask(taskAttemptId = 1L) { listener =>
      // Far smaller than a block, so the reservation should round up to exactly one block.
      listener.onAllocation(128L)
      assert(listener.reservedBytesForTask(1L) == blockSize)

      // Still inside the first block, so Spark is not asked again.
      listener.onAllocation(1024L)
      assert(listener.reservedBytesForTask(1L) == blockSize)
    }
  }

  test("a request larger than a block reserves enough to cover it") {
    withOffHeapTask(taskAttemptId = 2L) { listener =>
      listener.onAllocation(blockSize * 3 + 7L)
      assert(listener.reservedBytesForTask(2L) >= blockSize * 3 + 7L)
    }
  }

  test("releasing returns whole blocks to Spark") {
    withOffHeapTask(taskAttemptId = 3L) { listener =>
      listener.onAllocation(blockSize * 2)
      val afterAllocation = listener.reservedBytesForTask(3L)
      assert(afterAllocation >= blockSize * 2)

      listener.onRelease(blockSize * 2)
      assert(listener.reservedBytesForTask(3L) == 0L)
    }
  }

  test("no active task is a no-op rather than an error") {
    TaskContext.unset()
    val listener = new CometArrowAllocationListener
    // Broadcast coalescing and the cached batch serializer can allocate off a task thread.
    listener.onAllocation(4096L)
    listener.onRelease(4096L)
    assert(listener.trackedTaskCount == 0)
  }

  test("on-heap mode is not accounted") {
    val memoryManager = new TestMemoryManager(new SparkConf(false))
    memoryManager.limit(64L * 1024 * 1024)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 4L)
    withTaskContext(taskMemoryManager, taskAttemptId = 4L) {
      val listener = new CometArrowAllocationListener
      listener.onAllocation(blockSize)
      // Comet's on-heap mode exists so the Spark SQL suite can run without off-heap memory.
      // Charging an off-heap consumer there would be wrong, so nothing is tracked.
      assert(listener.trackedTaskCount == 0)
      assert(listener.reservedBytesForTask(4L) == 0L)
    }
  }

  private def withOffHeapTask(taskAttemptId: Long)(
      f: CometArrowAllocationListener => Unit): Unit = {
    val conf = new SparkConf(false)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "64m")
    val memoryManager = new TestMemoryManager(conf)
    memoryManager.limit(64L * 1024 * 1024)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, taskAttemptId)
    withTaskContext(taskMemoryManager, taskAttemptId) {
      f(new CometArrowAllocationListener)
    }
  }

  private def withTaskContext(taskMemoryManager: TaskMemoryManager, taskAttemptId: Long)(
      body: => Unit): Unit = {
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
      body
    } finally {
      try {
        taskMemoryManager.cleanUpAllAllocatedMemory()
      } finally {
        TaskContext.unset()
      }
    }
  }
}
