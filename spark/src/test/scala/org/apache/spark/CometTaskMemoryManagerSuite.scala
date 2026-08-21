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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager, TestMemoryManager}

class CometTaskMemoryManagerSuite extends AnyFunSuite {

  test("native memory usage is visible to Spark's memory consumer") {
    val memoryManager = new TestMemoryManager(new SparkConf())
    memoryManager.limit(1024)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0L)
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
    } finally {
      taskMemoryManager.cleanUpAllAllocatedMemory()
      TaskContext.unset()
    }
  }

  private def nativeMemoryConsumer(manager: CometTaskMemoryManager): MemoryConsumer = {
    val field = classOf[CometTaskMemoryManager].getDeclaredField("nativeMemoryConsumer")
    field.setAccessible(true)
    field.get(manager).asInstanceOf[MemoryConsumer]
  }
}
