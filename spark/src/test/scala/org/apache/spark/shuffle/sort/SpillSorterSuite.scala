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

package org.apache.spark.shuffle.sort

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocator
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.UnsafeAlignedOffset

/**
 * Unit tests for [[SpillSorter]].
 *
 * These tests verify SpillSorter behavior using Spark's test memory management infrastructure,
 * without needing a full SparkContext.
 */
class SpillSorterSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val INITIAL_SIZE = 1024
  private val UAO_SIZE = UnsafeAlignedOffset.getUaoSize
  private val PAGE_SIZE = 4 * 1024 * 1024 // 4MB

  private var conf: SparkConf = _
  private var memoryManager: TestMemoryManager = _
  private var taskMemoryManager: TaskMemoryManager = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "false")
    memoryManager = new TestMemoryManager(conf)
    memoryManager.limit(100 * 1024 * 1024) // 100MB
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
  }

  override def afterEach(): Unit = {
    if (taskMemoryManager != null) {
      taskMemoryManager.cleanUpAllAllocatedMemory()
      taskMemoryManager = null
    }
    memoryManager = null
    super.afterEach()
  }

  private def createTestSchema(): StructType = {
    new StructType().add("id", IntegerType)
  }

  private def createSpillSorter(
      spillCallback: SpillSorter.SpillCallback = () => {},
      spills: java.util.LinkedList[org.apache.spark.sql.comet.execution.shuffle.SpillInfo] =
        new java.util.LinkedList[org.apache.spark.sql.comet.execution.shuffle.SpillInfo](),
      partitionChecksums: Array[Long] = new Array[Long](10)): SpillSorter = {
    val allocator = CometShuffleMemoryAllocator.getInstance(conf, taskMemoryManager, PAGE_SIZE)
    val schema = createTestSchema()
    val writeMetrics = new ShuffleWriteMetrics()
    val taskContext = TaskContext.empty()

    new SpillSorter(
      allocator,
      INITIAL_SIZE,
      schema,
      UAO_SIZE,
      0.5, // preferDictionaryRatio
      "zstd", // compressionCodec
      1, // compressionLevel
      "adler32", // checksumAlgorithm
      partitionChecksums,
      writeMetrics,
      taskContext,
      spills,
      spillCallback)
  }

  test("initial state") {
    val sorter = createSpillSorter()
    try {
      assert(sorter.numRecords() === 0)
      assert(sorter.hasSpaceForAnotherRecord())
      assert(sorter.getMemoryUsage() > 0)
    } finally {
      sorter.freeMemory()
      sorter.freeArray()
    }
  }

  test("insert single record") {
    val sorter = createSpillSorter()
    try {
      val recordData = Array[Byte](1, 2, 3, 4)
      val partitionId = 0

      sorter.initialCurrentPage(recordData.length + UAO_SIZE)
      sorter.insertRecord(recordData, Platform.BYTE_ARRAY_OFFSET, recordData.length, partitionId)

      assert(sorter.numRecords() === 1)
    } finally {
      sorter.freeMemory()
      sorter.freeArray()
    }
  }

  test("insert multiple records") {
    val sorter = createSpillSorter()
    try {
      val recordData = Array[Byte](1, 2, 3, 4)
      val numRecords = 100

      sorter.initialCurrentPage(numRecords * (recordData.length + UAO_SIZE))

      for (i <- 0 until numRecords) {
        val partitionId = i % 10
        sorter.insertRecord(
          recordData,
          Platform.BYTE_ARRAY_OFFSET,
          recordData.length,
          partitionId)
      }

      assert(sorter.numRecords() === numRecords)
    } finally {
      sorter.freeMemory()
      sorter.freeArray()
    }
  }

  test("reset after free") {
    val sorter = createSpillSorter()
    try {
      val recordData = Array[Byte](1, 2, 3, 4)
      sorter.initialCurrentPage(recordData.length + UAO_SIZE)
      sorter.insertRecord(recordData, Platform.BYTE_ARRAY_OFFSET, recordData.length, 0)

      assert(sorter.numRecords() === 1)

      sorter.freeMemory()
      sorter.reset()

      assert(sorter.numRecords() === 0)
      assert(sorter.hasSpaceForAnotherRecord())
    } finally {
      sorter.freeMemory()
      sorter.freeArray()
    }
  }

  test("free memory returns correct value") {
    val sorter = createSpillSorter()
    try {
      sorter.initialCurrentPage(1024)
      val memoryBefore = sorter.getMemoryUsage()
      assert(memoryBefore > 0)

      val freed = sorter.freeMemory()
      assert(freed > 0)
    } finally {
      sorter.freeArray()
    }
  }

  test("spill callback not triggered during normal operations") {
    val spillCount = new AtomicInteger(0)
    val callback: SpillSorter.SpillCallback = () => spillCount.incrementAndGet()

    val sorter = createSpillSorter(spillCallback = callback)
    try {
      sorter.initialCurrentPage(1024)
      val recordData = Array[Byte](1, 2, 3, 4)
      sorter.insertRecord(recordData, Platform.BYTE_ARRAY_OFFSET, recordData.length, 0)

      assert(spillCount.get() === 0, "Spill callback should not be triggered during normal ops")
    } finally {
      sorter.freeMemory()
      sorter.freeArray()
    }
  }

  test("getMemoryUsage is thread-safe") {
    val sorter = createSpillSorter()
    try {
      sorter.initialCurrentPage(1024)

      val threads = (0 until 10).map { _ =>
        new Thread(() => {
          for (_ <- 0 until 100) {
            sorter.getMemoryUsage()
          }
        })
      }

      threads.foreach(_.start())
      threads.foreach(_.join())
      // Test passes if no exceptions thrown
    } finally {
      sorter.freeMemory()
      sorter.freeArray()
    }
  }

  test("expand pointer array") {
    val sorter = createSpillSorter()
    try {
      val initialMemory = sorter.getMemoryUsage()
      val allocator = CometShuffleMemoryAllocator.getInstance(conf, taskMemoryManager, PAGE_SIZE)
      val newArray = allocator.allocateArray(INITIAL_SIZE * 2)
      sorter.expandPointerArray(newArray)

      assert(sorter.getMemoryUsage() >= initialMemory)
    } finally {
      sorter.freeMemory()
      sorter.freeArray()
    }
  }

  test("records distributed across partitions") {
    val sorter = createSpillSorter()
    try {
      val recordData = Array[Byte](1, 2, 3, 4)
      val numPartitions = 5
      val recordsPerPartition = 20

      sorter.initialCurrentPage(
        numPartitions * recordsPerPartition * (recordData.length + UAO_SIZE))

      for (p <- 0 until numPartitions) {
        for (_ <- 0 until recordsPerPartition) {
          sorter.insertRecord(recordData, Platform.BYTE_ARRAY_OFFSET, recordData.length, p)
        }
      }

      assert(sorter.numRecords() === numPartitions * recordsPerPartition)
    } finally {
      sorter.freeMemory()
      sorter.freeArray()
    }
  }

}
