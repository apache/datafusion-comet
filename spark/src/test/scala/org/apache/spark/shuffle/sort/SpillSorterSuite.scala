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

import java.io.{ByteArrayInputStream, File}
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.Adler32

import scala.collection.mutable

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.shuffle.comet.{CometShuffleMemoryAllocator, CometShuffleMemoryAllocatorTrait}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.comet.execution.shuffle.{NativeBatchDecoderIterator, SpillInfo => CometSpillInfo}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.storage.TempShuffleBlockId
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.UnsafeAlignedOffset

import org.apache.comet.Native
import org.apache.comet.vector.NativeUtil

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
  private var allocator: CometShuffleMemoryAllocatorTrait = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "false")
    memoryManager = new TestMemoryManager(conf)
    memoryManager.limit(100 * 1024 * 1024) // 100MB
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
    // One allocator per test: pages are addressed by a page number in the allocator's own table,
    // so everything a sorter touches has to come from the same instance.
    allocator = CometShuffleMemoryAllocator.getInstance(taskMemoryManager, PAGE_SIZE.toLong)
  }

  override def afterEach(): Unit = {
    allocator = null
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

  /** Decodes the shuffle blocks in `bytes`, rows of `createTestSchema`, into their ids. */
  private def decodeIds(bytes: Array[Byte]): Seq[Int] = {
    val nativeUtil = new NativeUtil
    val decoder = NativeBatchDecoderIterator(
      new ByteArrayInputStream(bytes),
      new SQLMetric("decodeTime"),
      new Native,
      nativeUtil,
      tracingEnabled = false)
    try {
      decoder.flatMap(batch => (0 until batch.numRows()).map(batch.column(0).getInt)).toList
    } finally {
      decoder.close()
      nativeUtil.close()
    }
  }

  private def createSpillSorter(
      spillCallback: SpillSorter.SpillCallback = () => {},
      // Renamed on import: Spark 3.x has a `SpillInfo` of its own in this package.
      spills: java.util.LinkedList[CometSpillInfo] = new java.util.LinkedList[CometSpillInfo](),
      partitionChecksums: Array[Long] = new Array[Long](10),
      allocator: CometShuffleMemoryAllocatorTrait = this.allocator,
      checksumAlgorithm: String = "adler32",
      writeMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics(),
      taskContext: TaskContext = TaskContext.empty()): SpillSorter = {
    new SpillSorter(
      allocator,
      INITIAL_SIZE,
      createTestSchema(),
      UAO_SIZE,
      0.5, // preferDictionaryRatio
      "zstd", // compressionCodec
      1, // compressionLevel
      true, // offsetsAsLengths
      checksumAlgorithm,
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
      sorter.insertRecord(
        recordData,
        Platform.BYTE_ARRAY_OFFSET.toLong,
        recordData.length,
        partitionId)

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
          Platform.BYTE_ARRAY_OFFSET.toLong,
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
      sorter.insertRecord(recordData, Platform.BYTE_ARRAY_OFFSET.toLong, recordData.length, 0)

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
      sorter.insertRecord(recordData, Platform.BYTE_ARRAY_OFFSET.toLong, recordData.length, 0)

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
      val newArray = allocator.allocateArray((INITIAL_SIZE * 2).toLong)
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
          sorter.insertRecord(recordData, Platform.BYTE_ARRAY_OFFSET.toLong, recordData.length, p)
        }
      }

      assert(sorter.numRecords() === numPartitions * recordsPerPartition)
    } finally {
      sorter.freeMemory()
      sorter.freeArray()
    }
  }

  test("a sorted spill writes every partition in one native call") {
    // Rows of `createTestSchema` in partitions 0, 2, 3 and 5 of 7, over 1 KiB pages so that they
    // span several pages.
    val numPartitions = 7
    def partitionOf(i: Int): Int = Array(0, 2, 5, 3, 0, 2)(i % 6)
    val records = (0 until 300).map { i =>
      val row = new UnsafeRow(1)
      row.pointTo(new Array[Byte](16), 16)
      row.setInt(0, i)
      // A record as `UnsafeRowSerializer` writes it: the row's size, then the row.
      (ByteBuffer.allocate(4 + 16).putInt(16).put(row.getBytes).array(), partitionOf(i))
    }
    val usedPartitions = records.map(_._2).toSet

    def check(allocator: CometShuffleMemoryAllocatorTrait, checksumAlgorithm: String): Unit = {
      val partitionChecksums =
        if (checksumAlgorithm == null) Array.emptyLongArray
        else Array.fill(numPartitions)(Long.MinValue)
      val spills = new java.util.LinkedList[CometSpillInfo]()
      val writeMetrics = new ShuffleWriteMetrics()
      val taskContext = TaskContext.empty()
      val sorter = createSpillSorter(
        spills = spills,
        partitionChecksums = partitionChecksums,
        allocator = allocator,
        checksumAlgorithm = checksumAlgorithm,
        writeMetrics = writeMetrics,
        taskContext = taskContext)

      // A spill, then the final file, with the same records.
      val files = mutable.ArrayBuffer[File]()
      try {
        Seq(false, true).foreach { isLastFile =>
          records.foreach { case (record, partition) =>
            assert(sorter.acquireNewPageIfNecessary(record.length + UAO_SIZE))
            sorter.insertRecord(
              record,
              Platform.BYTE_ARRAY_OFFSET.toLong,
              record.length,
              partition)
          }
          val file = File.createTempFile("comet-spill-sorter", ".data")
          files += file
          sorter.setSpillInfo(
            new CometSpillInfo(numPartitions, file, TempShuffleBlockId(UUID.randomUUID())))
          sorter.writeSortedPartitions(isLastFile, false)
          sorter.freeMemory()
          if (!isLastFile) {
            sorter.reset()
          }
        }

        assert(spills.size === 2)
        val written = Seq(spills.get(0), spills.get(1))
        // The bytes of each partition in each file.
        val partitionBytes = written.map { spill =>
          assert(spill.partitionLengths.sum === spill.file.length())
          val bytes = Files.readAllBytes(spill.file.toPath)
          val starts = spill.partitionLengths.scanLeft(0L)(_ + _).map(_.toInt)
          (0 until numPartitions).map(p => bytes.slice(starts(p), starts(p + 1)))
        }

        // Native code found each record through the page table: in both files, every partition
        // decodes to its rows, in the order they were inserted.
        partitionBytes.foreach { partitions =>
          (0 until numPartitions).foreach { p =>
            val ids = records.indices.filter(i => records(i)._2 == p)
            assert(partitions(p).nonEmpty === ids.nonEmpty, s"partition $p")
            assert(decodeIds(partitions(p)) === ids, s"partition $p")
          }
        }

        if (checksumAlgorithm != null) {
          // A partition's checksum covers its bytes in every file, as Spark checksums the
          // merged partition. Partitions without rows keep the unset value.
          (0 until numPartitions).foreach { p =>
            if (usedPartitions(p)) {
              val adler32 = new Adler32()
              partitionBytes.foreach(partitions => adler32.update(partitions(p)))
              assert(partitionChecksums(p) === adler32.getValue, s"partition $p")
            } else {
              assert(partitionChecksums(p) === Long.MinValue, s"partition $p")
            }
          }
        }

        // Both writes count their records, but only the final file counts as shuffle bytes.
        assert(writeMetrics.recordsWritten === 2 * records.size)
        assert(writeMetrics.bytesWritten === written(1).file.length())
        assert(taskContext.taskMetrics.diskBytesSpilled === written(0).file.length())
      } finally {
        sorter.freeMemory()
        sorter.freeArray()
        files.foreach(_.delete())
      }
    }

    val pageSize = 1024L
    // On-heap mode: the allocator numbers pages in its own table.
    check(CometShuffleMemoryAllocator.getInstance(taskMemoryManager, pageSize), "adler32")
    // Shuffle checksums disabled: no checksum array and no algorithm.
    check(CometShuffleMemoryAllocator.getInstance(taskMemoryManager, pageSize), null)

    // Off-heap mode: pages are numbered in the TaskMemoryManager's table.
    val offHeapConf = new SparkConf(false)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "64m")
    val offHeapMemoryManager = new TestMemoryManager(offHeapConf)
    offHeapMemoryManager.limit(64L * 1024 * 1024)
    val offHeapTaskMemoryManager = new TaskMemoryManager(offHeapMemoryManager, 0)
    try {
      check(
        CometShuffleMemoryAllocator.getInstance(offHeapTaskMemoryManager, pageSize),
        "adler32")
    } finally {
      assert(offHeapTaskMemoryManager.cleanUpAllAllocatedMemory() === 0L)
    }
  }

  test("pointer array growth is sized from the array, not from the data pages") {
    // Use the unified (off-heap) allocator so that every allocation made by this sorter is
    // visible through `getUsed` on an allocator that is private to this test.
    val offHeapConf = new SparkConf(false)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "64m")
    val offHeapMemoryManager = new TestMemoryManager(offHeapConf)
    offHeapMemoryManager.limit(64L * 1024 * 1024)
    val offHeapTaskMemoryManager = new TaskMemoryManager(offHeapMemoryManager, 0)
    val allocator =
      CometShuffleMemoryAllocator.getInstance(offHeapTaskMemoryManager, PAGE_SIZE.toLong)
    // The block manager is only touched when spilling, which this test never does.
    val sorter = new CometShuffleExternalSorter(
      allocator,
      null,
      TaskContext.empty(),
      INITIAL_SIZE,
      2,
      offHeapConf,
      new ShuffleWriteMetrics(),
      createTestSchema())

    try {
      val recordData = new Array[Byte](16)
      def insert(i: Int): Unit =
        sorter.insertRecord(
          recordData,
          Platform.BYTE_ARRAY_OFFSET.toLong,
          recordData.length,
          i % 2)

      val initialArrayBytes = INITIAL_SIZE * 8L
      assert(allocator.getUsed === initialArrayBytes)

      insert(0)
      val pageBytes = allocator.getUsed - initialArrayBytes
      assert(pageBytes >= PAGE_SIZE)

      // With radix sort enabled the in-memory sorter uses half the array for records, so the
      // pointer array is grown when the (INITIAL_SIZE / 2 + 1)th record arrives. Growth must
      // double the pointer array, not request an array sized from the data pages as well.
      val recordsToTriggerGrowth = INITIAL_SIZE / 2 + 1
      (1 until recordsToTriggerGrowth).foreach(insert)
      assert(sorter.getPeakMemoryUsedBytes === pageBytes + 2 * initialArrayBytes)
      assert(allocator.getUsed === pageBytes + 2 * initialArrayBytes)
    } finally {
      sorter.cleanupResources()
      assert(offHeapTaskMemoryManager.cleanUpAllAllocatedMemory() === 0L)
    }
  }

}
