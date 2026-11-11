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

package org.apache.spark.sql.comet.execution.shuffle

import java.io.File
import java.util.{LinkedList => JLinkedList, Map => JMap, Properties}

import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{Partitioner, SparkConf, SparkContext, SparkEnv, TaskContextImpl}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter, ShufflePartitionWriter}
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocator
import org.apache.spark.shuffle.sort.SpillSorter
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, IntegerType, MetadataBuilder, StructField, StructType}
import org.apache.spark.unsafe.UnsafeAlignedOffset
import org.apache.spark.util.Utils

class CometDiskBlockWriterSuite extends AnyFunSuite {

  private val schema = StructType(Seq(StructField("a", BinaryType)))
  private val pageSize: Long = 256 * 1024

  private def newTaskContext(
      tmm: TaskMemoryManager,
      taskAttemptId: Long,
      localProperties: Properties = new Properties): TaskContextImpl = {
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = taskAttemptId.toInt,
      numPartitions = 2,
      taskAttemptId = taskAttemptId,
      attemptNumber = 0,
      taskMemoryManager = tmm,
      localProperties = localProperties,
      metricsSystem = null,
      taskMetrics = TaskMetrics.empty,
      cpus = 1,
      resources = Map.empty)
  }

  test("memory pressure spills only writers of the requesting task") {
    val conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
    val memoryManager = new TestMemoryManager(conf)
    // Task B holds two pages while task A's three writers fill the other four.
    memoryManager.limit(6 * pageSize)

    val tmmA = new TaskMemoryManager(memoryManager, 0L)
    val tmmB = new TaskMemoryManager(memoryManager, 1L)
    val taskContextA = newTaskContext(tmmA, 0L)
    val taskContextB = newTaskContext(tmmB, 1L)
    val allocatorA = CometShuffleMemoryAllocator.getInstance(tmmA, pageSize)
    val allocatorB = CometShuffleMemoryAllocator.getInstance(tmmB, pageSize)

    val tempDir = Utils.createTempDir()
    try {
      val writersA = new JLinkedList[CometDiskBlockWriter]()
      val writersB = new JLinkedList[CometDiskBlockWriter]()
      val serializer = new UnsafeRowSerializer(1).newInstance()
      val fileB = new File(tempDir, "taskB-partition0")
      val writeMetricsA = new ShuffleWriteMetrics
      val writeMetricsB = new ShuffleWriteMetrics
      def newTaskAWriter(partition: Int): CometDiskBlockWriter =
        new CometDiskBlockWriter(
          new File(tempDir, s"taskA-partition$partition"),
          allocatorA,
          pageSize,
          pageSize,
          taskContextA,
          serializer,
          schema,
          writeMetricsA,
          conf,
          false,
          writersA)
      val writerA0 = newTaskAWriter(0)
      val writerA1 = newTaskAWriter(1)
      val writerA2 = newTaskAWriter(2)
      val writerB = new CometDiskBlockWriter(
        fileB,
        allocatorB,
        pageSize,
        pageSize,
        taskContextB,
        serializer,
        schema,
        writeMetricsB,
        conf,
        false,
        writersB)

      val toUnsafe = UnsafeProjection.create(schema)
      def insert(writer: CometDiskBlockWriter, size: Int): Unit = {
        writer.insertRow(toUnsafe(InternalRow(new Array[Byte](size))), 0)
      }
      def insertOne(writer: CometDiskBlockWriter): Unit = {
        insert(writer, 1024)
      }

      // Task B buffers two pages worth of rows on its own thread and then idles, holding the
      // buffered rows in memory (as a concurrently running task would).
      var rowsB = 0L
      val threadB = new Thread(() => {
        while (allocatorB.getUsed < 2 * pageSize) {
          insertOne(writerB)
          rowsB += 1
        }
      })
      threadB.start()
      threadB.join()
      assert(allocatorB.getUsed == 2 * pageSize)

      // Task A fills two pages in one writer and one page in each sibling. When A0 needs another
      // page, spilling larger A1 alone would satisfy the request, but A0 must flush itself before
      // initialCurrentPage() replaces its page.
      var rowsA0 = 0L
      var rowsA1 = 0L
      var rowsA2 = 0L
      while (allocatorA.getUsed < 2 * pageSize) {
        insertOne(writerA1)
        rowsA1 += 1
      }
      while (allocatorA.getUsed < 3 * pageSize) {
        insertOne(writerA0)
        rowsA0 += 1
      }
      while (allocatorA.getUsed < 4 * pageSize) {
        insertOne(writerA2)
        rowsA2 += 1
      }
      while (writerA0.getOutputRecords == 0) {
        insertOne(writerA0)
        rowsA0 += 1
      }
      assert(writerA1.getOutputRecords == 0)
      assert(writerA2.getOutputRecords == 0)

      // A 900 KiB row needs more than A0 plus either sibling can free, so the spill loop must
      // continue through both A1 and A2.
      val outputA0BeforeLargeRow = writerA0.getOutputRecords
      insert(writerA0, 900 * 1024)
      rowsA0 += 1
      assert(writerA0.getOutputRecords > outputA0BeforeLargeRow)
      assert(writerA1.getOutputRecords > 0)
      assert(writerA2.getOutputRecords > 0)

      // Task A resolved its memory pressure by spilling its own buffered rows to its partition
      // files. Everything in those files so far was written by a spill...
      val filesA = Seq(writerA0, writerA1, writerA2).map(_.getFile)
      val spilledBytesA = filesA.map(_.length()).sum
      assert(spilledBytesA > 0)
      assert(taskContextA.taskMetrics.diskBytesSpilled == spilledBytesA)
      assert(taskContextA.taskMetrics.memoryBytesSpilled > 0)
      // ... and, as the files become the map output, also counts as shuffle bytes written...
      assert(writeMetricsA.bytesWritten == spilledBytesA)
      assert(writerA0.getOutputRecords > 0)
      // ... while task B's buffered rows were not spilled, not written out, and not charged.
      assert(allocatorB.getUsed == 2 * pageSize)
      assert(writerB.getActiveMemoryUsage == 2 * pageSize)
      assert(writerB.getOutputRecords == 0)
      assert(taskContextB.taskMetrics.diskBytesSpilled == 0)
      assert(taskContextB.taskMetrics.memoryBytesSpilled == 0)
      assert(writeMetricsB.bytesWritten == 0)
      assert(fileB.length() == 0)

      val segmentA0 = writerA0.close()
      val segmentA1 = writerA1.close()
      val segmentA2 = writerA2.close()
      val segmentB = writerB.close()
      assert(writerA0.getOutputRecords == rowsA0)
      assert(writerA1.getOutputRecords == rowsA1)
      assert(writerA2.getOutputRecords == rowsA2)
      assert(writerB.getOutputRecords == rowsB)
      assert(segmentA0.length > 0)
      assert(segmentA1.length > 0)
      assert(segmentA2.length > 0)
      assert(segmentB.length > 0)
      assert(writersA.isEmpty && writersB.isEmpty)
      // Closing writes the remaining rows as final batches, which count as shuffle bytes written
      // but not as spill.
      assert(filesA.map(_.length()).sum > spilledBytesA)
      assert(writeMetricsA.bytesWritten == filesA.map(_.length()).sum)
      assert(writeMetricsA.recordsWritten == rowsA0 + rowsA1 + rowsA2)
      assert(taskContextA.taskMetrics.diskBytesSpilled == spilledBytesA)
      assert(writeMetricsB.bytesWritten == fileB.length())
      assert(writeMetricsB.recordsWritten == rowsB)
      assert(taskContextB.taskMetrics.diskBytesSpilled == 0)
      assert(taskContextB.taskMetrics.memoryBytesSpilled == 0)
    } finally {
      Utils.deleteRecursively(tempDir)
      tmmA.cleanUpAllAllocatedMemory()
      tmmB.cleanUpAllAllocatedMemory()
    }
  }

  test("bypass writers with more partitions than full pages fit write without spilling") {
    // Every partition writer holds a page as soon as it has a row. With pages of Spark's page
    // size, a task fits only a few of them, so with more partitions than that each new page
    // spilled a sibling writer holding about one row (#7058).
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("CometDiskBlockWriterSuite")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
      .set("spark.buffer.pageSize", "1m")
    val sc = new SparkContext(conf)
    val memoryManager = new TestMemoryManager(conf)
    // Room for eight pages of Spark's size, for 32 partitions whose rows need far less.
    memoryManager.limit(8 * 1024 * 1024)
    val tmm = new TaskMemoryManager(memoryManager, 0L)
    try {
      val taskContext = newTaskContext(tmm, 0L)
      val partitions = 32
      val rowsPerPartition = 100
      val partitioner = new Partitioner {
        override def numPartitions: Int = partitions
        override def getPartition(key: Any): Int = key.asInstanceOf[Int] % partitions
      }
      val dep = new CometShuffleDependency[Int, UnsafeRow, UnsafeRow](
        _rdd = sc.parallelize(Seq.empty[(Int, UnsafeRow)], 1),
        partitioner = partitioner,
        serializer = new UnsafeRowSerializer(1),
        schema = Some(schema),
        decodeTime = new SQLMetric("nsTiming"))
      val shuffleExecutorComponents = new LocalDiskShuffleExecutorComponents(conf)
      shuffleExecutorComponents.initializeExecutor(
        sc.applicationId,
        "driver",
        JMap.of[String, String]())
      val writer = new CometBypassMergeSortShuffleWriter[Int, UnsafeRow](
        SparkEnv.get.blockManager,
        tmm,
        taskContext,
        new CometBypassMergeSortShuffleHandle[Int, UnsafeRow](dep.shuffleId, dep),
        0L,
        conf,
        taskContext.taskMetrics.shuffleWriteMetrics,
        shuffleExecutorComponents,
        null)

      val toUnsafe = UnsafeProjection.create(schema)
      // Round-robin, so that each partition gets a row before any gets a second one.
      val rows = (0 until partitions * rowsPerPartition).iterator.map { i =>
        (i % partitions, toUnsafe(InternalRow(new Array[Byte](100))))
      }
      writer.write(rows)
      assert(writer.stop(true).isDefined)

      // No writer had to make room for another, so each partition was written once, on close.
      assert(taskContext.taskMetrics.memoryBytesSpilled == 0)
      assert(taskContext.taskMetrics.diskBytesSpilled == 0)
      val writeMetrics = taskContext.taskMetrics.shuffleWriteMetrics
      assert(writeMetrics.recordsWritten == partitions * rowsPerPartition)
      assert(writer.getPartitionLengths.forall(_ > 0))
      assert(tmm.getMemoryConsumptionForThisTask == 0)
    } finally {
      sc.stop()
      tmm.cleanUpAllAllocatedMemory()
    }
  }

  test("a partition writer's pages double from a small first page up to the page size") {
    val conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
    val memoryManager = new TestMemoryManager(conf)
    memoryManager.limit(64L * 1024 * 1024)
    val tmm = new TaskMemoryManager(memoryManager, 0L)
    val taskContext = newTaskContext(tmm, 0L)
    val kib = 1024L
    val initialPageSize = 64 * kib
    val maxPageSize = 1024 * kib
    val allocator = CometShuffleMemoryAllocator.getInstance(tmm, initialPageSize)
    val tempDir = Utils.createTempDir()
    try {
      val writer = new CometDiskBlockWriter(
        new File(tempDir, "partition0"),
        allocator,
        initialPageSize,
        maxPageSize,
        taskContext,
        new UnsafeRowSerializer(1).newInstance(),
        schema,
        new ShuffleWriteMetrics,
        conf,
        false,
        new JLinkedList[CometDiskBlockWriter]())
      val toUnsafe = UnsafeProjection.create(schema)
      def insertOne(): Unit = writer.insertRow(toUnsafe(InternalRow(new Array[Byte](1000))), 0)

      // The memory the writer holds after each page it takes.
      val held = ArrayBuffer.empty[Long]
      while (held.length < 7) {
        insertOne()
        if (!held.lastOption.contains(writer.getActiveMemoryUsage)) {
          held += writer.getActiveMemoryUsage
        }
      }
      // 64, 128, 256 and 512 KiB pages, then pages of the 1 MiB maximum.
      assert(held == Seq(64, 192, 448, 960, 1984, 3008, 4032).map(_ * kib))

      // Writing out a full batch frees its pages, and the next batch starts small again.
      while (writer.getOutputRecords == 0) {
        insertOne()
      }
      assert(writer.getActiveMemoryUsage == initialPageSize)
      writer.close()
      assert(allocator.getUsed == 0)
    } finally {
      Utils.deleteRecursively(tempDir)
      tmm.cleanUpAllAllocatedMemory()
    }
  }

  test("growing pages under real memory pressure spill but still write full batches") {
    // The partitions' batches together do not fit, so writers must spill, and a growing writer
    // whose next page is refused spills itself and restarts at the small page. Spilling is
    // legitimate here, but it must not degrade to writing about one row per batch (#7058).
    val conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
    val memoryManager = new TestMemoryManager(conf)
    val kib = 1024L
    val initialPageSize = 64 * kib
    val maxPageSize = 1024 * kib
    // 32 partitions fill about 8 MiB below, so this holds well under half of the batches.
    memoryManager.limit(3 * 1024 * kib)
    val tmm = new TaskMemoryManager(memoryManager, 0L)
    val taskContext = newTaskContext(tmm, 0L)
    val allocator = CometShuffleMemoryAllocator.getInstance(tmm, initialPageSize)
    val tempDir = Utils.createTempDir()
    try {
      val partitions = 32
      val rowsPerPartition = 2000
      val currentWriters = new JLinkedList[CometDiskBlockWriter]()
      val serializer = new UnsafeRowSerializer(1).newInstance()
      val writers = (0 until partitions).map { partition =>
        new CometDiskBlockWriter(
          new File(tempDir, s"partition$partition"),
          allocator,
          initialPageSize,
          maxPageSize,
          taskContext,
          serializer,
          schema,
          new ShuffleWriteMetrics,
          conf,
          false,
          currentWriters)
      }

      val toUnsafe = UnsafeProjection.create(schema)
      // Round-robin, so that each partition gets a row before any gets a second one. A write
      // under memory pressure can be of any writer, so each insert checks all of them.
      val written = Array.fill(partitions)(0L)
      var batches = 0L
      var rowsInBatches = 0L
      for (_ <- 0 until rowsPerPartition; partition <- 0 until partitions) {
        writers(partition).insertRow(toUnsafe(InternalRow(new Array[Byte](100))), 0)
        for (i <- 0 until partitions if writers(i).getOutputRecords > written(i)) {
          batches += 1
          rowsInBatches += writers(i).getOutputRecords - written(i)
          written(i) = writers(i).getOutputRecords
        }
      }
      writers.foreach(_.close())

      assert(writers.map(_.getOutputRecords).sum == partitions * rowsPerPartition)
      assert(taskContext.taskMetrics.memoryBytesSpilled > 0)
      assert(taskContext.taskMetrics.diskBytesSpilled > 0)
      assert(tmm.getMemoryConsumptionForThisTask == 0)
      assert(allocator.getUsed == 0)
      // Writing a page's worth of rows is far more than one row. Pages of Spark's size, when
      // every writer held one from its first row, gave about one row per batch here.
      assert(batches > 0 && rowsInBatches / batches >= 100, s"$rowsInBatches rows in $batches")
    } finally {
      Utils.deleteRecursively(tempDir)
      tmm.cleanUpAllAllocatedMemory()
    }
  }

  test("a fatal error during write() frees the task's buffered pages") {
    // Spark's ShuffleWriteProcessor only calls stop(false) when write() throws an Exception, so a
    // fatal error such as SparkOutOfMemoryError skips it. write() itself must therefore free the
    // pages it buffered on the way out; until the task ends they would otherwise stay charged and
    // starve the other tasks sharing the pool.
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("CometDiskBlockWriterSuite")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
      .set("spark.buffer.pageSize", "256k")
    val sc = new SparkContext(conf)
    val memoryManager = new TestMemoryManager(conf)
    // Small enough that the rows below force real spilling.
    memoryManager.limit(1024 * 1024)
    val tmm = new TaskMemoryManager(memoryManager, 0L)
    try {
      val taskContext = newTaskContext(tmm, 0L)
      val partitioner = new Partitioner {
        override def numPartitions: Int = 3
        override def getPartition(key: Any): Int = key.asInstanceOf[Int] % 3
      }
      val dep = new CometShuffleDependency[Int, UnsafeRow, UnsafeRow](
        _rdd = sc.parallelize(Seq.empty[(Int, UnsafeRow)], 1),
        partitioner = partitioner,
        serializer = new UnsafeRowSerializer(1),
        schema = Some(schema),
        decodeTime = new SQLMetric("nsTiming"))
      val writer = new CometBypassMergeSortShuffleWriter[Int, UnsafeRow](
        SparkEnv.get.blockManager,
        tmm,
        taskContext,
        new CometBypassMergeSortShuffleHandle[Int, UnsafeRow](0, dep),
        0L,
        conf,
        taskContext.taskMetrics.shuffleWriteMetrics,
        newShuffleExecutorComponents(),
        null)
      val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager
      val filesBeforeWrite = diskBlockManager.getAllFiles().toSet
      var spillFiles = Set.empty[File]

      // Force at least one real spill file, then hit a fatal error mid-write.
      val toUnsafe = UnsafeProjection.create(schema)
      val rows: Iterator[Product2[Int, UnsafeRow]] =
        (0 until 2000).iterator.map { i =>
          (i % 3, toUnsafe(InternalRow(new Array[Byte](1024))))
        } ++ new Iterator[Product2[Int, UnsafeRow]] {
          override def hasNext: Boolean = true
          override def next(): Product2[Int, UnsafeRow] = {
            spillFiles = diskBlockManager.getAllFiles().toSet -- filesBeforeWrite
            assert(spillFiles.nonEmpty)
            throw new SparkOutOfMemoryError(
              "UNABLE_TO_ACQUIRE_MEMORY",
              java.util.Map.of("requestedBytes", "1", "receivedBytes", "0"))
          }
        }
      intercept[SparkOutOfMemoryError] {
        writer.write(rows)
      }
      assert(spillFiles.forall(!_.exists()))

      // Every page the failed task buffered was reclaimed by write() itself, before any
      // task-level cleanup ran.
      assert(tmm.getMemoryConsumptionForThisTask == 0)
    } finally {
      sc.stop()
      tmm.cleanUpAllAllocatedMemory()
    }
  }

  test("a failed SpillSorter constructor does not leak pool memory") {
    // The unsafe sorter's constructor first allocates a one-entry array (8 bytes) inside
    // ShuffleInMemorySorter and then its real pointer array. If the second allocation fails, the
    // first must be reclaimed: the writer is never handed to Spark, so no cleanup path short of
    // the task ending would ever free it.
    val conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
    val memoryManager = new TestMemoryManager(conf)
    val tmm = new TaskMemoryManager(memoryManager, 0L)
    val taskContext = newTaskContext(tmm, 0L)
    val allocator = CometShuffleMemoryAllocator.getInstance(tmm, pageSize)
    try {
      // Room for the one-entry array but not for the 4096-entry (32 KiB) pointer array.
      memoryManager.limit(1024)
      intercept[SparkOutOfMemoryError] {
        new SpillSorter(
          allocator,
          4096,
          schema,
          UnsafeAlignedOffset.getUaoSize(),
          1.0,
          "zstd",
          1,
          true,
          "adler32",
          new Array[Long](0),
          new ShuffleWriteMetrics,
          taskContext,
          new JLinkedList[SpillInfo](),
          () => ())
      }
      // With the constructor cleanup nothing is left charged; a leaked constructor allocation
      // would show up here.
      assert(allocator.getUsed == 0)

      // A failure after the pointer array is adopted (here: schema serialization rejecting an
      // out-of-range parquet.field.id) must free the adopted array as well. The enclosing
      // sorter field is never assigned in this case, so not even the unsafe writer's
      // task-completion listener could see the allocation.
      memoryManager.limit(1024 * 1024)
      val badField = StructField(
        "a",
        IntegerType,
        nullable = true,
        new MetadataBuilder().putLong("parquet.field.id", 2147483648L).build())
      val badSchema = StructType(Seq(StructField("s", StructType(Seq(badField)))))
      intercept[IllegalArgumentException] {
        new SpillSorter(
          allocator,
          4096,
          badSchema,
          UnsafeAlignedOffset.getUaoSize(),
          1.0,
          "zstd",
          1,
          true,
          "adler32",
          new Array[Long](0),
          new ShuffleWriteMetrics,
          taskContext,
          new JLinkedList[SpillInfo](),
          () => ())
      }
      assert(allocator.getUsed == 0)
    } finally {
      tmm.cleanUpAllAllocatedMemory()
    }
  }

  test("the unsafe writer allocates nothing before write()") {
    // Spark evaluates the shuffle input iterator between constructing the writer and calling
    // write(), and that evaluation can block on Spark's execution-memory pool (e.g. an eager
    // input sort). The writer must not retain pool memory across that window, or two tasks can
    // deadlock across the two pools; and when write() fails, even with a fatal error, everything
    // it allocated must be reclaimed.
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("CometDiskBlockWriterSuite")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
      .set("spark.buffer.pageSize", "256k")
    val sc = new SparkContext(conf)
    val memoryManager = new TestMemoryManager(conf)
    memoryManager.limit(1024 * 1024)
    val tmm = new TaskMemoryManager(memoryManager, 0L)
    try {
      val taskContext = newTaskContext(tmm, 0L)
      val partitioner = new Partitioner {
        override def numPartitions: Int = 4
        override def getPartition(key: Any): Int = key.asInstanceOf[Int] % 4
      }
      val dep = new CometShuffleDependency[Int, UnsafeRow, UnsafeRow](
        _rdd = sc.parallelize(Seq.empty[(Int, UnsafeRow)], 1),
        partitioner = partitioner,
        serializer = new UnsafeRowSerializer(1),
        schema = Some(schema),
        decodeTime = new SQLMetric("nsTiming"))
      def newUnsafeWriter(): CometUnsafeShuffleWriter[Int, UnsafeRow] =
        new CometUnsafeShuffleWriter[Int, UnsafeRow](
          SparkEnv.get.blockManager,
          tmm,
          new CometSerializedShuffleHandle[Int, UnsafeRow](0, dep),
          0L,
          taskContext,
          conf,
          taskContext.taskMetrics.shuffleWriteMetrics,
          newShuffleExecutorComponents(),
          null)

      // If the input fails before open() assigns the allocator, tracing cleanup must preserve the
      // original failure instead of masking it with an allocator null dereference.
      SQLConf.get.setConfString("spark.comet.tracing.enabled", "true")
      val tracingWriter =
        try {
          newUnsafeWriter()
        } finally {
          SQLConf.get.unsetConf("spark.comet.tracing.enabled")
        }
      val inputFailure = new RuntimeException("input failed before the first record")
      val thrown = intercept[RuntimeException] {
        tracingWriter.write(new Iterator[Product2[Int, UnsafeRow]] {
          override def hasNext: Boolean = throw inputFailure
          override def next(): Product2[Int, UnsafeRow] = throw new AssertionError("unreachable")
        })
      }
      assert(thrown eq inputFailure)

      // Construct the writer exactly as Spark does before evaluating the input iterator.
      val writer = newUnsafeWriter()
      // Construction must not have taken anything from the pool.
      assert(tmm.getMemoryConsumptionForThisTask == 0)

      // A fatal error from the record iterator mid-write must not leak the sorter's pages or
      // pointer array either, and the task-completion listener stays a no-op afterwards.
      val toUnsafe = UnsafeProjection.create(schema)
      val rows: Iterator[Product2[Int, UnsafeRow]] =
        (0 until 100).iterator.map { i =>
          (i % 4, toUnsafe(InternalRow(new Array[Byte](1024))))
        } ++ new Iterator[Product2[Int, UnsafeRow]] {
          override def hasNext: Boolean = true
          override def next(): Product2[Int, UnsafeRow] = {
            throw new SparkOutOfMemoryError(
              "UNABLE_TO_ACQUIRE_MEMORY",
              java.util.Map.of("requestedBytes", "1", "receivedBytes", "0"))
          }
        }
      intercept[SparkOutOfMemoryError] {
        writer.write(rows)
      }
      taskContext.markTaskCompleted(None)
      assert(tmm.getMemoryConsumptionForThisTask == 0)
    } finally {
      sc.stop()
      tmm.cleanUpAllAllocatedMemory()
    }
  }

  private def newShuffleExecutorComponents(): ShuffleExecutorComponents = {
    new ShuffleExecutorComponents {
      override def initializeExecutor(
          appId: String,
          execId: String,
          extraConfigs: java.util.Map[String, String]): Unit = {}
      override def createMapOutputWriter(
          shuffleId: Int,
          mapTaskId: Long,
          numPartitions: Int): ShuffleMapOutputWriter = new ShuffleMapOutputWriter {
        override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter =
          throw new UnsupportedOperationException
        override def commitAllPartitions(checksums: Array[Long]): MapOutputCommitMessage =
          throw new UnsupportedOperationException
        override def abort(error: Throwable): Unit = {}
      }
    }
  }
}
