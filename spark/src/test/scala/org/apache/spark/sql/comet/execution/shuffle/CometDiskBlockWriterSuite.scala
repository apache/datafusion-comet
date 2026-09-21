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
import java.util.{LinkedList => JLinkedList, Properties}
import java.util.concurrent.CountDownLatch

import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{Partitioner, SparkConf, SparkContext, SparkEnv, TaskContext, TaskContextImpl, TaskKilledException}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, SparkOutOfMemoryError, TaskMemoryManager, TestMemoryManager, UnifiedMemoryManager}
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter, ShufflePartitionWriter}
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage
import org.apache.spark.shuffle.comet.{CometBoundedShuffleMemoryAllocator, CometShuffleMemoryAllocator, CometShuffleMemoryAllocatorTrait}
import org.apache.spark.shuffle.sort.SpillSorter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, IntegerType, MetadataBuilder, StructField, StructType}
import org.apache.spark.unsafe.UnsafeAlignedOffset
import org.apache.spark.util.Utils

class CometDiskBlockWriterSuite extends AnyFunSuite with TimeLimits {

  private implicit val signaler: Signaler = ThreadSignaler

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
    val allocatorA = CometShuffleMemoryAllocator.getInstance(conf, tmmA, pageSize)
    val allocatorB = CometShuffleMemoryAllocator.getInstance(conf, tmmB, pageSize)

    val tempDir = Utils.createTempDir()
    try {
      val writersA = new JLinkedList[CometDiskBlockWriter]()
      val writersB = new JLinkedList[CometDiskBlockWriter]()
      val serializer = new UnsafeRowSerializer(1).newInstance()
      val fileB = new File(tempDir, "taskB-partition0")
      def newTaskAWriter(partition: Int): CometDiskBlockWriter =
        new CometDiskBlockWriter(
          new File(tempDir, s"taskA-partition$partition"),
          allocatorA,
          taskContextA,
          serializer,
          schema,
          new ShuffleWriteMetrics,
          conf,
          false,
          writersA)
      val writerA0 = newTaskAWriter(0)
      val writerA1 = newTaskAWriter(1)
      val writerA2 = newTaskAWriter(2)
      val writerB = new CometDiskBlockWriter(
        fileB,
        allocatorB,
        taskContextB,
        serializer,
        schema,
        new ShuffleWriteMetrics,
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

      // Task A resolved its memory pressure by spilling its own data...
      assert(taskContextA.taskMetrics.diskBytesSpilled > 0)
      assert(writerA0.getOutputRecords > 0)
      // ... and task B's buffered rows were not spilled, not written out, and not charged.
      assert(allocatorB.getUsed == 2 * pageSize)
      assert(writerB.getActiveMemoryUsage == 2 * pageSize)
      assert(writerB.getOutputRecords == 0)
      assert(taskContextB.taskMetrics.diskBytesSpilled == 0)
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
    } finally {
      Utils.deleteRecursively(tempDir)
      tmmA.cleanUpAllAllocatedMemory()
      tmmB.cleanUpAllAllocatedMemory()
    }
  }

  test("on-heap shared pool: a task with nothing to spill waits for other tasks to free memory") {
    // On-heap mode uses one executor-wide CometBoundedShuffleMemoryAllocator. A task whose own
    // writers hold nothing spillable must wait for other tasks to free pool memory rather than
    // fail with SparkOutOfMemoryError.
    val conf = new SparkConf().set("spark.comet.memoryOverhead", "1") // 1 MiB shared pool
    resetOnHeapAllocatorSingleton()
    val memoryManager = new TestMemoryManager(conf)
    val tmmA = new TaskMemoryManager(memoryManager, 0L)
    val tmmB = new TaskMemoryManager(memoryManager, 1L)
    val waitTimeoutKey = "spark.comet.shuffle.jvm.memoryWaitTimeout"
    val propertiesA = new Properties
    propertiesA.setProperty(waitTimeoutKey, "5s")
    val propertiesB = new Properties
    propertiesB.setProperty(waitTimeoutKey, "100ms")
    val taskContextA = newTaskContext(tmmA, 0L, propertiesA)
    val taskContextB = newTaskContext(tmmB, 1L, propertiesB)

    // Initialize the shared allocator under B's short timeout, then reuse it under A's longer
    // timeout. The wait below must use the requesting task's setting, not the first task's.
    TaskContext.setTaskContext(taskContextB)
    val allocatorB =
      try {
        CometShuffleMemoryAllocator.getInstance(conf, tmmB, pageSize)
      } finally {
        TaskContext.unset()
      }
    TaskContext.setTaskContext(taskContextA)
    val allocatorA =
      try {
        CometShuffleMemoryAllocator.getInstance(conf, tmmA, pageSize)
      } finally {
        TaskContext.unset()
      }
    assert(allocatorA eq allocatorB)

    val tempDir = Utils.createTempDir()
    try {
      val serializer = new UnsafeRowSerializer(1).newInstance()
      val fileA = new File(tempDir, "onheap-taskA")
      val fileB = new File(tempDir, "onheap-taskB")
      val writerA = new CometDiskBlockWriter(
        fileA,
        allocatorA,
        taskContextA,
        serializer,
        schema,
        new ShuffleWriteMetrics,
        conf,
        false,
        new JLinkedList[CometDiskBlockWriter]())
      val writerB = new CometDiskBlockWriter(
        fileB,
        allocatorB,
        taskContextB,
        serializer,
        schema,
        new ShuffleWriteMetrics,
        conf,
        false,
        new JLinkedList[CometDiskBlockWriter]())

      val toUnsafe = UnsafeProjection.create(schema)
      def insertOne(writer: CometDiskBlockWriter): Unit = {
        writer.insertRow(toUnsafe(InternalRow(new Array[Byte](1024))), 0)
      }

      // Task B fills the whole 1 MiB pool (4 pages) on its own thread, idles for a while, and
      // then finishes, freeing the pool.
      var rowsB = 0L
      var segmentBLength = 0L
      val poolFilled = new CountDownLatch(1)
      val threadB = new Thread(() => {
        TaskContext.setTaskContext(taskContextB)
        try {
          while (writerB.getActiveMemoryUsage < 4 * pageSize) {
            insertOne(writerB)
            rowsB += 1
          }
          poolFilled.countDown()
          Thread.sleep(500)
          segmentBLength = writerB.close().length
        } finally {
          TaskContext.unset()
        }
      })
      threadB.start()
      poolFilled.await()

      // Task A's first insert finds the pool exhausted and has nothing of its own to spill; it
      // must wait for task B to finish instead of throwing SparkOutOfMemoryError.
      var rowsA = 0L
      TaskContext.setTaskContext(taskContextA)
      try {
        (0 until 10).foreach { _ =>
          insertOne(writerA)
          rowsA += 1
        }
      } finally {
        TaskContext.unset()
      }
      threadB.join()

      val segmentA = writerA.close()
      assert(writerA.getOutputRecords == rowsA)
      assert(writerB.getOutputRecords == rowsB)
      assert(segmentA.length > 0)
      assert(segmentBLength > 0)
      // Neither task spilled: A waited instead of stealing B's memory, and B was never touched.
      assert(taskContextA.taskMetrics.diskBytesSpilled == 0)
      assert(taskContextB.taskMetrics.diskBytesSpilled == 0)
    } finally {
      Utils.deleteRecursively(tempDir)
      tmmA.cleanUpAllAllocatedMemory()
      tmmB.cleanUpAllAllocatedMemory()
      resetOnHeapAllocatorSingleton()
    }
  }

  test("on-heap shared pool: unsatisfiable allocations fail fast instead of waiting") {
    val conf = new SparkConf().set("spark.comet.memoryOverhead", "1") // 1 MiB shared pool
    resetOnHeapAllocatorSingleton()
    val memoryManager = new TestMemoryManager(conf)
    val tmm = new TaskMemoryManager(memoryManager, 0L)
    val taskContext = newTaskContext(tmm, 0L)
    val allocator = CometShuffleMemoryAllocator.getInstance(conf, tmm, pageSize)
    val tempDir = Utils.createTempDir()
    try {
      val writer = newWriter(new File(tempDir, "unsatisfiable"), allocator, taskContext, conf)
      val toUnsafe = UnsafeProjection.create(schema)
      failAfter(Span(60, Seconds)) {
        // A row larger than the whole pool can never be satisfied.
        intercept[SparkOutOfMemoryError] {
          writer.insertRow(toUnsafe(InternalRow(new Array[Byte](2 * 1024 * 1024))), 0)
        }
        // A request that does not fit next to memory this thread itself retains (like the
        // sorter's pointer array, which survives an empty spill) can never be satisfied either.
        val bounded = allocator.asInstanceOf[CometBoundedShuffleMemoryAllocator]
        val retained = bounded.allocateArray(pageSize / 8) // retain one page worth of longs
        try {
          intercept[SparkOutOfMemoryError] {
            writer.insertRow(toUnsafe(InternalRow(new Array[Byte](900 * 1024))), 0)
          }
        } finally {
          bounded.freeArray(retained)
        }
      }
      writer.freeMemory()
    } finally {
      Utils.deleteRecursively(tempDir)
      tmm.cleanUpAllAllocatedMemory()
      resetOnHeapAllocatorSingleton()
    }
  }

  test("on-heap shared pool: interruption or cancellation aborts a waiting task") {
    val conf = new SparkConf().set("spark.comet.memoryOverhead", "1") // 1 MiB shared pool
    resetOnHeapAllocatorSingleton()
    val memoryManager = new TestMemoryManager(conf)
    val tmmHolder = new TaskMemoryManager(memoryManager, 0L)
    val tmmWaiter = new TaskMemoryManager(memoryManager, 1L)
    val tmmCancelled = new TaskMemoryManager(memoryManager, 2L)
    val taskContextHolder = newTaskContext(tmmHolder, 0L)
    val taskContextWaiter = newTaskContext(tmmWaiter, 1L)
    val taskContextCancelled = newTaskContext(tmmCancelled, 2L)
    val allocatorHolder = CometShuffleMemoryAllocator.getInstance(conf, tmmHolder, pageSize)
    val allocatorWaiter = CometShuffleMemoryAllocator.getInstance(conf, tmmWaiter, pageSize)
    val allocatorCancelled =
      CometShuffleMemoryAllocator.getInstance(conf, tmmCancelled, pageSize)
    val tempDir = Utils.createTempDir()
    try {
      val writerHolder =
        newWriter(new File(tempDir, "holder"), allocatorHolder, taskContextHolder, conf)
      val writerWaiter =
        newWriter(new File(tempDir, "waiter"), allocatorWaiter, taskContextWaiter, conf)
      val writerCancelled =
        newWriter(new File(tempDir, "cancelled"), allocatorCancelled, taskContextCancelled, conf)
      val toUnsafe = UnsafeProjection.create(schema)
      def insertOne(writer: CometDiskBlockWriter): Unit = {
        writer.insertRow(toUnsafe(InternalRow(new Array[Byte](1024))), 0)
      }

      failAfter(Span(60, Seconds)) {
        // The holder fills the whole pool on its own thread and keeps it until released.
        val poolFilled = new CountDownLatch(1)
        val release = new CountDownLatch(1)
        val holderThread = new Thread(() => {
          while (writerHolder.getActiveMemoryUsage < 4 * pageSize) {
            insertOne(writerHolder)
          }
          poolFilled.countDown()
          release.await()
          writerHolder.freeMemory()
        })
        holderThread.start()
        poolFilled.await()

        // A Java interrupt aborts the wait without surfacing a fatal allocation error.
        @volatile var interruptedFailure: Throwable = null
        val waiterThread = new Thread(() => {
          try insertOne(writerWaiter)
          catch { case t: Throwable => interruptedFailure = t }
        })
        waiterThread.start()
        while (!isBlockedInWait(waiterThread)) {
          Thread.sleep(10)
        }
        waiterThread.interrupt()
        waiterThread.join()

        assert(interruptedFailure != null)
        assert(!interruptedFailure.isInstanceOf[OutOfMemoryError])
        assert(interruptedFailure.isInstanceOf[RuntimeException])
        assert(interruptedFailure.getCause.isInstanceOf[InterruptedException])

        // With interruptOnCancel=false Spark only marks TaskContext; it does not interrupt the
        // Java thread. The allocator must poll that flag and throw TaskKilledException promptly.
        @volatile var cancelledFailure: Throwable = null
        val cancelledThread = new Thread(() => {
          TaskContext.setTaskContext(taskContextCancelled)
          try insertOne(writerCancelled)
          catch { case t: Throwable => cancelledFailure = t }
          finally TaskContext.unset()
        })
        cancelledThread.start()
        while (!isBlockedInWait(cancelledThread)) {
          Thread.sleep(10)
        }
        taskContextCancelled.markInterrupted("test cancellation")
        cancelledThread.join(5000)
        val exitedOnCancellation = !cancelledThread.isAlive

        release.countDown()
        holderThread.join()
        cancelledThread.join()
        writerWaiter.freeMemory()
        writerCancelled.freeMemory()

        assert(exitedOnCancellation)
        assert(!cancelledThread.isInterrupted)
        assert(cancelledFailure.isInstanceOf[TaskKilledException])
      }
    } finally {
      Utils.deleteRecursively(tempDir)
      tmmHolder.cleanUpAllAllocatedMemory()
      tmmWaiter.cleanUpAllAllocatedMemory()
      tmmCancelled.cleanUpAllAllocatedMemory()
      resetOnHeapAllocatorSingleton()
    }
  }

  test("on-heap shared pool: no false deadlock while another waiter can proceed") {
    // Two waiters each retain a small pointer array while a holder owns most of the pool. When
    // the holder frees, the large waiter's request still does not fit, but the small waiter's
    // does: the small waiter must be allowed to proceed and finish, unblocking the large one,
    // instead of either waiter being declared deadlocked.
    val conf = new SparkConf().set("spark.comet.memoryOverhead", "1") // 1 MiB shared pool
    resetOnHeapAllocatorSingleton()
    val memoryManager = new TestMemoryManager(conf)
    val tmm = new TaskMemoryManager(memoryManager, 0L)
    val bounded = CometShuffleMemoryAllocator
      .getInstance(conf, tmm, pageSize)
      .asInstanceOf[CometBoundedShuffleMemoryAllocator]
    try {
      failAfter(Span(60, Seconds)) {
        val holderBlock = bounded.allocate(900 * 1024)

        @volatile var largeError: Throwable = null
        @volatile var smallError: Throwable = null
        val arraysAllocated = new CountDownLatch(2)
        val largeWaiter = new Thread(() => {
          val array = bounded.allocateArray(4096) // retains 32768 bytes
          arraysAllocated.countDown()
          try {
            bounded.free(bounded.allocateBlocking(999448))
          } catch {
            case t: Throwable => largeError = t
          } finally {
            bounded.freeArray(array)
          }
        })
        val smallWaiter = new Thread(() => {
          val array = bounded.allocateArray(4096) // retains 32768 bytes
          arraysAllocated.countDown()
          try {
            bounded.free(bounded.allocateBlocking(262144))
          } catch {
            case t: Throwable => smallError = t
          } finally {
            bounded.freeArray(array)
          }
        })
        largeWaiter.start()
        smallWaiter.start()
        arraysAllocated.await()
        while (!isBlockedInWait(largeWaiter) || !isBlockedInWait(smallWaiter)) {
          Thread.sleep(10)
        }

        bounded.free(holderBlock)
        largeWaiter.join()
        smallWaiter.join()
        assert(largeError == null)
        assert(smallError == null)
      }
    } finally {
      tmm.cleanUpAllAllocatedMemory()
      resetOnHeapAllocatorSingleton()
    }
  }

  private def newWriter(
      file: File,
      allocator: CometShuffleMemoryAllocatorTrait,
      taskContext: TaskContextImpl,
      conf: SparkConf): CometDiskBlockWriter = {
    new CometDiskBlockWriter(
      file,
      allocator,
      taskContext,
      new UnsafeRowSerializer(1).newInstance(),
      schema,
      new ShuffleWriteMetrics,
      conf,
      false,
      new JLinkedList[CometDiskBlockWriter]())
  }

  test("on-heap shared pool: a fatal error during write() frees the task's buffered pages") {
    // Spark's ShuffleWriteProcessor only calls stop(false) when write() throws an Exception, so
    // a fatal error such as SparkOutOfMemoryError skips it. The buffered pages live in the
    // executor-shared bounded pool where Spark's task-memory cleanup cannot see them, so
    // write() itself must free them on the way out or they starve other tasks forever.
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("CometDiskBlockWriterSuite")
      .set("spark.comet.memoryOverhead", "1") // 1 MiB shared pool
      .set("spark.buffer.pageSize", "256k")
    resetOnHeapAllocatorSingleton()
    val sc = new SparkContext(conf)
    val memoryManager = new TestMemoryManager(conf)
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

      // All pages buffered by the failed task were reclaimed, so a full-pool allocation
      // succeeds; without the reclaim the orphaned pages would make it fail forever.
      val bounded = CometShuffleMemoryAllocator
        .getInstance(conf, tmm, pageSize)
        .asInstanceOf[CometBoundedShuffleMemoryAllocator]
      bounded.free(bounded.allocate(1024 * 1024))
    } finally {
      sc.stop()
      tmm.cleanUpAllAllocatedMemory()
      resetOnHeapAllocatorSingleton()
    }
  }

  test("on-heap shared pool: a failed SpillSorter constructor does not leak pool memory") {
    // The unsafe sorter's constructor first allocates a one-entry array (8 bytes) inside
    // ShuffleInMemorySorter and then its real pointer array. If the second allocation fails, the
    // first must be reclaimed: the writer is never handed to Spark, so no cleanup path would
    // ever free it, and the orphaned bytes would make later full-pool allocations impossible.
    val conf = new SparkConf().set("spark.comet.memoryOverhead", "1") // 1 MiB shared pool
    resetOnHeapAllocatorSingleton()
    val memoryManager = new TestMemoryManager(conf)
    val tmm = new TaskMemoryManager(memoryManager, 0L)
    val taskContext = newTaskContext(tmm, 0L)
    val bounded = CometShuffleMemoryAllocator
      .getInstance(conf, tmm, pageSize)
      .asInstanceOf[CometBoundedShuffleMemoryAllocator]
    try {
      // A healthy task holds most of the pool, leaving room for the one-entry array but not for
      // the 4096-entry pointer array.
      val holderBlock = bounded.allocate(1008 * 1024)
      intercept[SparkOutOfMemoryError] {
        new SpillSorter(
          bounded,
          4096,
          schema,
          UnsafeAlignedOffset.getUaoSize(),
          1.0,
          "zstd",
          1,
          "adler32",
          new Array[Long](0),
          new ShuffleWriteMetrics,
          taskContext,
          new JLinkedList[SpillInfo](),
          () => ())
      }
      bounded.free(holderBlock)
      // With the constructor cleanup the pool is empty again, so a full-pool allocation
      // succeeds; a leaked constructor allocation would make it fail forever.
      bounded.free(bounded.allocate(1024 * 1024))

      // A failure after the pointer array is adopted (here: schema serialization rejecting an
      // out-of-range parquet.field.id) must free the adopted array as well. The enclosing
      // sorter field is never assigned in this case, so not even the unsafe writer's
      // task-completion listener could see the allocation.
      val badField = StructField(
        "a",
        IntegerType,
        nullable = true,
        new MetadataBuilder().putLong("parquet.field.id", 2147483648L).build())
      val badSchema = StructType(Seq(StructField("s", StructType(Seq(badField)))))
      intercept[IllegalArgumentException] {
        new SpillSorter(
          bounded,
          4096,
          badSchema,
          UnsafeAlignedOffset.getUaoSize(),
          1.0,
          "zstd",
          1,
          "adler32",
          new Array[Long](0),
          new ShuffleWriteMetrics,
          taskContext,
          new JLinkedList[SpillInfo](),
          () => ())
      }
      bounded.free(bounded.allocate(1024 * 1024))
    } finally {
      tmm.cleanUpAllAllocatedMemory()
      resetOnHeapAllocatorSingleton()
    }
  }

  test("on-heap shared pool: a waiter unwinds when the holder is blocked on Spark memory") {
    // Cross-pool cycle: a task blocked in Spark's execution-memory pool retains Comet pool
    // memory, while another task waits in the Comet pool for memory that only the blocked task
    // could free. Spark's pool may in turn only be freed by the Comet waiter, and dependencies
    // outside this pool are invisible to the allocator, so the wait is bounded: the Comet
    // waiter must unwind at the timeout so its task can release memory.
    val conf = new SparkConf()
      .set("spark.comet.memoryOverhead", "1") // 1 MiB Comet pool
      .set("spark.testing.memory", (8 * 1024 * 1024).toString)
      .set("spark.testing.reservedMemory", "0")
      .set("spark.memory.fraction", "1.0")
    resetOnHeapAllocatorSingleton()
    SQLConf.get.setConfString("spark.comet.shuffle.jvm.memoryWaitTimeout", "1s")
    val unified = UnifiedMemoryManager(conf, numCores = 2)
    val tmmSparkHog = new TaskMemoryManager(unified, 0L)
    val tmmHolder = new TaskMemoryManager(unified, 1L)
    val bounded = CometShuffleMemoryAllocator
      .getInstance(conf, new TaskMemoryManager(new TestMemoryManager(conf), 2L), pageSize)
      .asInstanceOf[CometBoundedShuffleMemoryAllocator]
    def newConsumer(tmm: TaskMemoryManager): MemoryConsumer =
      new MemoryConsumer(tmm, 1024 * 1024, MemoryMode.ON_HEAP) {
        override def spill(size: Long, trigger: MemoryConsumer): Long = 0
      }
    try {
      failAfter(Span(60, Seconds)) {
        // Another task owns the whole Spark execution pool.
        val sparkHog = newConsumer(tmmSparkHog)
        assert(sparkHog.acquireMemory(8 * 1024 * 1024) == 8 * 1024 * 1024)

        // The holder retains Comet pool memory and then blocks acquiring Spark memory.
        val holderReleased = new CountDownLatch(1)
        val holderThread = new Thread(() => {
          val cometBlock = bounded.allocate(500 * 1024)
          val holderConsumer = newConsumer(tmmHolder)
          try {
            val got = holderConsumer.acquireMemory(1024 * 1024) // blocks until sparkHog frees
            holderConsumer.freeMemory(got)
          } finally {
            bounded.free(cometBlock)
            holderReleased.countDown()
          }
        })
        holderThread.start()
        def parkedOnSparkMemory: Boolean =
          holderThread.getState == Thread.State.WAITING &&
            holderThread.getStackTrace
              .exists(_.getClassName == "org.apache.spark.memory.ExecutionMemoryPool")
        while (!parkedOnSparkMemory) {
          Thread.sleep(10)
        }

        // The Comet waiter cannot fit next to the blocked holder's memory and must unwind
        // instead of waiting for a release that can never come.
        intercept[SparkOutOfMemoryError] {
          bounded.allocateBlocking(800 * 1024)
        }

        // Once Spark memory frees, the holder resumes and releases its Comet memory.
        sparkHog.freeMemory(8 * 1024 * 1024)
        holderReleased.await()
        holderThread.join()
        bounded.free(bounded.allocate(1024 * 1024))
      }
    } finally {
      SQLConf.get.unsetConf("spark.comet.shuffle.jvm.memoryWaitTimeout")
      tmmSparkHog.cleanUpAllAllocatedMemory()
      tmmHolder.cleanUpAllAllocatedMemory()
      resetOnHeapAllocatorSingleton()
    }
  }

  test("on-heap shared pool: the unsafe writer allocates nothing before write()") {
    // Spark evaluates the shuffle input iterator between constructing the writer and calling
    // write(), and that evaluation can block on Spark's execution-memory pool (e.g. an eager
    // input sort). The writer must not retain Comet pool memory across that window, or two
    // tasks can deadlock across the two pools; and when write() fails, even with a fatal
    // error, everything it allocated must be reclaimed.
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("CometDiskBlockWriterSuite")
      .set("spark.comet.memoryOverhead", "1") // 1 MiB shared pool
      .set("spark.buffer.pageSize", "256k")
    resetOnHeapAllocatorSingleton()
    val sc = new SparkContext(conf)
    val memoryManager = new TestMemoryManager(conf)
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
      val bounded = CometShuffleMemoryAllocator
        .getInstance(conf, tmm, pageSize)
        .asInstanceOf[CometBoundedShuffleMemoryAllocator]
      // Construction must not have taken anything from the shared pool.
      bounded.free(bounded.allocate(1024 * 1024))

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
      bounded.free(bounded.allocate(1024 * 1024))
    } finally {
      sc.stop()
      tmm.cleanUpAllAllocatedMemory()
      resetOnHeapAllocatorSingleton()
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

  /** Whether the thread is parked in `Object.wait` (the allocator uses a timed wait). */
  private def isBlockedInWait(thread: Thread): Boolean = {
    val state = thread.getState
    state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING
  }

  /** Clears the CometShuffleMemoryAllocator singleton so this suite controls its pool size. */
  private def resetOnHeapAllocatorSingleton(): Unit = {
    val field = classOf[CometShuffleMemoryAllocator].getDeclaredField("INSTANCE")
    field.setAccessible(true)
    field.set(null, null)
  }
}
