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

import org.apache.spark.{SparkConf, TaskContextImpl}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.shuffle.comet.{CometBoundedShuffleMemoryAllocator, CometShuffleMemoryAllocator, CometShuffleMemoryAllocatorTrait}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.util.Utils

class CometDiskBlockWriterSuite extends AnyFunSuite with TimeLimits {

  private implicit val signaler: Signaler = ThreadSignaler

  private val schema = StructType(Seq(StructField("a", BinaryType)))
  private val pageSize: Long = 256 * 1024

  private def newTaskContext(tmm: TaskMemoryManager, taskAttemptId: Long): TaskContextImpl = {
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = taskAttemptId.toInt,
      numPartitions = 2,
      taskAttemptId = taskAttemptId,
      attemptNumber = 0,
      taskMemoryManager = tmm,
      localProperties = new Properties,
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
    // Enough memory for task B to buffer 12 pages and task A to buffer 10 pages; task A's
    // 11th page allocation must fail so that it goes through the spilling path.
    memoryManager.limit(22 * pageSize)

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
      val fileA = new File(tempDir, "taskA-partition0")
      val fileB = new File(tempDir, "taskB-partition0")
      val writerA = new CometDiskBlockWriter(
        fileA,
        allocatorA,
        taskContextA,
        serializer,
        schema,
        new ShuffleWriteMetrics,
        conf,
        false,
        writersA)
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
      def insertOne(writer: CometDiskBlockWriter): Unit = {
        writer.insertRow(toUnsafe(InternalRow(new Array[Byte](1024))), 0)
      }

      // Task B buffers 12 pages worth of rows on its own thread and then idles, holding the
      // buffered rows in memory (as a concurrently running task would).
      var rowsB = 0L
      val threadB = new Thread(() => {
        while (allocatorB.getUsed < 12 * pageSize) {
          insertOne(writerB)
          rowsB += 1
        }
      })
      threadB.start()
      threadB.join()
      assert(allocatorB.getUsed == 12 * pageSize)

      // Task A buffers 10 pages, then keeps inserting: its 11th page allocation fails, which
      // must spill task A's own writers and leave task B's writer untouched.
      var rowsA = 0L
      while (allocatorA.getUsed < 10 * pageSize) {
        insertOne(writerA)
        rowsA += 1
      }
      (0 until 400).foreach { _ =>
        insertOne(writerA)
        rowsA += 1
      }

      // Task A resolved its memory pressure by spilling its own data...
      assert(taskContextA.taskMetrics.diskBytesSpilled > 0)
      assert(writerA.getOutputRecords > 0)
      // ... and task B's buffered rows were not spilled, not written out, and not charged.
      assert(allocatorB.getUsed == 12 * pageSize)
      assert(writerB.getActiveMemoryUsage == 12 * pageSize)
      assert(writerB.getOutputRecords == 0)
      assert(taskContextB.taskMetrics.diskBytesSpilled == 0)
      assert(fileB.length() == 0)

      val segmentA = writerA.close()
      val segmentB = writerB.close()
      assert(writerA.getOutputRecords == rowsA)
      assert(writerB.getOutputRecords == rowsB)
      assert(segmentA.length > 0)
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
    val taskContextA = newTaskContext(tmmA, 0L)
    val taskContextB = newTaskContext(tmmB, 1L)
    // Both tasks get the same singleton allocator in on-heap mode.
    val allocatorA = CometShuffleMemoryAllocator.getInstance(conf, tmmA, pageSize)
    val allocatorB = CometShuffleMemoryAllocator.getInstance(conf, tmmB, pageSize)
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
        while (writerB.getActiveMemoryUsage < 4 * pageSize) {
          insertOne(writerB)
          rowsB += 1
        }
        poolFilled.countDown()
        Thread.sleep(500)
        segmentBLength = writerB.close().length
      })
      threadB.start()
      poolFilled.await()

      // Task A's first insert finds the pool exhausted and has nothing of its own to spill; it
      // must wait for task B to finish instead of throwing SparkOutOfMemoryError.
      var rowsA = 0L
      (0 until 10).foreach { _ =>
        insertOne(writerA)
        rowsA += 1
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

  test("on-heap shared pool: interrupting a waiting task does not throw a fatal error") {
    val conf = new SparkConf().set("spark.comet.memoryOverhead", "1") // 1 MiB shared pool
    resetOnHeapAllocatorSingleton()
    val memoryManager = new TestMemoryManager(conf)
    val tmmHolder = new TaskMemoryManager(memoryManager, 0L)
    val tmmWaiter = new TaskMemoryManager(memoryManager, 1L)
    val taskContextHolder = newTaskContext(tmmHolder, 0L)
    val taskContextWaiter = newTaskContext(tmmWaiter, 1L)
    val allocatorHolder = CometShuffleMemoryAllocator.getInstance(conf, tmmHolder, pageSize)
    val allocatorWaiter = CometShuffleMemoryAllocator.getInstance(conf, tmmWaiter, pageSize)
    val tempDir = Utils.createTempDir()
    try {
      val writerHolder =
        newWriter(new File(tempDir, "holder"), allocatorHolder, taskContextHolder, conf)
      val writerWaiter =
        newWriter(new File(tempDir, "waiter"), allocatorWaiter, taskContextWaiter, conf)
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

        // The waiter blocks on its first insert, and is then killed via interrupt.
        @volatile var thrown: Throwable = null
        val waiterThread = new Thread(() => {
          try insertOne(writerWaiter)
          catch { case t: Throwable => thrown = t }
        })
        waiterThread.start()
        while (waiterThread.getState != Thread.State.WAITING) {
          Thread.sleep(10)
        }
        waiterThread.interrupt()
        waiterThread.join()

        // The interruption must not surface as a fatal error such as SparkOutOfMemoryError,
        // otherwise an intentional task kill is reported as ExceptionFailure instead of
        // TaskKilled.
        assert(thrown != null)
        assert(!thrown.isInstanceOf[OutOfMemoryError])
        assert(thrown.isInstanceOf[RuntimeException])
        assert(thrown.getCause.isInstanceOf[InterruptedException])

        release.countDown()
        holderThread.join()
        writerWaiter.freeMemory()
      }
    } finally {
      Utils.deleteRecursively(tempDir)
      tmmHolder.cleanUpAllAllocatedMemory()
      tmmWaiter.cleanUpAllAllocatedMemory()
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
        while (largeWaiter.getState != Thread.State.WAITING ||
          smallWaiter.getState != Thread.State.WAITING) {
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

  /** Clears the CometShuffleMemoryAllocator singleton so this suite controls its pool size. */
  private def resetOnHeapAllocatorSingleton(): Unit = {
    val field = classOf[CometShuffleMemoryAllocator].getDeclaredField("INSTANCE")
    field.setAccessible(true)
    field.set(null, null)
  }
}
