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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, TaskContextImpl}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.util.Utils

class CometDiskBlockWriterSuite extends AnyFunSuite {

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
}
