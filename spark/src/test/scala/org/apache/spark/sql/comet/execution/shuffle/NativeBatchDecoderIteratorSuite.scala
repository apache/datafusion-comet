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

import java.io.{ByteArrayInputStream, EOFException, InputStream}
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, ShuffleBlockId}

import org.apache.comet.Native
import org.apache.comet.vector.NativeUtil

class NativeBatchDecoderIteratorSuite extends CometTestBase {

  import NativeShuffleTestFrames.{intFrame, TrackingInputStream}

  test("local decoder groups frames, drains passthrough output, and flushes the final batch") {
    val scenarios = Seq(
      (Seq.fill(8)(3), Seq(15, 9)),
      // The 8-row frame flushes the pending 6 rows and queues a second, uncopied output.
      (Seq(3, 3, 8, 2), Seq(6, 8, 2)))
    for {
      (rowCounts, coalescedSizes) <- scenarios
      coalescing <- Seq(None, Some(16))
    } {
      val offsets = rowCounts.scanLeft(0)(_ + _)
      val frames = offsets
        .zip(rowCounts)
        .flatMap { case (offset, count) =>
          intFrame(offset until offset + count)
        }
        .toArray
      val nativeUtil = new NativeUtil()
      val input = new TrackingInputStream(frames)
      val decoder = NativeBatchDecoderIterator(
        input,
        SQLMetrics.createNanoTimingMetric(spark.sparkContext, "decode time"),
        new Native(),
        nativeUtil,
        tracingEnabled = false,
        coalescing = coalescing)
      try {
        val sizes = scala.collection.mutable.ArrayBuffer.empty[Int]
        val values = scala.collection.mutable.ArrayBuffer.empty[Int]
        while (decoder.hasNext) {
          val batch = decoder.next()
          sizes += batch.numRows()
          values ++= (0 until batch.numRows()).map(batch.column(0).getInt)
        }
        assert(values.toSeq == (0 until rowCounts.sum))
        assert(sizes.toSeq == (if (coalescing.isDefined) coalescedSizes else rowCounts))
        assert(input.closeCalls == 1)
      } finally {
        decoder.close()
        nativeUtil.close()
      }
    }
  }

  test("coalescing keeps the task's read buffer across fetched blocks") {
    // The first frame outgrows the initial buffer. The two frames after it, one in the same
    // fetched block and one in the next, are read into that same grown buffer.
    val largeRows = 40 * 1024
    val firstBlock = intFrame(0 until largeRows) ++ intFrame(Seq(41))
    val secondBlock = intFrame(Seq(42))
    val input = CometBlockStoreShuffleReader.concatenate(
      Iterator(new ByteArrayInputStream(firstBlock), new ByteArrayInputStream(secondBlock)))
    val buffers = ArrayBuffer.empty[ByteBuffer]
    val native = new Native {
      override def appendShuffleBatch(
          handle: Long,
          shuffleBlock: ByteBuffer,
          length: Int,
          numColumns: Int,
          tracingEnabled: Boolean): Int = {
        buffers += shuffleBlock
        super.appendShuffleBatch(handle, shuffleBlock, length, numColumns, tracingEnabled)
      }
    }
    val nativeUtil = new NativeUtil()
    val decoder = NativeBatchDecoderIterator(
      input,
      SQLMetrics.createNanoTimingMetric(spark.sparkContext, "decode time"),
      native,
      nativeUtil,
      tracingEnabled = false,
      coalescing = Some(16))
    try {
      assert(decoder.next().numRows() == largeRows)
      val grouped = decoder.next()
      assert((0 until grouped.numRows()).map(grouped.column(0).getInt) == Seq(41, 42))
      assert(!decoder.hasNext)
      assert(buffers.size == 3)
      assert(buffers(0).capacity() > ShuffleReadBuffer.INITIAL_CAPACITY)
      assert(buffers.forall(_ eq buffers(0)))
    } finally {
      decoder.close()
      nativeUtil.close()
    }
  }

  test("a reduce task reads every fetched block into one buffer") {
    // One decoder iterator per fetched block, as CometBlockStoreShuffleReader.read creates them,
    // all sharing the task's buffer: the first block grows it once and the rest reuse it, where
    // a buffer per iterator would be allocated, and zeroed, again for every block over the
    // initial size.
    val largeRows = 40 * 1024
    val blocks = Seq(intFrame(0 until largeRows), intFrame(0 until largeRows), intFrame(Seq(7)))
    val dependency = new CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      spark.sparkContext.emptyRDD[(Int, ColumnarBatch)],
      new HashPartitioner(1),
      decodeTime = SQLMetrics.createNanoTimingMetric(spark.sparkContext, "decode time"),
      outputAttributes = Seq.empty)
    val context: TaskContext = TaskContext.empty()
    val reader = new CometBlockStoreShuffleReader[Int, ColumnarBatch](
      new BaseShuffleHandle(dependency.shuffleId, dependency),
      Iterator.empty,
      context,
      context.taskMetrics().createTempShuffleReadMetrics(),
      serializerManager = null,
      blockManager = null,
      mapOutputTracker = null) {
      override protected[shuffle] def fetchIterator: Iterator[(BlockId, InputStream)] =
        blocks.iterator.zipWithIndex.map { case (block, i) =>
          (ShuffleBlockId(dependency.shuffleId, i.toLong, 0), new ByteArrayInputStream(block))
        }
    }
    val allocationsBefore = ShuffleReadBuffer.allocations.get()
    try {
      val rows = reader.read().map(_._2.numRows()).sum
      assert(rows == 2 * largeRows + 1)
      assert(ShuffleReadBuffer.allocations.get() - allocationsBefore == 1L)
    } finally {
      context.markTaskCompleted(None)
    }
  }

  test("coalescing decoder releases native state on early close and truncated input") {
    for (truncated <- Seq(false, true)) {
      var releases = 0
      val native = new Native() {
        override def closeShuffleBatchCoalescer(handle: Long): Unit = {
          releases += 1
          super.closeShuffleBatchCoalescer(handle)
        }
      }
      val complete = Array.fill(8)(intFrame()).flatten
      val input = new TrackingInputStream(
        if (truncated) complete.take(complete.length - 1)
        else complete)
      val nativeUtil = new NativeUtil()
      val decoder = NativeBatchDecoderIterator(
        input,
        SQLMetrics.createNanoTimingMetric(spark.sparkContext, "decode time"),
        native,
        nativeUtil,
        tracingEnabled = false,
        coalescing = Some(if (truncated) 128 else 16))
      try {
        if (truncated) intercept[EOFException](decoder.hasNext)
        else assert(decoder.hasNext) // Close with both exported and pending native batches.
        decoder.close()
        decoder.close()
        assert(releases == 1)
        assert(input.closeCalls == 1)
        assert(!decoder.hasNext)
      } finally {
        decoder.close()
        nativeUtil.close()
      }
    }
  }
}
