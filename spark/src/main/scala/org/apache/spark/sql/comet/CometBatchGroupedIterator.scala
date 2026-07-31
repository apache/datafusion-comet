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

package org.apache.spark.sql.comet

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, InterpretedOrdering, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.{CometVector, NativeUtil}

/**
 * Splits sorted Comet batches into groups without materializing their data as rows.
 */
private[comet] class CometBatchGroupedIterator(
    input: Iterator[ColumnarBatch],
    groupingExpressions: Seq[Expression],
    inputAttributes: Seq[Attribute],
    projectedAttributes: Seq[Attribute],
    maxRowsPerBatch: Int = Int.MaxValue,
    maxBytesPerBatch: Long = Long.MaxValue,
    structInput: Boolean = false)
    extends Iterator[Iterator[ColumnarBatch]] {

  require(maxRowsPerBatch > 0)
  require(maxBytesPerBatch > 0)

  private val projection = UnsafeProjection.create(groupingExpressions, inputAttributes)
  private val projectedIndices = projectedAttributes.map { attr =>
    val index = inputAttributes.indexWhere(_.semanticEquals(attr))
    require(index >= 0, s"Cannot resolve $attr in ${inputAttributes.mkString(", ")}")
    index
  }
  private val keyOrdering =
    InterpretedOrdering.forSchema(groupingExpressions.map(_.dataType))
  private val keysEqual: (UnsafeRow, UnsafeRow) => Boolean =
    (left, right) => keyOrdering.compare(left, right) == 0

  private val nativeUtil = new NativeUtil
  private var currentBatch: ColumnarBatch = _
  private var currentRow = 0
  private var activeGroup: GroupIterator = _
  private var closed = false

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit] { _ => close() })

  advanceBatch()

  override def hasNext: Boolean = {
    require(
      activeGroup == null || !activeGroup.hasNext,
      "The previous Comet batch group must be consumed before requesting the next group")
    currentBatch != null
  }

  override def next(): Iterator[ColumnarBatch] = {
    if (!hasNext) {
      throw new NoSuchElementException
    }

    val key = keyAt(currentRow)
    activeGroup = new GroupIterator(key)
    activeGroup
  }

  private def keyAt(row: Int): UnsafeRow =
    projection(currentBatch.getRow(row)).copy()

  private def advanceBatch(): Unit = {
    if (currentBatch != null) {
      currentBatch.close()
      currentBatch = null
    }
    while (currentBatch == null && input.hasNext) {
      val next = input.next()
      if (next.numRows() == 0) {
        next.close()
      } else {
        currentBatch = next
        currentRow = 0
      }
    }
    if (currentBatch == null) {
      close()
    }
  }

  private class GroupIterator(key: UnsafeRow) extends Iterator[ColumnarBatch] {
    private var nextBatch: ColumnarBatch = _
    private var finished = false

    override def hasNext: Boolean = {
      if (nextBatch == null && !finished) {
        prepareNext()
      }
      nextBatch != null
    }

    override def next(): ColumnarBatch = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val result = nextBatch
      nextBatch = null
      result
    }

    private def prepareNext(): Unit = {
      if (currentBatch != null && currentRow == currentBatch.numRows()) {
        // The previously returned zero-copy slice has been consumed before Iterator.hasNext is
        // called again, so its source batch can now be closed safely.
        advanceBatch()
      }

      if (currentBatch == null || !keysEqual(key, keyAt(currentRow))) {
        finished = true
        activeGroup = null
        return
      }

      val start = currentRow
      val limit = start + math.min(maxRowsPerBatch, currentBatch.numRows() - start)
      while (currentRow < limit && keysEqual(key, keyAt(currentRow))) {
        currentRow += 1
      }
      var numRows = currentRow - start
      nextBatch = takeRows(start, numRows)
      if (numRows > 1 && exceedsByteLimit(nextBatch)) {
        nextBatch.close()
        var low = 1
        var high = numRows - 1
        while (low < high) {
          val mid = low + (high - low + 1) / 2
          val probe = takeRows(start, mid)
          val fits =
            try {
              !exceedsByteLimit(probe)
            } finally {
              probe.close()
            }
          if (fits) low = mid else high = mid - 1
        }
        numRows = low
        currentRow = start + numRows
        nextBatch = takeRows(start, numRows)
      }
    }

    private def takeRows(start: Int, numRows: Int): ColumnarBatch =
      nativeUtil.takeRows(currentBatch, start, numRows, projectedIndices)

    private def exceedsByteLimit(batch: ColumnarBatch): Boolean =
      maxBytesPerBatch != Long.MaxValue &&
        writableSize(batch) > maxBytesPerBatch

    private def writableSize(batch: ColumnarBatch): Long = {
      val vectorBytes = (0 until batch.numCols()).foldLeft(0L) { (size, index) =>
        size + batch.column(index).asInstanceOf[CometVector].getValueVector.getBufferSize
      }
      vectorBytes + (if (structInput) validityBytes(batch.numRows()) else 0L)
    }

    private def validityBytes(valueCount: Int): Long = (valueCount.toLong + 7L) / 8L

    def closePending(): Unit = {
      if (nextBatch != null) {
        nextBatch.close()
        nextBatch = null
      }
    }
  }

  private def close(): Unit = {
    if (!closed) {
      closed = true
      if (activeGroup != null) {
        activeGroup.closePending()
        activeGroup = null
      }
      if (currentBatch != null) {
        currentBatch.close()
        currentBatch = null
      }
      nativeUtil.close()
    }
  }
}
