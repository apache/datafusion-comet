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

package org.apache.spark.sql.comet.execution.arrow

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch}

import org.apache.comet.vector.NativeUtil

object CometArrowConverters extends Logging {
  // TODO: we should reuse the same root allocator in the comet code base?
  private val rootAllocator: BufferAllocator = new RootAllocator(Long.MaxValue)

  // This is similar how Spark converts internal row to Arrow format except that it is transforming
  // the result batch to Comet's ColumnarBatch instead of serialized bytes.
  // There's another big difference that Comet may consume the ColumnarBatch by exporting it to
  // the native side. Hence, we need to:
  // 1. reset the Arrow writer after the ColumnarBatch is consumed
  // 2. close the allocator when the task is finished but not when the iterator is all consumed
  // The reason for the second point is that when ColumnarBatch is exported to the native side, the
  // exported process increases the reference count of the Arrow vectors. The reference count is
  // only decreased when the native plan is done with the vectors, which is usually longer than
  // all the ColumnarBatches are consumed.

  abstract private[sql] class ArrowBatchIterBase(
      schema: StructType,
      timeZoneId: String,
      context: TaskContext)
      extends Iterator[ColumnarBatch]
      with AutoCloseable {

    protected val arrowSchema: Schema = Utils.toArrowSchema(schema, timeZoneId)
    // Reuse the same root allocator here.
    protected val allocator: BufferAllocator =
      rootAllocator.newChildAllocator(s"to${this.getClass.getSimpleName}", 0, Long.MaxValue)
    protected val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    protected val arrowWriter: ArrowWriter = ArrowWriter.create(root)

    protected var currentBatch: ColumnarBatch = null
    protected var closed: Boolean = false

    Option(context).foreach {
      _.addTaskCompletionListener[Unit] { _ =>
        close(true)
      }
    }

    override def close(): Unit = {
      close(false)
    }

    protected def close(closeAllocator: Boolean): Unit = {
      try {
        if (!closed) {
          if (currentBatch != null) {
            arrowWriter.reset()
            currentBatch.close()
            currentBatch = null
          }
          root.close()
          closed = true
        }
      } finally {
        // the allocator shall be closed when the task is finished
        if (closeAllocator) {
          allocator.close()
        }
      }
    }

    override def next(): ColumnarBatch = {
      currentBatch = nextBatch()
      currentBatch
    }

    protected def nextBatch(): ColumnarBatch

  }

  private[sql] class RowToArrowBatchIter(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      timeZoneId: String,
      context: TaskContext)
      extends ArrowBatchIterBase(schema, timeZoneId, context)
      with AutoCloseable {

    override def hasNext: Boolean = rowIter.hasNext || {
      close(false)
      false
    }

    override protected def nextBatch(): ColumnarBatch = {
      if (rowIter.hasNext) {
        // the arrow writer shall be reset before writing the next batch
        arrowWriter.reset()
        var rowCount = 0L
        while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
          val row = rowIter.next()
          arrowWriter.write(row)
          rowCount += 1
        }
        arrowWriter.finish()
        NativeUtil.rootAsBatch(root)
      } else {
        null
      }
    }
  }

  def rowToArrowBatchIter(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      timeZoneId: String,
      context: TaskContext): Iterator[ColumnarBatch] = {
    new RowToArrowBatchIter(rowIter, schema, maxRecordsPerBatch, timeZoneId, context)
  }

  private[sql] class ColumnBatchToArrowBatchIter(
      colBatch: ColumnarBatch,
      schema: StructType,
      maxRecordsPerBatch: Int,
      timeZoneId: String,
      context: TaskContext)
      extends ArrowBatchIterBase(schema, timeZoneId, context)
      with AutoCloseable {

    private var rowsProduced: Int = 0

    override def hasNext: Boolean = rowsProduced < colBatch.numRows() || {
      close(false)
      false
    }

    override protected def nextBatch(): ColumnarBatch = {
      val rowsInBatch = colBatch.numRows()
      if (rowsProduced < rowsInBatch) {
        // the arrow writer shall be reset before writing the next batch
        arrowWriter.reset()
        val rowsToProduce =
          if (maxRecordsPerBatch <= 0) rowsInBatch - rowsProduced
          else Math.min(maxRecordsPerBatch, rowsInBatch - rowsProduced)

        for (columnIndex <- 0 until colBatch.numCols()) {
          val column = colBatch.column(columnIndex)
          val columnArray = new ColumnarArray(column, rowsProduced, rowsToProduce)
          if (column.hasNull) {
            arrowWriter.writeCol(columnArray, columnIndex)
          } else {
            arrowWriter.writeColNoNull(columnArray, columnIndex)
          }
        }

        rowsProduced += rowsToProduce

        arrowWriter.finish()
        NativeUtil.rootAsBatch(root)
      } else {
        null
      }
    }
  }

  def columnarBatchToArrowBatchIter(
      colBatch: ColumnarBatch,
      schema: StructType,
      maxRecordsPerBatch: Int,
      timeZoneId: String,
      context: TaskContext): Iterator[ColumnarBatch] = {
    new ColumnBatchToArrowBatchIter(colBatch, schema, maxRecordsPerBatch, timeZoneId, context)
  }
}
