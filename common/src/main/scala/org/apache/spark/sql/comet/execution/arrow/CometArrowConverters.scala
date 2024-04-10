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
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.NativeUtil

object CometArrowConverters extends Logging {
  // TODO: we should reuse the same root allocator in the comet code base?
  val rootAllocator: BufferAllocator = new RootAllocator(Long.MaxValue)

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
  private[sql] class ArrowBatchIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      timeZoneId: String,
      context: TaskContext)
      extends Iterator[ColumnarBatch]
      with AutoCloseable {

    private val arrowSchema = Utils.toArrowSchema(schema, timeZoneId)
    // Reuse the same root allocator here.
    private val allocator =
      rootAllocator.newChildAllocator(s"to${this.getClass.getSimpleName}", 0, Long.MaxValue)
    private val root = VectorSchemaRoot.create(arrowSchema, allocator)
    private val arrowWriter = ArrowWriter.create(root)

    private var currentBatch: ColumnarBatch = null
    private var closed: Boolean = false

    Option(context).foreach {
      _.addTaskCompletionListener[Unit] { _ =>
        close(true)
      }
    }

    override def hasNext: Boolean = rowIter.hasNext || {
      close(false)
      false
    }

    override def next(): ColumnarBatch = {
      currentBatch = nextBatch()
      currentBatch
    }

    override def close(): Unit = {
      close(false)
    }

    private def nextBatch(): ColumnarBatch = {
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

    private def close(closeAllocator: Boolean): Unit = {
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
  }

  def toArrowBatchIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      timeZoneId: String,
      context: TaskContext): Iterator[ColumnarBatch] = {
    new ArrowBatchIterator(rowIter, schema, maxRecordsPerBatch, timeZoneId, context)
  }
}
