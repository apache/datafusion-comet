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

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch}

import org.apache.comet.vector.NativeUtil

/**
 * Convert Spark `InternalRow` / `ColumnarBatch` streams to a stream of independently-owned Arrow
 * `ColumnarBatch`es. Each emitted batch owns a fresh `VectorSchemaRoot` with newly allocated
 * buffers; the consumer is responsible for closing the batch.
 *
 * Buffers are allocated from the caller-provided `BufferAllocator`. The caller owns the
 * allocator's lifecycle (typically a child allocator closed at task completion). When emitted
 * batches reach `ColumnarBatchArrowReader.loadNextBatch`, ownership of their buffers is
 * transferred (via `VectorUnloader` / `loadFieldBuffers`) to the reader's allocator, after which
 * the source batch is closed and the producer's allocator returns to zero outstanding bytes.
 */
object CometArrowConverters extends Logging {

  /**
   * Convert an iterator of Spark `InternalRow`s into an iterator of Arrow `ColumnarBatch`es.
   *
   * Each call to `next()` allocates a fresh `VectorSchemaRoot`, writes up to `maxRecordsPerBatch`
   * rows into it, and emits a `ColumnarBatch` wrapping that root. The consumer must close every
   * emitted batch.
   */
  def rowToArrowBatchIter(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      timeZoneId: String,
      allocator: BufferAllocator): Iterator[ColumnarBatch] = {
    val arrowSchema: Schema = Utils.toArrowSchema(schema, timeZoneId)

    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = rowIter.hasNext

      override def next(): ColumnarBatch = {
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        val writer = ArrowWriter.create(root)
        var rowCount = 0L
        while (rowIter.hasNext &&
          (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
          writer.write(rowIter.next())
          rowCount += 1
        }
        writer.finish()
        NativeUtil.rootAsBatch(root)
      }
    }
  }

  /**
   * Slice a single Spark `ColumnarBatch` into one or more Arrow `ColumnarBatch`es of at most
   * `maxRecordsPerBatch` rows each. Each emitted batch owns a fresh `VectorSchemaRoot`.
   */
  def columnarBatchToArrowBatchIter(
      colBatch: ColumnarBatch,
      schema: StructType,
      maxRecordsPerBatch: Int,
      timeZoneId: String,
      allocator: BufferAllocator): Iterator[ColumnarBatch] = {
    val arrowSchema: Schema = Utils.toArrowSchema(schema, timeZoneId)
    val totalRows = colBatch.numRows()

    new Iterator[ColumnarBatch] {
      private var rowsProduced: Int = 0

      override def hasNext: Boolean = rowsProduced < totalRows

      override def next(): ColumnarBatch = {
        val rowsToProduce =
          if (maxRecordsPerBatch <= 0) totalRows - rowsProduced
          else math.min(maxRecordsPerBatch, totalRows - rowsProduced)

        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        val writer = ArrowWriter.create(root)

        for (columnIndex <- 0 until colBatch.numCols()) {
          val column = colBatch.column(columnIndex)
          val columnArray = new ColumnarArray(column, rowsProduced, rowsToProduce)
          if (column.hasNull) {
            writer.writeCol(columnArray, columnIndex)
          } else {
            writer.writeColNoNull(columnArray, columnIndex)
          }
        }

        rowsProduced += rowsToProduce
        writer.finish()
        NativeUtil.rootAsBatch(root)
      }
    }
  }
}
