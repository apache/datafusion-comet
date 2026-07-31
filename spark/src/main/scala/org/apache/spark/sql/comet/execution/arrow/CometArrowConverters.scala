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

import scala.util.control.NonFatal

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
 * Convert Spark data that is not Arrow-backed (`InternalRow`s, or `ColumnarBatch`es whose columns
 * are Spark/third-party `ColumnVector`s) into independently-owned Arrow `ColumnarBatch`es: each
 * emitted batch owns a fresh `VectorSchemaRoot` with newly allocated buffers and the consumer is
 * responsible for closing it.
 *
 * This differs from [[RowArrowReader]] and [[SparkColumnarArrowReader]], which reuse one stable
 * `VectorSchemaRoot` (release-and-replace) so only one batch is valid at a time. Use this when
 * multiple emitted batches must be alive simultaneously (e.g. tests that buffer several batches
 * before consuming). Buffers come from the caller-provided `BufferAllocator`, whose lifecycle the
 * caller owns.
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
        // Same ownership rule as columnarBatchToArrowBatch: the caller only owns the batch that
        // rootAsBatch returns, so a throw from writing a row has to release the root here.
        try {
          val writer = ArrowWriter.create(root)
          var rowCount = 0L
          while (rowIter.hasNext &&
            (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
            writer.write(rowIter.next())
            rowCount += 1
          }
          writer.finish()
          NativeUtil.rootAsBatch(root)
        } catch {
          case NonFatal(e) =>
            root.close()
            throw e
        }
      }
    }
  }

  /**
   * Copy `numRows` rows starting at `startRow` from a Spark `ColumnarBatch` into `root`.
   *
   * Spark's `ColumnVector` implementations do not expose Arrow buffers, so values are necessarily
   * copied element-wise. Shared by [[SparkColumnarArrowReader]], which slices into a stable root,
   * and [[columnarBatchToArrowBatch]], which fills a fresh one.
   */
  private[arrow] def writeColumns(
      root: VectorSchemaRoot,
      batch: ColumnarBatch,
      startRow: Int,
      numRows: Int): Unit = {
    val writer = ArrowWriter.create(root)
    var col = 0
    while (col < batch.numCols()) {
      val column = batch.column(col)
      val columnArray = new ColumnarArray(column, startRow, numRows)
      if (column.hasNull) {
        writer.writeCol(columnArray, col)
      } else {
        writer.writeColNoNull(columnArray, col)
      }
      col += 1
    }
    writer.finish()
    // ArrowWriter derives the root row count from its per-column writes, so a zero-column input
    // batch (Spark's count-from-metadata scan: numRows > 0, numCols == 0) would otherwise produce
    // a root with rowCount == 0 and silently drop the rows.
    root.setRowCount(numRows)
  }

  /**
   * Copy a Spark `ColumnarBatch` whose columns are not Arrow-backed (e.g.
   * `On/OffHeapColumnVector` from Spark's vectorized Parquet reader, or a third-party connector's
   * vectors) into a freshly allocated Arrow `ColumnarBatch` of `CometVector`s.
   *
   * The input batch is not consumed or closed; the caller owns the returned batch and must close
   * it.
   */
  def columnarBatchToArrowBatch(
      batch: ColumnarBatch,
      arrowSchema: Schema,
      allocator: BufferAllocator): ColumnarBatch = {
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    // The caller only owns the returned batch, so anything that throws before `rootAsBatch` wraps
    // the root has to release it here or the allocation leaks.
    try {
      writeColumns(root, batch, 0, batch.numRows())
      NativeUtil.rootAsBatch(root)
    } catch {
      case NonFatal(e) =>
        root.close()
        throw e
    }
  }
}
