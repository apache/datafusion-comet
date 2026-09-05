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
import org.apache.spark.sql.vectorized.ColumnarBatch

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
      maxRecordsPerBatch: Int,
      timeZoneId: String,
      allocator: BufferAllocator): Iterator[ColumnarBatch] = {
    require(maxRecordsPerBatch > 0, "Maximum records per batch must be positive")
    val arrowSchema: Schema = Utils.toArrowSchema(schema, timeZoneId)

    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = rowIter.hasNext

      override def next(): ColumnarBatch = {
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        // Same ownership rule as columnarBatchToArrowBatch: the caller only owns the batch that
        // rootAsBatch returns, so a throw from writing a row has to release the root here.
        closingRootOnFailure(root) {
          val writer = ArrowWriter.create(root, maxRecordsPerBatch)
          var rowCount = 0
          while (rowIter.hasNext && rowCount < maxRecordsPerBatch) {
            writer.write(rowIter.next())
            rowCount += 1
          }
          writer.finish(rowCount)
          NativeUtil.rootAsBatch(root)
        }
      }
    }
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
    val numRows = batch.numRows()
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    // The caller only owns the returned batch, so anything that throws before `rootAsBatch` wraps
    // the root has to release it here or the allocation leaks.
    closingRootOnFailure(root) {
      val writer = ArrowWriter.create(root, numRows)
      writer.writeColumns(batch, 0, numRows)
      writer.finish(numRows)
      NativeUtil.rootAsBatch(root)
    }
  }

  /**
   * Run `body`, closing `root` if it throws. On success the returned batch takes ownership of
   * `root`, so it is deliberately left open.
   *
   * A failing `close` is attached as a suppressed exception rather than replacing the original,
   * following `SparkErrorUtils.tryWithSafeFinally`: releasing an Arrow root can itself throw
   * (e.g. `IllegalStateException` for outstanding child allocations), and that is the less
   * informative of the two failures.
   */
  private def closingRootOnFailure(root: VectorSchemaRoot)(
      body: => ColumnarBatch): ColumnarBatch = {
    try {
      body
    } catch {
      case NonFatal(e) =>
        try {
          root.close()
        } catch {
          case NonFatal(closeError) => e.addSuppressed(closeError)
        }
        throw e
    }
  }
}
