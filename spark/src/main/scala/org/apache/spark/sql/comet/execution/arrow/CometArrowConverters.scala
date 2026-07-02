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
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.NativeUtil

/**
 * Convert a stream of Spark `InternalRow`s to a stream of independently-owned Arrow
 * `ColumnarBatch`es: each emitted batch owns a fresh `VectorSchemaRoot` with newly allocated
 * buffers and the consumer is responsible for closing it.
 *
 * This differs from [[RowArrowReader]], which reuses one stable `VectorSchemaRoot`
 * (release-and-replace) so only one batch is valid at a time. Use this when multiple emitted
 * batches must be alive simultaneously (e.g. tests that buffer several batches before consuming).
 * Buffers come from the caller-provided `BufferAllocator`, whose lifecycle the caller owns.
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
}
