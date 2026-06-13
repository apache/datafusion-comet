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
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.catalyst.InternalRow

/**
 * `ArrowReader` over an iterator of Spark `InternalRow`s, writing up to `maxRecordsPerBatch` rows
 * per call into the reader's stable VSR via `ArrowWriter`.
 *
 * `ArrowWriter.create(root)` calls `vector.allocateNew()`, which releases any prior buffers and
 * allocates fresh ones. This is required for FFI safety: previously-exported batches retain their
 * buffers via the C release callback, so reusing those buffers in place would corrupt native
 * consumers still holding the prior batch.
 */
private[comet] class RowArrowReader(
    allocator: BufferAllocator,
    arrowSchema: Schema,
    rowIter: Iterator[InternalRow],
    maxRecordsPerBatch: Long,
    onConversionNs: Long => Unit = _ => ())
    extends ArrowReader(allocator) {

  override protected def readSchema(): Schema = arrowSchema

  override def bytesRead(): Long = 0L

  override protected def closeReadSource(): Unit = ()

  override def loadNextBatch(): Boolean = {
    prepareLoadNextBatch()

    if (!rowIter.hasNext) {
      return false
    }

    val startNs = System.nanoTime()
    val writer = ArrowWriter.create(getVectorSchemaRoot)
    var rowCount = 0L
    while (rowIter.hasNext &&
      (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
      writer.write(rowIter.next())
      rowCount += 1
    }
    writer.finish()
    onConversionNs(System.nanoTime() - startNs)
    true
  }
}
