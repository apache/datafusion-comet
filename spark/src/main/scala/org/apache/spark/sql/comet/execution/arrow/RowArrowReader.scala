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

import java.util

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.catalyst.InternalRow

import org.apache.comet.vector.NativeUtil

/**
 * `ArrowReader` over an iterator of Spark `InternalRow`s, writing up to `maxRecordsPerBatch` rows
 * per call into the reader's stable VSR via `ArrowWriter`.
 *
 * `ArrowWriter.create` calls `vector.allocateNew`, which releases any prior buffers and allocates
 * fresh ones. This is required for FFI safety: previously-exported batches retain their buffers
 * via the C release callback, so reusing those buffers in place would corrupt native consumers
 * still holding the prior batch.
 */
private[comet] class RowArrowReader(
    allocator: BufferAllocator,
    arrowSchema: Schema,
    rowIter: Iterator[InternalRow],
    maxRecordsPerBatch: Int,
    onConversionNs: Long => Unit = _ => ())
    extends ArrowReader(allocator) {

  require(maxRecordsPerBatch > 0, "Maximum records per batch must be positive")

  private var cometInitialized = false
  private var cometClosed = false
  private var cometRoot: VectorSchemaRoot = _

  override protected def readSchema(): Schema = arrowSchema

  override protected def initialize(): Unit = {
    cometRoot = NativeUtil.createVectorSchemaRootForExport(readSchema(), allocator)
    cometInitialized = true
  }

  override protected def ensureInitialized(): Unit = {
    if (!cometInitialized) initialize()
  }

  override def getVectorSchemaRoot: VectorSchemaRoot = {
    ensureInitialized()
    cometRoot
  }

  override def getDictionaryVectors: util.Map[java.lang.Long, Dictionary] = {
    ensureInitialized()
    util.Collections.emptyMap()
  }

  override def lookup(id: Long): Dictionary = {
    if (!cometInitialized) {
      throw new IllegalStateException("Unable to lookup until reader has been initialized")
    }
    null
  }

  override def getDictionaryIds: util.Set[java.lang.Long] = {
    ensureInitialized()
    util.Collections.emptySet()
  }

  override protected def prepareLoadNextBatch(): Unit = {
    ensureInitialized()
    cometRoot.setRowCount(0)
  }

  override def bytesRead(): Long = 0L

  override protected def closeReadSource(): Unit = ()

  override def close(): Unit = close(closeReadSource = true)

  override def close(closeReadSource: Boolean): Unit = {
    if (!cometClosed) {
      cometClosed = true
      if (cometInitialized && cometRoot != null) cometRoot.close()
      if (closeReadSource) this.closeReadSource()
    }
  }

  override def loadNextBatch(): Boolean = {
    prepareLoadNextBatch()

    if (!rowIter.hasNext) {
      return false
    }

    val startNs = System.nanoTime()
    val writer = ArrowWriter.create(getVectorSchemaRoot, maxRecordsPerBatch)
    var rowCount = 0
    while (rowIter.hasNext && rowCount < maxRecordsPerBatch) {
      writer.write(rowIter.next())
      rowCount += 1
    }
    writer.finish()
    onConversionNs(System.nanoTime() - startNs)
    true
  }
}
