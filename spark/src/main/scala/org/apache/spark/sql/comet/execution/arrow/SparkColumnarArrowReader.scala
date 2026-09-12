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
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.NativeUtil

/**
 * `ArrowReader` over an iterator of Spark-side `ColumnarBatch`es (not Arrow-backed). Slices up to
 * `maxRecordsPerBatch` rows per `loadNextBatch` from the current Spark batch into the reader's
 * stable VSR via `ArrowWriter.writeColumns`. Spark's `ColumnVector` implementations aren't Arrow
 * buffers, so this reader necessarily copies element values into Arrow format.
 */
private[comet] class SparkColumnarArrowReader(
    allocator: BufferAllocator,
    arrowSchema: Schema,
    source: Iterator[ColumnarBatch],
    maxRecordsPerBatch: Int,
    onConversionNs: Long => Unit = _ => ())
    extends ArrowReader(allocator) {

  private var current: ColumnarBatch = _
  private var rowsConsumedInCurrent: Int = 0
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

  private def advanceToNonEmptyBatch(): Boolean = {
    while (current == null || rowsConsumedInCurrent >= current.numRows()) {
      if (current != null) {
        // We don't own Spark ColumnarBatches; just drop the reference.
        current = null
        rowsConsumedInCurrent = 0
      }
      if (!source.hasNext) {
        return false
      }
      current = source.next()
      rowsConsumedInCurrent = 0
    }
    true
  }

  override def loadNextBatch(): Boolean = {
    prepareLoadNextBatch()

    if (!advanceToNonEmptyBatch()) {
      return false
    }

    val startNs = System.nanoTime()
    val rowsRemaining = current.numRows() - rowsConsumedInCurrent
    val rowsToProduce =
      if (maxRecordsPerBatch <= 0) rowsRemaining
      else math.min(maxRecordsPerBatch, rowsRemaining)

    val writer = ArrowWriter.create(getVectorSchemaRoot, rowsToProduce)
    writer.writeColumns(current, rowsConsumedInCurrent, rowsToProduce)
    rowsConsumedInCurrent += rowsToProduce

    writer.finish()
    onConversionNs(System.nanoTime() - startNs)
    true
  }
}
