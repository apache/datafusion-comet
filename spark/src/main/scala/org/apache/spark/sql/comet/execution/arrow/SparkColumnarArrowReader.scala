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
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch}

/**
 * `ArrowReader` over an iterator of Spark-side `ColumnarBatch`es (not Arrow-backed). Slices up to
 * `maxRecordsPerBatch` rows per `loadNextBatch` from the current Spark batch into the reader's
 * stable VSR via `ArrowWriter.writeCol`. Spark's `ColumnVector` implementations aren't Arrow
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

  override protected def readSchema(): Schema = arrowSchema

  override def bytesRead(): Long = 0L

  override protected def closeReadSource(): Unit = ()

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

    val writer = ArrowWriter.create(getVectorSchemaRoot)
    var col = 0
    while (col < current.numCols()) {
      val column = current.column(col)
      val columnArray = new ColumnarArray(column, rowsConsumedInCurrent, rowsToProduce)
      if (column.hasNull) {
        writer.writeCol(columnArray, col)
      } else {
        writer.writeColNoNull(columnArray, col)
      }
      col += 1
    }
    rowsConsumedInCurrent += rowsToProduce

    writer.finish()
    onConversionNs(System.nanoTime() - startNs)
    true
  }
}
