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
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * `ArrowReader` over an iterator of Spark-side `ColumnarBatch`es (not Arrow-backed). Each
 * `loadNextBatch` fills the reader's stable VSR with up to `maxRecordsPerBatch` rows via
 * `ArrowWriter.writeColumns`, slicing a large Spark batch and appending consecutive small ones,
 * so the output batches are full whatever size the source produces: Spark's vectorized Parquet
 * reader emits 4096 rows a batch and its in-memory cache 10000. Spark's `ColumnVector`
 * implementations aren't Arrow buffers, so this reader necessarily copies element values into
 * Arrow format. Each Spark batch is copied before the next is requested, since producers reuse
 * them, and batches are not combined across input-file boundaries, since Spark evaluates
 * `input_file_*` expressions from the current [[InputFileBlockHolder]].
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
  private var currentInputFilePath: String = _
  private var currentInputFileStart: Long = -1L
  private var currentInputFileLength: Long = -1L

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
      currentInputFilePath = InputFileBlockHolder.getInputFilePath.toString
      currentInputFileStart = InputFileBlockHolder.getStartOffset
      currentInputFileLength = InputFileBlockHolder.getLength
    }
    true
  }

  private def restoreInputFile(path: String, start: Long, length: Long): Unit = {
    if (start < 0) {
      if (InputFileBlockHolder.getStartOffset >= 0) {
        InputFileBlockHolder.unset()
      }
    } else {
      InputFileBlockHolder.set(path, start, length)
    }
  }

  override def loadNextBatch(): Boolean = {
    prepareLoadNextBatch()

    if (!advanceToNonEmptyBatch()) {
      return false
    }
    // `current` may have been pulled while finishing the previous Arrow batch, then buffered
    // because its file context differed. Install its context only after that batch was consumed.
    restoreInputFile(currentInputFilePath, currentInputFileStart, currentInputFileLength)
    val outputInputFilePath = currentInputFilePath
    val outputInputFileStart = currentInputFileStart
    val outputInputFileLength = currentInputFileLength

    // Without a limit, each Spark batch becomes one Arrow batch.
    val batchSize =
      if (maxRecordsPerBatch <= 0) current.numRows() - rowsConsumedInCurrent
      else maxRecordsPerBatch
    // Pulling the next source batch is the producer's time, not conversion time.
    var conversionNs = 0L
    var startNs = System.nanoTime()
    val writer = ArrowWriter.create(getVectorSchemaRoot, batchSize)
    var rowsProduced = 0
    var more = true
    while (more) {
      val rows = math.min(batchSize - rowsProduced, current.numRows() - rowsConsumedInCurrent)
      writer.writeColumns(current, rowsConsumedInCurrent, rows)
      rowsConsumedInCurrent += rows
      rowsProduced += rows
      more = rowsProduced < batchSize && {
        conversionNs += System.nanoTime() - startNs
        val advanced = advanceToNonEmptyBatch()
        startNs = System.nanoTime()
        val sameInputFile = advanced &&
          currentInputFilePath == outputInputFilePath &&
          currentInputFileStart == outputInputFileStart &&
          currentInputFileLength == outputInputFileLength
        if (!sameInputFile) {
          // `source.next()` updates InputFileBlockHolder before the current Arrow batch reaches
          // downstream Spark expressions. Keep the new Spark batch buffered and restore the
          // context that describes every row in the Arrow batch being returned.
          restoreInputFile(outputInputFilePath, outputInputFileStart, outputInputFileLength)
        }
        sameInputFile
      }
    }
    writer.finish()
    onConversionNs(conversionNs + System.nanoTime() - startNs)
    true
  }
}
