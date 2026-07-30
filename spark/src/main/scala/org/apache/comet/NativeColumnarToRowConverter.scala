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

package org.apache.comet

import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.Platform

import org.apache.comet.serde.QueryPlanSerde
import org.apache.comet.vector.{CometVector, NativeUtil}

/**
 * Native converter that converts Arrow columnar data to Spark UnsafeRow format.
 *
 * This converter maintains a native handle that holds the conversion context and output buffer.
 * The buffer is reused across conversions to minimize allocations.
 *
 * The Arrow FFI structs used to hand the batch to native code are allocated once per converter
 * and reused for every batch, and their addresses are registered with the native context at
 * initialization. The (constant) C schema is only exported when a column's Arrow `Field` changes,
 * so steady-state conversion only exports the Arrow arrays.
 *
 * Memory Management:
 *   - The native side owns the output buffer
 *   - UnsafeRow objects returned by convert() point directly to native memory (zero-copy)
 *   - The buffer is valid until the next convert() call or close()
 *   - Always call close() when done to release native resources
 *
 * @param schema
 *   The schema of the data to convert
 * @param batchSize
 *   Maximum number of rows per batch (used for buffer pre-allocation)
 */
class NativeColumnarToRowConverter(schema: StructType, batchSize: Int) extends AutoCloseable {

  private val nativeLib = new Native()
  private val nativeUtil = new NativeUtil()

  private val numCols = schema.fields.length

  // Serialize the schema for native initialization
  private val serializedSchema: Array[Array[Byte]] = schema.fields.map { field =>
    QueryPlanSerde.serializeDataType(field.dataType) match {
      case Some(dataType) => dataType.toByteArray
      case None =>
        throw new UnsupportedOperationException(
          s"Data type ${field.dataType} is not supported for native columnar to row conversion")
    }
  }

  // Arrow FFI structs are allocated once and reused for every batch. Their addresses are stable,
  // so the native side records them at init and no addresses are passed per batch.
  private val (arrowArrays, arrowSchemas) = nativeUtil.allocateArrowStructs(numCols)

  // Initialize native context - handle is 0 if initialization failed
  private var c2rHandle: Long = nativeLib.columnarToRowInit(
    serializedSchema,
    batchSize,
    arrowArrays.map(_.memoryAddress()),
    arrowSchemas.map(_.memoryAddress()))

  // The Arrow Field of each column as of the last C schema export. The schema is only re-exported
  // when a field changes (for example when a column becomes dictionary-encoded).
  private val cachedFields = new Array[Field](numCols)

  // Reusable UnsafeRow for iteration
  private val unsafeRow = new UnsafeRow(schema.fields.length)

  // Reused across batches: rows from a previous batch are invalidated by the next convert() call
  // anyway, since the native output buffer is reused.
  private val rowIterator = new NativeRowIterator(unsafeRow)

  /**
   * Converts a ColumnarBatch to an iterator of InternalRows.
   *
   * The returned iterator yields UnsafeRow objects that point directly to native memory. These
   * rows are valid only until the next call to convert() or close().
   *
   * @param batch
   *   The columnar batch to convert
   * @return
   *   An iterator of InternalRows
   */
  def convert(batch: ColumnarBatch): Iterator[InternalRow] = {
    if (c2rHandle == 0) {
      throw new IllegalStateException("NativeColumnarToRowConverter has been closed")
    }

    val numRows = batch.numRows()
    if (numRows == 0) {
      return Iterator.empty
    }

    // Export the batch into the reused Arrow FFI structs, exporting the C schema only when a
    // column's Arrow Field has changed since the last export.
    val exportSchema = schemaChanged(batch)
    val exportedNumRows =
      nativeUtil.exportBatchToStructs(arrowArrays, arrowSchemas, batch, exportSchema)

    // Call native conversion. The returned address points at three longs: the row buffer, the
    // row offsets and the row lengths.
    val metaAddr = nativeLib.columnarToRowConvert(c2rHandle, exportedNumRows, exportSchema)

    rowIterator.reset(metaAddr, exportedNumRows)
    rowIterator
  }

  /**
   * Returns true if any column's Arrow `Field` differs from the one seen at the last C schema
   * export, meaning the schema has to be exported again for this batch.
   */
  private def schemaChanged(batch: ColumnarBatch): Boolean = {
    var changed = false
    var i = 0
    while (i < numCols) {
      batch.column(i) match {
        case v: CometVector =>
          val field = v.getValueVector.getField
          val cached = cachedFields(i)
          if (!(cached != null && (cached.eq(field) || cached.equals(field)))) {
            cachedFields(i) = field
            changed = true
          }
        case _ =>
          // Non-Arrow vectors (for example ConstantColumnVector) are materialized into a fresh
          // Arrow vector per batch, so always export their schema.
          changed = true
      }
      i += 1
    }
    changed
  }

  /**
   * Checks if this converter is still open and usable.
   */
  def isOpen: Boolean = c2rHandle != 0

  /**
   * Closes the converter and releases native resources.
   */
  override def close(): Unit = {
    if (c2rHandle != 0) {
      nativeLib.columnarToRowClose(c2rHandle)
      c2rHandle = 0
    }
    arrowArrays.foreach(_.close())
    arrowSchemas.foreach(_.close())
    nativeUtil.close()
  }
}

/**
 * Iterator that yields UnsafeRows backed by native memory.
 *
 * The UnsafeRow is reused across iterations - callers must copy the row if they need to retain it
 * beyond the current iteration.
 */
private class NativeRowIterator(unsafeRow: UnsafeRow) extends Iterator[InternalRow] {

  private var bufferAddr: Long = 0
  private var offsetsAddr: Long = 0
  private var lengthsAddr: Long = 0
  private var numRows: Int = 0
  private var currentIdx: Int = 0

  /** Points this iterator at the result of a conversion. */
  def reset(metaAddr: Long, numRows: Int): Unit = {
    bufferAddr = Platform.getLong(null, metaAddr)
    offsetsAddr = Platform.getLong(null, metaAddr + 8)
    lengthsAddr = Platform.getLong(null, metaAddr + 16)
    this.numRows = numRows
    currentIdx = 0
  }

  override def hasNext: Boolean = currentIdx < numRows

  override def next(): InternalRow = {
    if (!hasNext) {
      throw new NoSuchElementException("No more rows")
    }

    // Point the UnsafeRow to the native memory
    val rowOffset = Platform.getInt(null, offsetsAddr + 4L * currentIdx)
    val rowSize = Platform.getInt(null, lengthsAddr + 4L * currentIdx)

    unsafeRow.pointTo(null, bufferAddr + rowOffset, rowSize)
    currentIdx += 1

    unsafeRow.copy()
  }
}
