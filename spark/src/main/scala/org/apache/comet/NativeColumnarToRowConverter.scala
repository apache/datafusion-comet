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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.serde.QueryPlanSerde
import org.apache.comet.vector.NativeUtil

/**
 * Native converter that converts Arrow columnar data to Spark UnsafeRow format.
 *
 * This converter maintains a native handle that holds the conversion context and output buffer.
 * The buffer is reused across conversions to minimize allocations.
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

  // Serialize the schema for native initialization
  private val serializedSchema: Array[Array[Byte]] = schema.fields.map { field =>
    QueryPlanSerde.serializeDataType(field.dataType) match {
      case Some(dataType) => dataType.toByteArray
      case None =>
        throw new UnsupportedOperationException(
          s"Data type ${field.dataType} is not supported for native columnar to row conversion")
    }
  }

  // Initialize native context - handle is 0 if initialization failed
  private var c2rHandle: Long = nativeLib.columnarToRowInit(serializedSchema, batchSize)

  // Reusable UnsafeRow for iteration
  private val unsafeRow = new UnsafeRow(schema.fields.length)

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

    // Export the batch to Arrow FFI and get memory addresses
    val (arrayAddrs, schemaAddrs, exportedNumRows) = nativeUtil.exportBatchToAddresses(batch)

    // Call native conversion
    val info = nativeLib.columnarToRowConvert(c2rHandle, arrayAddrs, schemaAddrs, exportedNumRows)

    // Return an iterator that yields UnsafeRows pointing to native memory
    new NativeRowIterator(info, unsafeRow)
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
    nativeUtil.close()
  }
}

/**
 * Iterator that yields UnsafeRows backed by native memory.
 *
 * The UnsafeRow is reused across iterations - callers must copy the row if they need to retain it
 * beyond the current iteration.
 */
private class NativeRowIterator(info: NativeColumnarToRowInfo, unsafeRow: UnsafeRow)
    extends Iterator[InternalRow] {

  private var currentIdx = 0
  private val numRows = info.numRows()

  override def hasNext: Boolean = currentIdx < numRows

  override def next(): InternalRow = {
    if (!hasNext) {
      throw new NoSuchElementException("No more rows")
    }

    // Point the UnsafeRow to the native memory
    val rowAddress = info.memoryAddress + info.offsets(currentIdx)
    val rowSize = info.lengths(currentIdx)

    unsafeRow.pointTo(null, rowAddress, rowSize)
    currentIdx += 1

    unsafeRow
  }
}
