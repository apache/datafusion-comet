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

import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types._
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
 * @param copyRows
 *   Whether each row is copied to the JVM heap before being returned. When false the returned
 *   `UnsafeRow` is reused and points at the native output buffer, which stays valid until the
 *   next `convert` call - the same contract as Spark's own `ColumnarToRowExec`, which reuses its
 *   row on every `next()`. Callers that retain rows across batches (for example the broadcast
 *   path, see #3308) must pass true.
 */
class NativeColumnarToRowConverter(schema: StructType, batchSize: Int, copyRows: Boolean = true)
    extends AutoCloseable {

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

  // Scratch space in native memory that raw buffer addresses are written into, for batches that
  // can skip Arrow FFI altogether.
  private val rawAddrs: Long = nativeLib.columnarToRowRawAddrs(c2rHandle)

  // Whether every column's type can be handed over by raw buffer address.
  private val rawSupported: Boolean =
    schema.fields.forall(f => NativeColumnarToRowConverter.supportsRawTransfer(f.dataType))

  // Reusable UnsafeRow for iteration
  private val unsafeRow = new UnsafeRow(schema.fields.length)

  // Reused across batches: rows from a previous batch are invalidated by the next convert() call
  // anyway, since the native output buffer is reused.
  private val rowIterator = new NativeRowIterator(unsafeRow, copyRows)

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

    // Hand the batch to native code. Preferred route is raw buffer addresses, which skips Arrow
    // FFI entirely; that needs the native side to already have the Arrow types from a previous
    // schema export, so the first batch (and any batch whose fields changed) goes through FFI.
    val exportSchema = schemaChanged(batch)
    val mode =
      if (!exportSchema && rawSupported && writeRawAddrs(batch, numRows)) {
        NativeColumnarToRowConverter.MODE_RAW
      } else {
        nativeUtil.exportBatchToStructs(arrowArrays, arrowSchemas, batch, exportSchema)
        if (exportSchema) NativeColumnarToRowConverter.MODE_FFI_WITH_SCHEMA
        else NativeColumnarToRowConverter.MODE_FFI
      }

    // The returned address points at three longs: the row buffer, the row offsets and the row
    // lengths.
    val metaAddr = nativeLib.columnarToRowConvert(c2rHandle, numRows, mode)

    rowIterator.reset(metaAddr, numRows)
    rowIterator
  }

  /**
   * Writes the raw Arrow buffer addresses of every column into the native scratch space.
   *
   * Returns false without completing if any column cannot be handed over this way (a
   * dictionary-encoded column, a non-Arrow vector, an unsupported vector layout, or a row count
   * that disagrees with the batch), in which case the caller falls back to Arrow FFI.
   */
  private def writeRawAddrs(batch: ColumnarBatch, numRows: Int): Boolean = {
    var i = 0
    while (i < numCols) {
      batch.column(i) match {
        case v: CometVector =>
          val vector = v.getValueVector
          if (vector.getField.getDictionary != null || vector.getValueCount != numRows) {
            return false
          }

          val base = rawAddrs + i * NativeColumnarToRowConverter.RAW_SLOTS_PER_COLUMN * 8

          // A zero validity address tells native code the column has no nulls.
          if (vector.getNullCount == 0) {
            Platform.putLong(null, base, 0L)
            Platform.putLong(null, base + 8, 0L)
          } else {
            val validity = vector.getValidityBuffer
            Platform.putLong(null, base, validity.memoryAddress())
            Platform.putLong(null, base + 8, validity.capacity())
          }

          vector match {
            case fixed: BaseFixedWidthVector =>
              val data = fixed.getDataBuffer
              Platform.putLong(null, base + 16, data.memoryAddress())
              Platform.putLong(null, base + 24, data.capacity())
              Platform.putLong(null, base + 32, 0L)
              Platform.putLong(null, base + 40, 0L)
            case varWidth: BaseVariableWidthVector =>
              val offsets = varWidth.getOffsetBuffer
              val data = varWidth.getDataBuffer
              Platform.putLong(null, base + 16, offsets.memoryAddress())
              Platform.putLong(null, base + 24, offsets.capacity())
              Platform.putLong(null, base + 32, data.memoryAddress())
              Platform.putLong(null, base + 40, data.capacity())
            case _ =>
              // For example large var-width vectors, which use 64-bit offsets.
              return false
          }
        case _ =>
          return false
      }
      i += 1
    }
    true
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

object NativeColumnarToRowConverter {

  /** Batch handed over through Arrow FFI, with the C schema exported for this batch. */
  private[comet] val MODE_FFI_WITH_SCHEMA = 0

  /** Batch handed over through Arrow FFI, reusing the Arrow types cached natively. */
  private[comet] val MODE_FFI = 1

  /** Batch handed over as raw Arrow buffer addresses, skipping Arrow FFI. */
  private[comet] val MODE_RAW = 2

  /** Must match `RAW_SLOTS_PER_COLUMN` in `columnar_to_row.rs`. */
  private[comet] val RAW_SLOTS_PER_COLUMN = 6

  /**
   * Whether a column of this type can be handed to native code as raw Arrow buffer addresses.
   * Nested types keep going through Arrow FFI, which rebuilds their child arrays.
   */
  private[comet] def supportsRawTransfer(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType => true
    case DateType | TimestampType | TimestampNTZType => true
    case _: DecimalType => true
    case StringType | BinaryType => true
    case _ => false
  }
}

/**
 * Iterator that yields UnsafeRows backed by native memory.
 *
 * When `copyRows` is false the UnsafeRow is reused across iterations and points at the native
 * output buffer - callers must copy the row if they need to retain it beyond the current batch.
 */
private class NativeRowIterator(unsafeRow: UnsafeRow, copyRows: Boolean)
    extends Iterator[InternalRow] {

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

    if (copyRows) unsafeRow.copy() else unsafeRow
  }
}
