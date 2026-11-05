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

import org.apache.arrow.vector.{BaseLargeVariableWidthVector, BaseVariableWidthVector, FieldVector}
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.Platform

import org.apache.comet.serde.QueryPlanSerde
import org.apache.comet.vector.{CometPlainVector, NativeUtil}

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
class NativeColumnarToRowConverter(
    schema: StructType,
    batchSize: Int,
    heapRows: Boolean = !java.lang.Boolean.getBoolean("comet.c2r.legacy"))
    extends AutoCloseable {

  private val nativeLib = new Native()
  private val nativeUtil = new NativeUtil()

  // serializeDataType is broader than native C2R support. Production construction is gated by
  // CometNativeColumnarToRowExec.supportsSchema, which recursively rejects VariantType.
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

  // Heap-rows mode reuses one set of FFI structs for every batch.
  private val numCols = schema.fields.length
  private lazy val (ffiArrays, ffiSchemas) = nativeUtil.allocateArrowStructs(numCols)
  private lazy val ffiArrayAddrs = ffiArrays.map(_.memoryAddress())
  private lazy val ffiSchemaAddrs = ffiSchemas.map(_.memoryAddress())

  // Raw mode: the Arrow minor types each column may arrive as, for reading its buffers directly.
  private val rawTypes: Array[Set[MinorType]] =
    schema.fields.map(f => NativeColumnarToRowConverter.rawMinorTypes(f.dataType))
  private val rawEligible = heapRows && rawTypes.forall(_.nonEmpty)
  private val rawDesc = new Array[Long](numCols * 4)

  /** Fills rawDesc from the batch's buffers, or returns false when a column can't be read raw. */
  private def fillRawDesc(batch: ColumnarBatch): Boolean = {
    val numRows = batch.numRows()
    var c = 0
    while (c < numCols) {
      batch.column(c) match {
        case v: CometPlainVector if v.getValueVector.getField.getDictionary == null =>
          val vv = v.getValueVector
          if (vv.getValueCount != numRows || !rawTypes(c).contains(vv.getMinorType)) return false
          val fv = vv.asInstanceOf[FieldVector]
          rawDesc(c * 4) = fv.getValidityBufferAddress
          vv match {
            case bv: BaseVariableWidthVector =>
              rawDesc(c * 4 + 1) = bv.getOffsetBufferAddress
              rawDesc(c * 4 + 3) = 0
            case lv: BaseLargeVariableWidthVector =>
              rawDesc(c * 4 + 1) = lv.getOffsetBufferAddress
              rawDesc(c * 4 + 3) = 1
            case _ =>
              rawDesc(c * 4 + 1) = 0
              rawDesc(c * 4 + 3) = 0
          }
          rawDesc(c * 4 + 2) = fv.getDataBufferAddress
        case _ => return false
      }
      c += 1
    }
    true
  }

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

    if (rawEligible && fillRawDesc(batch)) {
      // Each batch gets its own offsets, since its iterator may outlive the next convert().
      val offsets = new Array[Int](numRows + 1)
      val bytes = nativeLib.columnarToRowConvertRaw(c2rHandle, rawDesc, numRows, offsets)
      return new HeapRowIterator(bytes, offsets, numRows, numCols)
    }

    if (heapRows) {
      val exported = nativeUtil.exportBatch(ffiArrayAddrs, ffiSchemaAddrs, batch)
      val offsets = new Array[Int](exported + 1)
      val bytes =
        nativeLib.columnarToRowConvertToArray(
          c2rHandle,
          ffiArrayAddrs,
          ffiSchemaAddrs,
          exported,
          offsets)
      return new HeapRowIterator(bytes, offsets, exported, numCols)
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
    if (heapRows && numCols > 0) {
      ffiArrays.foreach(_.close())
      ffiSchemas.foreach(_.close())
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

    unsafeRow.copy()
  }
}

object NativeColumnarToRowConverter {

  /** The Arrow minor types a column of `dataType` can be read from raw, or empty if none. */
  private[comet] def rawMinorTypes(dataType: DataType): Set[MinorType] = dataType match {
    case BooleanType => Set(MinorType.BIT)
    case ByteType => Set(MinorType.TINYINT)
    case ShortType => Set(MinorType.SMALLINT)
    case IntegerType => Set(MinorType.INT)
    case LongType => Set(MinorType.BIGINT)
    case FloatType => Set(MinorType.FLOAT4)
    case DoubleType => Set(MinorType.FLOAT8)
    case DateType => Set(MinorType.DATEDAY)
    case TimestampType => Set(MinorType.TIMESTAMPMICROTZ)
    case TimestampNTZType => Set(MinorType.TIMESTAMPMICRO)
    case _: DecimalType => Set(MinorType.DECIMAL)
    case _: StringType => Set(MinorType.VARCHAR, MinorType.LARGEVARCHAR)
    case BinaryType => Set(MinorType.VARBINARY, MinorType.LARGEVARBINARY)
    case _ => Set.empty
  }
}

/**
 * Iterator over rows that native conversion wrote into a JVM byte array, one array per batch.
 * Each row is a new UnsafeRow over that array, so it stays valid after later batches without
 * copying its bytes.
 */
private class HeapRowIterator(
    bytes: Array[Byte],
    offsets: Array[Int],
    numRows: Int,
    numFields: Int)
    extends Iterator[InternalRow] {

  private var currentIdx = 0

  override def hasNext: Boolean = currentIdx < numRows

  override def next(): InternalRow = {
    if (!hasNext) {
      throw new NoSuchElementException("No more rows")
    }
    val start = offsets(currentIdx)
    val row = new UnsafeRow(numFields)
    row.pointTo(bytes, Platform.BYTE_ARRAY_OFFSET + start, offsets(currentIdx + 1) - start)
    currentIdx += 1
    row
  }
}
