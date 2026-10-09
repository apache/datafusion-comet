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

import java.math.{BigDecimal => JavaBigDecimal, BigInteger}
import java.nio.ByteOrder

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.arrow.vector.util.OversizedAllocationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.vectorized.{ConstantColumnVector, OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.Platform

/**
 * This file is mostly copied from Spark SQL's
 * org.apache.spark.sql.execution.arrow.ArrowWriter.scala. Comet shadows Arrow classes to avoid
 * potential conflicts with Spark's Arrow dependencies, hence we cannot reuse Spark's ArrowWriter
 * directly.
 */
private[arrow] object ArrowWriter {
  def create(root: VectorSchemaRoot, fixedWidthCapacity: Int): ArrowWriter = {
    require(fixedWidthCapacity >= 0, "Fixed-width capacity must be non-negative")
    val children = root.getFieldVectors().asScala.map { vector =>
      vector match {
        case fixedWidth: BaseFixedWidthVector =>
          fixedWidth.allocateNew(fixedWidthCapacity)
        case _ =>
          vector.allocateNew()
      }
      createFieldWriter(vector)
    }
    new ArrowWriter(root, children.toArray)
  }

  private[sql] def createFieldWriter(vector: ValueVector): ArrowFieldWriter = {
    val field = vector.getField()
    (Utils.fromArrowField(field), vector) match {
      case (BooleanType, vector: BitVector) => new BooleanWriter(vector)
      case (ByteType, vector: TinyIntVector) => new ByteWriter(vector)
      case (ShortType, vector: SmallIntVector) => new ShortWriter(vector)
      case (IntegerType, vector: IntVector) => new IntegerWriter(vector)
      case (LongType, vector: BigIntVector) => new LongWriter(vector)
      case (FloatType, vector: Float4Vector) => new FloatWriter(vector)
      case (DoubleType, vector: Float8Vector) => new DoubleWriter(vector)
      case (DecimalType.Fixed(precision, scale), vector: DecimalVector) =>
        new DecimalWriter(vector, precision, scale)
      case (StringType, vector: VarCharVector) => new StringWriter(vector)
      case (StringType, vector: LargeVarCharVector) => new LargeStringWriter(vector)
      case (BinaryType, vector: VarBinaryVector) => new BinaryWriter(vector)
      case (BinaryType, vector: LargeVarBinaryVector) => new LargeBinaryWriter(vector)
      case (DateType, vector: DateDayVector) => new DateWriter(vector)
      case (TimestampType, vector: TimeStampMicroTZVector) => new TimestampWriter(vector)
      case (TimestampNTZType, vector: TimeStampMicroVector) => new TimestampNTZWriter(vector)
      case (dt, vector: TimeNanoVector) if Utils.isTimeType(dt) => new TimeNanoWriter(vector)
      case (ArrayType(_, _), vector: ListVector) =>
        val elementVector = createFieldWriter(vector.getDataVector())
        new ArrayWriter(vector, elementVector)
      case (MapType(_, _, _), vector: MapVector) =>
        val structVector = vector.getDataVector.asInstanceOf[StructVector]
        val keyWriter = createFieldWriter(structVector.getChild(MapVector.KEY_NAME))
        val valueWriter = createFieldWriter(structVector.getChild(MapVector.VALUE_NAME))
        new MapWriter(vector, structVector, keyWriter, valueWriter)
      case (StructType(_), vector: StructVector) =>
        val children = (0 until vector.size()).map { ordinal =>
          createFieldWriter(vector.getChildByOrdinal(ordinal))
        }
        new StructWriter(vector, children.toArray)
      case (NullType, vector: NullVector) => new NullWriter(vector)
      case (_: YearMonthIntervalType, vector: IntervalYearVector) =>
        new IntervalYearWriter(vector)
      case (_: DayTimeIntervalType, vector: DurationVector) => new DurationWriter(vector)
      case (CalendarIntervalType, vector: IntervalMonthDayNanoVector) =>
        new IntervalMonthDayNanoWriter(vector)
      case (dt, _) =>
        throw QueryExecutionErrors.notSupportTypeError(dt)
    }
  }
}

/**
 * Materialises a Spark `ConstantColumnVector` (partition values / per-batch constants) into a
 * fresh Arrow `FieldVector` holding the constant repeated `numRows` times.
 *
 * Reuses the per-type `ArrowFieldWriter`s above -- so EVERY type is covered (scalars, decimal,
 * timestamps, and complex struct/array/map) and the logic stays in sync with Spark -- rather than
 * a hand-rolled per-type switch. `ConstantColumnVector` returns its constant for any rowId, so a
 * `ColumnarArray` view over rows `[0, numRows)` writes the constant (or null) `numRows` times.
 *
 * Lives in this package because `ArrowWriter` is `private[arrow]`. The caller owns the returned
 * vector and must close it (or hand it to Arrow's exporter, which takes ownership).
 *
 * Comet's serialize/export callers pass `timeZoneId = "UTC"` -- deliberately, NOT the
 * session-local timezone that `toArrowSchema` threads through. These constants are materialised
 * alongside non-constant columns in the same batch/`VectorSchemaRoot`, and Comet's non-constant
 * `TimestampType` columns are Arrow vectors exported from native execution, where Comet always
 * tags them `Timestamp(us, "UTC")` (see native `serde.rs`). Spark itself stores `TimestampType`
 * as micros in UTC, so the constant's value is already a UTC instant. Tagging the materialised
 * constant "UTC" keeps its Arrow timezone metadata consistent with its sibling timestamp columns;
 * threading the session-local timezone here would instead introduce the mismatch.
 * `TimestampNTZType` carries no zone regardless of this argument.
 */
object ConstantColumnVectors {
  def materialize(
      cv: ConstantColumnVector,
      dt: DataType,
      numRows: Int,
      name: String,
      allocator: BufferAllocator,
      timeZoneId: String): FieldVector = {
    val field = Utils.toArrowField(name, dt, nullable = true, timeZoneId)
    val vector = field.createVector(allocator)
    vector.allocateNew()
    val writer = ArrowWriter.createFieldWriter(vector)
    writer.writeCol(new ColumnarArray(cv, 0, numRows))
    writer.finish()
    vector
  }
}

class ArrowWriter(val root: VectorSchemaRoot, fields: Array[ArrowFieldWriter]) {

  def schema: StructType = Utils.fromArrowSchema(root.getSchema())

  private var count: Int = 0

  def write(row: InternalRow): Unit = {
    var i = 0
    row match {
      case unsafe: UnsafeRow =>
        while (i < fields.length) {
          fields(i).writeUnsafeRowField(unsafe, i)
          i += 1
        }
      case _ =>
        while (i < fields.length) {
          fields(i).write(row, i)
          i += 1
        }
    }
    count += 1
  }

  def writeCol(input: ColumnarArray, columnIndex: Int): Unit = {
    fields(columnIndex).writeCol(input)
    count = input.numElements()
  }

  def writeColNoNull(input: ColumnarArray, columnIndex: Int): Unit = {
    fields(columnIndex).writeColNoNull(input)
    count = input.numElements()
  }

  // Driven by the writer's fields rather than by the input's width, because a producer may hand
  // over a batch wider than the schema it is written under. Iceberg's vectorized reader does:
  // it reads with the schema its delete filter required, which carries `_pos` after the projected
  // columns when a data file has position deletes, and trims the extras back only when the file
  // also has equality deletes. Those extras are trailing -- `removeExtraColumns` keeps the leading
  // `expectedSchema` prefix when it does trim -- so writing the first `fields.length` columns
  // writes exactly the columns the schema describes. A batch with fewer columns than the schema
  // has no such reading and is refused rather than written short.
  //
  // The rows are appended after any already written, so one Arrow batch can be filled from
  // several Spark batches.
  def writeColumns(input: ColumnarBatch, startRow: Int, numRows: Int): Unit = {
    require(
      input.numCols() >= fields.length,
      s"Cannot write ${fields.length} columns from a batch of ${input.numCols()} " +
        (if (input.numCols() == 1) "column" else "columns"))
    var columnIndex = 0
    while (columnIndex < fields.length) {
      fields(columnIndex).startInputBatch()
      fields(columnIndex).writeColumnSlice(input.column(columnIndex), startRow, numRows)
      columnIndex += 1
    }
    count += numRows
  }

  def finish(): Unit = {
    root.setRowCount(count)
    fields.foreach(_.finish())
  }

  def reset(): Unit = {
    root.setRowCount(0)
    count = 0
    fields.foreach(_.reset())
  }
}

private[arrow] object ArrowFieldWriter {
  val LittleEndian: Boolean = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN

  // Spark's on-heap bulk getters allocate and fill a temporary array before the copy into Arrow.
  // Below this many rows the per-value loop is faster.
  val MinOnHeapBulkCopyRows = 32

  // Below this many bytes, copying a word at a time beats Unsafe.copyMemory's checks and call.
  // Most strings, and the elements of most arrays, are shorter.
  private final val MaxWordCopyBytes = 64

  /** Copies `length` bytes from `srcOffset` in `src` to the native address `dst`. */
  def copyMemory(src: AnyRef, srcOffset: Long, dst: Long, length: Long): Unit = {
    if (length > MaxWordCopyBytes || !Platform.unaligned()) {
      Platform.copyMemory(src, srcOffset, null, dst, length)
    } else {
      var i = 0L
      while (i + 8 <= length) {
        Platform.putLong(null, dst + i, Platform.getLong(src, srcOffset + i))
        i += 8
      }
      if (i + 4 <= length) {
        Platform.putInt(null, dst + i, Platform.getInt(src, srcOffset + i))
        i += 4
      }
      if (i + 2 <= length) {
        Platform.putShort(null, dst + i, Platform.getShort(src, srcOffset + i))
        i += 2
      }
      if (i < length) {
        Platform.putByte(null, dst + i, Platform.getByte(src, srcOffset + i))
      }
    }
  }

  /** Spark's own writable vectors, whose storage layout the columnar fast paths rely on. */
  def isSparkVector(input: ColumnVector): Boolean =
    input.isInstanceOf[OnHeapColumnVector] || input.isInstanceOf[OffHeapColumnVector]

  // Arrow's setOne calls Unsafe.setMemory, which the JIT does not inline, so shorter runs of whole
  // bytes are set one at a time.
  private final val MinSetOneBytes = 64

  /** Marks bits `[start, start + numRows)` of `validity` as valid. */
  def setValid(validity: ArrowBuf, start: Int, numRows: Int): Unit = {
    val end = start + numRows
    val firstByte = (start + 7) >> 3
    val endByte = end >> 3
    if (firstByte >= endByte) {
      // No whole byte, as for most of a map's entries.
      writeBits(validity, start, -1L, numRows)
    } else {
      writeBits(validity, start, -1L, (firstByte << 3) - start)
      if (endByte - firstByte >= MinSetOneBytes) {
        validity.setOne(firstByte.toLong, (endByte - firstByte).toLong)
      } else {
        var b = firstByte
        while (b < endByte) {
          validity.setByte(b.toLong, 0xff)
          b += 1
        }
      }
      writeBits(validity, endByte << 3, -1L, end - (endByte << 3))
    }
  }

  /**
   * Sets bits `[start, start + numRows)` of `validity` from the nulls of rows `[startRow,
   * startRow + numRows)` of `input`, eight rows to a byte where the bits are byte-aligned.
   */
  def writeValidity(
      validity: ArrowBuf,
      start: Int,
      input: ColumnVector,
      startRow: Int,
      numRows: Int): Unit = {
    if (!input.hasNull) {
      setValid(validity, start, numRows)
      return
    }
    var i = 0
    while (i < numRows && ((start + i) & 7) != 0) {
      writeBit(validity, start + i, !input.isNullAt(startRow + i))
      i += 1
    }
    while (i + 8 <= numRows) {
      val row = startRow + i
      var bits = 0
      if (!input.isNullAt(row)) bits |= 1
      if (!input.isNullAt(row + 1)) bits |= 2
      if (!input.isNullAt(row + 2)) bits |= 4
      if (!input.isNullAt(row + 3)) bits |= 8
      if (!input.isNullAt(row + 4)) bits |= 16
      if (!input.isNullAt(row + 5)) bits |= 32
      if (!input.isNullAt(row + 6)) bits |= 64
      if (!input.isNullAt(row + 7)) bits |= 128
      validity.setByte(((start + i) >> 3).toLong, bits)
      i += 8
    }
    while (i < numRows) {
      writeBit(validity, start + i, !input.isNullAt(startRow + i))
      i += 1
    }
  }

  def writeBit(buffer: ArrowBuf, index: Int, set: Boolean): Unit = {
    if (set) {
      BitVectorHelper.setBit(buffer, index.toLong)
    } else {
      BitVectorHelper.unsetBit(buffer, index)
    }
  }

  /** Writes the low `numBits` bits of `bits`, at most 64, to `[start, start + numBits)`. */
  def writeBits(buffer: ArrowBuf, start: Int, bits: Long, numBits: Int): Unit = {
    val end = start + numBits
    var remaining = bits
    var i = start
    while (i < end) {
      val shift = i & 7
      val n = Math.min(8 - shift, end - i)
      val mask = ((1 << n) - 1) << shift
      val index = (i >> 3).toLong
      buffer.setByte(index, (buffer.getByte(index) & ~mask) | ((remaining.toInt << shift) & mask))
      remaining >>>= n
      i += n
    }
  }

  /**
   * Sets the validity of the elements of `array` from bit `start` of `validity`. Spark keeps an
   * unsafe array's null bits in 64-bit words after its element count, a set bit marking a null.
   */
  def writeArrayValidity(validity: ArrowBuf, start: Int, array: UnsafeArrayData): Unit = {
    val numElements = array.numElements()
    val nulls = array.getBaseOffset + 8
    var i = 0
    while (i < numElements) {
      val nullBits = Platform.getLong(array.getBaseObject, nulls + (i >> 3))
      writeBits(validity, start + i, ~nullBits, Math.min(64, numElements - i))
      i += 64
    }
  }

  /**
   * Clears each bit of `validity` in `[start, start + numRows)` whose bit in `parent` is clear.
   * Whole bytes are combined: the bits before `start` in the first byte belong to rows already
   * masked the same way, and the bits after the range are unwritten in both buffers.
   */
  def maskValidity(validity: ArrowBuf, parent: ArrowBuf, start: Int, numRows: Int): Unit = {
    if (numRows > 0) {
      var b = (start >> 3).toLong
      val last = ((start + numRows - 1) >> 3).toLong
      while (b <= last) {
        validity.setByte(b, validity.getByte(b) & parent.getByte(b))
        b += 1
      }
    }
  }

  /**
   * Calls `writeRun(childStart, childLength)` for each run of child rows (or string bytes) that
   * rows `[startRow, startRow + numRows)` of a Spark array, map or string column hold, merging
   * rows stored back to back. Spark's nested Parquet reader leaves a child slot for each null or
   * empty collection, so the elements of a slice are rarely one run. Null and empty rows are
   * skipped: Spark leaves their offsets unset.
   */
  def writeChildRuns(
      input: WritableColumnVector,
      startRow: Int,
      numRows: Int,
      writeRun: (Int, Int) => Unit): Unit = {
    val hasNull = input.hasNull
    var runStart = 0
    var runEnd = -1
    var i = 0
    while (i < numRows) {
      val row = startRow + i
      if (!hasNull || !input.isNullAt(row)) {
        val length = input.getArrayLength(row)
        if (length > 0) {
          val offset = input.getArrayOffset(row)
          if (offset != runEnd) {
            if (runEnd > runStart) {
              writeRun(runStart, runEnd - runStart)
            }
            runStart = offset
          }
          runEnd = offset + length
        }
      }
      i += 1
    }
    if (runEnd > runStart) {
      writeRun(runStart, runEnd - runStart)
    }
  }

  /**
   * Writes the validity and offsets of rows `[startRow, startRow + numRows)` of a Spark array or
   * map column into `vector` at `outStart`, and returns how many child elements they hold.
   */
  def writeListOffsets(
      vector: ListVector,
      outStart: Int,
      input: WritableColumnVector,
      startRow: Int,
      numRows: Int): Int = {
    // A null written by ArrayWriter.setNull leaves a hole in the offsets; fill it before reading
    // where this slice starts.
    if (vector.getLastSet < outStart - 1) {
      vector.setNull(outStart - 1)
    }
    // Grows the validity and offset buffers. The bit it sets is rewritten below.
    vector.setNotNull(outStart + numRows - 1)
    val offsets = vector.getOffsetBuffer
    val base = offsets.getInt(outStart.toLong * BaseRepeatedValueVector.OFFSET_WIDTH)
    val hasNull = input.hasNull
    var end = base
    var i = 0
    while (i < numRows) {
      val row = startRow + i
      if (!hasNull || !input.isNullAt(row)) {
        end = Math.addExact(end, input.getArrayLength(row))
      }
      offsets.setInt((outStart + i + 1).toLong * BaseRepeatedValueVector.OFFSET_WIDTH, end)
      i += 1
    }
    writeValidity(vector.getValidityBuffer, outStart, input, startRow, numRows)
    vector.setLastSet(outStart + numRows - 1)
    end - base
  }

  /**
   * Appends rows `[startRow, startRow + numRows)` of a Spark string or binary column at
   * `outStart`: one pass writes the offsets and validity, then the bytes are copied in one block
   * when the rows are stored back to back, as Spark's readers store them, or one run of adjacent
   * rows at a time if not.
   */
  def writeVariableWidth(
      vector: BaseVariableWidthVector,
      outStart: Int,
      input: WritableColumnVector,
      startRow: Int,
      numRows: Int): Unit = {
    if (numRows == 0) {
      return
    }
    reserveValues(vector, outStart, numRows)
    val offsets = vector.getOffsetBuffer
    val dataStart = vector.getStartOffset(outStart)
    val hasNull = input.hasNull
    var end = dataStart.toLong
    var first = -1
    var next = 0
    var contiguous = true
    var i = 0
    while (i < numRows) {
      val row = startRow + i
      if (!hasNull || !input.isNullAt(row)) {
        val length = input.getArrayLength(row)
        if (length > 0) {
          val offset = input.getArrayOffset(row)
          if (first < 0) {
            first = offset
          } else if (offset != next) {
            contiguous = false
          }
          next = offset + length
          end += length
          checkDataEnd(end)
        }
      }
      offsets.setInt((outStart + i + 1).toLong * BaseVariableWidthVector.OFFSET_WIDTH, end.toInt)
      i += 1
    }
    writeValidity(vector.getValidityBuffer, outStart, input, startRow, numRows)
    if (vector.getDataBuffer.capacity < end) {
      vector.reallocDataBuffer(end)
    }
    val bytes = input.arrayData()
    val dataAddress = vector.getDataBuffer.memoryAddress
    if (contiguous) {
      if (end > dataStart) {
        copyBytes(bytes, first, dataAddress + dataStart, end - dataStart)
      }
    } else {
      var target = dataAddress + dataStart
      writeChildRuns(
        input,
        startRow,
        numRows,
        (offset, length) => {
          copyBytes(bytes, offset, target, length.toLong)
          target += length
        })
    }
    vector.setLastSet(outStart + numRows - 1)
  }

  // Dictionary ids from this one up are decoded every time rather than cached, which bounds the
  // cache a batch allocates. A batch holds 8192 rows by default, so a larger dictionary repeats
  // few of its ids within one.
  private val MaxCachedDictionaryId = 1 << 14

  /**
   * Appends rows `[startRow, startRow + numRows)` of a dictionary-encoded Spark string or binary
   * column at `outStart`. Spark decodes an id to a fresh array, Parquet's dictionary copying the
   * bytes each time, so each id is decoded once and its later rows copy the bytes its first row
   * wrote, as recorded in `cache`.
   */
  def writeDictionaryVariableWidth(
      vector: BaseVariableWidthVector,
      outStart: Int,
      input: WritableColumnVector,
      startRow: Int,
      numRows: Int,
      cache: DictionaryCache): Unit = {
    if (numRows == 0) {
      return
    }
    reserveValues(vector, outStart, numRows)
    val offsets = vector.getOffsetBuffer
    val ids = input.getDictionaryIds
    val hasNull = input.hasNull
    var firstRow = cache.firstRowsOf(input)
    var end = vector.getStartOffset(outStart).toLong
    var data = vector.getDataBuffer
    var i = 0
    while (i < numRows) {
      val row = startRow + i
      if (!hasNull || !input.isNullAt(row)) {
        val id = ids.getDictId(row)
        if (id >= firstRow.length && id < MaxCachedDictionaryId) {
          firstRow = cache.grow(id)
        }
        // A negative id, which only corrupt data holds, fails in Spark's decoding as before.
        val cached = id >= 0 && id < firstRow.length
        val seen = if (cached) firstRow(id) else 0
        if (seen > 0) {
          val start = offsets.getInt((seen - 1).toLong * BaseVariableWidthVector.OFFSET_WIDTH)
          val length = offsets.getInt(seen.toLong * BaseVariableWidthVector.OFFSET_WIDTH) - start
          data = reserveData(vector, end + length)
          Platform.copyMemory(
            null,
            data.memoryAddress + start,
            null,
            data.memoryAddress + end,
            length.toLong)
          end += length
        } else {
          val bytes = input.getBinary(row)
          data = reserveData(vector, end + bytes.length)
          Platform.copyMemory(
            bytes,
            Platform.BYTE_ARRAY_OFFSET.toLong,
            null,
            data.memoryAddress + end,
            bytes.length.toLong)
          end += bytes.length
          if (cached) {
            firstRow(id) = outStart + i + 1
          }
        }
      }
      offsets.setInt((outStart + i + 1).toLong * BaseVariableWidthVector.OFFSET_WIDTH, end.toInt)
      i += 1
    }
    writeValidity(vector.getValidityBuffer, outStart, input, startRow, numRows)
    vector.setLastSet(outStart + numRows - 1)
  }

  /** Rejects variable-width data ending at `end`, past what Arrow's 32-bit offsets address. */
  def checkDataEnd(end: Long): Unit = {
    if (end > Integer.MAX_VALUE) {
      throw new OversizedAllocationException(
        s"Arrow variable-width data would exceed ${Integer.MAX_VALUE} bytes")
    }
  }

  /**
   * Grows the validity and offset buffers of `vector` to hold values `[outStart, outStart +
   * numValues)`, after filling in the offsets of any values skipped since the last one set.
   */
  def reserveValues(vector: BaseVariableWidthVector, outStart: Int, numValues: Int): Unit = {
    if (vector.getLastSet < outStart - 1) {
      vector.fillEmpties(outStart)
    }
    while (vector.getValueCapacity < outStart + numValues) {
      vector.reallocValidityAndOffsetBuffers()
    }
  }

  /** The data buffer of `vector`, grown to hold at least `end` bytes. */
  def reserveData(vector: BaseVariableWidthVector, end: Long): ArrowBuf = {
    checkDataEnd(end)
    if (vector.getDataBuffer.capacity < end) {
      vector.reallocDataBuffer(end)
    }
    vector.getDataBuffer
  }

  /** Copies `length` bytes from the byte child of a Spark string column to `target`. */
  private def copyBytes(
      bytes: WritableColumnVector,
      offset: Int,
      target: Long,
      length: Long): Unit =
    bytes match {
      case offHeap: OffHeapColumnVector =>
        Platform.copyMemory(null, offHeap.valuesNativeAddress() + offset, null, target, length)
      case _ =>
        // On-heap, this wraps the backing array without copying it.
        val buffer = bytes.getByteBuffer(offset, length.toInt)
        Platform.copyMemory(
          buffer.array(),
          Platform.BYTE_ARRAY_OFFSET.toLong + buffer.arrayOffset() + buffer.position(),
          null,
          target,
          length)
    }
}

/**
 * For a dictionary-encoded input, one more than the output index of the first row written for
 * each dictionary id, or zero. An output row holds an id's bytes only while the input, and with
 * it the dictionary, stays the same: the cache is dropped for a different input vector and at the
 * start of each input batch, since Spark's readers reuse a vector across row groups whose
 * dictionaries differ.
 */
private[arrow] final class DictionaryCache {
  private var input: ColumnVector = _
  private var firstRows: Array[Int] = new Array[Int](64)

  def clear(): Unit = {
    if (input != null) {
      java.util.Arrays.fill(firstRows, 0)
      input = null
    }
  }

  def firstRowsOf(vector: ColumnVector): Array[Int] = {
    if (input ne vector) {
      clear()
      input = vector
    }
    firstRows
  }

  def grow(id: Int): Array[Int] = {
    firstRows = java.util.Arrays.copyOf(firstRows, Integer.highestOneBit(id) << 1)
    firstRows
  }
}

private[arrow] abstract class ArrowFieldWriter {

  def valueVector: ValueVector

  def name: String = valueVector.getField().getName()
  def dataType: DataType = Utils.fromArrowField(valueVector.getField())
  def nullable: Boolean = valueVector.getField().isNullable()

  def setNull(): Unit
  def setValue(input: SpecializedGetters, ordinal: Int): Unit

  private[arrow] var count: Int = 0

  def write(input: SpecializedGetters, ordinal: Int): Unit = {
    if (input.isNullAt(ordinal)) {
      setNull()
    } else {
      setValue(input, ordinal)
    }
    count += 1
  }

  /**
   * Appends field `ordinal` of `row`. Most rows Spark produces are unsafe rows, and writers that
   * can read one's memory directly override this.
   */
  private[arrow] def writeUnsafeRowField(row: UnsafeRow, ordinal: Int): Unit = write(row, ordinal)

  /** Appends every element of `array`, which writers that can copy them in bulk override. */
  private[arrow] def writeArrayElements(array: UnsafeArrayData): Unit = {
    val numElements = array.numElements()
    var i = 0
    while (i < numElements) {
      write(array, i)
      i += 1
    }
  }

  def writeCol(input: ColumnarArray): Unit = {
    val inputNumElements = input.numElements()
    valueVector.setInitialCapacity(inputNumElements)
    while (count < inputNumElements) {
      if (input.isNullAt(count)) {
        setNull()
      } else {
        setValue(input, count)
      }
      count += 1
    }
  }

  def writeColNoNull(input: ColumnarArray): Unit = {
    val inputNumElements = input.numElements()
    valueVector.setInitialCapacity(inputNumElements)
    while (count < inputNumElements) {
      setValue(input, count)
      count += 1
    }
  }

  /**
   * Appends rows `[startRow, startRow + numRows)` of `input` after the values already written.
   */
  def writeColumnSlice(input: ColumnVector, startRow: Int, numRows: Int): Unit = {
    val slice = new ColumnarArray(input, startRow, numRows)
    var i = 0
    if (input.hasNull) {
      while (i < numRows) {
        if (slice.isNullAt(i)) {
          setNull()
        } else {
          setValue(slice, i)
        }
        count += 1
        i += 1
      }
    } else {
      while (i < numRows) {
        setValue(slice, i)
        count += 1
        i += 1
      }
    }
  }

  /** Called before the rows of each input batch, to drop anything cached about the last one. */
  private[arrow] def startInputBatch(): Unit = {}

  /**
   * Whether [[maskNulls]] can null out this field's values under null parents. A list or map
   * would need its offsets rewritten as well.
   */
  private[arrow] def supportsNullMask: Boolean = true

  /**
   * Nulls this field's values in `[start, start + numRows)` wherever `parentValidity` marks the
   * enclosing struct null, matching what [[StructWriter.setNull]] writes row by row.
   */
  private[arrow] def maskNulls(parentValidity: ArrowBuf, start: Int, numRows: Int): Unit =
    valueVector match {
      case vector: FieldVector =>
        ArrowFieldWriter.maskValidity(vector.getValidityBuffer, parentValidity, start, numRows)
      case _ =>
    }

  def finish(): Unit = {
    valueVector.setValueCount(count)
  }

  def reset(): Unit = {
    valueVector.reset()
    count = 0
  }
}

/**
 * `vector` is `valueVector`. The methods called per value read it through this field rather than
 * the subclass's accessor, a virtual call.
 */
private[arrow] abstract class FixedWidthArrowFieldWriter(vector: BaseFixedWidthVector)
    extends ArrowFieldWriter {
  import ArrowFieldWriter._

  override def valueVector: BaseFixedWidthVector

  protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit

  // Arrow keeps a fixed-width vector's capacity in a field, so checking it per value is cheap.
  protected def ensureCapacity(inputNumElements: Int): Unit = {
    while (vector.getValueCapacity < inputNumElements) {
      vector.reAlloc()
    }
  }

  /**
   * Copies the values of rows `[startRow, startRow + numRows)` of `input` into the data buffer at
   * index `count`, leaving validity alone. What lands under a null row is unspecified. Returns
   * false, having written nothing, for a vector type with no such copy.
   *
   * Spark's own vectors without a dictionary are copied in bulk, on-heap ones from
   * [[ArrowFieldWriter.MinOnHeapBulkCopyRows]] rows. Everything else, including a
   * dictionary-encoded vector, is read value by value, skipping null rows, because a dictionary
   * id under a null may be garbage.
   */
  protected def copyValues(input: ColumnVector, startRow: Int, numRows: Int): Boolean = {
    val target = valueVector.getDataBufferAddress + count.toLong * valueVector.getTypeWidth
    val bulk = input match {
      // Spark marks fields missing from a Parquet file all null without growing their value
      // storage to the surrounding collection's size. Reading that storage is unnecessary and,
      // for on-heap vectors, can run past the short backing array.
      case vector: OffHeapColumnVector =>
        LittleEndian && !vector.hasDictionary && !vector.isAllNull
      case vector: OnHeapColumnVector =>
        LittleEndian && !vector.hasDictionary && !vector.isAllNull &&
        numRows >= MinOnHeapBulkCopyRows
      case _ => false
    }
    val hasNull = input.hasNull
    var i = 0
    valueVector match {
      case _: TinyIntVector =>
        if (bulk) {
          bulkCopy(input, startRow, numRows, target, 1, input.getBytes(_, _))
        } else {
          while (i < numRows) {
            val row = startRow + i
            if (!hasNull || !input.isNullAt(row)) {
              Platform.putByte(null, target + i, input.getByte(row))
            }
            i += 1
          }
        }
      case _: SmallIntVector =>
        if (bulk) {
          bulkCopy(input, startRow, numRows, target, 2, input.getShorts(_, _))
        } else {
          while (i < numRows) {
            val row = startRow + i
            if (!hasNull || !input.isNullAt(row)) {
              Platform.putShort(null, target + i * 2L, input.getShort(row))
            }
            i += 1
          }
        }
      case _: IntVector | _: DateDayVector | _: IntervalYearVector =>
        if (bulk) {
          bulkCopy(input, startRow, numRows, target, 4, input.getInts(_, _))
        } else {
          while (i < numRows) {
            val row = startRow + i
            if (!hasNull || !input.isNullAt(row)) {
              Platform.putInt(null, target + i * 4L, input.getInt(row))
            }
            i += 1
          }
        }
      case _: BigIntVector | _: TimeStampMicroTZVector | _: TimeStampMicroVector |
          _: DurationVector | _: TimeNanoVector =>
        if (bulk) {
          bulkCopy(input, startRow, numRows, target, 8, input.getLongs(_, _))
        } else {
          while (i < numRows) {
            val row = startRow + i
            if (!hasNull || !input.isNullAt(row)) {
              Platform.putLong(null, target + i * 8L, input.getLong(row))
            }
            i += 1
          }
        }
      case _: Float4Vector =>
        if (bulk) {
          bulkCopy(input, startRow, numRows, target, 4, input.getFloats(_, _))
        } else {
          while (i < numRows) {
            val row = startRow + i
            if (!hasNull || !input.isNullAt(row)) {
              Platform.putFloat(null, target + i * 4L, input.getFloat(row))
            }
            i += 1
          }
        }
      case _: Float8Vector =>
        if (bulk) {
          bulkCopy(input, startRow, numRows, target, 8, input.getDoubles(_, _))
        } else {
          while (i < numRows) {
            val row = startRow + i
            if (!hasNull || !input.isNullAt(row)) {
              Platform.putDouble(null, target + i * 8L, input.getDouble(row))
            }
            i += 1
          }
        }
      case _ =>
        return false
    }
    true
  }

  /**
   * Copies from an off-heap vector's memory directly, and from an on-heap vector through the
   * array its public bulk getter fills, since Spark exposes no on-heap backing arrays.
   */
  private def bulkCopy(
      input: ColumnVector,
      startRow: Int,
      numRows: Int,
      target: Long,
      width: Int,
      getArray: (Int, Int) => AnyRef): Unit = input match {
    case offHeap: OffHeapColumnVector =>
      Platform.copyMemory(
        null,
        offHeap.valuesNativeAddress() + startRow.toLong * width,
        null,
        target,
        numRows.toLong * width)
    case _ =>
      val array = getArray(startRow, numRows)
      val arrayOffset = array match {
        case _: Array[Byte] => Platform.BYTE_ARRAY_OFFSET
        case _: Array[Short] => Platform.SHORT_ARRAY_OFFSET
        case _: Array[Int] => Platform.INT_ARRAY_OFFSET
        case _: Array[Long] => Platform.LONG_ARRAY_OFFSET
        case _: Array[Float] => Platform.FLOAT_ARRAY_OFFSET
        case _: Array[Double] => Platform.DOUBLE_ARRAY_OFFSET
      }
      Platform.copyMemory(array, arrayOffset.toLong, null, target, numRows.toLong * width)
  }

  override def setNull(): Unit = {
    vector.setNull(count)
  }

  protected def setNullUnsafe(): Unit = {
    BitVectorHelper.unsetBit(vector.getValidityBuffer, count)
  }

  // The width of this type's values when Spark's unsafe formats hold the bits Arrow stores, at the
  // vector's type width and little-endian: an unsafe row in an 8-byte slot, an unsafe array back
  // to back. 0 for a type whose values need converting.
  private val unsafeValueWidth: Int = vector match {
    case _ if !LittleEndian => 0
    case _: TinyIntVector | _: SmallIntVector | _: IntVector | _: DateDayVector |
        _: IntervalYearVector | _: Float4Vector | _: BigIntVector | _: TimeStampMicroTZVector |
        _: TimeStampMicroVector | _: DurationVector | _: TimeNanoVector | _: Float8Vector =>
      vector.getTypeWidth
    case _ => 0
  }

  override private[arrow] def writeUnsafeRowField(row: UnsafeRow, ordinal: Int): Unit = {
    ensureCapacity(count + 1)
    if (row.isNullAt(ordinal)) {
      setNullUnsafe()
    } else if (unsafeValueWidth == 0) {
      setValueUnsafe(row, ordinal)
    } else {
      val target = vector.getDataBufferAddress + count.toLong * unsafeValueWidth
      unsafeValueWidth match {
        case 8 => Platform.putLong(null, target, row.getLong(ordinal))
        case 4 => Platform.putInt(null, target, row.getInt(ordinal))
        case 2 => Platform.putShort(null, target, row.getShort(ordinal))
        case _ => Platform.putByte(null, target, row.getByte(ordinal))
      }
      BitVectorHelper.setBit(vector.getValidityBuffer, count.toLong)
    }
    count += 1
  }

  override private[arrow] def writeArrayElements(array: UnsafeArrayData): Unit = {
    val numElements = array.numElements()
    ensureCapacity(count + numElements)
    if (unsafeValueWidth == 0) {
      var i = 0
      while (i < numElements) {
        if (array.isNullAt(i)) {
          setNullUnsafe()
        } else {
          setValueUnsafe(array, i)
        }
        count += 1
        i += 1
      }
    } else {
      // The values are copied in one block, so what lands under a null element is whatever Spark
      // left there, as with the columnar copies.
      copyMemory(
        array.getBaseObject,
        array.getBaseOffset + UnsafeArrayData.calculateHeaderPortionInBytes(numElements),
        vector.getDataBufferAddress + count.toLong * unsafeValueWidth,
        numElements.toLong * unsafeValueWidth)
      writeArrayValidity(vector.getValidityBuffer, count, array)
      count += numElements
    }
  }

  override def writeColumnSlice(input: ColumnVector, startRow: Int, numRows: Int): Unit = {
    ensureCapacity(count + numRows)
    if (copyValues(input, startRow, numRows)) {
      writeValidity(vector.getValidityBuffer, count, input, startRow, numRows)
      count += numRows
    } else {
      val slice = new ColumnarArray(input, startRow, numRows)
      var i = 0
      while (i < numRows) {
        if (slice.isNullAt(i)) {
          setNullUnsafe()
        } else {
          setValueUnsafe(slice, i)
        }
        count += 1
        i += 1
      }
    }
  }

  override def writeCol(input: ColumnarArray): Unit = {
    val inputNumElements = input.numElements()
    ensureCapacity(inputNumElements)
    while (count < inputNumElements) {
      if (input.isNullAt(count)) {
        setNullUnsafe()
      } else {
        setValueUnsafe(input, count)
      }
      count += 1
    }
  }

  override def writeColNoNull(input: ColumnarArray): Unit = {
    val inputNumElements = input.numElements()
    ensureCapacity(inputNumElements)
    while (count < inputNumElements) {
      setValueUnsafe(input, count)
      count += 1
    }
  }
}

private[arrow] class BooleanWriter(val valueVector: BitVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, if (input.getBoolean(ordinal)) 1 else 0)
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, if (input.getBoolean(ordinal)) 1 else 0)
  }

  // Packs eight values to a byte where the bits are byte-aligned. Null rows get a clear bit.
  override def writeColumnSlice(input: ColumnVector, startRow: Int, numRows: Int): Unit = {
    ensureCapacity(count + numRows)
    val values = valueVector.getDataBuffer
    val hasNull = input.hasNull
    var i = 0
    while (i < numRows && ((count + i) & 7) != 0) {
      val row = startRow + i
      ArrowFieldWriter.writeBit(
        values,
        count + i,
        (!hasNull || !input.isNullAt(row)) && input.getBoolean(row))
      i += 1
    }
    while (i + 8 <= numRows) {
      var bits = 0
      var j = 0
      while (j < 8) {
        val row = startRow + i + j
        if ((!hasNull || !input.isNullAt(row)) && input.getBoolean(row)) {
          bits |= 1 << j
        }
        j += 1
      }
      values.setByte(((count + i) >> 3).toLong, bits)
      i += 8
    }
    while (i < numRows) {
      val row = startRow + i
      ArrowFieldWriter.writeBit(
        values,
        count + i,
        (!hasNull || !input.isNullAt(row)) && input.getBoolean(row))
      i += 1
    }
    ArrowFieldWriter.writeValidity(valueVector.getValidityBuffer, count, input, startRow, numRows)
    count += numRows
  }

  // An unsafe array holds a byte per value, each of which becomes a bit. Null elements get a
  // clear bit, as in writeColumnSlice.
  override private[arrow] def writeArrayElements(array: UnsafeArrayData): Unit = {
    val numElements = array.numElements()
    ensureCapacity(count + numElements)
    val values = valueVector.getDataBuffer
    var i = 0
    while (i < numElements) {
      ArrowFieldWriter.writeBit(values, count + i, !array.isNullAt(i) && array.getBoolean(i))
      i += 1
    }
    ArrowFieldWriter.writeArrayValidity(valueVector.getValidityBuffer, count, array)
    count += numElements
  }
}

private[arrow] class ByteWriter(val valueVector: TinyIntVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getByte(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getByte(ordinal))
  }
}

private[arrow] class ShortWriter(val valueVector: SmallIntVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getShort(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getShort(ordinal))
  }
}

private[arrow] class IntegerWriter(val valueVector: IntVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getInt(ordinal))
  }
}

private[arrow] class LongWriter(val valueVector: BigIntVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getLong(ordinal))
  }
}

private[arrow] class FloatWriter(val valueVector: Float4Vector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getFloat(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getFloat(ordinal))
  }
}

private[arrow] class DoubleWriter(val valueVector: Float8Vector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getDouble(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getDouble(ordinal))
  }
}

/**
 * Writes the unscaled value directly, as a sign-extended long up to 18 digits and otherwise as
 * the 128-bit value of its two's-complement bytes, rather than through Arrow's `BigDecimal`
 * setter, which allocates a `BigInteger` and two byte arrays per value. A wide decimal held as
 * bytes, as Spark's unsafe rows and vectors hold it, is range-checked in place, so the `Decimal`
 * that `getDecimal` would build is only built for a value that does not fit, to fail as before.
 */
private[arrow] class DecimalWriter(val valueVector: DecimalVector, precision: Int, scale: Int)
    extends FixedWidthArrowFieldWriter(valueVector) {
  import ArrowFieldWriter.LittleEndian

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    ensureCapacity(count + 1)
    setValueUnsafe(input, ordinal)
  }

  // Spark's unsafe rows and arrays hold up to 18 digits as the unscaled long, and more as the
  // unscaled bytes, which are read in place. An unsafe row's getDecimal does not check the long
  // against the precision, but an unsafe array's does, so a long that does not fit is left to it.
  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    val written = LittleEndian && (input match {
      case row: UnsafeRow if precision <= Decimal.MAX_LONG_DIGITS =>
        putLong(valueAddress, row.getLong(ordinal))
        true
      case array: UnsafeArrayData if precision <= Decimal.MAX_LONG_DIGITS =>
        val unscaled = array.getLong(ordinal)
        val fits = DecimalWriter.fitsPrecision(unscaled >> 63, unscaled, precision)
        if (fits) {
          putLong(valueAddress, unscaled)
        }
        fits
      case row: UnsafeRow =>
        putUnsafeBytesIfFits(row.getBaseObject, row.getBaseOffset, row.getLong(ordinal))
      case array: UnsafeArrayData =>
        putUnsafeBytesIfFits(array.getBaseObject, array.getBaseOffset, array.getLong(ordinal))
      case _ => false
    })
    if (written) {
      BitVectorHelper.setBit(valueVector.getValidityBuffer, count.toLong)
    } else {
      val decimal = input.getDecimal(ordinal, precision, scale)
      if (decimal.changePrecision(precision, scale)) {
        setUnscaled(count, decimal)
      } else {
        setNullUnsafe()
      }
    }
  }

  private def valueAddress: Long =
    valueVector.getDataBufferAddress + count.toLong * DecimalVector.TYPE_WIDTH

  /**
   * Writes the unscaled bytes that `offsetAndSize` locates in an unsafe row or array to value
   * `count`, leaving validity alone, if they fit the precision. Returns whether it did.
   */
  private def putUnsafeBytesIfFits(base: AnyRef, baseOffset: Long, offsetAndSize: Long): Boolean =
    putBigEndianIfFits(count, base, baseOffset + (offsetAndSize >> 32), offsetAndSize.toInt)

  /** Sets the value and validity of `index`, which must be within capacity. */
  private def setUnscaled(index: Int, decimal: Decimal): Unit = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      valueVector.set(index, decimal.toUnscaledLong)
    } else {
      val unscaled = decimal.toJavaBigDecimal.unscaledValue()
      if (unscaled.bitLength() < java.lang.Long.SIZE) {
        valueVector.set(index, unscaled.longValue())
      } else {
        val bytes = unscaled.toByteArray
        if (LittleEndian &&
          putBigEndianIfFits(index, bytes, Platform.BYTE_ARRAY_OFFSET.toLong, bytes.length)) {
          BitVectorHelper.setBit(valueVector.getValidityBuffer, index.toLong)
        } else {
          valueVector.set(index, decimal.toJavaBigDecimal)
        }
      }
    }
  }

  /**
   * Writes the big-endian two's-complement value of the `length` bytes at `offset` in `base` to
   * `index`, leaving validity alone, if it has at most 16 bytes and fits the precision. Returns
   * whether it did.
   */
  private def putBigEndianIfFits(index: Int, base: AnyRef, offset: Long, length: Int): Boolean = {
    if (length <= 0 || length > DecimalVector.TYPE_WIDTH) {
      return false
    }
    var low = if (Platform.getByte(base, offset) < 0) -1L else 0L
    var high = low
    var b = 0
    while (b < length) {
      high = (high << 8) | (low >>> 56)
      low = (low << 8) | (Platform.getByte(base, offset + b) & 0xffL)
      b += 1
    }
    if (!DecimalWriter.fitsPrecision(high, low, precision)) {
      return false
    }
    val address = valueVector.getDataBufferAddress + index.toLong * DecimalVector.TYPE_WIDTH
    Platform.putLong(null, address, low)
    Platform.putLong(null, address + 8, high)
    true
  }

  override def writeColumnSlice(input: ColumnVector, startRow: Int, numRows: Int): Unit = {
    input match {
      case vector: WritableColumnVector
          if LittleEndian && ArrowFieldWriter.isSparkVector(vector) =>
        ensureCapacity(count + numRows)
        val target = valueVector.getDataBufferAddress + count.toLong * DecimalVector.TYPE_WIDTH
        val hasNull = vector.hasNull
        var i = 0
        // Spark stores the unscaled value as an int up to 9 digits and a long up to 18, and its
        // getDecimal never checks those against the precision, so neither does this.
        if (precision <= Decimal.MAX_INT_DIGITS) {
          while (i < numRows) {
            val row = startRow + i
            if (!hasNull || !vector.isNullAt(row)) {
              putLong(target + i * 16L, vector.getInt(row).toLong)
            }
            i += 1
          }
        } else if (precision <= Decimal.MAX_LONG_DIGITS) {
          while (i < numRows) {
            val row = startRow + i
            if (!hasNull || !vector.isNullAt(row)) {
              putLong(target + i * 16L, vector.getLong(row))
            }
            i += 1
          }
        } else {
          // Past 18 digits Spark stores the unscaled bytes, read in place unless decoded from a
          // dictionary.
          val bytes = if (vector.hasDictionary) null else vector.arrayData()
          while (i < numRows) {
            val row = startRow + i
            if (!hasNull || !vector.isNullAt(row)) {
              val fits = bytes match {
                case null =>
                  val value = vector.getBinary(row)
                  putBigEndianIfFits(
                    count + i,
                    value,
                    Platform.BYTE_ARRAY_OFFSET.toLong,
                    value.length)
                case offHeap: OffHeapColumnVector =>
                  putBigEndianIfFits(
                    count + i,
                    null,
                    offHeap.valuesNativeAddress() + vector.getArrayOffset(row),
                    vector.getArrayLength(row))
                case onHeap =>
                  val buffer =
                    onHeap.getByteBuffer(vector.getArrayOffset(row), vector.getArrayLength(row))
                  putBigEndianIfFits(
                    count + i,
                    buffer.array(),
                    Platform.BYTE_ARRAY_OFFSET.toLong + buffer.arrayOffset() + buffer.position(),
                    buffer.remaining())
              }
              if (!fits) {
                // Builds the Decimal as getDecimal does, which throws on precision overflow.
                val value = vector.getBinary(row)
                setUnscaled(
                  count + i,
                  Decimal(new JavaBigDecimal(new BigInteger(value), scale), precision, scale))
              }
            }
            i += 1
          }
        }
        ArrowFieldWriter.writeValidity(
          valueVector.getValidityBuffer,
          count,
          input,
          startRow,
          numRows)
        count += numRows
      case _ =>
        super.writeColumnSlice(input, startRow, numRows)
    }
  }

  /** Writes `unscaled`, sign-extended, as a little-endian 128-bit value at `address`. */
  private def putLong(address: Long, unscaled: Long): Unit = {
    Platform.putLong(null, address, unscaled)
    Platform.putLong(null, address + 8, unscaled >> 63)
  }
}

private[arrow] object DecimalWriter {
  // 10^p as unsigned 128-bit values, split into their high and low longs.
  private val powersOfTen = (0 to DecimalType.MAX_PRECISION).map(BigInteger.TEN.pow)
  private val tenPowHigh = powersOfTen.map(_.shiftRight(java.lang.Long.SIZE).longValue()).toArray
  private val tenPowLow = powersOfTen.map(_.longValue()).toArray

  /** Whether the two's-complement 128-bit value `high:low` has at most `precision` digits. */
  def fitsPrecision(high: Long, low: Long, precision: Int): Boolean = {
    var magnitudeHigh = high
    var magnitudeLow = low
    if (high < 0) {
      magnitudeLow = ~low + 1
      magnitudeHigh = ~high + (if (magnitudeLow == 0L) 1L else 0L)
    }
    val compareHigh = java.lang.Long.compareUnsigned(magnitudeHigh, tenPowHigh(precision))
    compareHigh < 0 ||
    (compareHigh == 0 && java.lang.Long.compareUnsigned(magnitudeLow, tenPowLow(precision)) < 0)
  }
}

/**
 * Writes strings or binaries. Spark's own vectors are copied in bulk, and its unsafe rows and
 * arrays straight from their memory: both keep a value's offset and size in its fixed-length
 * slot, so its bytes are copied without a `UTF8String` or Arrow's per-value setters. Other input
 * goes through `setValue`.
 */
private[arrow] abstract class VariableWidthArrowFieldWriter extends ArrowFieldWriter {
  import ArrowFieldWriter._

  override def valueVector: BaseVariableWidthVector

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def writeColumnSlice(input: ColumnVector, startRow: Int, numRows: Int): Unit = {
    input match {
      case vector: WritableColumnVector if isSparkVector(vector) =>
        if (vector.hasDictionary) {
          writeDictionaryVariableWidth(
            valueVector,
            count,
            vector,
            startRow,
            numRows,
            dictionaryCache)
        } else {
          writeVariableWidth(valueVector, count, vector, startRow, numRows)
        }
        count += numRows
      case _ =>
        super.writeColumnSlice(input, startRow, numRows)
    }
  }

  private val dictionaryCache = new DictionaryCache

  override private[arrow] def startInputBatch(): Unit = dictionaryCache.clear()

  override def reset(): Unit = {
    super.reset()
    dictionaryCache.clear()
  }

  // The value is checked before Arrow grows anything, so one that cannot be written changes
  // nothing.
  override private[arrow] def writeUnsafeRowField(row: UnsafeRow, ordinal: Int): Unit = {
    if (row.isNullAt(ordinal)) {
      setNull()
    } else {
      val offsetAndSize = row.getLong(ordinal)
      val start = valueVector.getStartOffset(valueVector.getLastSet + 1)
      val length = (checkedEnd(start.toLong, offsetAndSize) - start).toInt
      // Grows the buffers, and gives null rows written since the last value their empty offsets.
      valueVector.setValueLengthSafe(count, length)
      copyMemory(
        row.getBaseObject,
        row.getBaseOffset + (offsetAndSize >> 32),
        valueVector.getDataBuffer.memoryAddress + start,
        length.toLong)
      BitVectorHelper.setBit(valueVector.getValidityBuffer, count.toLong)
    }
    count += 1
  }

  // Every element is checked before anything changes, as one value is, so the buffers grow once
  // and the bytes are copied without further checks.
  override private[arrow] def writeArrayElements(array: UnsafeArrayData): Unit = {
    val numElements = array.numElements()
    if (numElements == 0) {
      return
    }
    val start = valueVector.getStartOffset(valueVector.getLastSet + 1)
    var end = start.toLong
    var i = 0
    while (i < numElements) {
      if (!array.isNullAt(i)) {
        end = checkedEnd(end, array.getLong(i))
      }
      i += 1
    }
    reserveValues(valueVector, count, numElements)
    reserveData(valueVector, end)
    val base = array.getBaseObject
    val baseOffset = array.getBaseOffset
    val data = valueVector.getDataBuffer.memoryAddress
    val offsets = valueVector.getOffsetBuffer
    var offset = start.toLong
    i = 0
    while (i < numElements) {
      if (!array.isNullAt(i)) {
        val offsetAndSize = array.getLong(i)
        val length = offsetAndSize.toInt
        copyMemory(base, baseOffset + (offsetAndSize >> 32), data + offset, length.toLong)
        offset += length
      }
      offsets.setInt((count + i + 1).toLong * BaseVariableWidthVector.OFFSET_WIDTH, offset.toInt)
      i += 1
    }
    writeArrayValidity(valueVector.getValidityBuffer, count, array)
    valueVector.setLastSet(count + numElements - 1)
    count += numElements
  }

  /** The end of a value written at `start` with the size packed into `offsetAndSize`. */
  private def checkedEnd(start: Long, offsetAndSize: Long): Long = {
    val length = offsetAndSize.toInt
    if (length < 0) {
      val kind = if (valueVector.isInstanceOf[VarCharVector]) "String" else "Binary"
      throw new IllegalArgumentException(s"$kind length must be non-negative")
    }
    val end = start + length
    checkDataEnd(end)
    end
  }
}

private[arrow] class StringWriter(val valueVector: VarCharVector)
    extends VariableWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    if (utf8.getBaseObject == null) {
      val length = utf8.numBytes()
      require(length >= 0, "String length must be non-negative")
      valueVector.setValueLengthSafe(count, length)

      // Reservation can replace the buffer. Copy into its current address while Spark still owns
      // the source bytes, without staging the off-heap payload in a JVM byte array.
      val data = valueVector.getDataBuffer
      val offset = valueVector.getStartOffset(count).toLong
      require(offset >= 0 && offset + length <= data.capacity(), "Invalid Arrow string range")
      utf8.writeToMemory(null, Math.addExact(data.memoryAddress(), offset))
      valueVector.setIndexDefined(count)
    } else {
      val utf8ByteBuffer = utf8.getByteBuffer
      valueVector.setSafe(count, utf8ByteBuffer, utf8ByteBuffer.position(), utf8.numBytes())
    }
  }
}

private[arrow] class LargeStringWriter(val valueVector: LargeVarCharVector)
    extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    val utf8ByteBuffer = utf8.getByteBuffer
    // todo: for off-heap UTF8String, how to pass in to arrow without copy?
    valueVector.setSafe(count, utf8ByteBuffer, utf8ByteBuffer.position(), utf8.numBytes())
  }
}

private[arrow] class BinaryWriter(val valueVector: VarBinaryVector)
    extends VariableWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueVector.setSafe(count, bytes, 0, bytes.length)
  }
}

private[arrow] class LargeBinaryWriter(val valueVector: LargeVarBinaryVector)
    extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueVector.setSafe(count, bytes, 0, bytes.length)
  }
}

private[arrow] class DateWriter(val valueVector: DateDayVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getInt(ordinal))
  }
}

private[arrow] class TimestampWriter(val valueVector: TimeStampMicroTZVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getLong(ordinal))
  }
}

private[arrow] class TimestampNTZWriter(val valueVector: TimeStampMicroVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getLong(ordinal))
  }
}

private[arrow] class TimeNanoWriter(val valueVector: TimeNanoVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getLong(ordinal))
  }
}

private[arrow] class ArrayWriter(val valueVector: ListVector, val elementWriter: ArrowFieldWriter)
    extends ArrowFieldWriter {

  override def setNull(): Unit = {}

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    input.getArray(ordinal) match {
      case unsafe: UnsafeArrayData =>
        writeElements(unsafe)
      case array =>
        val numElements = array.numElements()
        valueVector.startNewValue(count)
        var i = 0
        while (i < numElements) {
          elementWriter.write(array, i)
          i += 1
        }
        valueVector.endValue(count, numElements)
    }
  }

  // Each unsafe array gets a view of its own from Spark's getter. A view reused across values
  // would keep the last row it read reachable after that row's values were copied.
  override private[arrow] def writeUnsafeRowField(row: UnsafeRow, ordinal: Int): Unit = {
    if (row.isNullAt(ordinal)) {
      setNull()
    } else {
      writeElements(row.getArray(ordinal))
    }
    count += 1
  }

  override private[arrow] def writeArrayElements(array: UnsafeArrayData): Unit = {
    val numElements = array.numElements()
    var i = 0
    while (i < numElements) {
      if (array.isNullAt(i)) {
        setNull()
      } else {
        writeElements(array.getArray(i))
      }
      count += 1
      i += 1
    }
  }

  private def writeElements(array: UnsafeArrayData): Unit = {
    valueVector.startNewValue(count)
    elementWriter.writeArrayElements(array)
    valueVector.endValue(count, array.numElements())
  }

  // Spark's vectors keep every array's elements in one child vector. The offsets are written in
  // one pass and the elements one run of adjacent rows at a time.
  override def writeColumnSlice(input: ColumnVector, startRow: Int, numRows: Int): Unit = {
    input match {
      case vector: WritableColumnVector if numRows > 0 =>
        ArrowFieldWriter.writeListOffsets(valueVector, count, vector, startRow, numRows)
        val elements = vector.arrayData()
        ArrowFieldWriter.writeChildRuns(
          vector,
          startRow,
          numRows,
          (childStart, length) => elementWriter.writeColumnSlice(elements, childStart, length))
        count += numRows
      case _ =>
        super.writeColumnSlice(input, startRow, numRows)
    }
  }

  override private[arrow] def startInputBatch(): Unit = elementWriter.startInputBatch()

  override private[arrow] def supportsNullMask: Boolean = false

  override private[arrow] def maskNulls(parent: ArrowBuf, start: Int, numRows: Int): Unit =
    throw new IllegalStateException("Cannot mask the nulls of an array column")

  override def finish(): Unit = {
    super.finish()
    elementWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    elementWriter.reset()
  }
}

private[arrow] class StructWriter(
    val valueVector: StructVector,
    children: Array[ArrowFieldWriter])
    extends ArrowFieldWriter {

  override def setNull(): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).setNull()
      children(i).count += 1
      i += 1
    }
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    input.getStruct(ordinal, children.length) match {
      case unsafe: UnsafeRow =>
        writeFields(unsafe)
      case struct =>
        var i = 0
        valueVector.setIndexDefined(count)
        while (i < struct.numFields) {
          children(i).write(struct, i)
          i += 1
        }
    }
  }

  // Each unsafe struct gets a view of its own, as in ArrayWriter.
  override private[arrow] def writeUnsafeRowField(row: UnsafeRow, ordinal: Int): Unit = {
    if (row.isNullAt(ordinal)) {
      setNull()
    } else {
      writeFields(row.getStruct(ordinal, children.length))
    }
    count += 1
  }

  override private[arrow] def writeArrayElements(array: UnsafeArrayData): Unit = {
    val numElements = array.numElements()
    var i = 0
    while (i < numElements) {
      if (array.isNullAt(i)) {
        setNull()
      } else {
        writeFields(array.getStruct(i, children.length))
      }
      count += 1
      i += 1
    }
  }

  private def writeFields(struct: UnsafeRow): Unit = {
    valueVector.setIndexDefined(count)
    var i = 0
    while (i < children.length) {
      children(i).writeUnsafeRowField(struct, i)
      i += 1
    }
  }

  private val childrenSupportNullMask = children.forall(_.supportsNullMask)

  // Writes each field as a column. Under a null struct the fields must come out null, as setNull
  // writes them, so with nulls this takes the row path unless every field can be masked after.
  // Writing the fields as columns also reads them under a null struct, so with nulls it is only
  // done for Spark's own vectors, whose producers write those fields. A struct missing from a
  // Parquet file is the exception: Spark marks it all null and never writes its fields.
  override def writeColumnSlice(input: ColumnVector, startRow: Int, numRows: Int): Unit = {
    val hasNull = input.hasNull
    val readsFields = input match {
      case vector: WritableColumnVector if ArrowFieldWriter.isSparkVector(vector) =>
        !vector.isAllNull && (!hasNull || childrenSupportNullMask)
      case _ => !hasNull
    }
    if (numRows == 0 || !readsFields) {
      super.writeColumnSlice(input, startRow, numRows)
      return
    }
    // Grows the validity buffer. The bit it sets is rewritten below.
    valueVector.setIndexDefined(count + numRows - 1)
    ArrowFieldWriter.writeValidity(valueVector.getValidityBuffer, count, input, startRow, numRows)
    var i = 0
    while (i < children.length) {
      children(i).writeColumnSlice(input.getChild(i), startRow, numRows)
      if (hasNull) {
        children(i).maskNulls(valueVector.getValidityBuffer, count, numRows)
      }
      i += 1
    }
    count += numRows
  }

  override private[arrow] def startInputBatch(): Unit = children.foreach(_.startInputBatch())

  override private[arrow] def supportsNullMask: Boolean = childrenSupportNullMask

  override private[arrow] def maskNulls(parent: ArrowBuf, start: Int, numRows: Int): Unit = {
    super.maskNulls(parent, start, numRows)
    var i = 0
    while (i < children.length) {
      children(i).maskNulls(valueVector.getValidityBuffer, start, numRows)
      i += 1
    }
  }

  override def finish(): Unit = {
    super.finish()
    children.foreach(_.finish())
  }

  override def reset(): Unit = {
    super.reset()
    children.foreach(_.reset())
  }
}

private[arrow] class MapWriter(
    val valueVector: MapVector,
    val structVector: StructVector,
    val keyWriter: ArrowFieldWriter,
    val valueWriter: ArrowFieldWriter)
    extends ArrowFieldWriter {

  override def setNull(): Unit = {}

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    input.getMap(ordinal) match {
      case unsafe: UnsafeMapData =>
        writeEntries(unsafe)
      case map =>
        val numElements = map.numElements()
        valueVector.startNewValue(count)
        val keys = map.keyArray()
        val values = map.valueArray()
        var i = 0
        while (i < numElements) {
          structVector.setIndexDefined(keyWriter.count)
          keyWriter.write(keys, i)
          valueWriter.write(values, i)
          i += 1
        }
        valueVector.endValue(count, numElements)
    }
  }

  // Each unsafe map gets a view of its own, as in ArrayWriter.
  override private[arrow] def writeUnsafeRowField(row: UnsafeRow, ordinal: Int): Unit = {
    if (row.isNullAt(ordinal)) {
      setNull()
    } else {
      writeEntries(row.getMap(ordinal))
    }
    count += 1
  }

  override private[arrow] def writeArrayElements(array: UnsafeArrayData): Unit = {
    val numElements = array.numElements()
    var i = 0
    while (i < numElements) {
      if (array.isNullAt(i)) {
        setNull()
      } else {
        writeEntries(array.getMap(i))
      }
      count += 1
      i += 1
    }
  }

  private def writeEntries(map: UnsafeMapData): Unit = {
    val numElements = map.numElements()
    valueVector.startNewValue(count)
    if (numElements > 0) {
      setEntriesValid(keyWriter.count, numElements)
      keyWriter.writeArrayElements(map.keyArray())
      valueWriter.writeArrayElements(map.valueArray())
    }
    valueVector.endValue(count, numElements)
  }

  /** Marks entries `[start, start + numEntries)` valid. */
  private def setEntriesValid(start: Int, numEntries: Int): Unit = {
    // Grows the validity buffer to the last entry.
    structVector.setIndexDefined(start + numEntries - 1)
    ArrowFieldWriter.setValid(structVector.getValidityBuffer, start, numEntries)
  }

  // Like ArrayWriter.writeColumnSlice, with the keys and values in Spark's two child vectors.
  override def writeColumnSlice(input: ColumnVector, startRow: Int, numRows: Int): Unit = {
    input match {
      case vector: WritableColumnVector if numRows > 0 =>
        val entryStart = keyWriter.count
        val numEntries =
          ArrowFieldWriter.writeListOffsets(valueVector, count, vector, startRow, numRows)
        if (numEntries > 0) {
          setEntriesValid(entryStart, numEntries)
        }
        val keys = vector.getChild(0)
        val values = vector.getChild(1)
        ArrowFieldWriter.writeChildRuns(
          vector,
          startRow,
          numRows,
          (childStart, length) => {
            keyWriter.writeColumnSlice(keys, childStart, length)
            valueWriter.writeColumnSlice(values, childStart, length)
          })
        count += numRows
      case _ =>
        super.writeColumnSlice(input, startRow, numRows)
    }
  }

  override private[arrow] def startInputBatch(): Unit = {
    keyWriter.startInputBatch()
    valueWriter.startInputBatch()
  }

  override private[arrow] def supportsNullMask: Boolean = false

  override private[arrow] def maskNulls(parent: ArrowBuf, start: Int, numRows: Int): Unit =
    throw new IllegalStateException("Cannot mask the nulls of a map column")

  override def finish(): Unit = {
    super.finish()
    keyWriter.finish()
    valueWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    keyWriter.reset()
    valueWriter.reset()
  }
}

private[arrow] class NullWriter(val valueVector: NullVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {}

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {}

  override private[arrow] def maskNulls(parent: ArrowBuf, start: Int, numRows: Int): Unit = {}
}

private[arrow] class IntervalYearWriter(val valueVector: IntervalYearVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getInt(ordinal))
  }
}

private[arrow] class DurationWriter(val valueVector: DurationVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getLong(ordinal))
  }
}

private[arrow] class IntervalMonthDayNanoWriter(val valueVector: IntervalMonthDayNanoVector)
    extends FixedWidthArrowFieldWriter(valueVector) {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val ci = input.getInterval(ordinal)
    valueVector.setSafe(count, ci.months, ci.days, Math.multiplyExact(ci.microseconds, 1000L))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    val ci = input.getInterval(ordinal)
    valueVector.set(count, ci.months, ci.days, Math.multiplyExact(ci.microseconds, 1000L))
  }
}
