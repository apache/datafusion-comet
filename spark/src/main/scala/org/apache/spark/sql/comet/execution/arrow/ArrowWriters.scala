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

import java.nio.ByteOrder

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.vectorized.{ConstantColumnVector, OffHeapColumnVector, OnHeapColumnVector}
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
      case (CalendarIntervalType, vector: StructVector) =>
        val children = (0 until vector.size()).map { ordinal =>
          createFieldWriter(vector.getChildByOrdinal(ordinal))
        }
        new CalendarIntervalStructWriter(vector, children.toArray)
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
    while (i < fields.length) {
      fields(i).writeUnsafe(row, i)
      i += 1
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

  def writeColumns(input: ColumnarBatch, startRow: Int, numRows: Int): Unit = {
    var columnIndex = 0
    while (columnIndex < input.numCols()) {
      fields(columnIndex).writeColumnSlice(input.column(columnIndex), startRow, numRows)
      columnIndex += 1
    }
    count = numRows
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

  def writeUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    write(input, ordinal)
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

  def writeColumnSlice(input: ColumnVector, startRow: Int, numRows: Int): Unit = {
    val slice = new ColumnarArray(input, startRow, numRows)
    if (input.hasNull) {
      writeCol(slice)
    } else {
      writeColNoNull(slice)
    }
  }

  def finish(): Unit = {
    valueVector.setValueCount(count)
  }

  def reset(): Unit = {
    valueVector.reset()
    count = 0
  }
}

private[arrow] abstract class FixedWidthArrowFieldWriter extends ArrowFieldWriter {

  override def valueVector: BaseFixedWidthVector

  protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit

  private def ensureCapacity(inputNumElements: Int): Unit = {
    while (valueVector.getValueCapacity < inputNumElements) {
      valueVector.reAlloc()
    }
  }

  private def tryBulkCopyNoNull(input: ColumnVector, startRow: Int, numRows: Int): Boolean = {
    // Spark's bulk getters allocate and fill a temporary array before the copy into Arrow. Keep
    // slices below 32 on the scalar path to avoid the observed tiny-slice regression.
    if (count != 0 || numRows < 32 || ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
      return false
    }

    val supportedInput = input match {
      case vector: OnHeapColumnVector => !vector.hasDictionary
      case vector: OffHeapColumnVector => !vector.hasDictionary
      case _ => false
    }
    if (!supportedInput) {
      return false
    }

    // Spark has no stable direct access to on-heap backing arrays. Its public bulk getters are
    // the portable path for both on-heap and off-heap vectors.
    val (sourceArray, sourceOffset): (AnyRef, Long) = valueVector match {
      case _: TinyIntVector =>
        (input.getBytes(startRow, numRows), Platform.BYTE_ARRAY_OFFSET.toLong)
      case _: SmallIntVector =>
        (input.getShorts(startRow, numRows), Platform.SHORT_ARRAY_OFFSET.toLong)
      case _: IntVector | _: DateDayVector | _: IntervalYearVector =>
        (input.getInts(startRow, numRows), Platform.INT_ARRAY_OFFSET.toLong)
      case _: BigIntVector | _: TimeStampMicroTZVector | _: TimeStampMicroVector |
          _: DurationVector =>
        (input.getLongs(startRow, numRows), Platform.LONG_ARRAY_OFFSET.toLong)
      case _: Float4Vector =>
        (input.getFloats(startRow, numRows), Platform.FLOAT_ARRAY_OFFSET.toLong)
      case _: Float8Vector =>
        (input.getDoubles(startRow, numRows), Platform.DOUBLE_ARRAY_OFFSET.toLong)
      case _ => return false
    }

    ensureCapacity(numRows)
    Platform.copyMemory(
      sourceArray,
      sourceOffset,
      null,
      valueVector.getDataBufferAddress,
      numRows.toLong * valueVector.getTypeWidth)
    valueVector.getValidityBuffer
      .setOne(0L, BitVectorHelper.getValidityBufferSize(numRows).toLong)
    count = numRows
    true
  }

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  protected def setNullUnsafe(): Unit = {
    BitVectorHelper.unsetBit(valueVector.getValidityBuffer, count)
  }

  override def writeUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    if (input.isNullAt(ordinal)) {
      setNullUnsafe()
    } else {
      setValueUnsafe(input, ordinal)
    }
    count += 1
  }

  override def writeColumnSlice(input: ColumnVector, startRow: Int, numRows: Int): Unit = {
    if (input.hasNull || !tryBulkCopyNoNull(input, startRow, numRows)) {
      super.writeColumnSlice(input, startRow, numRows)
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
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, if (input.getBoolean(ordinal)) 1 else 0)
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, if (input.getBoolean(ordinal)) 1 else 0)
  }
}

private[arrow] class ByteWriter(val valueVector: TinyIntVector)
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getByte(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getByte(ordinal))
  }
}

private[arrow] class ShortWriter(val valueVector: SmallIntVector)
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getShort(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getShort(ordinal))
  }
}

private[arrow] class IntegerWriter(val valueVector: IntVector)
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getInt(ordinal))
  }
}

private[arrow] class LongWriter(val valueVector: BigIntVector)
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getLong(ordinal))
  }
}

private[arrow] class FloatWriter(val valueVector: Float4Vector)
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getFloat(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getFloat(ordinal))
  }
}

private[arrow] class DoubleWriter(val valueVector: Float8Vector)
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getDouble(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getDouble(ordinal))
  }
}

private[arrow] class DecimalWriter(val valueVector: DecimalVector, precision: Int, scale: Int)
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val decimal = input.getDecimal(ordinal, precision, scale)
    if (decimal.changePrecision(precision, scale)) {
      valueVector.setSafe(count, decimal.toJavaBigDecimal)
    } else {
      setNull()
    }
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    val decimal = input.getDecimal(ordinal, precision, scale)
    if (decimal.changePrecision(precision, scale)) {
      valueVector.set(count, decimal.toJavaBigDecimal)
    } else {
      setNullUnsafe()
    }
  }
}

private[arrow] class StringWriter(val valueVector: VarCharVector) extends ArrowFieldWriter {

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

private[arrow] class BinaryWriter(val valueVector: VarBinaryVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

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
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getInt(ordinal))
  }
}

private[arrow] class TimestampWriter(val valueVector: TimeStampMicroTZVector)
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getLong(ordinal))
  }
}

private[arrow] class TimestampNTZWriter(val valueVector: TimeStampMicroVector)
    extends FixedWidthArrowFieldWriter {

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
    val array = input.getArray(ordinal)
    var i = 0
    valueVector.startNewValue(count)
    while (i < array.numElements()) {
      elementWriter.write(array, i)
      i += 1
    }
    valueVector.endValue(count, array.numElements())
  }

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
    val struct = input.getStruct(ordinal, children.length)
    var i = 0
    valueVector.setIndexDefined(count)
    while (i < struct.numFields) {
      children(i).write(struct, i)
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

private[arrow] class CalendarIntervalStructWriter(
    valueVector: StructVector,
    children: Array[ArrowFieldWriter])
    extends StructWriter(valueVector, children) {

  private val row = new GenericInternalRow(3)

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setIndexDefined(count)
    val interval = input.getInterval(ordinal)
    row.update(0, interval.months)
    row.update(1, interval.days)
    row.update(2, interval.microseconds)
    children.indices.foreach(i => children(i).write(row, i))
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
    val map = input.getMap(ordinal)
    valueVector.startNewValue(count)
    val keys = map.keyArray()
    val values = map.valueArray()
    var i = 0
    while (i < map.numElements()) {
      structVector.setIndexDefined(keyWriter.count)
      keyWriter.write(keys, i)
      valueWriter.write(values, i)
      i += 1
    }

    valueVector.endValue(count, map.numElements())
  }

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
}

private[arrow] class IntervalYearWriter(val valueVector: IntervalYearVector)
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getInt(ordinal))
  }
}

private[arrow] class DurationWriter(val valueVector: DurationVector)
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.set(count, input.getLong(ordinal))
  }
}

private[arrow] class IntervalMonthDayNanoWriter(val valueVector: IntervalMonthDayNanoVector)
    extends FixedWidthArrowFieldWriter {

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val ci = input.getInterval(ordinal)
    valueVector.setSafe(count, ci.months, ci.days, Math.multiplyExact(ci.microseconds, 1000L))
  }

  override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
    val ci = input.getInterval(ordinal)
    valueVector.set(count, ci.months, ci.days, Math.multiplyExact(ci.microseconds, 1000L))
  }
}
