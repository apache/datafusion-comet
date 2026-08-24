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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.arrow.memory.{AllocationListener, RootAllocator}
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseValueVector, BigIntVector, BitVector, DecimalVector, IntervalMonthDayNanoVector, IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecializedGetters}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.vectorized.{Dictionary, OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types.{BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StructField, StructType, TimestampNTZType, TimestampType, YearMonthIntervalType}
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.CalendarInterval

import org.apache.comet.vector.{CometPlainVector, CometVector}

/**
 * Direct tests for [[CometArrowStream.reconcileStreamSchema]]. The end-to-end regression that
 * motivated this (Spark Long vs DataFusion Int32 for `width_bucket`) lives in
 * `CometMathExpressionSuite`, but that test only catches *one* function-level type drift. This
 * suite covers the boundary contract independently of any specific function.
 */
class CometArrowStreamSuite extends AnyFunSuite with Matchers {

  private def expectedSchema(types: (String, ArrowType)*): Schema = {
    val fields = types.map { case (name, t) =>
      new Field(name, new FieldType(true, t, null), java.util.Collections.emptyList[Field]())
    }
    new Schema(fields.asJava)
  }

  private def batchOf(vectors: CometVector*): ColumnarBatch = {
    val numRows = if (vectors.isEmpty) 0 else vectors.head.getValueVector.getValueCount
    new ColumnarBatch(vectors.toArray, numRows)
  }

  test("CalendarIntervalType round-trips through Arrow writer and Comet vector") {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val field = Utils.toArrowField("interval", CalendarIntervalType, nullable = true, "UTC")
    Utils.fromArrowField(field) shouldBe CalendarIntervalType
    val root = VectorSchemaRoot.create(new Schema(Seq(field).asJava), allocator)
    try {
      val expected = new CalendarInterval(14, -3, 1234567L)
      val writer = ArrowWriter.create(root, 2)
      writer.write(new GenericInternalRow(Array[Any](expected)))
      writer.write(new GenericInternalRow(Array[Any](null)))
      writer.finish()

      val arrow = root.getVector(0).asInstanceOf[IntervalMonthDayNanoVector]
      IntervalMonthDayNanoVector.getMonths(arrow.getDataBuffer, 0) shouldBe expected.months
      IntervalMonthDayNanoVector.getDays(arrow.getDataBuffer, 0) shouldBe expected.days
      IntervalMonthDayNanoVector.getNanoseconds(arrow.getDataBuffer, 0) shouldBe
        expected.microseconds * 1000L

      val comet = new CometPlainVector(arrow, false)
      comet.getInterval(0) shouldBe expected
      comet.getInterval(1) shouldBe null
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("pre-sized Arrow writer avoids fixed-width reallocations") {
    var allocatedBytes = 0L
    val allocator = new RootAllocator(
      new AllocationListener {
        override def onAllocation(size: Long): Unit = allocatedBytes += size
      },
      Long.MaxValue)
    val numRows = BaseValueVector.INITIAL_VALUE_ALLOCATION + 1
    val decimalType = DecimalType(5, 2)
    val schema = StructType(
      Seq(
        StructField("nullable_int", IntegerType, nullable = true),
        StructField("required_int", IntegerType, nullable = false),
        StructField("required_boolean", BooleanType, nullable = false),
        StructField("required_decimal", decimalType, nullable = false)))
    val nullableInput = new OnHeapColumnVector(numRows, IntegerType)
    val requiredInput = new OnHeapColumnVector(numRows, IntegerType)
    val booleanInput = new OnHeapColumnVector(numRows, BooleanType)
    val decimalInput = new OnHeapColumnVector(numRows, decimalType)
    val inputBatch =
      new ColumnarBatch(Array(nullableInput, requiredInput, booleanInput, decimalInput), numRows)
    val reader = new SparkColumnarArrowReader(
      allocator,
      Utils.toArrowSchema(schema, "UTC"),
      Iterator.single(inputBatch),
      numRows)

    try {
      var i = 0
      while (i < numRows) {
        if ((i & 1) == 0) {
          nullableInput.putNull(i)
        } else {
          nullableInput.putInt(i, i)
        }
        requiredInput.putInt(i, -i)
        booleanInput.putBoolean(i, (i & 1) == 0)
        val decimal = Decimal(i % 10000, decimalType.precision, decimalType.scale)
        decimalInput.putDecimal(i, decimal, decimalType.precision)
        i += 1
      }

      reader.loadNextBatch() shouldBe true
      val root = reader.getVectorSchemaRoot
      val nullableArrow = root.getVector(0).asInstanceOf[IntVector]
      val requiredArrow = root.getVector(1).asInstanceOf[IntVector]
      val booleanArrow = root.getVector(2).asInstanceOf[BitVector]
      val decimalArrow = root.getVector(3).asInstanceOf[DecimalVector]
      root.getRowCount shouldBe numRows
      nullableArrow.getValueCapacity should be >= numRows
      requiredArrow.getValueCapacity should be >= numRows
      booleanArrow.getValueCapacity should be >= numRows
      decimalArrow.getValueCapacity should be >= numRows
      i = 0
      while (i < numRows) {
        nullableArrow.isNull(i) shouldBe ((i & 1) == 0)
        if ((i & 1) != 0) {
          nullableArrow.get(i) shouldBe i
        }
        requiredArrow.get(i) shouldBe -i
        booleanArrow.get(i) shouldBe (if ((i & 1) == 0) 1 else 0)
        decimalArrow.getObject(i) shouldBe
          Decimal(i % 10000, decimalType.precision, decimalType.scale).toJavaBigDecimal
        i += 1
      }
      // A realloc frees the old buffers, so cumulative allocations would exceed live memory.
      allocatedBytes shouldBe allocator.getAllocatedMemory
    } finally {
      reader.close()
      nullableInput.close()
      requiredInput.close()
      booleanInput.close()
      decimalInput.close()
      allocator.close()
    }
  }

  test("bulk copy dispatch handles heap, off-heap, slices, and dictionary fallback") {
    val allocator = new RootAllocator(Long.MaxValue)
    val startRow = 3
    val numRows = 32
    val inputCapacity = startRow + numRows
    val onHeap = new OnHeapColumnVector(inputCapacity, LongType)
    val offHeap = new OffHeapColumnVector(inputCapacity, LongType)
    val dictionary = new OnHeapColumnVector(inputCapacity, LongType)

    class CountingLongWriter(vector: BigIntVector) extends LongWriter(vector) {
      var scalarWrites: Int = 0

      override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit = {
        scalarWrites += 1
        super.setValueUnsafe(input, ordinal)
      }
    }

    def check(
        input: ColumnVector,
        rows: Int,
        expected: Int => Long,
        expectedScalarWrites: Int): Unit = {
      val output = new BigIntVector("long", allocator)
      output.allocateNew(rows)
      val writer = new CountingLongWriter(output)
      try {
        writer.writeColumnSlice(input, startRow, rows)
        writer.finish()

        writer.scalarWrites shouldBe expectedScalarWrites
        output.getValueCount shouldBe rows
        var i = 0
        while (i < rows) {
          output.isNull(i) shouldBe false
          output.get(i) shouldBe expected(i)
          i += 1
        }
      } finally {
        output.close()
      }
    }

    try {
      var i = 0
      while (i < inputCapacity) {
        onHeap.putLong(i, 1000L + i)
        offHeap.putLong(i, 2000L + i)
        i += 1
      }

      def scalarWrites(rows: Int): Int =
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) 0 else rows
      check(onHeap, numRows, i => 1000L + startRow + i, scalarWrites(numRows))
      check(offHeap, numRows, i => 2000L + startRow + i, scalarWrites(numRows))
      check(onHeap, 1, i => 1000L + startRow + i, scalarWrites(1))

      dictionary.setDictionary(new Dictionary {
        override def decodeToInt(id: Int): Int = id
        override def decodeToLong(id: Int): Long = 3000L + id
        override def decodeToFloat(id: Int): Float = id.toFloat
        override def decodeToDouble(id: Int): Double = id.toDouble
        override def decodeToBinary(id: Int): Array[Byte] = Array(id.toByte)
      })
      val dictionaryIds = dictionary.reserveDictionaryIds(inputCapacity)
      i = 0
      while (i < inputCapacity) {
        dictionaryIds.putInt(i, i)
        i += 1
      }
      check(dictionary, numRows, i => 3000L + startRow + i, numRows)
    } finally {
      onHeap.close()
      offHeap.close()
      dictionary.close()
      allocator.close()
    }
  }

  test("bulk copy dispatch covers supported fixed-width Arrow vectors") {
    val allocator = new RootAllocator(Long.MaxValue)
    val numRows = 32
    class CountingFixedWidthWriter(override val valueVector: BaseFixedWidthVector)
        extends FixedWidthArrowFieldWriter {
      var scalarWrites: Int = 0
      override def setValue(input: SpecializedGetters, ordinal: Int): Unit = scalarWrites += 1
      override protected def setValueUnsafe(input: SpecializedGetters, ordinal: Int): Unit =
        scalarWrites += 1
    }
    val dataTypes: Seq[DataType] = Seq(
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      DateType,
      TimestampType,
      TimestampNTZType,
      YearMonthIntervalType(),
      DayTimeIntervalType())

    try {
      dataTypes.foreach { dataType =>
        val input = new OnHeapColumnVector(numRows, dataType)
        val output = Utils
          .toArrowField("value", dataType, nullable = false, "UTC")
          .createVector(allocator)
          .asInstanceOf[BaseFixedWidthVector]
        output.allocateNew(numRows)
        val writer = new CountingFixedWidthWriter(output)

        try {
          writer.writeColumnSlice(input, 0, numRows)
          writer.scalarWrites shouldBe
            (if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) 0 else numRows)
        } finally {
          input.close()
          output.close()
        }
      }
    } finally {
      allocator.close()
    }
  }

  test("Arrow writer preserves row count for zero-column batches") {
    val allocator = new RootAllocator(Long.MaxValue)
    val root = VectorSchemaRoot.create(new Schema(Seq.empty[Field].asJava), allocator)
    val numRows = 17
    val input = new ColumnarBatch(Array.empty[ColumnVector], numRows)

    try {
      val writer = ArrowWriter.create(root, numRows)
      writer.writeColumns(input, 0, numRows)
      writer.finish()

      root.getRowCount shouldBe numRows
    } finally {
      input.close()
      root.close()
      allocator.close()
    }
  }

  test("Spark columnar reader preserves split input slice offsets") {
    val allocator = new RootAllocator(Long.MaxValue)
    val numRows = 12
    val maxRecordsPerBatch = 5
    val schema = StructType(Seq(StructField("long", LongType, nullable = false)))
    val column = new OnHeapColumnVector(numRows, LongType)
    val input = new ColumnarBatch(Array(column), numRows)
    val reader = new SparkColumnarArrowReader(
      allocator,
      Utils.toArrowSchema(schema, "UTC"),
      Iterator.single(input),
      maxRecordsPerBatch)

    try {
      var i = 0
      while (i < numRows) {
        column.putLong(i, i * 7L)
        i += 1
      }

      var startRow = 0
      while (startRow < numRows) {
        reader.loadNextBatch() shouldBe true
        val outputRows = math.min(maxRecordsPerBatch, numRows - startRow)
        val root = reader.getVectorSchemaRoot
        root.getRowCount shouldBe outputRows

        val longs = root.getVector(0).asInstanceOf[BigIntVector]
        i = 0
        while (i < outputRows) {
          val sourceRow = startRow + i
          longs.isNull(i) shouldBe false
          longs.get(i) shouldBe sourceRow * 7L
          i += 1
        }
        startRow += outputRows
      }
      reader.loadNextBatch() shouldBe false
    } finally {
      reader.close()
      input.close()
      allocator.close()
    }
  }

  test("fixed-width decimal write clears validity on precision overflow") {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new DecimalVector("decimal", allocator, 5, 2)
    vector.allocateNew(1)
    vector.set(0, Decimal(0, 5, 2).toJavaBigDecimal)
    class TestDecimalWriter extends DecimalWriter(vector, 5, 2) {
      def setValueUnsafeForTest(input: GenericInternalRow): Unit = setValueUnsafe(input, 0)
    }
    val writer = new TestDecimalWriter

    try {
      vector.isNull(0) shouldBe false
      writer.setValueUnsafeForTest(new GenericInternalRow(Array[Any](Decimal(123456L, 6, 2))))
      vector.isNull(0) shouldBe true
    } finally {
      vector.close()
      allocator.close()
    }
  }

  test("fixed-width column write grows an undersized vector") {
    val allocator = new RootAllocator(Long.MaxValue)
    val numRows = BaseValueVector.INITIAL_VALUE_ALLOCATION + 1
    val input = new OnHeapColumnVector(numRows, IntegerType)
    val schema = StructType(Seq(StructField("int", IntegerType, nullable = false)))
    val root = VectorSchemaRoot.create(Utils.toArrowSchema(schema, "UTC"), allocator)

    try {
      var i = 0
      while (i < numRows) {
        input.putInt(i, i)
        i += 1
      }
      val writer = ArrowWriter.create(root, BaseValueVector.INITIAL_VALUE_ALLOCATION)
      val vector = root.getVector(0).asInstanceOf[IntVector]
      vector.getValueCapacity should be < numRows

      writer.writeColNoNull(new ColumnarArray(input, 0, numRows), 0)
      writer.finish()

      vector.getValueCapacity should be >= numRows
      vector.get(numRows - 1) shouldBe numRows - 1
    } finally {
      root.close()
      input.close()
      allocator.close()
    }
  }

  test("reconcileStreamSchema returns expected schema unchanged on empty iterator") {
    val expected = expectedSchema("c0" -> new ArrowType.Int(64, true))
    val (returned, iter) =
      CometArrowStream.reconcileStreamSchema("test", expected, Iterator.empty)
    returned shouldBe expected
    iter.hasNext shouldBe false
  }

  test("reconcileStreamSchema returns expected schema when types match") {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    try {
      val v = new BigIntVector("col_0", allocator)
      v.allocateNew()
      v.setSafe(0, 1L)
      v.setValueCount(1)
      val cv = new CometPlainVector(v, false)
      val batch = batchOf(cv)
      val expected = expectedSchema("c0" -> new ArrowType.Int(64, true))

      val (returned, iter) = CometArrowStream
        .reconcileStreamSchema("test", expected, Iterator.single(batch))

      returned.getFields.get(0).getType shouldBe new ArrowType.Int(64, true)
      iter.hasNext shouldBe true
      iter.next() should be theSameInstanceAs batch

      cv.close()
    } finally {
      allocator.close()
    }
  }

  test("reconcileStreamSchema rebuilds schema from actual vector types when they differ") {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    try {
      // Producer produced Int32 (e.g., DataFusion-Spark width_bucket pre-fix), consumer expects
      // Int64 (Spark catalyst WidthBucket.dataType = LongType). The truthful schema is Int32 so
      // native ScanExec's build_record_batch can cast at the boundary.
      val v = new IntVector("col_0", allocator)
      v.allocateNew()
      v.setSafe(0, 1)
      v.setValueCount(1)
      val cv = new CometPlainVector(v, false)
      val batch = batchOf(cv)
      val expected = expectedSchema("c0" -> new ArrowType.Int(64, true))

      val (returned, iter) = CometArrowStream
        .reconcileStreamSchema("test", expected, Iterator.single(batch))

      val returnedField = returned.getFields.get(0)
      returnedField.getType shouldBe new ArrowType.Int(32, true)
      // Names come from `expected` so name-indexed consumers keep working.
      returnedField.getName shouldBe "c0"
      iter.hasNext shouldBe true
      iter.next() should be theSameInstanceAs batch

      cv.close()
    } finally {
      allocator.close()
    }
  }

  test(
    "reconcileStreamSchema preserves nullability when expected is nullable but actual is not") {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    try {
      // Spark catalyst declares the column nullable; the first batch happens to come from a
      // vector whose Field reports non-nullable. Subsequent batches may carry nulls, so the
      // wire schema must stay nullable or native validation rejects the next null with
      // "declared as non-nullable but contains null values".
      val v = new BigIntVector(
        new Field(
          "col_0",
          new FieldType(false, new ArrowType.Int(64, true), null),
          java.util.Collections.emptyList[Field]()),
        allocator)
      v.allocateNew()
      v.setSafe(0, 1L)
      v.setValueCount(1)
      val cv = new CometPlainVector(v, false)
      val batch = batchOf(cv)
      val expected = expectedSchema("c0" -> new ArrowType.Int(64, true)) // nullable=true

      val (returned, _) = CometArrowStream
        .reconcileStreamSchema("test", expected, Iterator.single(batch))

      val returnedField = returned.getFields.get(0)
      returnedField.isNullable shouldBe true

      cv.close()
    } finally {
      allocator.close()
    }
  }
}
