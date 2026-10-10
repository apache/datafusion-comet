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

import java.lang.reflect.Modifier
import java.math.{BigDecimal => JavaBigDecimal, BigInteger}
import java.util.{ArrayDeque, Collections, IdentityHashMap}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.arrow.memory.{ArrowBuf, RootAllocator}
import org.apache.arrow.vector.{BaseVariableWidthVector, DecimalVector, FieldVector, IntVector, ValueVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, StructVector}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData, UnsafeMapData, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.vectorized.{ConstantColumnVector, Dictionary, OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometSparkSessionExtensions.isSpark41Plus

/**
 * The columnar paths of [[ArrowWriter]] copy Spark's vectors in bulk where they can. Whatever
 * they take, they must produce what the row path writes for the same rows, which reads every
 * value through Spark's getters one at a time.
 */
class CometArrowWriterSuite extends AnyFunSuite with Matchers {

  private val numRows = 300

  private val nanosPerDay = 24L * 60 * 60 * 1000 * 1000 * 1000

  // `TimeType` only exists from Spark 4.1, so it is parsed from DDL rather than named.
  private val timeTypes: Seq[DataType] =
    if (isSpark41Plus) Seq(DataType.fromDDL("TIME")) else Seq.empty

  private val primitiveTypes: Seq[DataType] = Seq(
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType(5, 2),
    DecimalType(18, 4),
    DecimalType(38, 10),
    StringType,
    BinaryType,
    DateType,
    TimestampType,
    TimestampNTZType,
    YearMonthIntervalType(),
    DayTimeIntervalType()) ++ timeTypes

  private val nestedTypes: Seq[DataType] = Seq(
    ArrayType(IntegerType),
    ArrayType(StringType),
    ArrayType(DecimalType(18, 2)),
    ArrayType(ArrayType(LongType)),
    ArrayType(new StructType().add("x", IntegerType).add("y", StringType)),
    new StructType().add("a", IntegerType).add("b", StringType),
    new StructType()
      .add("a", BooleanType)
      .add("b", new StructType().add("c", LongType).add("d", StringType))
      .add("e", DecimalType(38, 4)),
    new StructType().add("a", IntegerType).add("b", ArrayType(IntegerType)),
    MapType(StringType, StringType),
    MapType(IntegerType, ArrayType(LongType)),
    MapType(StringType, new StructType().add("x", IntegerType).add("y", StringType)))

  private def newVector(capacity: Int, dataType: DataType, offHeap: Boolean) =
    if (offHeap) new OffHeapColumnVector(capacity, dataType)
    else new OnHeapColumnVector(capacity, dataType)

  private def randomBytes(rnd: Random): Array[Byte] =
    Array.fill(rnd.nextInt(20))(('a' + rnd.nextInt(26)).toByte)

  private def randomDecimal(rnd: Random, dt: DecimalType): Decimal = {
    val bound = BigInteger.TEN.pow(1 + rnd.nextInt(dt.precision))
    val unscaled = new BigInteger(bound.bitLength + 8, rnd.self).mod(bound)
    Decimal(
      new JavaBigDecimal(if (rnd.nextBoolean()) unscaled.negate() else unscaled, dt.scale),
      dt.precision,
      dt.scale)
  }

  private def putValue(v: WritableColumnVector, dataType: DataType, row: Int, rnd: Random) =
    dataType match {
      case BooleanType => v.putBoolean(row, rnd.nextBoolean())
      case ByteType => v.putByte(row, rnd.nextInt().toByte)
      case ShortType => v.putShort(row, rnd.nextInt().toShort)
      case IntegerType | DateType | _: YearMonthIntervalType => v.putInt(row, rnd.nextInt())
      case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType =>
        v.putLong(row, rnd.nextLong())
      case FloatType => v.putFloat(row, rnd.nextFloat())
      case DoubleType => v.putDouble(row, rnd.nextDouble())
      case StringType | BinaryType => v.putByteArray(row, randomBytes(rnd))
      case dt: DecimalType => v.putDecimal(row, randomDecimal(rnd, dt), dt.precision)
      case dt if Utils.isTimeType(dt) =>
        v.putLong(row, Math.floorMod(rnd.nextLong(), nanosPerDay))
    }

  /**
   * Fills rows `[0, n)`. Array and map rows are laid out back to back, or from the last row
   * backwards when `reversed`, and null rows get no offsets at all, as in Spark's readers.
   * Collections hold fewer than `maxLength` elements.
   */
  private def fill(
      v: WritableColumnVector,
      dataType: DataType,
      n: Int,
      rnd: Random,
      nullFraction: Double,
      reversed: Boolean,
      maxLength: Int = 6): Unit = dataType match {
    case st: StructType =>
      (0 until n).foreach(i => if (rnd.nextDouble() < nullFraction) v.putNull(i))
      st.fields.zipWithIndex.foreach { case (field, ordinal) =>
        val child = v.getChild(ordinal)
        child.reserve(n)
        fill(child, field.dataType, n, rnd, nullFraction, reversed, maxLength)
        // Spark's Parquet reader nulls the fields of a null struct. Other producers need not.
        (0 until n).foreach(i => if (v.isNullAt(i) && rnd.nextBoolean()) child.putNull(i))
      }
    case _: ArrayType | _: MapType =>
      val lengths = Array.fill(n)(rnd.nextInt(maxLength))
      val children = dataType match {
        case _: ArrayType => Seq(v.arrayData())
        case _ => Seq(v.getChild(0), v.getChild(1))
      }
      children.foreach(_.reserve(lengths.sum))
      var offset = 0
      (if (reversed) (n - 1 to 0 by -1) else (0 until n)).foreach { i =>
        if (rnd.nextDouble() < nullFraction) {
          v.putNull(i)
        } else {
          v.putArray(i, offset, lengths(i))
          offset += lengths(i)
        }
      }
      dataType match {
        case ArrayType(elementType, _) =>
          fill(v.arrayData(), elementType, offset, rnd, nullFraction, reversed, maxLength)
        case MapType(keyType, valueType, _) =>
          fill(v.getChild(0), keyType, offset, rnd, 0.0, reversed, maxLength)
          fill(v.getChild(1), valueType, offset, rnd, nullFraction, reversed, maxLength)
      }
    case _ =>
      (0 until n).foreach { i =>
        if (rnd.nextDouble() < nullFraction) v.putNull(i) else putValue(v, dataType, i, rnd)
      }
  }

  /**
   * Dictionary-encodes rows `[0, n)`. A null row's id is out of range, so decoding it throws, as
   * it may in Spark, where ids under nulls are garbage.
   */
  private def fillDictionary(
      v: WritableColumnVector,
      dataType: DataType,
      n: Int,
      rnd: Random,
      nullFraction: Double): Unit = {
    val size = 16
    val source = newVector(size, dataType, offHeap = false)
    (0 until size).foreach(i => putValue(source, dataType, i, rnd))
    v.setDictionary(new Dictionary {
      // Spark decodes bytes and shorts through decodeToInt.
      override def decodeToInt(id: Int): Int = dataType match {
        case ByteType => source.getByte(id).toInt
        case ShortType => source.getShort(id).toInt
        case _ => source.getInt(id)
      }
      override def decodeToLong(id: Int): Long = source.getLong(id)
      override def decodeToFloat(id: Int): Float = source.getFloat(id)
      override def decodeToDouble(id: Int): Double = source.getDouble(id)
      override def decodeToBinary(id: Int): Array[Byte] = source.getBinary(id)
    })
    val ids = v.reserveDictionaryIds(n)
    (0 until n).foreach { i =>
      if (rnd.nextDouble() < nullFraction) {
        v.putNull(i)
        ids.putInt(i, size + 1000)
      } else {
        ids.putInt(i, rnd.nextInt(size))
      }
    }
  }

  /** Both roots must hold the same rows, compared vector by vector. */
  private def assertSameRoots(expected: VectorSchemaRoot, actual: VectorSchemaRoot): Unit = {
    actual.getRowCount shouldBe expected.getRowCount
    expected.getFieldVectors.asScala.zip(actual.getFieldVectors.asScala).foreach { case (e, a) =>
      assertSameVectors(e, a, "")
    }
  }

  /** Each vector in the tree must match, leaf values under null parents included. */
  private def assertSameVectors(
      expected: ValueVector,
      actual: ValueVector,
      path: String): Unit = {
    val name = s"$path/${expected.getName}"
    withClue(name) {
      actual.getValueCount shouldBe expected.getValueCount
      (0 until expected.getValueCount).foreach { i =>
        withClue(s"[$i]") {
          actual.isNull(i) shouldBe expected.isNull(i)
          (expected, actual) match {
            case (e: ListVector, a: ListVector) =>
              a.getElementStartIndex(i) shouldBe e.getElementStartIndex(i)
              a.getElementEndIndex(i) shouldBe e.getElementEndIndex(i)
            case (_: StructVector, _) =>
            case _ =>
              // A reader takes each value's start from the end of the one before, null or not.
              // The paths may differ in the bytes under a null struct, but never go backwards.
              actual match {
                case a: BaseVariableWidthVector =>
                  a.getStartOffset(i + 1) should be >= a.getStartOffset(i)
                case _ =>
              }
              if (!expected.isNull(i)) {
                (expected.getObject(i), actual.getObject(i)) match {
                  case (e: Array[Byte], a: Array[Byte]) => a.toSeq shouldBe e.toSeq
                  case (e, a) => a shouldBe e
                }
              }
          }
        }
      }
    }
    (expected, actual) match {
      case (e: FieldVector, a: FieldVector) =>
        e.getChildrenFromFields.asScala.zip(a.getChildrenFromFields.asScala).foreach {
          case (ec, ac) => assertSameVectors(ec, ac, name)
        }
      case _ =>
    }
  }

  /**
   * Writes rows `[start, start + length)` of `input` columnar, in uneven appended chunks into an
   * undersized root so every buffer has to grow, and row by row, and compares the two.
   */
  private def assertColumnarMatchesRows(
      input: ColumnarBatch,
      schema: StructType,
      start: Int,
      length: Int): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowSchema = Utils.toArrowSchema(schema, "UTC")
    val columnar = VectorSchemaRoot.create(arrowSchema, allocator)
    val rows = VectorSchemaRoot.create(arrowSchema, allocator)
    try {
      val columnarWriter = ArrowWriter.create(columnar, 1)
      val chunks = Seq(1, length / 3, length - 1 - length / 3).filter(_ > 0)
      var row = start
      chunks.foreach { chunk =>
        columnarWriter.writeColumns(input, row, chunk)
        row += chunk
      }
      columnarWriter.finish()

      val rowWriter = ArrowWriter.create(rows, length)
      (start until start + length).foreach(i => rowWriter.write(input.getRow(i)))
      rowWriter.finish()

      columnar.getRowCount shouldBe length
      assertSameRoots(rows, columnar)
    } finally {
      columnar.close()
      rows.close()
      allocator.close()
    }
  }

  for (offHeap <- Seq(false, true); nullFraction <- Seq(0.0, 0.2, 1.0)) {
    test(s"columnar slices match the row path: offHeap=$offHeap, nulls=$nullFraction") {
      val cases = primitiveTypes.map((_, false, false)) ++
        primitiveTypes.filter(_ != BooleanType).map((_, true, false)) ++
        nestedTypes.map((_, false, false)) ++
        nestedTypes.collect { case t @ (_: ArrayType | _: MapType) => (t, false, true) }
      cases.foreach { case (dataType, dictionary, reversed) =>
        withClue(s"$dataType dictionary=$dictionary reversed=$reversed: ") {
          val rnd = new Random(dataType.hashCode)
          val schema = new StructType().add("c", dataType)
          val v = newVector(numRows, dataType, offHeap)
          try {
            if (dictionary) fillDictionary(v, dataType, numRows, rnd, nullFraction)
            else fill(v, dataType, numRows, rnd, nullFraction, reversed)
            val batch = new ColumnarBatch(Array[ColumnVector](v), numRows)
            assertColumnarMatchesRows(batch, schema, 0, numRows)
            assertColumnarMatchesRows(batch, schema, 7, 40)
            assertColumnarMatchesRows(batch, schema, 33, 31)
          } finally {
            v.close()
          }
        }
      }
    }
  }

  test("missing fixed-width vectors do not read their unallocated value storage") {
    val rows = 5000
    Seq(false, true).foreach { offHeap =>
      val v = newVector(4096, IntegerType, offHeap)
      try {
        // Spark's Parquet reader marks an evolved field missing instead of growing its backing
        // storage with a surrounding collection. Every getter is null-aware, but a bulk read of
        // all 5000 values would overrun this vector's 4096 allocated slots.
        // Spark 4 calls this setMissing; in Spark 3 the equivalent API is setAllNull.
        val setter = v.getClass.getMethods.find(_.getName == "setMissing").getOrElse {
          v.getClass.getMethod("setAllNull")
        }
        setter.invoke(v)
        val batch = new ColumnarBatch(Array[ColumnVector](v), rows)
        assertColumnarMatchesRows(batch, new StructType().add("missing", IntegerType), 0, rows)
      } finally {
        v.close()
      }
    }
  }

  test("dictionary strings decode against each input batch's dictionary") {
    // Spark's readers reuse one vector across row groups whose dictionaries differ, and the
    // reader joins their batches into one Arrow batch.
    def dictionary(prefix: String): Dictionary = new Dictionary {
      override def decodeToInt(id: Int): Int = throw new UnsupportedOperationException
      override def decodeToLong(id: Int): Long = throw new UnsupportedOperationException
      override def decodeToFloat(id: Int): Float = throw new UnsupportedOperationException
      override def decodeToDouble(id: Int): Double = throw new UnsupportedOperationException
      override def decodeToBinary(id: Int): Array[Byte] = s"$prefix$id".getBytes
    }
    val strings = new OnHeapColumnVector(4, StringType)
    val arrays = new OnHeapColumnVector(4, ArrayType(StringType))
    val elements = arrays.arrayData()
    elements.reserve(8)
    val stringIds = strings.reserveDictionaryIds(4)
    val elementIds = elements.reserveDictionaryIds(8)
    val batches = Seq("a", "b", "c").iterator.map { prefix =>
      strings.setDictionary(dictionary(prefix))
      elements.setDictionary(dictionary(prefix))
      (0 until 4).foreach { i =>
        stringIds.putInt(i, i % 2)
        arrays.putArray(i, 2 * i, 2)
        elementIds.putInt(2 * i, i % 2)
        elementIds.putInt(2 * i + 1, 1)
      }
      new ColumnarBatch(Array[ColumnVector](strings, arrays), 4)
    }
    val schema = new StructType().add("s", StringType).add("a", ArrayType(StringType))
    val allocator = new RootAllocator(Long.MaxValue)
    val reader =
      new SparkColumnarArrowReader(allocator, Utils.toArrowSchema(schema, "UTC"), batches, 12)
    try {
      reader.loadNextBatch() shouldBe true
      val root = reader.getVectorSchemaRoot
      root.getRowCount shouldBe 12
      val outStrings = root.getVector(0).asInstanceOf[VarCharVector]
      val outArrays = root.getVector(1).asInstanceOf[ListVector]
      (0 until 12).foreach { row =>
        val prefix = Seq("a", "b", "c")(row / 4)
        new String(outStrings.get(row)) shouldBe s"$prefix${row % 2}"
        outArrays.getObject(row).asScala.map(_.toString) shouldBe
          Seq(s"$prefix${row % 2}", s"${prefix}1")
      }
      reader.loadNextBatch() shouldBe false
    } finally {
      reader.close()
      strings.close()
      arrays.close()
      allocator.close()
    }
  }

  test("decimals keep their unscaled values at every storage width") {
    val values = Seq(
      DecimalType(5, 2) -> Seq("0", "-0.01", "999.99", "-999.99", "12.34"),
      DecimalType(18, 4) -> Seq("0", "99999999999999.9999", "-99999999999999.9999", "-1.0001"),
      DecimalType(38, 10) -> Seq(
        "0",
        "-1",
        "922337203.6854775807",
        "922337203.6854775808",
        "-922337203.6854775809",
        "9999999999999999999999999999.9999999999",
        "-9999999999999999999999999999.9999999999"))
    values.foreach { case (dt, decimals) =>
      Seq(false, true).foreach { offHeap =>
        val v = newVector(decimals.size, dt, offHeap)
        val allocator = new RootAllocator(Long.MaxValue)
        val root = VectorSchemaRoot.create(
          Utils.toArrowSchema(new StructType().add("d", dt), "UTC"),
          allocator)
        try {
          decimals.zipWithIndex.foreach { case (d, i) =>
            v.putDecimal(i, Decimal(new JavaBigDecimal(d), dt.precision, dt.scale), dt.precision)
          }
          val writer = ArrowWriter.create(root, decimals.size)
          writer.writeColumns(new ColumnarBatch(Array[ColumnVector](v), decimals.size), 0, 3)
          decimals.indices.drop(3).foreach { i =>
            writer.write(
              new GenericInternalRow(Array[Any](v.getDecimal(i, dt.precision, dt.scale))))
          }
          writer.finish()
          val arrow = root.getVector(0).asInstanceOf[DecimalVector]
          decimals.zipWithIndex.foreach { case (d, i) =>
            withClue(s"$dt $d offHeap=$offHeap: ") {
              arrow.getObject(i) shouldBe new JavaBigDecimal(d).setScale(dt.scale)
            }
          }
        } finally {
          root.close()
          allocator.close()
          v.close()
        }
      }
    }
  }

  test("wide decimals from unsafe rows and arrays keep their values") {
    // Unsafe rows and arrays hold a decimal past 18 digits as its unscaled bytes, which the row
    // path range-checks in place.
    Seq(DecimalType(19, 0), DecimalType(20, 0), DecimalType(38, 0), DecimalType(38, 10)).foreach {
      dt =>
        val max = BigInteger.TEN.pow(dt.precision).subtract(BigInteger.ONE)
        val decimals = Seq(
          BigInteger.ZERO,
          BigInteger.ONE.negate(),
          BigInteger.valueOf(Long.MaxValue),
          BigInteger.valueOf(Long.MaxValue).add(BigInteger.ONE),
          BigInteger.valueOf(Long.MinValue),
          BigInteger.valueOf(Long.MinValue).subtract(BigInteger.ONE),
          max,
          max.negate()).map(u => Decimal(new JavaBigDecimal(u, dt.scale), dt.precision, dt.scale))
        val schema = new StructType().add("d", dt).add("a", ArrayType(dt))
        val project = UnsafeProjection.create(schema)
        val allocator = new RootAllocator(Long.MaxValue)
        val root = VectorSchemaRoot.create(Utils.toArrowSchema(schema, "UTC"), allocator)
        try {
          val writer = ArrowWriter.create(root, decimals.size + 1)
          decimals.foreach { d =>
            val array = new GenericArrayData(Array[Any](d, null, d))
            writer.write(project(new GenericInternalRow(Array[Any](d, array))))
          }
          writer.write(project(new GenericInternalRow(Array[Any](null, null))))
          writer.finish()
          val values = root.getVector(0).asInstanceOf[DecimalVector]
          val arrays = root.getVector(1).asInstanceOf[ListVector]
          decimals.zipWithIndex.foreach { case (d, i) =>
            withClue(s"$dt $d: ") {
              values.getObject(i) shouldBe d.toJavaBigDecimal
              arrays.getObject(i).asScala.toSeq shouldBe
                Seq(d.toJavaBigDecimal, null, d.toJavaBigDecimal)
            }
          }
          values.isNull(decimals.size) shouldBe true
          arrays.isNull(decimals.size) shouldBe true
        } finally {
          root.close()
          allocator.close()
        }
    }
  }

  test("a wide decimal past its precision still fails the row path") {
    // Written as decimal(38,0) and read as decimal(20,0), so the unsafe row holds 21 digits.
    val wide = UnsafeProjection.create(new StructType().add("d", DecimalType(38, 0)))
    val row = wide(
      new GenericInternalRow(
        Array[Any](Decimal(new JavaBigDecimal("123456789012345678901"), 38, 0))))
    val allocator = new RootAllocator(Long.MaxValue)
    val root =
      VectorSchemaRoot.create(
        Utils.toArrowSchema(new StructType().add("d", DecimalType(20, 0)), "UTC"),
        allocator)
    try {
      val writer = ArrowWriter.create(root, 1)
      intercept[ArithmeticException](writer.write(row))
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("a decimal past its precision in an unsafe array still fails the row path") {
    // Written at a wider precision and read at precision p, so the array holds 10^p or -10^p, the
    // smallest values past p. Up to 18 digits the array holds the unscaled long, and past that the
    // unscaled bytes.
    Seq(
      (DecimalType(18, 2), DecimalType(5, 2), "1000.00"),
      (DecimalType(38, 0), DecimalType(20, 0), "100000000000000000000")).foreach {
      case (written, read, magnitude) =>
        Seq(magnitude, s"-$magnitude").foreach { value =>
          val decimal = Decimal(new JavaBigDecimal(value), written.precision, written.scale)
          val row = UnsafeProjection.create(new StructType().add("a", ArrayType(written)))(
            new GenericInternalRow(Array[Any](new GenericArrayData(Array[Any](decimal)))))
          val allocator = new RootAllocator(Long.MaxValue)
          val root = VectorSchemaRoot.create(
            Utils.toArrowSchema(new StructType().add("a", ArrayType(read)), "UTC"),
            allocator)
          try {
            val writer = ArrowWriter.create(root, 1)
            withClue(s"$read $value: ") {
              intercept[ArithmeticException](writer.write(row))
            }
          } finally {
            root.close()
            allocator.close()
          }
        }
    }
  }

  test("a narrow decimal with more digits than its precision passes through") {
    // Spark's getDecimal does not check an int- or long-backed value against the precision, so
    // neither path does. Arrow's BigDecimal setter, which the writer used before, threw instead.
    val dt = DecimalType(5, 2)
    val v = new OnHeapColumnVector(numRows, dt)
    val allocator = new RootAllocator(Long.MaxValue)
    val schema = Utils.toArrowSchema(new StructType().add("d", dt), "UTC")
    val columnar = VectorSchemaRoot.create(schema, allocator)
    val rows = VectorSchemaRoot.create(schema, allocator)
    try {
      (0 until numRows).foreach(i => v.putInt(i, 1234567))
      val batch = new ColumnarBatch(Array[ColumnVector](v), numRows)
      val columnarWriter = ArrowWriter.create(columnar, numRows)
      columnarWriter.writeColumns(batch, 0, numRows)
      columnarWriter.finish()
      val rowWriter = ArrowWriter.create(rows, 1)
      rowWriter.write(batch.getRow(0))
      rowWriter.finish()
      val expected = new JavaBigDecimal("12345.67")
      columnar.getVector(0).asInstanceOf[DecimalVector].getObject(numRows - 1) shouldBe expected
      rows.getVector(0).asInstanceOf[DecimalVector].getObject(0) shouldBe expected
    } finally {
      columnar.close()
      rows.close()
      allocator.close()
      v.close()
    }
  }

  test("a wide decimal past its precision still fails the columnar path") {
    val dt = DecimalType(20, 0)
    val v = new OnHeapColumnVector(1, dt)
    val allocator = new RootAllocator(Long.MaxValue)
    val root =
      VectorSchemaRoot.create(
        Utils.toArrowSchema(new StructType().add("d", dt), "UTC"),
        allocator)
    try {
      // 21 digits, stored as Spark stores any decimal past 18 digits.
      v.putByteArray(0, new BigInteger("123456789012345678901").toByteArray)
      val writer = ArrowWriter.create(root, 1)
      intercept[ArithmeticException] {
        writer.writeColumns(new ColumnarBatch(Array[ColumnVector](v), 1), 0, 1)
      }
    } finally {
      root.close()
      allocator.close()
      v.close()
    }
  }

  test("fields of a null struct come out null when written as columns") {
    val dt = new StructType().add("i", IntegerType).add("s", StringType)
    val v = new OnHeapColumnVector(4, dt)
    val allocator = new RootAllocator(Long.MaxValue)
    val root =
      VectorSchemaRoot.create(
        Utils.toArrowSchema(new StructType().add("st", dt), "UTC"),
        allocator)
    try {
      (0 until 4).foreach { i =>
        v.getChild(0).putInt(i, i)
        v.getChild(1).putByteArray(i, s"v$i".getBytes)
      }
      v.putNull(1)
      v.putNull(2)
      val writer = ArrowWriter.create(root, 4)
      writer.writeColumns(new ColumnarBatch(Array[ColumnVector](v), 4), 0, 4)
      writer.finish()
      val struct = root.getVector(0).asInstanceOf[StructVector]
      val ints = struct.getChild("i").asInstanceOf[IntVector]
      val strings = struct.getChild("s").asInstanceOf[VarCharVector]
      (0 until 4).map(struct.isNull) shouldBe Seq(false, true, true, false)
      (0 until 4).map(ints.isNull) shouldBe Seq(false, true, true, false)
      (0 until 4).map(strings.isNull) shouldBe Seq(false, true, true, false)
      ints.get(3) shouldBe 3
      new String(strings.get(3)) shouldBe "v3"
    } finally {
      root.close()
      allocator.close()
      v.close()
    }
  }

  test("the fields of a null constant struct are not read") {
    // ConstantColumnVector never creates the fields of a null struct.
    val dt = new StructType().add("a", IntegerType).add("s", StringType)
    val v = new ConstantColumnVector(numRows, dt)
    try {
      v.setNull()
      assertColumnarMatchesRows(
        new ColumnarBatch(Array[ColumnVector](v), numRows),
        new StructType().add("st", dt),
        0,
        numRows)
    } finally {
      v.close()
    }
  }

  test("a dictionary-encoded field appends as a column after its struct took the row path") {
    // A struct with a collection field takes the row path in a batch that holds null structs,
    // which leaves the offsets of its string field's trailing nulls for the next value to fill. A
    // dictionary-encoded field has to fill them, and grow its buffers, before it appends a column.
    val st = new StructType().add("a", ArrayType(IntegerType)).add("s", StringType)
    val schema = new StructType().add("st", st)
    val rnd = new Random(5)
    val batches = Seq((0.5, 100), (0.0, 5000), (0.5, 100), (0.0, 5000)).map {
      case (nullFraction, n) =>
        val v = newVector(n, st, offHeap = false)
        (0 until n).foreach { i =>
          if (rnd.nextDouble() < nullFraction || (nullFraction > 0 && i == n - 1)) {
            v.putNull(i)
          }
        }
        fill(v.getChild(0), ArrayType(IntegerType), n, rnd, nullFraction = 0.0, reversed = false)
        fillDictionary(v.getChild(1), StringType, n, rnd, nullFraction = 0.0)
        // Spark's Parquet reader nulls the fields of a null struct.
        (0 until n).foreach { i =>
          if (v.isNullAt(i)) {
            v.getChild(0).putNull(i)
            v.getChild(1).putNull(i)
          }
        }
        new ColumnarBatch(Array[ColumnVector](v), n)
    }
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowSchema = Utils.toArrowSchema(schema, "UTC")
    val columnar = VectorSchemaRoot.create(arrowSchema, allocator)
    val rows = VectorSchemaRoot.create(arrowSchema, allocator)
    try {
      val columnarWriter = ArrowWriter.create(columnar, 1)
      batches.foreach(b => columnarWriter.writeColumns(b, 0, b.numRows()))
      columnarWriter.finish()
      val rowWriter = ArrowWriter.create(rows, 1)
      batches.foreach(b => (0 until b.numRows()).foreach(i => rowWriter.write(b.getRow(i))))
      rowWriter.finish()
      assertSameRoots(rows, columnar)
    } finally {
      columnar.close()
      rows.close()
      allocator.close()
      batches.foreach(_.close())
    }
  }

  test("a struct that switches between the columnar and row paths across appended batches") {
    // A struct with an array or a map field takes the row path only in batches that hold null
    // structs, so its fields' writers keep appending where the other path left off.
    val st = new StructType()
      .add("a", ArrayType(StringType))
      .add("m", MapType(StringType, StringType))
      .add("i", IntegerType)
      .add("s", StringType)
      .add("d", DecimalType(38, 10))
    val schema = new StructType().add("st", st).add("top", ArrayType(StringType))
    for (offHeap <- Seq(false, true); reversed <- Seq(false, true)) {
      withClue(s"offHeap=$offHeap reversed=$reversed: ") {
        val rnd = new Random(42)
        val batches = Seq(0.0, 0.5, 0.0, 1.0, 0.0, 0.3, 0.9, 0.0).zipWithIndex.map {
          case (nullFraction, k) =>
            val n = 37 + k * 13
            val vectors = schema.fields.map { f =>
              val v = newVector(n, f.dataType, offHeap)
              fill(v, f.dataType, n, rnd, nullFraction, reversed)
              v: ColumnVector
            }
            new ColumnarBatch(vectors, n)
        }
        val allocator = new RootAllocator(Long.MaxValue)
        val arrowSchema = Utils.toArrowSchema(schema, "UTC")
        val columnar = VectorSchemaRoot.create(arrowSchema, allocator)
        val rows = VectorSchemaRoot.create(arrowSchema, allocator)
        try {
          val columnarWriter = ArrowWriter.create(columnar, 1)
          batches.foreach { batch =>
            // Two appends per batch, so each batch also starts part way through the Arrow one.
            val half = batch.numRows() / 2
            columnarWriter.writeColumns(batch, 0, half)
            columnarWriter.writeColumns(batch, half, batch.numRows() - half)
          }
          columnarWriter.finish()
          val rowWriter = ArrowWriter.create(rows, batches.map(_.numRows()).sum)
          batches.foreach(b => (0 until b.numRows()).foreach(i => rowWriter.write(b.getRow(i))))
          rowWriter.finish()
          assertSameRoots(rows, columnar)
        } finally {
          columnar.close()
          rows.close()
          allocator.close()
          batches.foreach(_.close())
        }
      }
    }
  }

  // Nested shapes whose unsafe forms take paths of their own: an array of each primitive type not
  // in `nestedTypes`, whose elements are either copied in one block or converted one at a time,
  // collections inside collections, and a struct wider than one word of null bits.
  private val moreNestedTypes: Seq[DataType] =
    primitiveTypes.map(ArrayType(_)).filterNot(nestedTypes.contains) ++ Seq(
      ArrayType(MapType(StringType, IntegerType)),
      MapType(StringType, ArrayType(StringType)),
      MapType(LongType, DecimalType(9, 2)),
      new StructType()
        .add("m", MapType(IntegerType, StringType))
        .add("s", new StructType().add("x", BinaryType).add("y", DecimalType(18, 4))),
      StructType(
        (0 until 70).map(i => StructField(s"f$i", if (i % 7 == 3) StringType else LongType))))

  /**
   * Writes `numRows` rows, `row(i)` through the generic row path and its unsafe projection
   * through the unsafe one, the latter copied to off-heap memory when `offHeap`, as Spark's
   * off-heap pages hold rows. A third writer alternates between the two kinds of row, so each
   * path appends where the other left off, and a fourth writes generic rows holding the unsafe
   * row's values, as a copied row holds unsafe arrays. All of them must write the same vectors.
   */
  private def assertUnsafeRowsMatchGeneric(numRows: Int, schema: StructType, offHeap: Boolean)(
      row: Int => InternalRow): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowSchema = Utils.toArrowSchema(schema, "UTC")
    val roots = Seq.fill(4)(VectorSchemaRoot.create(arrowSchema, allocator))
    val buffers = ArrayBuffer.empty[ArrowBuf]
    try {
      // Every writer starts undersized, so each buffer has to grow.
      val writers = roots.map(ArrowWriter.create(_, 1))
      val project = UnsafeProjection.create(schema)
      (0 until numRows).foreach { i =>
        val generic = row(i)
        val projected = project(generic)
        val unsafe = if (offHeap) {
          val buffer = allocator.buffer(math.max(projected.getSizeInBytes, 8).toLong)
          buffers += buffer
          projected.writeToMemory(null, buffer.memoryAddress())
          val copy = new UnsafeRow(projected.numFields())
          copy.pointTo(null, buffer.memoryAddress(), projected.getSizeInBytes)
          copy
        } else {
          projected
        }
        writers(0).write(generic)
        writers(1).write(unsafe)
        writers(2).write(if (i % 3 == 1) generic else unsafe)
        writers(3).write(new GenericInternalRow(Array.tabulate[Any](schema.length) { ordinal =>
          unsafe.get(ordinal, schema(ordinal).dataType)
        }))
      }
      writers.foreach(_.finish())
      roots.head.getRowCount shouldBe numRows
      roots.tail.foreach(assertSameRoots(roots.head, _))
    } finally {
      roots.foreach(_.close())
      buffers.foreach(_.close())
      allocator.close()
    }
  }

  /** Fills `n` rows of `schema` as Spark's readers lay them out, and checks them as above. */
  private def assertFilledRowsMatch(
      schema: StructType,
      n: Int,
      nullFraction: Double,
      offHeap: Boolean,
      maxLength: Int = 6): Unit = {
    val rnd = new Random(schema.hashCode)
    val vectors = schema.fields.map(f => newVector(n, f.dataType, offHeap = false))
    try {
      schema.fields.zip(vectors).foreach { case (field, v) =>
        fill(v, field.dataType, n, rnd, nullFraction, reversed = false, maxLength)
      }
      val batch = new ColumnarBatch(vectors.toArray[ColumnVector], n)
      assertUnsafeRowsMatchGeneric(n, schema, offHeap)(batch.getRow)
    } finally {
      vectors.foreach(_.close())
    }
  }

  for (offHeap <- Seq(false, true); nullFraction <- Seq(0.0, 0.2, 1.0)) {
    test(s"unsafe rows match the generic row path: offHeap=$offHeap, nulls=$nullFraction") {
      val types = primitiveTypes ++ nestedTypes ++ moreNestedTypes
      val schemas = types.map(t => new StructType().add("c", t)) :+
        // A row wider than one word of null bits, with every type in it.
        StructType(types.zipWithIndex.flatMap { case (t, i) =>
          Seq(StructField(s"a$i", t), StructField(s"b$i", t), StructField(s"c$i", t))
        })
      schemas.foreach { schema =>
        withClue(s"${schema.simpleString}: ") {
          assertFilledRowsMatch(schema, numRows, nullFraction, offHeap)
        }
      }
    }
  }

  test("unsafe rows match the generic row path with collections past 64 elements") {
    // An unsafe array keeps its null bits in 64-bit words, so long collections span several,
    // and their elements land at every bit offset of the Arrow validity buffer.
    val types = (nestedTypes ++ moreNestedTypes).collect { case t @ (_: ArrayType | _: MapType) =>
      t
    }
    for (dataType <- types; nullFraction <- Seq(0.0, 0.3)) {
      withClue(s"$dataType nulls=$nullFraction: ") {
        val schema = new StructType().add("c", dataType)
        assertFilledRowsMatch(schema, 24, nullFraction, offHeap = false, maxLength = 150)
      }
    }
  }

  /**
   * Whether `target` is reachable from `root` through the fields of the Arrow writers and of any
   * Spark unsafe views they hold. Arrow's vectors hold only Arrow memory, so they are skipped.
   */
  private def reachable(root: AnyRef, target: AnyRef): Boolean = {
    val writerPackage = classOf[ArrowWriter].getPackage.getName + "."
    val seen = Collections.newSetFromMap(new IdentityHashMap[AnyRef, java.lang.Boolean])
    val pending = new ArrayDeque[AnyRef]
    def pushFields(o: AnyRef): Unit = {
      var c: Class[_] = o.getClass
      while (c != null) {
        c.getDeclaredFields.foreach { field =>
          if (!field.getType.isPrimitive && !Modifier.isStatic(field.getModifiers)) {
            field.setAccessible(true)
            val value = field.get(o)
            if (value != null) {
              pending.push(value)
            }
          }
        }
        c = c.getSuperclass
      }
    }
    pending.push(root)
    while (!pending.isEmpty) {
      val o = pending.pop()
      if (o eq target) {
        return true
      }
      if (seen.add(o)) {
        o match {
          case objects: Array[AnyRef] => objects.foreach(x => if (x != null) pending.push(x))
          case _: UnsafeArrayData | _: UnsafeMapData | _: UnsafeRow => pushFields(o)
          case _ if o.getClass.getName.startsWith(writerPackage) => pushFields(o)
          case _ =>
        }
      }
    }
    false
  }

  test("the row path keeps no reference to an unsafe row once it is written") {
    // Arrays, maps and structs at the top level, inside one another and as array elements. A view
    // reused across values would keep the last row it read, and the row's memory, reachable.
    val schema = new StructType()
      .add("a", ArrayType(ArrayType(IntegerType)))
      .add("s", new StructType().add("x", StringType).add("y", ArrayType(LongType)))
      .add("m", MapType(StringType, new StructType().add("z", BinaryType)))
      .add("as", ArrayType(new StructType().add("w", IntegerType)))
      .add("am", ArrayType(MapType(IntegerType, StringType)))
    val n = 20
    val rnd = new Random(7)
    val vectors = schema.fields.map(f => newVector(n, f.dataType, offHeap = false))
    val allocator = new RootAllocator(Long.MaxValue)
    val root = VectorSchemaRoot.create(Utils.toArrowSchema(schema, "UTC"), allocator)
    try {
      schema.fields.zip(vectors).foreach { case (field, v) =>
        fill(v, field.dataType, n, rnd, nullFraction = 0.0, reversed = false)
      }
      val batch = new ColumnarBatch(vectors.toArray[ColumnVector], n)
      val project = UnsafeProjection.create(schema)
      val writer = ArrowWriter.create(root, n)
      (0 until n).foreach { i =>
        val row = project(batch.getRow(i)).copy()
        writer.write(row)
        withClue(s"row $i: ") {
          reachable(writer, row.getBaseObject) shouldBe false
        }
      }
    } finally {
      root.close()
      allocator.close()
      vectors.foreach(_.close())
    }
  }

  test("unsafe rows copy strings and binaries of every length") {
    // Short values are copied a word at a time and long ones in one call, so this covers both
    // sides of the threshold and every remainder.
    val schema = new StructType()
      .add("s", StringType)
      .add("b", BinaryType)
      .add("a", ArrayType(StringType))
      .add("m", MapType(StringType, BinaryType))
    val rows = (0 to 140).map { length =>
      if (length % 17 == 5) {
        new GenericInternalRow(4)
      } else {
        val bytes = Array.tabulate[Byte](length)(i => (i * 31 + length).toByte)
        val text = UTF8String.fromBytes(bytes)
        val half = UTF8String.fromBytes(bytes, length / 3, length / 2)
        new GenericInternalRow(
          Array[Any](
            text,
            bytes,
            new GenericArrayData(Array[Any](text, null, half)),
            new ArrayBasedMapData(
              new GenericArrayData(Array[Any](text, UTF8String.fromString(s"key-$length"))),
              new GenericArrayData(Array[Any](null, bytes)))))
      }
    }
    Seq(false, true).foreach { offHeap =>
      assertUnsafeRowsMatchGeneric(rows.size, schema, offHeap)(rows)
    }
  }
}
