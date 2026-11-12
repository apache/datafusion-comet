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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.vector.{DecimalVector, FieldVector, IntVector}
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, FieldType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.comet.execution.arrow.CometArrowConverters
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.vector.{CometDictionary, CometDictionaryVector, CometPlainVector}

/**
 * Verifies that [[DirectColumnarToRowConverter]] produces rows byte-identical to
 * `UnsafeProjection`, which matters because `UnsafeRow` equality and hashing are byte-wise.
 */
class DirectColumnarToRowConverterSuite extends AnyFunSuite {

  private def assertMatchesUnsafeProjection(
      schema: StructType,
      rows: Seq[InternalRow],
      batchSize: Int): Unit = {
    val batches = CometArrowConverters
      .rowToArrowBatchIter(rows.iterator, schema, batchSize, "UTC", CometArrowAllocator)
      .toArray
    try {
      val proj = UnsafeProjection.create(schema.fields.map(_.dataType))
      val converter = new DirectColumnarToRowConverter(schema)
      var rowIdx = 0
      for (batch <- batches) {
        converter.setBatch(batch)
        val it = batch.rowIterator()
        var i = 0
        while (it.hasNext) {
          val expected = proj(it.next())
          val actual = converter.convertRow(i)
          assert(
            expected.getBytes.toSeq == actual.getBytes.toSeq,
            s"row $rowIdx differs:\n expected ${expected.getBytes.toSeq}\n actual   " +
              s"${actual.getBytes.toSeq}")
          i += 1
          rowIdx += 1
        }
      }
    } finally {
      batches.foreach(_.close())
    }
  }

  test("all supported types with nulls match UnsafeProjection bytes") {
    val schema = new StructType()
      .add("bool", BooleanType)
      .add("byte", ByteType)
      .add("short", ShortType)
      .add("int", IntegerType)
      .add("long", LongType)
      .add("float", FloatType)
      .add("double", DoubleType)
      .add("date", DateType)
      .add("ts", TimestampType)
      .add("ts_ntz", TimestampNTZType)
      .add("str", StringType)
      .add("dec_compact", DecimalType(12, 2))
      .add("dec_wide", DecimalType(38, 10))

    val rows = (0 until 1000).map { i =>
      def nullEvery(k: Int, v: Any): Any = if (i % k == 0) null else v
      val sign = if (i % 2 == 0) 1L else -1L
      new GenericInternalRow(
        Array[Any](
          nullEvery(3, i % 2 == 0),
          nullEvery(4, ((i % 256) - 128).toByte),
          nullEvery(5, (i - 500).toShort),
          nullEvery(6, i * sign.toInt),
          nullEvery(7, i.toLong * sign * 1000003L),
          nullEvery(8, if (i % 50 == 1) Float.NaN else i.toFloat * sign),
          nullEvery(9, if (i % 50 == 2) Double.NaN else i.toDouble * sign),
          nullEvery(10, 8000 + i % 2500),
          nullEvery(11, i.toLong * 1000000L),
          nullEvery(13, i.toLong * -1000000L),
          nullEvery(12, UTF8String.fromString(if (i % 13 == 0) "" else s"value_$i")),
          nullEvery(3, Decimal.createUnsafe(i * sign * 97, 12, 2)),
          nullEvery(
            4, {
              val magnitude = new java.math.BigInteger(s"${i + 1}" * 3)
              val unscaled = if (sign < 0) magnitude.negate() else magnitude
              Decimal(new java.math.BigDecimal(unscaled, 10))
            })))
    }

    assertMatchesUnsafeProjection(schema, rows, batchSize = 64)
  }

  test("all-fixed-width schema takes the columnar fast path and matches bytes") {
    val schema = new StructType()
      .add("bool", BooleanType)
      .add("byte", ByteType)
      .add("short", ShortType)
      .add("int", IntegerType)
      .add("long", LongType)
      .add("float", FloatType)
      .add("double", DoubleType)
      .add("date", DateType)
      .add("ts", TimestampType)
      .add("ts_ntz", TimestampNTZType)
      .add("dec_compact", DecimalType(12, 2))

    val rows = (0 until 1000).map { i =>
      def nullEvery(k: Int, v: Any): Any = if (i % k == 0) null else v
      val sign = if (i % 2 == 0) 1L else -1L
      new GenericInternalRow(
        Array[Any](
          nullEvery(3, i % 2 == 0),
          nullEvery(4, ((i % 256) - 128).toByte),
          nullEvery(5, (i - 500).toShort),
          nullEvery(6, i * sign.toInt),
          nullEvery(7, i.toLong * sign * 1000003L),
          nullEvery(8, if (i % 50 == 1) Float.NaN else i.toFloat * sign),
          nullEvery(9, if (i % 50 == 2) Double.NaN else i.toDouble * sign),
          nullEvery(10, 8000 + i % 2500),
          nullEvery(11, i.toLong * 1000000L),
          nullEvery(13, i.toLong * -1000000L),
          nullEvery(3, Decimal.createUnsafe(i * sign * 97, 12, 2))))
    }

    assertMatchesUnsafeProjection(schema, rows, batchSize = 64)
  }

  test("multi-word null bitset matches UnsafeProjection bytes") {
    val numCols = 70
    val schema =
      (0 until numCols).foldLeft(new StructType())((s, i) => s.add(s"c$i", LongType))
    val rows = (0 until 200).map { i =>
      new GenericInternalRow((0 until numCols).map { c =>
        if ((i + c) % 3 == 0) null else (i.toLong * 31 + c): Any
      }.toArray)
    }
    assertMatchesUnsafeProjection(schema, rows, batchSize = 33)
  }

  test("wide decimal boundary values match UnsafeProjection bytes") {
    val schema = new StructType().add("d", DecimalType(38, 0))
    val big = new java.math.BigInteger("99999999999999999999999999999999999999")
    val values = Seq(
      java.math.BigInteger.ZERO,
      java.math.BigInteger.ONE,
      java.math.BigInteger.ONE.negate(),
      java.math.BigInteger.valueOf(Long.MaxValue),
      java.math.BigInteger.valueOf(Long.MinValue),
      java.math.BigInteger.valueOf(127),
      java.math.BigInteger.valueOf(128),
      java.math.BigInteger.valueOf(-128),
      java.math.BigInteger.valueOf(-129),
      big,
      big.negate())
    val rows = values.map { v =>
      new GenericInternalRow(Array[Any](Decimal(new java.math.BigDecimal(v, 0))))
    }
    assertMatchesUnsafeProjection(schema, rows, batchSize = 4)
  }

  test("noncanonical NaN payloads match UnsafeProjection bytes") {
    val floatValue = java.lang.Float.intBitsToFloat(0x7fc12345)
    val doubleValue = java.lang.Double.longBitsToDouble(0x7ff8000000000001L)

    for (includeString <- Seq(false, true)) {
      val fixedSchema = new StructType().add("f", FloatType).add("d", DoubleType)
      val schema = if (includeString) fixedSchema.add("s", StringType) else fixedSchema
      val floatCol = new ConstantColumnVector(1, FloatType)
      floatCol.setFloat(floatValue)
      val doubleCol = new ConstantColumnVector(1, DoubleType)
      doubleCol.setDouble(doubleValue)
      val columns = if (includeString) {
        val stringCol = new ConstantColumnVector(1, StringType)
        stringCol.setUtf8String(UTF8String.fromString("general path"))
        Array[ColumnVector](floatCol, doubleCol, stringCol)
      } else {
        Array[ColumnVector](floatCol, doubleCol)
      }
      val batch = new ColumnarBatch(columns, 1)

      try {
        val expected = UnsafeProjection
          .create(schema.fields.map(_.dataType))(batch.getRow(0))
          .getBytes
        val converter = new DirectColumnarToRowConverter(schema)
        converter.setBatch(batch)
        assert(expected.sameElements(converter.convertRow(0).getBytes))
      } finally {
        batch.close()
      }
    }
  }

  test("oversized fixed-width batch is rejected before allocation") {
    val schema = new StructType().add("value", LongType)
    val rowSize = UnsafeRow.calculateBitSetWidthInBytes(1) + 8
    val numRows =
      org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / rowSize + 1
    val column = new ConstantColumnVector(numRows, LongType)
    val batch = new ColumnarBatch(Array[ColumnVector](column), numRows)

    try {
      val converter = new DirectColumnarToRowConverter(schema)
      val error = intercept[IllegalArgumentException](converter.setBatch(batch))
      assert(error.getMessage.contains("Batch too large"))
    } finally {
      batch.close()
    }
  }

  test("dictionary-encoded compact and wide decimals match UnsafeProjection bytes") {
    // The native scan hands this converter plain vectors, so build dictionary vectors directly to
    // reach the non-CometPlainVector branches of compactDecimalValue and writeWideDecimal.
    val compactType = DecimalType(18, 2)
    val wideType = DecimalType(38, 10)
    val compactDictionary =
      Seq("123.45", "-9999999999999999.99", "0.01").map(new java.math.BigDecimal(_))
    val wideDictionary = Seq(
      "12345678901234567890.1234567890",
      "-999999999999999999999999999.9999999999",
      "0.0000000000").map(new java.math.BigDecimal(_))
    val compactIndices = Seq(Some(0), Some(1), None, Some(2), Some(1))
    val wideIndices = Seq(Some(2), None, Some(0), Some(1), Some(0))
    val numRows = compactIndices.size

    val allocator = CometArrowAllocator.newChildAllocator("dictionary-decimals", 0, Long.MaxValue)
    val provider = new MapDictionaryProvider()
    val vectors = ArrayBuffer.empty[FieldVector]
    val indexType = new ArrowType.Int(32, true)
    def dictionaryColumn(
        name: String,
        id: Long,
        dataType: DecimalType,
        dictionary: Seq[java.math.BigDecimal],
        indices: Seq[Option[Int]]): CometDictionaryVector = {
      val values =
        new DecimalVector(s"${name}_values", allocator, dataType.precision, dataType.scale)
      vectors += values
      values.allocateNew(dictionary.size)
      dictionary.zipWithIndex.foreach { case (v, i) => values.setSafe(i, v) }
      values.setValueCount(dictionary.size)
      val encoding = new DictionaryEncoding(id, false, indexType)
      provider.put(new Dictionary(values, encoding))
      val ids = new IntVector(name, new FieldType(true, indexType, encoding), allocator)
      vectors += ids
      ids.allocateNew(indices.size)
      indices.zipWithIndex.foreach {
        case (Some(v), i) => ids.set(i, v)
        case (None, i) => ids.setNull(i)
      }
      ids.setValueCount(indices.size)
      new CometDictionaryVector(
        new CometPlainVector(ids),
        new CometDictionary(new CometPlainVector(values)),
        provider)
    }

    try {
      val compact =
        dictionaryColumn("compact", 1L, compactType, compactDictionary, compactIndices)
      val wide = dictionaryColumn("wide", 2L, wideType, wideDictionary, wideIndices)
      def decimal(dictionary: Seq[java.math.BigDecimal], index: Option[Int], dt: DecimalType) =
        index.map(i => Decimal(dictionary(i), dt.precision, dt.scale)).orNull

      // Compact-only schemas take the fixed-width path; adding the wide column takes the general
      // one. Both must write the same bytes as Spark's projection over the decoded values.
      Seq(
        (new StructType().add("compact", compactType), Array[ColumnVector](compact)),
        (
          new StructType().add("compact", compactType).add("wide", wideType),
          Array[ColumnVector](compact, wide))).foreach { case (schema, columns) =>
        val proj = UnsafeProjection.create(schema.fields.map(_.dataType))
        val converter = new DirectColumnarToRowConverter(schema)
        converter.setBatch(new ColumnarBatch(columns, numRows))
        (0 until numRows).foreach { i =>
          val values = Seq(decimal(compactDictionary, compactIndices(i), compactType)) ++
            (if (columns.length > 1) Seq(decimal(wideDictionary, wideIndices(i), wideType))
             else Seq.empty)
          val expected = proj(new GenericInternalRow(values.toArray[Any]))
          val actual = converter.convertRow(i)
          assert(
            expected.getBytes.toSeq == actual.getBytes.toSeq,
            s"${schema.simpleString} row $i differs:\n expected ${expected.getBytes.toSeq}\n " +
              s"actual   ${actual.getBytes.toSeq}")
        }
      }
    } finally {
      vectors.foreach(_.close())
      allocator.close()
    }
  }
}
