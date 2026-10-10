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

package org.apache.spark.sql.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.comet.execution.arrow.CometArrowConverters
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.NativeColumnarToRowConverter

/**
 * Isolated columnar-to-row microbenchmark that excludes the parquet scan entirely. Batches are
 * built once in memory; each case converts them to rows and consumes the values so that the
 * conversion itself is what gets measured.
 */
object CometC2RIsolatedBench {

  private val totalRows = 1024 * 1024

  private val flatSchema = new StructType()
    .add("a", LongType)
    .add("b", IntegerType)
    .add("c", DoubleType)
    .add("d", StringType)

  /** Struct, array, map and array-of-struct columns, each null for some rows. */
  private val nestedSchema = new StructType()
    .add("a", LongType)
    .add(
      "s",
      new StructType()
        .add("x", IntegerType)
        .add("y", StringType)
        .add("z", new StructType().add("p", DoubleType).add("q", LongType)))
    .add("ia", ArrayType(IntegerType))
    .add("sa", ArrayType(StringType))
    .add("m", MapType(StringType, IntegerType))
    .add("as", ArrayType(new StructType().add("i", IntegerType).add("n", StringType)))

  private def flatRow(i: Int): InternalRow =
    InternalRow(i.toLong, i, i.toDouble, UTF8String.fromString(s"value_$i"))

  private def nestedRow(i: Int): InternalRow = {
    val struct =
      if (i % 7 == 0) null
      else {
        InternalRow(
          i,
          UTF8String.fromString(s"y_$i"),
          if (i % 4 == 0) null else InternalRow(i.toDouble, i.toLong))
      }
    val ints =
      if (i % 6 == 0) null
      else new GenericArrayData(Array[Any](i, if (i % 3 == 0) null else i + 1, i + 2))
    val strings =
      if (i % 6 == 1) null
      else
        new GenericArrayData(
          Array[Any](UTF8String.fromString(s"a_$i"), UTF8String.fromString(s"b_$i")))
    val map =
      if (i % 8 == 0) null
      else
        ArrayBasedMapData(
          Array[Any](UTF8String.fromString("k1"), UTF8String.fromString("k2")),
          Array[Any](i, i + 1))
    val structs = new GenericArrayData(
      Array.tabulate[Any](i % 4)(j => InternalRow(i + j, UTF8String.fromString(s"n_$i"))))
    InternalRow(i.toLong, struct, ints, strings, map, structs)
  }

  private def makeBatches(
      schema: StructType,
      batchSize: Int,
      row: Int => InternalRow): Array[ColumnarBatch] = {
    val rows = (0 until totalRows).iterator.map(row)
    CometArrowConverters
      .rowToArrowBatchIter(rows, schema, batchSize, "UTC", org.apache.comet.CometArrowAllocator)
      .toArray
  }

  private def runForBatchSize(
      label: String,
      schema: StructType,
      batchSize: Int,
      row: Int => InternalRow,
      consume: InternalRow => Long): Unit = {
    val batches = makeBatches(schema, batchSize, row)

    val benchmark =
      new Benchmark(s"Isolated C2R (no scan), $label, batchSize=$batchSize", totalRows.toLong)

    benchmark.addCase("JVM rowIterator + UnsafeProjection") { _ =>
      val proj = UnsafeProjection.create(schema.fields.map(_.dataType))
      var sink = 0L
      var b = 0
      while (b < batches.length) {
        val it = batches(b).rowIterator()
        while (it.hasNext) {
          sink += consume(proj(it.next()))
        }
        b += 1
      }
      if (sink == Long.MinValue) println(sink)
    }

    benchmark.addCase("Native converter") { _ =>
      val converter = new NativeColumnarToRowConverter(schema, batchSize)
      var sink = 0L
      try {
        var b = 0
        while (b < batches.length) {
          val it = converter.convert(batches(b))
          while (it.hasNext) {
            sink += consume(it.next())
          }
          b += 1
        }
      } finally {
        converter.close()
      }
      if (sink == Long.MinValue) println(sink)
    }

    benchmark.run()

    batches.foreach(_.close())
  }

  /** Reads every field of the flat schema. */
  private def consumeFlat(u: InternalRow): Long =
    u.getLong(0) + u.getInt(1) + u.getDouble(2).toLong + u.getUTF8String(3).numBytes()

  /**
   * Reads each nested column through its accessor, descending into the struct, array and map
   * elements, so that the cost of materialising a nested value is part of the measurement.
   */
  private def consumeNested(u: InternalRow): Long = {
    var sink = u.getLong(0)
    if (!u.isNullAt(1)) {
      val s = u.getStruct(1, 3)
      sink += s.getInt(0) + s.getUTF8String(1).numBytes()
      if (!s.isNullAt(2)) sink += s.getStruct(2, 2).getLong(1)
    }
    if (!u.isNullAt(2)) {
      val a = u.getArray(2)
      var i = 0
      while (i < a.numElements()) {
        if (!a.isNullAt(i)) sink += a.getInt(i)
        i += 1
      }
    }
    if (!u.isNullAt(3)) {
      val a = u.getArray(3)
      var i = 0
      while (i < a.numElements()) {
        sink += a.getUTF8String(i).numBytes()
        i += 1
      }
    }
    if (!u.isNullAt(4)) {
      val m = u.getMap(4)
      sink += m.numElements() + m.valueArray().getInt(0)
    }
    val structs = u.getArray(5)
    var i = 0
    while (i < structs.numElements()) {
      val e = structs.getStruct(i, 2)
      sink += e.getInt(0) + e.getUTF8String(1).numBytes()
      i += 1
    }
    sink
  }

  def main(args: Array[String]): Unit = {
    runForBatchSize("flat", flatSchema, 8192, flatRow, consumeFlat)
    runForBatchSize("flat", flatSchema, 512, flatRow, consumeFlat)
    runForBatchSize("flat", flatSchema, 32, flatRow, consumeFlat)

    runForBatchSize("nested", nestedSchema, 8192, nestedRow, consumeNested)
    runForBatchSize("nested", nestedSchema, 512, nestedRow, consumeNested)
    runForBatchSize("nested", nestedSchema, 32, nestedRow, consumeNested)
  }
}
