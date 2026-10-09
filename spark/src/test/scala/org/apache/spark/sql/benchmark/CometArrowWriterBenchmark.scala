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
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.vectorized.{Dictionary, OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch, ColumnVector}

/**
 * Benchmark Spark row/columnar-to-Arrow conversion, for fixed-width vectors at several batch
 * sizes and for one column of each common type at Comet's default batch size.
 *
 * To run this benchmark:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.comet.execution.arrow.CometArrowWriterBenchmark
 * }}}
 */
object CometArrowWriterBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(args: Array[String]): Unit = {
    val schema = StructType(
      Seq(
        StructField("int", IntegerType, nullable = true),
        StructField("long", LongType, nullable = true),
        StructField("double", DoubleType, nullable = true)))
    val arrowSchema = Utils.toArrowSchema(schema, "UTC")
    val allocator = new RootAllocator(Long.MaxValue)

    def batch(numRows: Int, offHeap: Boolean, nullEveryOtherRow: Boolean): ColumnarBatch = {
      def vector(dataType: org.apache.spark.sql.types.DataType): WritableColumnVector = {
        if (offHeap) {
          new OffHeapColumnVector(numRows, dataType)
        } else {
          new OnHeapColumnVector(numRows, dataType)
        }
      }

      val intColumn = vector(IntegerType)
      val longColumn = vector(LongType)
      val doubleColumn = vector(DoubleType)
      var i = 0
      while (i < numRows) {
        if (nullEveryOtherRow && (i & 1) == 0) {
          intColumn.putNull(i)
          longColumn.putNull(i)
          doubleColumn.putNull(i)
        } else {
          intColumn.putInt(i, i)
          longColumn.putLong(i, -i.toLong)
          doubleColumn.putDouble(i, i + 0.5d)
        }
        i += 1
      }
      new ColumnarBatch(Array[ColumnVector](intColumn, longColumn, doubleColumn), numRows)
    }

    def writeBatch(input: ColumnarBatch, bulkCopy: Boolean, root: VectorSchemaRoot): Unit = {
      val writer = ArrowWriter.create(root, input.numRows())
      if (bulkCopy) {
        writer.writeColumns(input, 0, input.numRows())
      } else {
        var col = 0
        while (col < input.numCols()) {
          writer.writeColNoNull(new ColumnarArray(input.column(col), 0, input.numRows()), col)
          col += 1
        }
      }
      writer.finish()
    }

    try {
      Seq(1, 8, 32, 512, 8192).foreach { numRows =>
        val onHeap = batch(numRows, offHeap = false, nullEveryOtherRow = false)
        val offHeap = batch(numRows, offHeap = true, nullEveryOtherRow = false)
        val nullable = batch(numRows, offHeap = false, nullEveryOtherRow = true)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        try {
          val benchmark =
            new Benchmark(
              s"Spark columnar to Arrow ($numRows rows)",
              numRows.toLong,
              output = output)
          benchmark.addCase("on-heap optimized path") { _ =>
            writeBatch(onHeap, bulkCopy = true, root)
          }
          benchmark.addCase("on-heap scalar copy") { _ =>
            writeBatch(onHeap, bulkCopy = false, root)
          }
          benchmark.addCase("off-heap optimized path") { _ =>
            writeBatch(offHeap, bulkCopy = true, root)
          }
          benchmark.addCase("off-heap scalar copy") { _ =>
            writeBatch(offHeap, bulkCopy = false, root)
          }
          benchmark.addCase("on-heap, 50% nulls") { _ =>
            writeBatch(nullable, bulkCopy = true, root)
          }
          benchmark.run()
        } finally {
          root.close()
          onHeap.close()
          offHeap.close()
          nullable.close()
        }
      }

      val numRows = 8192
      val projection = UnsafeProjection.create(schema)
      val row = projection(new GenericInternalRow(Array[Any](1, -1L, 1.5d))).copy()
      val nullRow = projection(new GenericInternalRow(Array[Any](null, null, null))).copy()
      val noNullRowReader =
        new RowArrowReader(allocator, arrowSchema, Iterator.continually(row), numRows)
      val nullableRowReader = new RowArrowReader(
        allocator,
        arrowSchema,
        Iterator.from(0).map(i => if ((i & 1) == 0) nullRow else row),
        numRows)
      val rowBenchmark = new Benchmark("Spark rows to Arrow", numRows.toLong, output = output)
      try {
        rowBenchmark.addCase("fixed-width, no nulls") { _ => noNullRowReader.loadNextBatch() }
        rowBenchmark.addCase("fixed-width, 50% nulls") { _ => nullableRowReader.loadNextBatch() }
        rowBenchmark.run()
      } finally {
        noNullRowReader.close()
        nullableRowReader.close()
      }

      // One column per case, every tenth row null, laid out as Spark's readers lay it out.
      val typeCases: Seq[(String, DataType, Boolean)] = Seq(
        ("int", IntegerType, false),
        ("int, dictionary-encoded", IntegerType, true),
        ("boolean", BooleanType, false),
        ("decimal(9,2)", DecimalType(9, 2), false),
        ("decimal(18,2)", DecimalType(18, 2), false),
        ("decimal(38,10)", DecimalType(38, 10), false),
        ("string", StringType, false),
        ("string, dictionary-encoded", StringType, true),
        (
          "struct<int,long,double,date>",
          new StructType()
            .add("i", IntegerType)
            .add("l", LongType)
            .add("d", DoubleType)
            .add("dt", DateType),
          false),
        ("array<int>", ArrayType(IntegerType), false),
        ("array<string>", ArrayType(StringType), false),
        ("map<string,string>", MapType(StringType, StringType), false))
      val columnarByType = new Benchmark(
        s"Spark columnar to Arrow by type ($numRows rows)",
        numRows.toLong,
        output = output)
      val rowsByType =
        new Benchmark(
          s"Spark rows to Arrow by type ($numRows rows)",
          numRows.toLong,
          output = output)
      val resources = ArrayBuffer.empty[AutoCloseable]
      try {
        typeCases.foreach { case (name, dataType, dictionary) =>
          val schema = new StructType().add("c", dataType)
          val input = new ColumnarBatch(
            Array[ColumnVector](typedColumn(numRows, dataType, dictionary)),
            numRows)
          resources += input
          val columnarRoot =
            VectorSchemaRoot.create(Utils.toArrowSchema(schema, "UTC"), allocator)
          resources += columnarRoot
          columnarByType.addCase(name) { _ =>
            val writer = ArrowWriter.create(columnarRoot, numRows)
            writer.writeColumns(input, 0, numRows)
            writer.finish()
          }
          if (!dictionary) {
            val projection = UnsafeProjection.create(schema)
            val rows: Array[InternalRow] =
              Array.tabulate(numRows)(i => projection(input.getRow(i)).copy())
            val rowRoot = VectorSchemaRoot.create(Utils.toArrowSchema(schema, "UTC"), allocator)
            resources += rowRoot
            rowsByType.addCase(name) { _ =>
              val writer = ArrowWriter.create(rowRoot, numRows)
              var i = 0
              while (i < numRows) {
                writer.write(rows(i))
                i += 1
              }
              writer.finish()
            }
          }
        }
        columnarByType.run()
        rowsByType.run()
      } finally {
        resources.reverse.foreach(_.close())
      }
    } finally {
      allocator.close()
    }
  }

  /**
   * One column of `dataType` with every tenth row null. Dictionary-encoded columns draw from 64
   * values. Arrays and maps leave a child slot for each null or empty collection, as Spark's
   * nested Parquet reader does.
   */
  private def typedColumn(
      numRows: Int,
      dataType: DataType,
      dictionary: Boolean): WritableColumnVector = {
    val vector = new OnHeapColumnVector(numRows, dataType)
    if (dictionary) {
      vector.setDictionary(new Dictionary {
        override def decodeToInt(id: Int): Int = id * 7919
        override def decodeToLong(id: Int): Long = id * 7919L
        override def decodeToFloat(id: Int): Float = id.toFloat
        override def decodeToDouble(id: Int): Double = id.toDouble
        override def decodeToBinary(id: Int): Array[Byte] = s"dictionary-$id".getBytes(UTF_8)
      })
      val ids = vector.reserveDictionaryIds(numRows)
      var i = 0
      while (i < numRows) {
        if (i % 10 == 0) vector.putNull(i) else ids.putInt(i, i % 64)
        i += 1
      }
    } else {
      fill(vector, dataType, numRows, withNulls = true)
    }
    vector
  }

  private def fill(
      vector: WritableColumnVector,
      dataType: DataType,
      numRows: Int,
      withNulls: Boolean): Unit = dataType match {
    case struct: StructType =>
      struct.fields.indices.foreach { f =>
        fill(vector.getChild(f), struct.fields(f).dataType, numRows, withNulls)
      }
      // Spark's Parquet reader leaves the fields of a null struct null.
      (0 until numRows by 10).foreach { i =>
        vector.putNull(i)
        struct.fields.indices.foreach(f => vector.getChild(f).putNull(i))
      }
    case _: ArrayType | _: MapType =>
      val children = dataType match {
        case _: ArrayType => Seq(vector.arrayData())
        case _ => Seq(vector.getChild(0), vector.getChild(1))
      }
      children.foreach(_.reserve(numRows * 6))
      var offset = 0
      var i = 0
      while (i < numRows) {
        val length = i % 6
        if (i % 10 == 0) {
          vector.putNull(i)
        } else {
          vector.putArray(i, offset, length)
        }
        offset += math.max(if (i % 10 == 0) 0 else length, 1)
        i += 1
      }
      dataType match {
        case ArrayType(elementType, _) => fill(vector.arrayData(), elementType, offset, withNulls)
        case MapType(keyType, valueType, _) =>
          fill(vector.getChild(0), keyType, offset, withNulls = false)
          fill(vector.getChild(1), valueType, offset, withNulls)
      }
    case _ =>
      var i = 0
      while (i < numRows) {
        if (withNulls && i % 10 == 0) {
          vector.putNull(i)
        } else {
          dataType match {
            case BooleanType => vector.putBoolean(i, i % 3 == 0)
            case IntegerType | DateType => vector.putInt(i, i * 7919)
            case LongType => vector.putLong(i, i * 7919L)
            case DoubleType => vector.putDouble(i, i + 0.5d)
            case StringType =>
              vector.putByteArray(i, s"value-${i * 31 % 100003}".getBytes(UTF_8))
            case dt: DecimalType =>
              // Past 18 digits, use values too large for a long.
              val unscaled = BigInteger.valueOf(i * 7919L % 1000000000L)
              val value =
                if (dt.precision <= 18) unscaled else unscaled.multiply(BigInteger.TEN.pow(20))
              vector.putDecimal(
                i,
                Decimal(new JavaBigDecimal(value, dt.scale), dt.precision, dt.scale),
                dt.precision)
          }
        }
        i += 1
      }
  }
}
