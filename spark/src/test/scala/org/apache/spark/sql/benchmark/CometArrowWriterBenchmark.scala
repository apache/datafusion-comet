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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch, ColumnVector}

/**
 * Benchmark Spark row/columnar-to-Arrow conversion for fixed-width vectors.
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
      writer.finish(input.numRows())
    }

    try {
      Seq(1, 8, 32, 512, 8192).foreach { numRows =>
        val onHeap = batch(numRows, offHeap = false, nullEveryOtherRow = false)
        val offHeap = batch(numRows, offHeap = true, nullEveryOtherRow = false)
        val nullable = batch(numRows, offHeap = false, nullEveryOtherRow = true)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        try {
          val benchmark =
            new Benchmark(s"Spark columnar to Arrow ($numRows rows)", numRows, output = output)
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
          benchmark.addCase("nullable fallback") { _ =>
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
      val rowBenchmark = new Benchmark("Spark rows to Arrow", numRows, output = output)
      try {
        rowBenchmark.addCase("fixed-width, no nulls") { _ => noNullRowReader.loadNextBatch() }
        rowBenchmark.addCase("fixed-width, 50% nulls") { _ => nullableRowReader.loadNextBatch() }
        rowBenchmark.run()
      } finally {
        noNullRowReader.close()
        nullableRowReader.close()
      }
    } finally {
      allocator.close()
    }
  }
}
