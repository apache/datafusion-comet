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
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

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
    val numRows = 4096
    val schema = StructType(
      Seq(
        StructField("left", LongType, nullable = true),
        StructField("right", LongType, nullable = true)))
    val arrowSchema = Utils.toArrowSchema(schema, "UTC")
    val allocator = new RootAllocator(Long.MaxValue)

    def batch(nullEveryOtherRow: Boolean): ColumnarBatch = {
      val left = new OnHeapColumnVector(numRows, LongType)
      val right = new OnHeapColumnVector(numRows, LongType)
      var i = 0
      while (i < numRows) {
        if (nullEveryOtherRow && (i & 1) == 0) {
          left.putNull(i)
          right.putNull(i)
        } else {
          left.putLong(i, i.toLong)
          right.putLong(i, -i.toLong)
        }
        i += 1
      }
      new ColumnarBatch(Array(left, right), numRows)
    }

    val noNullBatch = batch(nullEveryOtherRow = false)
    val nullableBatch = batch(nullEveryOtherRow = true)
    val noNullReader = new SparkColumnarArrowReader(
      allocator,
      arrowSchema,
      Iterator.continually(noNullBatch),
      numRows)
    val nullableReader = new SparkColumnarArrowReader(
      allocator,
      arrowSchema,
      Iterator.continually(nullableBatch),
      numRows)
    val projection = UnsafeProjection.create(schema)
    val row = projection(new GenericInternalRow(Array[Any](1L, -1L))).copy()
    val nullRow = projection(new GenericInternalRow(Array[Any](null, null))).copy()
    val noNullRowReader =
      new RowArrowReader(allocator, arrowSchema, Iterator.continually(row), numRows)
    val nullableRowReader = new RowArrowReader(
      allocator,
      arrowSchema,
      Iterator.from(0).map(i => if ((i & 1) == 0) nullRow else row),
      numRows)

    try {
      val benchmark = new Benchmark("Spark columnar to Arrow", numRows, output = output)
      benchmark.addCase("fixed-width, no nulls") { _ =>
        noNullReader.loadNextBatch()
      }
      benchmark.addCase("fixed-width, 50% nulls") { _ =>
        nullableReader.loadNextBatch()
      }
      benchmark.run()

      val rowBenchmark = new Benchmark("Spark rows to Arrow", numRows, output = output)
      rowBenchmark.addCase("fixed-width, no nulls") { _ =>
        noNullRowReader.loadNextBatch()
      }
      rowBenchmark.addCase("fixed-width, 50% nulls") { _ =>
        nullableRowReader.loadNextBatch()
      }
      rowBenchmark.run()
    } finally {
      noNullReader.close()
      nullableReader.close()
      noNullRowReader.close()
      nullableRowReader.close()
      noNullBatch.close()
      nullableBatch.close()
      allocator.close()
    }
  }
}
