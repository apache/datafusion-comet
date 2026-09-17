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

  private def makeBatches(schema: StructType, batchSize: Int): Array[ColumnarBatch] = {
    val rows = (0 until totalRows).iterator.map { i =>
      InternalRow(i.toLong, i, i.toDouble, UTF8String.fromString(s"value_$i"))
    }
    CometArrowConverters
      .rowToArrowBatchIter(rows, schema, batchSize, "UTC", org.apache.comet.CometArrowAllocator)
      .toArray
  }

  private def runForBatchSize(schema: StructType, batchSize: Int): Unit = {
    val batches = makeBatches(schema, batchSize)

    val benchmark =
      new Benchmark(s"Isolated C2R (no scan), batchSize=$batchSize", totalRows.toLong)

    benchmark.addCase("JVM rowIterator + UnsafeProjection") { _ =>
      val proj = UnsafeProjection.create(schema.fields.map(_.dataType))
      var sink = 0L
      var b = 0
      while (b < batches.length) {
        val it = batches(b).rowIterator()
        while (it.hasNext) {
          val u = proj(it.next())
          sink += u.getLong(0) + u.getUTF8String(3).numBytes()
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
            val u = it.next()
            sink += u.getLong(0) + u.getUTF8String(3).numBytes()
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

  def main(args: Array[String]): Unit = {
    val schema = new StructType()
      .add("a", LongType)
      .add("b", IntegerType)
      .add("c", DoubleType)
      .add("d", StringType)

    runForBatchSize(schema, 8192)
    runForBatchSize(schema, 512)
    runForBatchSize(schema, 32)
  }
}
