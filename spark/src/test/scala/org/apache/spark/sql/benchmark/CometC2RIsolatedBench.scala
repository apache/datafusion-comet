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
 *
 * The schema scenarios exist because the two implementations have very different per-value costs
 * by data type. The JVM path allocates a Decimal object per non-null decimal value (and a
 * BigInteger/BigDecimal chain for precision > 18), while the native path writes the unscaled
 * value directly. The native path also has a column-wise fast path that only engages when every
 * column in the schema is fixed-width.
 */
object CometC2RIsolatedBench {

  private val totalRows = 1024 * 1024

  private case class Scenario(
      name: String,
      schema: StructType,
      makeRow: Int => InternalRow,
      consume: InternalRow => Long)

  private val mixedPrimitives = Scenario(
    "long, int, double, string",
    new StructType()
      .add("a", LongType)
      .add("b", IntegerType)
      .add("c", DoubleType)
      .add("d", StringType),
    i => InternalRow(i.toLong, i, i.toDouble, UTF8String.fromString(s"value_$i")),
    row => row.getLong(0) + row.getUTF8String(3).numBytes())

  // The columns a TPC-H lineitem aggregation reaches C2R with: four compact decimals, a date,
  // and two short flag strings. The strings force the native general (row-at-a-time) path.
  private val tpchDecimalsWithStrings = Scenario(
    "4 x decimal(12,2), date, 2 x string (tpch-like)",
    new StructType()
      .add("l_quantity", DecimalType(12, 2))
      .add("l_extendedprice", DecimalType(12, 2))
      .add("l_discount", DecimalType(12, 2))
      .add("l_tax", DecimalType(12, 2))
      .add("l_shipdate", DateType)
      .add("l_returnflag", StringType)
      .add("l_linestatus", StringType),
    i =>
      InternalRow(
        Decimal.createUnsafe(i % 5000000, 12, 2),
        Decimal.createUnsafe(i % 9000000, 12, 2),
        Decimal.createUnsafe(i % 10, 12, 2),
        Decimal.createUnsafe(i % 8, 12, 2),
        8000 + i % 2500,
        UTF8String.fromString(if (i % 2 == 0) "A" else "R"),
        UTF8String.fromString(if (i % 3 == 0) "F" else "O")),
    row => row.getDecimal(0, 12, 2).toUnscaledLong + row.getInt(4))

  // Same decimals without any var-length column, so the native column-wise fast path engages.
  private val decimalsAllFixedWidth = Scenario(
    "4 x decimal(12,2), date, long (all fixed-width)",
    new StructType()
      .add("a", DecimalType(12, 2))
      .add("b", DecimalType(12, 2))
      .add("c", DecimalType(12, 2))
      .add("d", DecimalType(12, 2))
      .add("e", DateType)
      .add("f", LongType),
    i =>
      InternalRow(
        Decimal.createUnsafe(i % 5000000, 12, 2),
        Decimal.createUnsafe(i % 9000000, 12, 2),
        Decimal.createUnsafe(i % 10, 12, 2),
        Decimal.createUnsafe(i % 8, 12, 2),
        8000 + i % 2500,
        i.toLong),
    row => row.getDecimal(0, 12, 2).toUnscaledLong + row.getLong(5))

  // Precision > 18: the JVM accessor allocates byte[] -> BigInteger -> java BigDecimal ->
  // scala BigDecimal -> Decimal per value; the native side writes 16 bytes.
  private val highPrecisionDecimals = Scenario(
    "2 x decimal(38,10), long (high precision)",
    new StructType()
      .add("a", DecimalType(38, 10))
      .add("b", DecimalType(38, 10))
      .add("c", LongType),
    i =>
      InternalRow(
        Decimal(new java.math.BigDecimal(java.math.BigInteger.valueOf(i.toLong * 1000003), 10)),
        Decimal(new java.math.BigDecimal(java.math.BigInteger.valueOf(i.toLong * 999983), 10)),
        i.toLong),
    // Consume only the cheap column: reading a decimal(38,10) back out of the UnsafeRow
    // allocates on both sides and would mask the conversion cost being measured.
    row => row.getLong(2))

  private val widePrimitives = Scenario(
    "16 x long (wide primitives)",
    (0 until 16).foldLeft(new StructType())((s, i) => s.add(s"c$i", LongType)),
    i => InternalRow((0 until 16).map(c => i.toLong + c): _*),
    row => row.getLong(0) + row.getLong(15))

  private def makeBatches(scenario: Scenario, batchSize: Int): Array[ColumnarBatch] = {
    val rows = (0 until totalRows).iterator.map(scenario.makeRow)
    CometArrowConverters
      .rowToArrowBatchIter(
        rows,
        scenario.schema,
        batchSize,
        "UTC",
        org.apache.comet.CometArrowAllocator)
      .toArray
  }

  /** Bytes allocated on the JVM heap by the current thread while running `body`. */
  private def measureAllocatedBytes(body: => Unit): Long = {
    val bean = java.lang.management.ManagementFactory.getThreadMXBean
      .asInstanceOf[com.sun.management.ThreadMXBean]
    val tid = Thread.currentThread().getId
    val before = bean.getThreadAllocatedBytes(tid)
    body
    bean.getThreadAllocatedBytes(tid) - before
  }

  private def runForBatchSize(scenario: Scenario, batchSize: Int): Unit = {
    val batches = makeBatches(scenario, batchSize)

    val benchmark =
      new Benchmark(
        s"Isolated C2R (no scan), ${scenario.name}, batchSize=$batchSize",
        totalRows.toLong)

    benchmark.addCase("JVM rowIterator + UnsafeProjection") { _ =>
      val proj = UnsafeProjection.create(scenario.schema.fields.map(_.dataType))
      var sink = 0L
      var b = 0
      while (b < batches.length) {
        val it = batches(b).rowIterator()
        while (it.hasNext) {
          val u = proj(it.next())
          sink += scenario.consume(u)
        }
        b += 1
      }
      if (sink == Long.MinValue) println(sink)
    }

    benchmark.addCase("Native converter") { _ =>
      val converter = new NativeColumnarToRowConverter(scenario.schema, batchSize)
      var sink = 0L
      try {
        var b = 0
        while (b < batches.length) {
          val it = converter.convert(batches(b))
          while (it.hasNext) {
            sink += scenario.consume(it.next())
          }
          b += 1
        }
      } finally {
        converter.close()
      }
      if (sink == Long.MinValue) println(sink)
    }

    benchmark.run()

    // GC pressure comparison: the JVM path allocates per object-typed value (Decimal,
    // UTF8String) but reuses its output row buffer, while the native path allocates a
    // byte[] + UnsafeRow per row for the defensive copy in NativeRowIterator.
    val jvmAlloc = measureAllocatedBytes {
      val proj = UnsafeProjection.create(scenario.schema.fields.map(_.dataType))
      var sink = 0L
      for (batch <- batches) {
        val it = batch.rowIterator()
        while (it.hasNext) {
          sink += scenario.consume(proj(it.next()))
        }
      }
      if (sink == Long.MinValue) println(sink)
    }
    val nativeAlloc = measureAllocatedBytes {
      val converter = new NativeColumnarToRowConverter(scenario.schema, batchSize)
      var sink = 0L
      try {
        for (batch <- batches) {
          val it = converter.convert(batch)
          while (it.hasNext) {
            sink += scenario.consume(it.next())
          }
        }
      } finally {
        converter.close()
      }
      if (sink == Long.MinValue) println(sink)
    }
    println(
      f"JVM heap allocation per row: JVM path ${jvmAlloc.toDouble / totalRows}%.1f bytes, " +
        f"native path ${nativeAlloc.toDouble / totalRows}%.1f bytes%n")

    batches.foreach(_.close())
  }

  def main(args: Array[String]): Unit = {
    val scenarios =
      Seq(
        mixedPrimitives,
        tpchDecimalsWithStrings,
        decimalsAllFixedWidth,
        highPrecisionDecimals,
        widePrimitives)

    for (scenario <- scenarios; batchSize <- Seq(8192, 512, 32)) {
      runForBatchSize(scenario, batchSize)
    }
  }
}
