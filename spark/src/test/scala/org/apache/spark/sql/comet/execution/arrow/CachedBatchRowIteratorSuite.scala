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

import java.nio.charset.StandardCharsets.UTF_8

import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, InterpretedUnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.vector.CometPlainVector

class CachedBatchRowIteratorSuite extends AnyFunSuite {
  Seq("CODEGEN_ONLY", "NO_CODEGEN").foreach { mode =>
    def withMode(settings: (String, String)*)(f: => Unit): Unit = {
      val conf = new SQLConf
      conf.setConfString(SQLConf.CODEGEN_FACTORY_MODE.key, mode)
      settings.foreach { case (key, value) => conf.setConfString(key, value) }
      SQLConf.withExistingConf(conf)(f)
    }

    test(s"$mode: rows own Arrow values across batch release and reuse the output buffer") {
      withMode() {
        val allocator = new RootAllocator(Long.MaxValue)
        val vectors = Seq(Seq("first", null), Seq("字" * 1000, "last")).map { values =>
          val vector = new VarCharVector("s", allocator)
          values.zipWithIndex.foreach { case (value, i) =>
            if (value == null) vector.setNull(i) else vector.setSafe(i, value.getBytes(UTF_8))
          }
          vector.setValueCount(values.size)
          vector
        }
        try {
          // Match the cache decoder: hasNext releases a consumed batch before the next is read.
          val batches = vectors.iterator.flatMap { vector =>
            new Iterator[ColumnarBatch] {
              private var emitted = false
              override def hasNext: Boolean = {
                if (emitted) vector.close()
                !emitted
              }
              override def next(): ColumnarBatch = {
                emitted = true
                new ColumnarBatch(Array(new CometPlainVector(vector, false)), 2)
              }
            }
          }
          val attributes = Seq(AttributeReference("s", StringType, nullable = true)())
          val rows = new CachedBatchRowIterator(attributes).createObject(batches)
          assert(rows.hasNext && rows.hasNext)
          val first = rows.next().asInstanceOf[UnsafeRow]
          val saved = first.copy()
          assert(first.getUTF8String(0).toString == "first")
          assert(rows.next() eq first)
          assert(first.isNullAt(0))
          assert(rows.hasNext && rows.hasNext)
          assert(first.isNullAt(0))
          assert(rows.next().getUTF8String(0).toString == "字" * 1000)
          val last = rows.next()
          assert(!rows.hasNext && !rows.hasNext)
          assert(allocator.getAllocatedMemory == 0)
          assert(last.getUTF8String(0).toString == "last")
          assert(saved.getUTF8String(0).toString == "first")
          intercept[NoSuchElementException](rows.next())
        } finally {
          vectors.foreach(_.close())
          allocator.close()
        }
      }
    }

    test(s"$mode: empty input, empty batches, and zero-column rows") {
      withMode() {
        val factory = new CachedBatchRowIterator(Seq.empty)
        val empty = factory.createObject(Iterator.empty)
        assert(!empty.hasNext)
        intercept[NoSuchElementException](empty.next())
        val batches = Seq(0, 2, 0, 3, 0).map { n =>
          new ColumnarBatch(Array.empty[ColumnVector], n)
        }
        val rows = factory.createObject(batches.iterator)
        assert(rows.map { row =>
          assert(row.isInstanceOf[UnsafeRow] && row.numFields == 0)
          1
        }.sum == 5)
        intercept[NoSuchElementException](rows.next())
      }
    }

    // Before the generated reader let GenerateUnsafeProjection split its writer, next() passed
    // HotSpot's 8000-byte JIT limit near 100 columns and Janino's 64 KB method limit near 1500.
    // The generated reader is only kept when none of its methods is above the former.
    val reader = if (mode == "CODEGEN_ONLY") "the generated reader" else "the interpreted reader"
    Seq(100, 200, 1500).foreach { width =>
      test(s"$mode: $width-column projections use $reader") {
        withMode() {
          val (attributes, batches) = wideInput(width)
          try {
            val rows = new CachedBatchRowIterator(attributes).createObject(batches.iterator)
            rows match {
              case projected: ProjectedRows =>
                assert(mode == "NO_CODEGEN", "The generated reader has a method too large to JIT")
                assert(projected.projection.isInstanceOf[InterpretedUnsafeProjection])
              case _ => assert(mode == "CODEGEN_ONLY")
            }
            checkWideRows(attributes, rows)
          } finally batches.foreach(_.close())
        }
      }
    }

    test(
      s"$mode: a generated method above the huge-method limit falls back to UnsafeProjection") {
      // Below any generated method, as if the reader had grown past HotSpot's limit.
      withMode(SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key -> "1") {
        val (attributes, batches) = wideInput(200)
        try {
          val rows = new CachedBatchRowIterator(attributes).createObject(batches.iterator)
          val projection = rows.asInstanceOf[ProjectedRows].projection
          assert(projection.isInstanceOf[InterpretedUnsafeProjection] == (mode == "NO_CODEGEN"))
          checkWideRows(attributes, rows)
        } finally batches.foreach(_.close())
      }
    }
  }

  private val wideTypes = Seq(IntegerType, LongType, StringType)

  /**
   * Nullable and required int, bigint and string columns, two rows in each of two batches, so
   * that every column is bound again at the batch boundary. Odd rows are null where a column
   * allows it.
   */
  private def wideInput(width: Int): (Seq[AttributeReference], Seq[ColumnarBatch]) = {
    val attributes = (0 until width).map { i =>
      AttributeReference(s"c$i", wideTypes(i % 3), nullable = i % 2 == 0)()
    }
    val batches = Seq(0, 2).map { firstRow =>
      val columns = attributes.zipWithIndex.map { case (attr, i) =>
        val column = new OnHeapColumnVector(2, attr.dataType)
        Seq(0, 1).foreach { r =>
          val row = firstRow + r
          if (attr.nullable && row % 2 == 1) {
            column.putNull(r)
          } else {
            attr.dataType match {
              case IntegerType => column.putInt(r, wideValue(i, row).asInstanceOf[Int])
              case LongType => column.putLong(r, wideValue(i, row).asInstanceOf[Long])
              case _ => column.putByteArray(r, wideValue(i, row).toString.getBytes(UTF_8))
            }
          }
        }
        column
      }
      new ColumnarBatch(columns.toArray[ColumnVector], 2)
    }
    (attributes, batches)
  }

  private def wideValue(column: Int, row: Int): Any = wideTypes(column % 3) match {
    case IntegerType => column * 10 + row
    case LongType => column * 10000000000L + row
    case _ => s"$column:$row"
  }

  private def checkWideRows(
      attributes: Seq[AttributeReference],
      rows: Iterator[InternalRow]): Unit = {
    (0 until 4).foreach { row =>
      assert(rows.hasNext)
      val actual = rows.next()
      assert(actual.numFields == attributes.size)
      attributes.zipWithIndex.foreach { case (attr, i) =>
        if (attr.nullable && row % 2 == 1) {
          assert(actual.isNullAt(i), s"c$i in row $row")
        } else {
          val value = attr.dataType match {
            case IntegerType => actual.getInt(i)
            case LongType => actual.getLong(i)
            case _ => actual.getUTF8String(i).toString
          }
          assert(value == wideValue(i, row), s"c$i in row $row")
        }
      }
    }
    assert(!rows.hasNext)
  }
}
