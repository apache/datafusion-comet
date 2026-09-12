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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeRow}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.vector.CometPlainVector

class CachedBatchRowIteratorSuite extends AnyFunSuite {
  Seq("CODEGEN_ONLY", "NO_CODEGEN").foreach { mode =>
    def withMode(f: => Unit): Unit = {
      val conf = new SQLConf
      conf.setConfString(SQLConf.CODEGEN_FACTORY_MODE.key, mode)
      SQLConf.withExistingConf(conf)(f)
    }

    test(s"$mode: rows own Arrow values across batch release and reuse the output buffer") {
      withMode {
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
      withMode {
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

    test(s"$mode: wide projections preserve nullable and required columns") {
      withMode {
        val attributes = (0 until 150).map { i =>
          AttributeReference(s"c$i", IntegerType, nullable = i % 2 == 0)()
        }
        val columns = attributes.indices.map { i =>
          val column = new OnHeapColumnVector(2, IntegerType)
          column.putInt(0, i)
          if (i % 2 == 0) column.putNull(1) else column.putInt(1, -i)
          column
        }
        val batch = new ColumnarBatch(columns.toArray[ColumnVector], 2)
        try {
          val rows = new CachedBatchRowIterator(attributes).createObject(Iterator.single(batch))
          val first = rows.next().copy()
          val second = rows.next()
          attributes.indices.foreach { i =>
            assert(first.getInt(i) == i)
            if (i % 2 == 0) assert(second.isNullAt(i)) else assert(second.getInt(i) == -i)
          }
          assert(!rows.hasNext)
        } finally batch.close()
      }
    }
  }
}
