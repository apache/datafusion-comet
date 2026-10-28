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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class CacheBatchCoalescerSuite extends AnyFunSuite {

  private val numColumns = 4
  private val attrs = (0 until numColumns).map(i => AttributeReference(s"s$i", StringType)())

  // A batch of Spark's own on-heap vectors, which the coalescer always copies. Row `i` of column
  // `c` holds "c<c>r<first + i>", so the output order can be checked.
  private def sparkBatch(first: Int, rows: Int): ColumnarBatch = {
    val columns = (0 until numColumns).map { c =>
      val vector = new OnHeapColumnVector(rows, StringType)
      (0 until rows).foreach(i => vector.putByteArray(i, s"c${c}r${first + i}".getBytes(UTF_8)))
      vector: ColumnVector
    }
    new ColumnarBatch(columns.toArray, rows)
  }

  // Runs `f` on a coalescer over `inputs`, checks that closing it returns every byte it took, and
  // returns the most the allocator held at once.
  private def withCoalescer(limit: Long, inputs: Seq[ColumnarBatch], target: Int)(
      f: CacheBatchCoalescer => Unit): Long = {
    val allocator = new RootAllocator(limit)
    val coalescer = new CacheBatchCoalescer(inputs.iterator, attrs, target, allocator)
    try {
      f(coalescer)
    } finally {
      coalescer.close()
    }
    assert(allocator.getAllocatedMemory == 0L)
    val peak = allocator.getPeakMemoryAllocation
    allocator.close()
    peak
  }

  test("splitting one large input copies one output batch at a time") {
    val inputRows = 4096
    val target = 10
    // The limit is far above what one output batch needs and far below what 410 of them need.
    val limit = 4L * 1024 * 1024
    val peak = withCoalescer(limit, Seq(sparkBatch(0, inputRows)), target) { coalescer =>
      var rows = 0
      var outputs = 0
      while (coalescer.hasNext) {
        val out = coalescer.next()
        assert(out.numRows() <= target)
        (0 until out.numRows()).foreach { i =>
          (0 until numColumns).foreach { c =>
            assert(out.column(c).getUTF8String(i).toString == s"c${c}r${rows + i}")
          }
        }
        rows += out.numRows()
        outputs += 1
      }
      assert(rows == inputRows)
      assert(outputs == (inputRows + target - 1) / target)
    }
    // Only the batch being emitted is open, so the allocation does not grow with the number of
    // outputs. Eagerly splitting the input would hold all of them at once.
    assert(peak < 1024 * 1024, peak)
  }

  test("rows left over from one input are filled up from the next") {
    val inputs = Seq(sparkBatch(0, 25), sparkBatch(25, 7), sparkBatch(32, 30))
    withCoalescer(Long.MaxValue, inputs, 10) { coalescer =>
      val sizes = Seq.newBuilder[Int]
      var rows = 0
      while (coalescer.hasNext) {
        val out = coalescer.next()
        (0 until out.numRows()).foreach { i =>
          assert(out.column(0).getUTF8String(i).toString == s"c0r${rows + i}")
        }
        rows += out.numRows()
        sizes += out.numRows()
      }
      assert(sizes.result() == Seq(10, 10, 10, 10, 10, 10, 2))
    }
  }

  test("an empty input batch is skipped") {
    val inputs = Seq(sparkBatch(0, 0), sparkBatch(0, 3), sparkBatch(3, 0))
    withCoalescer(Long.MaxValue, inputs, 10) { coalescer =>
      assert(coalescer.next().numRows() == 3)
      assert(!coalescer.hasNext)
    }
  }
}
