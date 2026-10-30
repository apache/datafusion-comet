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

package org.apache.comet.exec

import java.nio.charset.StandardCharsets

import org.apache.arrow.vector.{BigIntVector, VarCharVector}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.CometPlainVector

/**
 * Batches of `id`, counting from 0, and `s`, "v" followed by the id, one batch per element of
 * `sizes`: a row count, and whether the batch holds Arrow vectors or Spark's own on-heap ones.
 * Like a native producer, it releases each batch when it is asked for the next one.
 */
private class ReleasingBatches(sizes: Seq[(Int, Boolean)]) extends Iterator[ColumnarBatch] {
  private var remaining = sizes.toList
  private var nextId = 0L
  private var previous: ColumnarBatch = null

  override def hasNext: Boolean = {
    if (previous != null) {
      previous.close()
      previous = null
    }
    remaining.nonEmpty
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more batches")
    }
    val (rows, arrow) = remaining.head
    remaining = remaining.tail
    val batch = if (arrow) arrowBatch(rows) else sparkBatch(rows)
    nextId += rows
    previous = batch
    batch
  }

  private def value(i: Int): Array[Byte] = s"v${nextId + i}".getBytes(StandardCharsets.UTF_8)

  private def arrowBatch(rows: Int): ColumnarBatch = {
    val ids = new BigIntVector("id", CometArrowAllocator)
    val strings = new VarCharVector("s", CometArrowAllocator)
    ids.allocateNew(rows)
    strings.allocateNew(rows.toLong * 8, rows)
    (0 until rows).foreach { i =>
      ids.set(i, nextId + i)
      strings.setSafe(i, value(i))
    }
    ids.setValueCount(rows)
    strings.setValueCount(rows)
    new ColumnarBatch(
      Array[ColumnVector](new CometPlainVector(ids), new CometPlainVector(strings)),
      rows)
  }

  private def sparkBatch(rows: Int): ColumnarBatch = {
    val ids = new OnHeapColumnVector(rows, LongType)
    val strings = new OnHeapColumnVector(rows, StringType)
    (0 until rows).foreach { i =>
      ids.putLong(i, nextId + i)
      strings.putByteArray(i, value(i))
    }
    new ColumnarBatch(Array[ColumnVector](ids, strings), rows)
  }
}
