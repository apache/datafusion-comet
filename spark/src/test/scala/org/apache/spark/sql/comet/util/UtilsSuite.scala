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

package org.apache.spark.sql.comet.util

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class UtilsSuite extends CometTestBase {

  test("serializeBatches preserves row count for a zero-column batch") {
    val numRows = 5
    val batch = new ColumnarBatch(Array.empty[ColumnVector], numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    val decoded = Utils.decodeBatches(buf, "test").toSeq
    assert(decoded.map(_.numRows()).sum == numRows)
  }

  test("coalesceBroadcastBatches preserves row count across zero-column inputs") {
    val numRows = 5
    val numBatches = 3
    val batches =
      (0 until numBatches).map(_ => new ColumnarBatch(Array.empty[ColumnVector], numRows))

    val bufs = Utils.serializeBatches(batches.iterator).map(_._2).toSeq.iterator
    val (coalesced, batchCount, totalRows) = Utils.coalesceBroadcastBatches(bufs)

    val expected = numRows.toLong * numBatches
    assert(batchCount == numBatches)
    assert(totalRows == expected)

    val decoded = coalesced.iterator.flatMap(b => Utils.decodeBatches(b, "test")).toSeq
    assert(decoded.map(_.numRows()).sum == expected)
  }

  test("serializeBatches materializes ConstantColumnVector columns") {
    // Spark wraps file-source partition columns and other per-batch constants in
    // ConstantColumnVector. When such a batch reaches Comet's serialization/export path
    // (getBatchFieldVectors), it must be materialized to an Arrow vector rather than
    // rejected with "Comet execution only takes Arrow Arrays".
    val numRows = 4

    val valueCol = new ConstantColumnVector(numRows, IntegerType)
    valueCol.setInt(42)
    val nullCol = new ConstantColumnVector(numRows, IntegerType)
    nullCol.setNull()
    val batch = new ColumnarBatch(Array[ColumnVector](valueCol, nullCol), numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    // Read the decoded values eagerly: ArrowReaderIterator releases a batch's buffers once the
    // iterator advances past it (hasNext closes the previous batch), so values must be read from
    // the current batch before calling hasNext/next again.
    val it = Utils.decodeBatches(buf, "test")
    assert(it.hasNext)
    val out = it.next()
    assert(out.numCols() == 2)
    assert(out.numRows() == numRows)
    val values = (0 until numRows).map(i => out.column(0).getInt(i))
    val nulls = (0 until numRows).map(i => out.column(1).isNullAt(i))
    assert(!it.hasNext)

    assert(values.forall(_ == 42), s"expected all 42, got $values")
    assert(nulls.forall(identity), s"expected all null, got $nulls")
  }
}
