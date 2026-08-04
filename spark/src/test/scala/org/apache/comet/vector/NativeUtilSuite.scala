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

package org.apache.comet.vector

import org.apache.arrow.c.{ArrowArray, ArrowSchema}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class NativeUtilSuite extends CometTestBase {

  test("exportBatch round-trips a ConstantColumnVector through Arrow FFI") {
    // Smoke test for the ConstantColumnVector arm of NativeUtil.exportBatch: a batch carrying
    // Spark ConstantColumnVectors (partition values / per-batch constants) is exported across the
    // Arrow C Data Interface and imported back, exercising materializeConstantColumnVector +
    // Data.exportVector + the allocator handoff -- the FFI wiring that the serializeBatches test
    // does not cover. Mirrors the export/import round trip that NativeUtil.getNextBatch performs
    // in production, just without a native callee.
    val numRows = 4

    val valueCol = new ConstantColumnVector(numRows, IntegerType)
    valueCol.setInt(42)
    val nullCol = new ConstantColumnVector(numRows, IntegerType)
    nullCol.setNull()

    // A struct constant exercises the complex-type export path (getStruct/getChild) through FFI.
    val structSchema = StructType(
      Seq(StructField("id", IntegerType), StructField("name", StringType, nullable = true)))
    val structCol = new ConstantColumnVector(numRows, structSchema)
    structCol.setNotNull()
    val idChild = new ConstantColumnVector(numRows, IntegerType)
    idChild.setInt(7)
    val nameChild = new ConstantColumnVector(numRows, StringType)
    nameChild.setNull()
    structCol.setChild(0, idChild)
    structCol.setChild(1, nameChild)

    val batch =
      new ColumnarBatch(Array[ColumnVector](valueCol, nullCol, structCol), numRows)

    val nativeUtil = new NativeUtil
    var imported: ColumnarBatch = null
    try {
      val (arrayAddrs, schemaAddrs, exportedRows) = nativeUtil.exportBatchToAddresses(batch)
      assert(exportedRows == numRows)

      val arrays = arrayAddrs.map(ArrowArray.wrap)
      val schemas = schemaAddrs.map(ArrowSchema.wrap)
      val vectors = nativeUtil.importVector(arrays, schemas)
      imported = new ColumnarBatch(vectors.toArray, numRows)

      assert(imported.numCols() == 3)
      assert(imported.numRows() == numRows)

      val values = (0 until numRows).map(i => imported.column(0).getInt(i))
      assert(values.forall(_ == 42), s"expected all 42, got $values")

      val nulls = (0 until numRows).map(i => imported.column(1).isNullAt(i))
      assert(nulls.forall(identity), s"expected all null, got $nulls")

      val ids = (0 until numRows).map(i => imported.column(2).getStruct(i).getInt(0))
      assert(ids.forall(_ == 7), s"expected all id 7, got $ids")
      val nameNulls = (0 until numRows).map(i => imported.column(2).getStruct(i).isNullAt(1))
      assert(nameNulls.forall(identity), s"expected all name null, got $nameNulls")
    } finally {
      if (imported != null) {
        imported.close()
      }
      nativeUtil.close()
    }
  }
}
