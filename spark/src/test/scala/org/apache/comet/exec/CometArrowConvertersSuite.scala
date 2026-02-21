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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VarCharVector}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.ArrowCDataExport
import org.apache.spark.sql.comet.execution.arrow.CometArrowConverters
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.{CDataUtil, CometVector}

class CometArrowConvertersSuite extends CometTestBase {

  test("zero-copy import via ArrowCDataExport and CDataUtil.importBatch") {
    val srcAllocator = new RootAllocator(Long.MaxValue)
    val cometAllocator =
      CometArrowAllocator.newChildAllocator("test-zero-copy", 0, Long.MaxValue)
    try {
      val intVector = new IntVector("intCol", srcAllocator)
      intVector.allocateNew(3)
      intVector.set(0, 10)
      intVector.set(1, 20)
      intVector.setNull(2)
      intVector.setValueCount(3)

      val varcharVector = new VarCharVector("strCol", srcAllocator)
      varcharVector.allocateNew()
      varcharVector.setSafe(0, "hello".getBytes)
      varcharVector.setSafe(1, "world".getBytes)
      varcharVector.setNull(2)
      varcharVector.setValueCount(3)

      val arrowCol0 = new ArrowColumnVector(intVector)
      val arrowCol1 = new ArrowColumnVector(varcharVector)
      val inputBatch = new ColumnarBatch(Array(arrowCol0, arrowCol1), 3)

      val exportFn = ArrowCDataExport.makeExportFn(inputBatch)
      assert(exportFn.isDefined, "Should detect ArrowColumnVector and return Some")

      val outputBatch = CDataUtil.importBatch(2, 3, cometAllocator, exportFn.get)
      assert(outputBatch.numRows() == 3)
      assert(outputBatch.numCols() == 2)
      assert(outputBatch.column(0).isInstanceOf[CometVector])
      assert(outputBatch.column(1).isInstanceOf[CometVector])

      assert(outputBatch.column(0).getInt(0) == 10)
      assert(outputBatch.column(0).getInt(1) == 20)
      assert(outputBatch.column(0).isNullAt(2))
      assert(outputBatch.column(1).getUTF8String(0).toString == "hello")
      assert(outputBatch.column(1).getUTF8String(1).toString == "world")
      assert(outputBatch.column(1).isNullAt(2))

      outputBatch.close()
      inputBatch.close()
    } finally {
      cometAllocator.close()
      srcAllocator.close()
    }
  }

  test("ArrowCDataExport returns None for non-Arrow batches") {
    val sparkCol = new OnHeapColumnVector(10, IntegerType)
    val batch = new ColumnarBatch(Array(sparkCol), 10)

    try {
      val result = ArrowCDataExport.makeExportFn(batch)
      assert(result.isEmpty, "Should return None for non-ArrowColumnVector batches")
    } finally {
      batch.close()
    }
  }

  test("columnarBatchToArrowBatchIter works for ArrowColumnVector input") {
    val srcAllocator = new RootAllocator(Long.MaxValue)
    try {
      val intVector = new IntVector("intCol", srcAllocator)
      intVector.allocateNew(3)
      intVector.set(0, 10)
      intVector.set(1, 20)
      intVector.setNull(2)
      intVector.setValueCount(3)

      val varcharVector = new VarCharVector("strCol", srcAllocator)
      varcharVector.allocateNew()
      varcharVector.setSafe(0, "hello".getBytes)
      varcharVector.setSafe(1, "world".getBytes)
      varcharVector.setNull(2)
      varcharVector.setValueCount(3)

      val arrowCol0 = new ArrowColumnVector(intVector)
      val arrowCol1 = new ArrowColumnVector(varcharVector)
      val batch = new ColumnarBatch(Array(arrowCol0, arrowCol1), 3)
      val schema =
        StructType(Seq(StructField("intCol", IntegerType), StructField("strCol", StringType)))

      val exportFn = ArrowCDataExport.makeExportFn(batch)
      val iter = CometArrowConverters.columnarBatchToArrowBatchIter(
        batch,
        schema,
        maxRecordsPerBatch = 0,
        "UTC",
        context = null,
        exportFn)

      assert(iter.hasNext)
      val outputBatch = iter.next()
      assert(outputBatch.numRows() == 3)
      assert(outputBatch.numCols() == 2)

      assert(outputBatch.column(0).getInt(0) == 10)
      assert(outputBatch.column(0).getInt(1) == 20)
      assert(outputBatch.column(0).isNullAt(2))
      assert(outputBatch.column(1).getUTF8String(0).toString == "hello")
      assert(outputBatch.column(1).getUTF8String(1).toString == "world")
      assert(outputBatch.column(1).isNullAt(2))

      assert(!iter.hasNext)
      batch.close()
    } finally {
      srcAllocator.close()
    }
  }

  test("columnarBatchToArrowBatchIter falls back for non-Arrow batches") {
    val sparkCol = new OnHeapColumnVector(10, IntegerType)
    for (i <- 0 until 10) {
      sparkCol.putInt(i, i * 100)
    }
    val batch = new ColumnarBatch(Array(sparkCol), 10)

    val iterSchema = StructType(Seq(StructField("col", IntegerType)))
    val iter = CometArrowConverters.columnarBatchToArrowBatchIter(
      batch,
      iterSchema,
      maxRecordsPerBatch = 0,
      "UTC",
      context = null)

    assert(iter.hasNext)
    val outputBatch = iter.next()
    assert(outputBatch.numRows() == 10)
    assert(outputBatch.column(0).getInt(0) == 0)
    assert(outputBatch.column(0).getInt(9) == 900)

    assert(!iter.hasNext)
    batch.close()
  }
}
