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

package org.apache.comet.udf

import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{IntVector, ValueVector}
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.{CometArrowAllocator, CometArrowImportAllocator}
import org.apache.comet.vector.NativeUtil

/**
 * A UDF that reports which allocator owns its inputs.
 *
 * The bridge closes the imported inputs before `evaluate` returns, so this can only be observed
 * from inside the call.
 */
class ImportAllocatorProbeUdf extends CometUDF {
  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    ImportAllocatorProbeUdf.inputAllocator = Some(inputs.head.getAllocator)

    // Allocated from the root: a UDF result is memory the JVM allocated.
    val out = new IntVector("out", CometArrowAllocator)
    out.allocateNew(numRows)
    (0 until numRows).foreach(row => out.setSafe(row, 7))
    out.setValueCount(numRows)
    out
  }
}

object ImportAllocatorProbeUdf {
  @volatile var inputAllocator: Option[BufferAllocator] = None
}

class CometUdfBridgeSuite extends AnyFunSuite {

  test("evaluate imports its input vectors against the FFI import allocator") {
    val numRows = 4
    val col = new ConstantColumnVector(numRows, IntegerType)
    col.setInt(42)
    val batch = new ColumnarBatch(Array[ColumnVector](col), numRows)

    val nativeUtil = new NativeUtil
    try {
      val (inputArrayAddrs, inputSchemaAddrs, _) = nativeUtil.exportBatchToAddresses(batch)
      val (outArrays, outSchemas) = nativeUtil.allocateArrowStructs(1)

      try {
        CometUdfBridge.evaluate(
          classOf[ImportAllocatorProbeUdf].getName,
          inputArrayAddrs,
          inputSchemaAddrs,
          outArrays(0).memoryAddress(),
          outSchemas(0).memoryAddress(),
          numRows,
          null,
          null)

        assert(
          ImportAllocatorProbeUdf.inputAllocator.contains(CometArrowImportAllocator),
          "the UDF's inputs were imported against " +
            s"${ImportAllocatorProbeUdf.inputAllocator.map(_.getName)}, so their bytes are not " +
            "reported as imported memory")
      } finally {
        outArrays(0).release()
        outArrays(0).close()
        outSchemas(0).release()
        outSchemas(0).close()
      }
    } finally {
      nativeUtil.close()
    }
  }
}
