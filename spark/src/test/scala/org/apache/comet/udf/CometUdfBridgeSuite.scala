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

import org.apache.arrow.vector.{IntVector, ValueVector}
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.{CometArrowAllocator, CometArrowImportAllocator}
import org.apache.comet.vector.NativeUtil

/**
 * A UDF that reports how much memory the FFI import allocator holds while its inputs are alive.
 *
 * The bridge closes the imported inputs before `evaluate` returns, so the only place the
 * accounting for them can be observed is from inside the call.
 */
class ImportAllocatorProbeUdf extends CometUDF {
  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    ImportAllocatorProbeUdf.importAllocatedDuringCall =
      CometArrowImportAllocator.getAllocatedMemory

    // Allocated from the root: a UDF result really is memory the JVM allocated, and keeping it
    // off the import allocator is what makes the two counters mean different things.
    val out = new IntVector("out", CometArrowAllocator)
    out.allocateNew(numRows)
    (0 until numRows).foreach(row => out.setSafe(row, 7))
    out.setValueCount(numRows)
    out
  }
}

object ImportAllocatorProbeUdf {
  @volatile var importAllocatedDuringCall: Long = -1L
}

class CometUdfBridgeSuite extends AnyFunSuite {

  test("evaluate charges imported input vectors to the FFI import allocator") {
    val numRows = 4
    val col = new ConstantColumnVector(numRows, IntegerType)
    col.setInt(42)
    val batch = new ColumnarBatch(Array[ColumnVector](col), numRows)

    val nativeUtil = new NativeUtil
    try {
      val (inputArrayAddrs, inputSchemaAddrs, _) = nativeUtil.exportBatchToAddresses(batch)
      val (outArrays, outSchemas) = nativeUtil.allocateArrowStructs(1)

      // Read immediately before the call rather than trusting a baseline of zero: the allocator
      // is process-wide, so anything imported earlier in this JVM is already counted here.
      val importedBefore = CometArrowImportAllocator.getAllocatedMemory
      ImportAllocatorProbeUdf.importAllocatedDuringCall = -1L

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
        ImportAllocatorProbeUdf.importAllocatedDuringCall >= 0,
        "the probe UDF never ran, so the bridge call proves nothing")
      assert(
        ImportAllocatorProbeUdf.importAllocatedDuringCall > importedBefore,
        "the UDF's imported inputs were charged somewhere other than the FFI import allocator, " +
          s"which still held $importedBefore bytes while they were alive")

      outArrays(0).release()
      outArrays(0).close()
      outSchemas(0).release()
      outSchemas(0).close()
    } finally {
      nativeUtil.close()
    }
  }
}
