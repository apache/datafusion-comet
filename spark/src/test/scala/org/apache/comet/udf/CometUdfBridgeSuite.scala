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

/**
 * A UDF that allocates its output from the allocator that owns its inputs.
 *
 * The `CometUDF` interface permits this, and nothing about it is wrong from the UDF's side. It
 * matters here because that allocator is the FFI import allocator, so without intervention the
 * output would be charged to it and counted as imported memory.
 */
class InputAllocatorOutputUdf extends CometUDF {
  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    val out = new IntVector("out", inputs.head.getAllocator)
    out.allocateNew(numRows)
    (0 until numRows).foreach(row => out.setSafe(row, 7))
    out.setValueCount(numRows)
    out
  }
}

class CometUdfBridgeSuite extends AnyFunSuite {

  /** Exports a single-column batch and invokes the bridge against it. */
  private def withBridgeCall(udfClassName: String, numRows: Int)(check: () => Unit): Unit = {
    val col = new ConstantColumnVector(numRows, IntegerType)
    col.setInt(42)
    val batch = new ColumnarBatch(Array[ColumnVector](col), numRows)

    val nativeUtil = new NativeUtil
    try {
      val (inputArrayAddrs, inputSchemaAddrs, _) = nativeUtil.exportBatchToAddresses(batch)
      val (outArrays, outSchemas) = nativeUtil.allocateArrowStructs(1)

      try {
        CometUdfBridge.evaluate(
          udfClassName,
          inputArrayAddrs,
          inputSchemaAddrs,
          outArrays(0).memoryAddress(),
          outSchemas(0).memoryAddress(),
          numRows,
          null,
          null)

        // Checked before releasing the exported structs: the export keeps the result's buffers
        // alive, so whichever allocator owns them is still charged at this point.
        check()
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

  test("evaluate imports its input vectors against the FFI import allocator") {
    withBridgeCall(classOf[ImportAllocatorProbeUdf].getName, 4) { () =>
      assert(
        ImportAllocatorProbeUdf.inputAllocator.contains(CometArrowImportAllocator),
        "the UDF's inputs were imported against " +
          s"${ImportAllocatorProbeUdf.inputAllocator.map(_.getName)}, so their bytes are not " +
          "reported as imported memory")
    }
  }

  test("a UDF output allocated from the input allocator is not left charged as imported") {
    // The CometUDF interface lets a UDF allocate its result from inputs.head.getAllocator, which
    // is the import allocator. Data.exportVector does not re-own the buffers, so without a
    // transfer the output stays charged to the import allocator for as long as the export holds
    // it, and jvm_arrow_imported counts JVM-created bytes as imported native memory.
    val numRows = 4096
    val before = CometArrowImportAllocator.getAllocatedMemory

    withBridgeCall(classOf[InputAllocatorOutputUdf].getName, numRows) { () =>
      val during = CometArrowImportAllocator.getAllocatedMemory
      assert(
        during - before < numRows.toLong * 4,
        s"the UDF's output is still charged to the import allocator ($before -> $during bytes, " +
          s"output is ${numRows * 4} bytes), so it would be reported as imported native memory")
    }
  }
}
