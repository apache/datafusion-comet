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

import java.io.IOException

import scala.util.Using

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, UInt4Vector, VarCharVector}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class NativeUtilSuite extends CometTestBase {

  private def withIsolatedStructAllocator(
      check: (NativeUtil, RootAllocator, () => Array[ArrowArray]) => Unit): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    var allocatedArrays = Array.empty[ArrowArray]
    val nativeUtil = new NativeUtil {
      override def allocateArrowStructs(numCols: Int): (Array[ArrowArray], Array[ArrowSchema]) = {
        allocatedArrays = Array.fill(numCols)(ArrowArray.allocateNew(allocator))
        val schemas = Array.fill(numCols)(ArrowSchema.allocateNew(allocator))
        (allocatedArrays, schemas)
      }
    }
    try check(nativeUtil, allocator, () => allocatedArrays)
    finally {
      nativeUtil.close()
      allocator.close()
    }
  }

  test("getNextBatch releases unconsumed Arrow structs on native failure and EOF") {
    Seq(false, true).foreach { eof =>
      withIsolatedStructAllocator { (nativeUtil, allocator, _) =>
        val expected = new IOException("native decode failed")
        val read = () =>
          nativeUtil.getNextBatch(
            2,
            (_, _) => {
              if (eof) -1L else throw expected
            })
        if (eof) assert(read().isEmpty)
        else assert(intercept[IOException](read()) eq expected)
        assert(allocator.getAllocatedMemory == 0)
      }
    }
  }

  test("getNextBatch invokes C release callbacks for partially exported native results") {
    Seq(false, true).foreach { eof =>
      withIsolatedStructAllocator { (nativeUtil, allocator, _) =>
        val vector = new IntVector("value", allocator)
        vector.allocateNew(4)
        vector.setSafe(0, 42)
        vector.setValueCount(1)
        val expected = new IOException("native decode failed after exporting one column")
        val read = () =>
          nativeUtil.getNextBatch(
            2,
            (arrays, schemas) => {
              Data.exportVector(
                allocator,
                vector,
                null,
                ArrowArray.wrap(arrays(0)),
                ArrowSchema.wrap(schemas(0)))
              // Only the exported C data now retains the vector's buffers.
              vector.close()
              if (eof) -1L else throw expected
            })
        try {
          if (eof) assert(read().isEmpty)
          else assert(intercept[IOException](read()) eq expected)
          assert(allocator.getAllocatedMemory == 0)
        } finally {
          vector.close()
        }
      }
    }
  }

  test("getNextBatch releases imported vectors and Arrow structs when vector import fails") {
    withIsolatedStructAllocator { (nativeUtil, allocator, _) =>
      Using.resource(new IntVector("value", allocator)) { vector =>
        vector.allocateNew(4)
        vector.setSafe(0, 42)
        vector.setValueCount(1)
        val failure = intercept[IllegalStateException] {
          nativeUtil.getNextBatch(
            2,
            (arrays, schemas) => {
              Data.exportVector(
                allocator,
                vector,
                null,
                ArrowArray.wrap(arrays(0)),
                ArrowSchema.wrap(schemas(0)))
              vector.close()
              1L
            })
        }
        assert(failure.getMessage == "Cannot import released ArrowSchema")
        assert(failure.getSuppressed.isEmpty)
        assert(allocator.getAllocatedMemory == 0)
      }
    }
  }

  test("getNextBatch releases an imported vector when its Comet wrapper rejects the type") {
    withIsolatedStructAllocator { (nativeUtil, allocator, _) =>
      Using.resource(new UInt4Vector("value", allocator)) { vector =>
        vector.allocateNew(1)
        vector.setSafe(0, 42)
        vector.setValueCount(1)
        val failure = intercept[UnsupportedOperationException] {
          nativeUtil.getNextBatch(
            2,
            (arrays, schemas) => {
              Data.exportVector(
                allocator,
                vector,
                null,
                ArrowArray.wrap(arrays(0)),
                ArrowSchema.wrap(schemas(0)))
              vector.close()
              1L
            })
        }
        assert(failure.getSuppressed.isEmpty)
        assert(allocator.getAllocatedMemory == 0)
      }
    }
  }

  test("importVector releases partially imported vectors when Arrow array import fails") {
    withIsolatedStructAllocator { (nativeUtil, allocator, _) =>
      val intType = FieldType.nullable(new ArrowType.Int(32, true))
      val stringType = FieldType.nullable(new ArrowType.Utf8())
      val intStruct = StructVector.empty("int_struct", allocator)
      val firstInt = intStruct.addOrGet("first", intType, classOf[IntVector])
      val secondInt = intStruct.addOrGet("second", intType, classOf[IntVector])
      intStruct.allocateNew()
      intStruct.setIndexDefined(0)
      firstInt.setSafe(0, 1)
      secondInt.setSafe(0, 2)
      intStruct.setValueCount(1)

      val stringStruct = StructVector.empty("string_struct", allocator)
      stringStruct.addOrGet("first", intType, classOf[IntVector])
      stringStruct.addOrGet("second", stringType, classOf[VarCharVector])
      stringStruct.allocateNew()
      stringStruct.setValueCount(1)

      val (arrays, schemas) = nativeUtil.allocateArrowStructs(2)
      Data.exportVector(allocator, intStruct, null, arrays(0), schemas(0))
      Data.exportVector(allocator, stringStruct, null, arrays(1), schemas(1))
      intStruct.close()
      stringStruct.close()

      try {
        val failure = intercept[IllegalArgumentException] {
          nativeUtil.importVector(Array(arrays(0)), Array(schemas(1)))
        }
        assert(failure.getSuppressed.isEmpty)
      } finally {
        arrays(1).release()
        arrays(1).close()
        schemas(0).release()
        schemas(0).close()
      }
      assert(allocator.getAllocatedMemory == 0)
    }
  }

  test("getNextBatch preserves a native failure and attempts all remaining struct cleanup") {
    withIsolatedStructAllocator { (nativeUtil, allocator, arrays) =>
      val expected = new IOException("native decode failed")
      val actual = intercept[IOException] {
        nativeUtil.getNextBatch(
          2,
          (_, _) => {
            // Simulate another owner retiring one struct before cleanup. Its release() fails,
            // but that must not hide the decode failure or leave any other struct allocated.
            arrays()(0).close()
            throw expected
          })
      }
      assert(actual eq expected)
      assert(actual.getSuppressed.length == 1)
      assert(actual.getSuppressed.head.isInstanceOf[NullPointerException])
      assert(allocator.getAllocatedMemory == 0)
    }
  }

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
