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

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, IntVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.{CometPlainVector, CometVector}

/**
 * Direct tests for [[CometArrowStream.reconcileStreamSchema]]. The end-to-end regression that
 * motivated this (Spark Long vs DataFusion Int32 for `width_bucket`) lives in
 * `CometMathExpressionSuite`, but that test only catches *one* function-level type drift. This
 * suite covers the boundary contract independently of any specific function.
 */
class CometArrowStreamSuite extends AnyFunSuite with Matchers {

  private def expectedSchema(types: (String, ArrowType)*): Schema = {
    val fields = types.map { case (name, t) =>
      new Field(name, new FieldType(true, t, null), java.util.Collections.emptyList[Field]())
    }
    new Schema(fields.asJava)
  }

  private def batchOf(vectors: CometVector*): ColumnarBatch = {
    val numRows = if (vectors.isEmpty) 0 else vectors.head.getValueVector.getValueCount
    new ColumnarBatch(vectors.toArray, numRows)
  }

  test("reconcileStreamSchema returns expected schema unchanged on empty iterator") {
    val expected = expectedSchema("c0" -> new ArrowType.Int(64, true))
    val (returned, iter) =
      CometArrowStream.reconcileStreamSchema("test", expected, Iterator.empty)
    returned shouldBe expected
    iter.hasNext shouldBe false
  }

  test("reconcileStreamSchema returns expected schema when types match") {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    try {
      val v = new BigIntVector("col_0", allocator)
      v.allocateNew()
      v.setSafe(0, 1L)
      v.setValueCount(1)
      val cv = new CometPlainVector(v, false)
      val batch = batchOf(cv)
      val expected = expectedSchema("c0" -> new ArrowType.Int(64, true))

      val (returned, iter) = CometArrowStream
        .reconcileStreamSchema("test", expected, Iterator.single(batch))

      returned.getFields.get(0).getType shouldBe new ArrowType.Int(64, true)
      iter.hasNext shouldBe true
      iter.next() should be theSameInstanceAs batch

      cv.close()
    } finally {
      allocator.close()
    }
  }

  test("reconcileStreamSchema rebuilds schema from actual vector types when they differ") {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    try {
      // Producer produced Int32 (e.g., DataFusion-Spark width_bucket pre-fix), consumer expects
      // Int64 (Spark catalyst WidthBucket.dataType = LongType). The truthful schema is Int32 so
      // native ScanExec's build_record_batch can cast at the boundary.
      val v = new IntVector("col_0", allocator)
      v.allocateNew()
      v.setSafe(0, 1)
      v.setValueCount(1)
      val cv = new CometPlainVector(v, false)
      val batch = batchOf(cv)
      val expected = expectedSchema("c0" -> new ArrowType.Int(64, true))

      val (returned, iter) = CometArrowStream
        .reconcileStreamSchema("test", expected, Iterator.single(batch))

      val returnedField = returned.getFields.get(0)
      returnedField.getType shouldBe new ArrowType.Int(32, true)
      // Names come from `expected` so name-indexed consumers keep working.
      returnedField.getName shouldBe "c0"
      iter.hasNext shouldBe true
      iter.next() should be theSameInstanceAs batch

      cv.close()
    } finally {
      allocator.close()
    }
  }

  test(
    "reconcileStreamSchema preserves nullability when expected is nullable but actual is not") {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    try {
      // Spark catalyst declares the column nullable; the first batch happens to come from a
      // vector whose Field reports non-nullable. Subsequent batches may carry nulls, so the
      // wire schema must stay nullable or native validation rejects the next null with
      // "declared as non-nullable but contains null values".
      val v = new BigIntVector(
        new Field(
          "col_0",
          new FieldType(false, new ArrowType.Int(64, true), null),
          java.util.Collections.emptyList[Field]()),
        allocator)
      v.allocateNew()
      v.setSafe(0, 1L)
      v.setValueCount(1)
      val cv = new CometPlainVector(v, false)
      val batch = batchOf(cv)
      val expected = expectedSchema("c0" -> new ArrowType.Int(64, true)) // nullable=true

      val (returned, _) = CometArrowStream
        .reconcileStreamSchema("test", expected, Iterator.single(batch))

      val returnedField = returned.getFields.get(0)
      returnedField.isNullable shouldBe true

      cv.close()
    } finally {
      allocator.close()
    }
  }
}
