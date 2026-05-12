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

import java.nio.charset.StandardCharsets

import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.vector.{IntVector, VarCharVector}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, StructsToJson}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import org.apache.comet.CometArrowAllocator

class ToJsonUDFSuite extends AnyFunSuite {

  private def stringVec(name: String, values: Seq[Option[String]]): VarCharVector = {
    val v = new VarCharVector(name, CometArrowAllocator)
    v.allocateNew(values.size)
    values.zipWithIndex.foreach {
      case (Some(s), i) => v.setSafe(i, s.getBytes(StandardCharsets.UTF_8))
      case (None, i) => v.setNull(i)
    }
    v.setValueCount(values.size)
    v
  }

  test("serialize struct<a:int, b:string> rows to JSON") {
    val schema = StructType(
      Seq(
        StructField("a", IntegerType, nullable = true),
        StructField("b", StringType, nullable = true)))
    val sparkExpr = StructsToJson(
      Map.empty[String, String],
      BoundReference(0, schema, nullable = true),
      Some("UTC"))
    val key = CometLambdaRegistry.register(sparkExpr)
    try {
      val udf = new ToJsonUDF

      val struct = StructVector.empty("in", CometArrowAllocator)
      val aVec =
        struct.addOrGet(
          "a",
          new FieldType(true, new ArrowType.Int(32, true), null),
          classOf[IntVector])
      val bVec =
        struct.addOrGet(
          "b",
          new FieldType(true, ArrowType.Utf8.INSTANCE, null),
          classOf[VarCharVector])
      struct.allocateNew()

      aVec.setSafe(0, 1); bVec.setSafe(0, "x".getBytes(StandardCharsets.UTF_8))
      aVec.setNull(1); bVec.setSafe(1, "y".getBytes(StandardCharsets.UTF_8))
      struct.setIndexDefined(0)
      struct.setIndexDefined(1)
      aVec.setValueCount(2); bVec.setValueCount(2); struct.setValueCount(2)

      val keyVec = stringVec("key", Seq(Some(key)))
      try {
        val out = udf.evaluate(Array(struct, keyVec)).asInstanceOf[VarCharVector]
        try {
          assert(out.getValueCount == 2)
          assert(new String(out.get(0), StandardCharsets.UTF_8) == """{"a":1,"b":"x"}""")
          // Spark drops null fields by default; the second row's `a` is null,
          // so `to_json` produces only `b`.
          assert(new String(out.get(1), StandardCharsets.UTF_8) == """{"b":"y"}""")
        } finally out.close()
      } finally { struct.close(); keyVec.close() }
    } finally CometLambdaRegistry.remove(key)
  }

  test("empty input vector produces empty output") {
    val schema = StructType(Seq(StructField("a", IntegerType, nullable = true)))
    val sparkExpr = StructsToJson(
      Map.empty[String, String],
      BoundReference(0, schema, nullable = true),
      Some("UTC"))
    val key = CometLambdaRegistry.register(sparkExpr)
    try {
      val udf = new ToJsonUDF
      val struct = StructVector.empty("in", CometArrowAllocator)
      struct.addOrGet(
        "a",
        new FieldType(true, new ArrowType.Int(32, true), null),
        classOf[IntVector])
      struct.allocateNew()
      struct.setValueCount(0)
      val keyVec = stringVec("key", Seq(Some(key)))
      try {
        val out = udf.evaluate(Array(struct, keyVec)).asInstanceOf[VarCharVector]
        try assert(out.getValueCount == 0)
        finally out.close()
      } finally { struct.close(); keyVec.close() }
    } finally CometLambdaRegistry.remove(key)
  }
}
