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
import org.apache.spark.sql.catalyst.expressions.{BoundReference, JsonToStructs}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import org.apache.comet.CometArrowAllocator

class FromJsonUDFSuite extends AnyFunSuite {

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

  test("parse struct<a:int, b:string> from JSON strings") {
    val schema = StructType(
      Seq(
        StructField("a", IntegerType, nullable = true),
        StructField("b", StringType, nullable = true)))
    val sparkExpr = JsonToStructs(
      schema,
      Map.empty[String, String],
      BoundReference(0, StringType, nullable = true),
      Some("UTC"))
    val key = CometLambdaRegistry.register(sparkExpr)
    try {
      val udf = new FromJsonUDF
      val json = stringVec(
        "json",
        Seq(Some("""{"a":1,"b":"x"}"""), Some("""{"a":null,"b":"y"}"""), None, Some("bad")))
      val keyVec = stringVec("key", Seq(Some(key)))
      try {
        val out = udf.evaluate(Array(json, keyVec)).asInstanceOf[StructVector]
        try {
          assert(out.getValueCount == 4)
          val aVec = out.getChildByOrdinal(0).asInstanceOf[IntVector]
          val bVec = out.getChildByOrdinal(1).asInstanceOf[VarCharVector]
          // Row 0: a=1, b="x"
          assert(!out.isNull(0))
          assert(aVec.get(0) == 1)
          assert(new String(bVec.get(0), StandardCharsets.UTF_8) == "x")
          // Row 1: a=null, b="y"
          assert(!out.isNull(1))
          assert(aVec.isNull(1))
          assert(new String(bVec.get(1), StandardCharsets.UTF_8) == "y")
          // Row 2: input was null
          assert(out.isNull(2))
          // Row 3: malformed JSON, PERMISSIVE → struct of all nulls
          assert(!out.isNull(3))
          assert(aVec.isNull(3))
          assert(bVec.isNull(3))
        } finally out.close()
      } finally { json.close(); keyVec.close() }
    } finally CometLambdaRegistry.remove(key)
  }

  test("empty input vector produces empty output struct") {
    val schema = StructType(Seq(StructField("a", IntegerType, nullable = true)))
    val sparkExpr = JsonToStructs(
      schema,
      Map.empty[String, String],
      BoundReference(0, StringType, nullable = true),
      Some("UTC"))
    val key = CometLambdaRegistry.register(sparkExpr)
    try {
      val udf = new FromJsonUDF
      val json = stringVec("json", Seq.empty)
      val keyVec = stringVec("key", Seq(Some(key)))
      try {
        val out = udf.evaluate(Array(json, keyVec)).asInstanceOf[StructVector]
        try assert(out.getValueCount == 0)
        finally out.close()
      } finally { json.close(); keyVec.close() }
    } finally CometLambdaRegistry.remove(key)
  }
}
