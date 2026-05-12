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

import org.apache.arrow.vector.VarCharVector

import org.apache.comet.CometArrowAllocator

class GetJsonObjectUDFSuite extends AnyFunSuite {

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

  private def get(out: VarCharVector, i: Int): Option[String] =
    if (out.isNull(i)) None else Some(new String(out.get(i), StandardCharsets.UTF_8))

  test("extract values using a scalar path") {
    val udf = new GetJsonObjectUDF
    val json = stringVec(
      "json",
      Seq(Some("""{"a":{"b":42}}"""), Some("""{"a":{"b":7}}"""), None, Some("garbage")))
    val path = stringVec("path", Seq(Some("$.a.b")))
    try {
      val out = udf.evaluate(Array(json, path)).asInstanceOf[VarCharVector]
      try {
        assert(out.getValueCount == 4)
        assert(get(out, 0) == Some("42"))
        assert(get(out, 1) == Some("7"))
        assert(get(out, 2).isEmpty)
        assert(get(out, 3).isEmpty)
      } finally out.close()
    } finally {
      json.close(); path.close()
    }
  }

  test("empty input vector produces empty output") {
    val udf = new GetJsonObjectUDF
    val json = stringVec("json", Seq.empty)
    val path = stringVec("path", Seq(Some("$.a")))
    try {
      val out = udf.evaluate(Array(json, path)).asInstanceOf[VarCharVector]
      try assert(out.getValueCount == 0)
      finally out.close()
    } finally {
      json.close(); path.close()
    }
  }

  test("different scalar paths across separate evaluations share the same UDF instance") {
    val udf = new GetJsonObjectUDF
    val json = stringVec("json", Seq(Some("""{"a":1,"b":2}""")))
    val pathA = stringVec("pathA", Seq(Some("$.a")))
    val pathB = stringVec("pathB", Seq(Some("$.b")))
    try {
      val outA = udf.evaluate(Array(json, pathA)).asInstanceOf[VarCharVector]
      try assert(get(outA, 0) == Some("1"))
      finally outA.close()
      val json2 = stringVec("json2", Seq(Some("""{"a":1,"b":2}""")))
      try {
        val outB = udf.evaluate(Array(json2, pathB)).asInstanceOf[VarCharVector]
        try assert(get(outB, 0) == Some("2"))
        finally outB.close()
      } finally json2.close()
    } finally {
      json.close(); pathA.close(); pathB.close()
    }
  }
}
