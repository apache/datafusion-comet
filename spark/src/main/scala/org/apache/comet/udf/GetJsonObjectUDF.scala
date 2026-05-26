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

import org.apache.arrow.vector.{ValueVector, VarCharVector}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, GetJsonObject, Literal, RuntimeReplaceable}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometArrowAllocator

/**
 * `get_json_object(json, path)` implemented via Spark's `GetJsonObject` expression for byte-exact
 * compatibility. Path must be a non-null scalar (enforced by the serde when routing here).
 *
 * Inputs:
 *   - inputs(0): VarCharVector json column
 *   - inputs(1): VarCharVector path (scalar, length-1)
 *
 * Output: VarCharVector, same length as json input.
 *
 * A fresh Spark expression is built per `evaluate` call (per batch). Spark's `GetJsonObject`
 * holds mutable per-row state in its evaluator, so a shared cross-thread instance is unsafe; the
 * JVM UDF framework reuses one UDF instance across native worker threads.
 */
class GetJsonObjectUDF extends CometUDF {

  override def evaluate(inputs: Array[ValueVector]): ValueVector = {
    require(inputs.length == 2, s"GetJsonObjectUDF expects 2 inputs, got ${inputs.length}")
    val jsonVec = inputs(0).asInstanceOf[VarCharVector]
    val pathVec = inputs(1).asInstanceOf[VarCharVector]
    require(
      pathVec.getValueCount >= 1 && !pathVec.isNull(0),
      "GetJsonObjectUDF requires a non-null scalar path")

    val pathStr = new String(pathVec.get(0), StandardCharsets.UTF_8)
    val sparkExpr = GetJsonObject(
      BoundReference(0, StringType, nullable = true),
      Literal(UTF8String.fromString(pathStr), StringType))
    val evalExpr: Expression = sparkExpr match {
      case r: RuntimeReplaceable => r.replacement
      case other => other
    }

    val n = jsonVec.getValueCount
    val out = new VarCharVector("get_json_object_result", CometArrowAllocator)
    out.allocateNew(n)

    val row = new GenericInternalRow(1)
    var i = 0
    while (i < n) {
      if (jsonVec.isNull(i)) {
        out.setNull(i)
      } else {
        row.update(0, UTF8String.fromBytes(jsonVec.get(i)))
        val result = evalExpr.eval(row)
        if (result == null) out.setNull(i)
        else out.setSafe(i, result.asInstanceOf[UTF8String].getBytes)
      }
      i += 1
    }
    out.setValueCount(n)
    out
  }
}
