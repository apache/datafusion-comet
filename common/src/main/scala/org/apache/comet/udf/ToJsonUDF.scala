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

import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TinyIntVector, ValueVector, VarCharVector}
import org.apache.arrow.vector.complex.StructVector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, RuntimeReplaceable, StructsToJson}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometArrowAllocator

/**
 * `to_json(struct)` implemented via Spark's `StructsToJson` for byte-exact compatibility.
 *
 * The registered expression is looked up from `CometLambdaRegistry` using a scalar key argument.
 * The schema is read from the registered expression's child datatype. The UDF is stateless: every
 * call resolves the expression from the registry, so a single UDF instance can be shared across
 * native worker threads.
 *
 * Inputs:
 *   - inputs(0): StructVector of arbitrary supported schema
 *   - inputs(1): VarCharVector scalar (length-1) containing the registry key string
 *
 * Output: VarCharVector of JSON strings.
 *
 * Supported field types (matching `CometStructsToJson.isSupportedType`): Boolean, Byte, Short,
 * Integer, Long, Float, Double, String, plus nested struct of those.
 */
class ToJsonUDF extends CometUDF {

  override def evaluate(inputs: Array[ValueVector]): ValueVector = {
    require(inputs.length == 2, s"ToJsonUDF expects 2 inputs (struct, key), got ${inputs.length}")
    val struct = inputs(0).asInstanceOf[StructVector]
    val keyVec = inputs(1).asInstanceOf[VarCharVector]
    require(
      keyVec.getValueCount >= 1 && !keyVec.isNull(0),
      "ToJsonUDF requires a non-null scalar registry key")

    val key = new String(keyVec.get(0), StandardCharsets.UTF_8)
    val configExpr = CometLambdaRegistry.get(key).asInstanceOf[StructsToJson]
    val schema = configExpr.child.dataType.asInstanceOf[StructType]
    // Build a fresh expression per call: Spark's StructsToJsonEvaluator holds mutable
    // per-row state, so a shared cross-thread instance is unsafe (the JVM UDF framework
    // reuses one UDF instance across native worker threads).
    val sparkExpr =
      StructsToJson(
        configExpr.options,
        BoundReference(0, schema, nullable = true),
        configExpr.timeZoneId)
    val evalExpr: Expression = sparkExpr match {
      case r: RuntimeReplaceable => r.replacement
      case other => other
    }

    val n = struct.getValueCount
    val out = new VarCharVector("to_json_result", CometArrowAllocator)
    out.allocateNew(n)

    val row = new GenericInternalRow(1)
    var i = 0
    while (i < n) {
      if (struct.isNull(i)) {
        out.setNull(i)
      } else {
        val inner = arrowStructRowToSparkRow(struct, i, schema)
        row.update(0, inner)
        val result = evalExpr.eval(row)
        if (result == null) out.setNull(i)
        else out.setSafe(i, result.asInstanceOf[UTF8String].getBytes)
      }
      i += 1
    }
    out.setValueCount(n)
    out
  }

  private def arrowStructRowToSparkRow(
      struct: StructVector,
      rowIdx: Int,
      schema: StructType): InternalRow = {
    val values = new Array[Any](schema.fields.length)
    var f = 0
    while (f < schema.fields.length) {
      val child = struct.getChildByOrdinal(f)
      if (child.isNull(rowIdx)) {
        values(f) = null
      } else {
        values(f) = readScalar(child, rowIdx, schema.fields(f).dataType)
      }
      f += 1
    }
    new GenericInternalRow(values)
  }

  private def readScalar(v: ValueVector, i: Int, dt: DataType): Any = dt match {
    case BooleanType => v.asInstanceOf[BitVector].get(i) == 1
    case ByteType => v.asInstanceOf[TinyIntVector].get(i)
    case ShortType => v.asInstanceOf[SmallIntVector].get(i)
    case IntegerType => v.asInstanceOf[IntVector].get(i)
    case LongType => v.asInstanceOf[BigIntVector].get(i)
    case FloatType => v.asInstanceOf[Float4Vector].get(i)
    case DoubleType => v.asInstanceOf[Float8Vector].get(i)
    case StringType => UTF8String.fromBytes(v.asInstanceOf[VarCharVector].get(i))
    case nested: StructType =>
      arrowStructRowToSparkRow(v.asInstanceOf[StructVector], i, nested)
    case other =>
      throw new UnsupportedOperationException(s"ToJsonUDF: unsupported type $other")
  }
}
