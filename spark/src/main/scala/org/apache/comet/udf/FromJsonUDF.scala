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

import org.apache.arrow.vector.{BigIntVector, BitVector, FieldVector, Float4Vector, Float8Vector, IntVector, ValueVector, VarCharVector}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, JsonToStructs, RuntimeReplaceable}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometArrowAllocator

/**
 * `from_json(json, schema)` implemented via Spark's `JsonToStructs` for byte-exact compatibility.
 *
 * The registered expression is looked up from `CometLambdaRegistry` using a scalar key argument.
 * The output schema is read from the registered expression's `dataType`. The UDF is stateless:
 * every call resolves the expression from the registry, so a single UDF instance can be shared
 * across native worker threads.
 *
 * Inputs:
 *   - inputs(0): VarCharVector json column
 *   - inputs(1): VarCharVector scalar (length-1) containing the registry key string
 *
 * Output: StructVector matching the registered schema. The caller owns the returned vector and
 * must close it; closing the StructVector recursively closes its child vectors.
 *
 * Supported field types (matching `CometJsonToStructs.isSupportedType`): Boolean, Integer, Long,
 * Float, Double, String, plus nested struct of those.
 */
class FromJsonUDF extends CometUDF {

  override def evaluate(inputs: Array[ValueVector]): ValueVector = {
    require(inputs.length == 2, s"FromJsonUDF expects 2 inputs (json, key), got ${inputs.length}")
    val json = inputs(0).asInstanceOf[VarCharVector]
    val keyVec = inputs(1).asInstanceOf[VarCharVector]
    require(
      keyVec.getValueCount >= 1 && !keyVec.isNull(0),
      "FromJsonUDF requires a non-null scalar registry key")

    val key = new String(keyVec.get(0), StandardCharsets.UTF_8)
    val configExpr = CometLambdaRegistry.get(key).asInstanceOf[JsonToStructs]
    val schema = configExpr.dataType.asInstanceOf[StructType]
    // Build a fresh expression per call: Spark's JsonToStructsEvaluator may hold mutable
    // per-row state, so a shared cross-thread instance is unsafe (the JVM UDF framework
    // reuses one UDF instance across native worker threads).
    val sparkExpr = JsonToStructs(
      schema,
      configExpr.options,
      BoundReference(0, StringType, nullable = true),
      configExpr.timeZoneId)
    val evalExpr: Expression = sparkExpr match {
      case r: RuntimeReplaceable => r.replacement
      case other => other
    }

    val n = json.getValueCount
    val out = StructVector.empty("from_json_result", CometArrowAllocator)
    schema.fields.foreach(f => addChild(out, f.name, f.dataType, f.nullable))
    out.setInitialCapacity(n)
    out.allocateNew()

    val row = new GenericInternalRow(1)
    var i = 0
    while (i < n) {
      if (json.isNull(i)) {
        // entry stays null (default after allocateNew)
      } else {
        row.update(0, UTF8String.fromBytes(json.get(i)))
        val result = evalExpr.eval(row)
        if (result == null) {
          // null result → null struct entry
        } else {
          out.setIndexDefined(i)
          val struct = result.asInstanceOf[InternalRow]
          var f = 0
          while (f < schema.fields.length) {
            writeChild(out.getChildByOrdinal(f), i, schema.fields(f).dataType, struct, f)
            f += 1
          }
        }
      }
      i += 1
    }
    // Set value count on each child so its validity buffer is sized correctly.
    var f = 0
    while (f < schema.fields.length) {
      out.getChildByOrdinal(f).setValueCount(n)
      f += 1
    }
    out.setValueCount(n)
    out
  }

  private def addChild(
      parent: StructVector,
      name: String,
      dt: DataType,
      nullable: Boolean): Unit = {
    dt match {
      case BooleanType =>
        parent.addOrGet(
          name,
          new FieldType(nullable, ArrowType.Bool.INSTANCE, null),
          classOf[BitVector])
      case IntegerType =>
        parent.addOrGet(
          name,
          new FieldType(nullable, new ArrowType.Int(32, true), null),
          classOf[IntVector])
      case LongType =>
        parent.addOrGet(
          name,
          new FieldType(nullable, new ArrowType.Int(64, true), null),
          classOf[BigIntVector])
      case FloatType =>
        parent.addOrGet(
          name,
          new FieldType(
            nullable,
            new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
            null),
          classOf[Float4Vector])
      case DoubleType =>
        parent.addOrGet(
          name,
          new FieldType(
            nullable,
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
            null),
          classOf[Float8Vector])
      case StringType =>
        parent.addOrGet(
          name,
          new FieldType(nullable, ArrowType.Utf8.INSTANCE, null),
          classOf[VarCharVector])
      case nested: StructType =>
        parent.addOrGet(
          name,
          new FieldType(nullable, ArrowType.Struct.INSTANCE, null),
          classOf[StructVector])
        val child = parent.getChild(name).asInstanceOf[StructVector]
        nested.fields.foreach(ff => addChild(child, ff.name, ff.dataType, ff.nullable))
      case other =>
        throw new UnsupportedOperationException(s"FromJsonUDF: unsupported type $other")
    }
  }

  private def writeChild(
      child: ValueVector,
      i: Int,
      dt: DataType,
      struct: InternalRow,
      f: Int): Unit = {
    if (struct.isNullAt(f)) {
      child.asInstanceOf[FieldVector].setNull(i)
      return
    }
    dt match {
      case BooleanType =>
        child.asInstanceOf[BitVector].setSafe(i, if (struct.getBoolean(f)) 1 else 0)
      case IntegerType =>
        child.asInstanceOf[IntVector].setSafe(i, struct.getInt(f))
      case LongType =>
        child.asInstanceOf[BigIntVector].setSafe(i, struct.getLong(f))
      case FloatType =>
        child.asInstanceOf[Float4Vector].setSafe(i, struct.getFloat(f))
      case DoubleType =>
        child.asInstanceOf[Float8Vector].setSafe(i, struct.getDouble(f))
      case StringType =>
        child.asInstanceOf[VarCharVector].setSafe(i, struct.getUTF8String(f).getBytes)
      case nested: StructType =>
        val sv = child.asInstanceOf[StructVector]
        sv.setIndexDefined(i)
        val inner = struct.getStruct(f, nested.fields.length)
        var ff = 0
        while (ff < nested.fields.length) {
          writeChild(sv.getChildByOrdinal(ff), i, nested.fields(ff).dataType, inner, ff)
          ff += 1
        }
      case other =>
        throw new UnsupportedOperationException(s"FromJsonUDF: unsupported type $other")
    }
  }
}
