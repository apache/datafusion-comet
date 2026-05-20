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

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.spark.sql.catalyst.expressions.{ArrayExists, LambdaFunction, NamedLambdaVariable}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometArrowAllocator

/**
 * JVM UDF implementing Spark's `exists(array, x -> predicate(x))` higher-order function.
 *
 * Inputs:
 *   - inputs(0): ListVector (the array column)
 *   - inputs(1): VarCharVector length-1 scalar (registry key for the lambda expression)
 *
 * Output: BitVector (nullable boolean), same length as the input array vector.
 *
 * Implements Spark's three-valued logic:
 *   - true if any element satisfies the predicate
 *   - null if no element satisfies but the predicate returned null for at least one element
 *   - false if all elements produce false
 */
class ArrayExistsUDF extends CometUDF {

  override def evaluate(inputs: Array[ValueVector]): ValueVector = {
    require(inputs.length == 2, s"ArrayExistsUDF expects 2 inputs, got ${inputs.length}")
    val listVec = inputs(0).asInstanceOf[ListVector]
    val keyVec = inputs(1).asInstanceOf[VarCharVector]
    require(
      keyVec.getValueCount >= 1 && !keyVec.isNull(0),
      "ArrayExistsUDF requires a non-null scalar registry key")

    val registryKey = new String(keyVec.get(0), StandardCharsets.UTF_8)
    val arrayExistsExpr = CometLambdaRegistry.get(registryKey).asInstanceOf[ArrayExists]

    val LambdaFunction(_, Seq(elementVar: NamedLambdaVariable), _) = arrayExistsExpr.function
    val body = arrayExistsExpr.functionForEval
    val followThreeValuedLogic = arrayExistsExpr.followThreeValuedLogic
    val elementType = elementVar.dataType

    val dataVec = listVec.getDataVector
    val n = listVec.getValueCount
    val out = new BitVector("exists_result", CometArrowAllocator)
    out.allocateNew(n)

    var i = 0
    while (i < n) {
      if (listVec.isNull(i)) {
        out.setNull(i)
      } else {
        val startIdx = listVec.getElementStartIndex(i)
        val endIdx = listVec.getElementEndIndex(i)
        var exists = false
        var foundNull = false
        var j = startIdx
        while (j < endIdx && !exists) {
          if (dataVec.isNull(j)) {
            elementVar.value.set(null)
            val ret = body.eval(null)
            if (ret == null) foundNull = true
            else if (ret.asInstanceOf[Boolean]) exists = true
          } else {
            val elem = getSparkValue(dataVec, j, elementType)
            elementVar.value.set(elem)
            val ret = body.eval(null)
            if (ret == null) foundNull = true
            else if (ret.asInstanceOf[Boolean]) exists = true
          }
          j += 1
        }
        if (exists) {
          out.set(i, 1)
        } else if (followThreeValuedLogic && foundNull) {
          out.setNull(i)
        } else {
          out.set(i, 0)
        }
      }
      i += 1
    }
    out.setValueCount(n)
    out
  }

  private def getSparkValue(vec: ValueVector, index: Int, sparkType: DataType): Any = {
    sparkType match {
      case BooleanType =>
        vec.asInstanceOf[BitVector].get(index) == 1
      case ByteType =>
        vec.asInstanceOf[TinyIntVector].get(index).toByte
      case ShortType =>
        vec.asInstanceOf[SmallIntVector].get(index).toShort
      case IntegerType =>
        vec.asInstanceOf[IntVector].get(index)
      case LongType =>
        vec.asInstanceOf[BigIntVector].get(index)
      case FloatType =>
        vec.asInstanceOf[Float4Vector].get(index)
      case DoubleType =>
        vec.asInstanceOf[Float8Vector].get(index)
      case StringType =>
        val bytes = vec.asInstanceOf[VarCharVector].get(index)
        UTF8String.fromBytes(bytes)
      case BinaryType =>
        vec.asInstanceOf[VarBinaryVector].get(index)
      case _: DecimalType =>
        val dt = sparkType.asInstanceOf[DecimalType]
        val decimal = vec.asInstanceOf[DecimalVector].getObject(index)
        Decimal(decimal, dt.precision, dt.scale)
      case DateType =>
        vec.asInstanceOf[DateDayVector].get(index)
      case TimestampType =>
        vec.asInstanceOf[TimeStampMicroTZVector].get(index)
      case _ =>
        throw new UnsupportedOperationException(
          s"ArrayExistsUDF does not yet support element type: $sparkType")
    }
  }
}
