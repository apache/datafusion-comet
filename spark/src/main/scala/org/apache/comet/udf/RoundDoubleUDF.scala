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

import org.apache.arrow.vector.{Float8Vector, IntVector, ValueVector}

import org.apache.comet.CometArrowAllocator

/**
 * `round(double, scale)` implemented by delegating to Scala's `BigDecimal(d)`, which goes through
 * `java.lang.Double.toString` before applying the requested scale. This matches Spark's
 * `RoundBase` for `DoubleType` exactly on whatever JDK the executor is running, so output stays
 * consistent across Java 17 / 21 even though the underlying `Double.toString` algorithm differs.
 *
 * Inputs:
 *   - inputs(0): Float8Vector value column (length = numRows, or length 1 when literal-folded)
 *   - inputs(1): IntVector scale, length-1 scalar (serde guarantees this)
 *
 * Output: Float8Vector, length numRows.
 */
class RoundDoubleUDF extends CometUDF {

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    require(inputs.length == 2, s"RoundDoubleUDF expects 2 inputs, got ${inputs.length}")
    val values = inputs(0).asInstanceOf[Float8Vector]
    val scaleVec = inputs(1).asInstanceOf[IntVector]
    require(
      scaleVec.getValueCount >= 1 && !scaleVec.isNull(0),
      "RoundDoubleUDF requires a non-null scalar scale")
    val scale = scaleVec.get(0)

    val out = new Float8Vector("round_double", CometArrowAllocator)
    out.allocateNew(numRows)

    val valueIsScalar = values.getValueCount == 1 && numRows != 1
    if (valueIsScalar) {
      if (values.isNull(0)) {
        var i = 0
        while (i < numRows) { out.setNull(i); i += 1 }
      } else {
        val rounded = RoundDoubleUDF.roundDouble(values.get(0), scale)
        var i = 0
        while (i < numRows) { out.set(i, rounded); i += 1 }
      }
    } else {
      var i = 0
      while (i < numRows) {
        if (values.isNull(i)) {
          out.setNull(i)
        } else {
          out.set(i, RoundDoubleUDF.roundDouble(values.get(i), scale))
        }
        i += 1
      }
    }
    out.setValueCount(numRows)
    out
  }
}

object RoundDoubleUDF {
  def roundDouble(v: Double, scale: Int): Double = {
    if (v.isNaN || v.isInfinite) v
    else BigDecimal(v).setScale(scale, BigDecimal.RoundingMode.HALF_UP).doubleValue
  }
}
