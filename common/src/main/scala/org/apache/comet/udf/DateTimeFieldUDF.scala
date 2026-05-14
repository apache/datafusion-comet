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
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}

import org.apache.arrow.vector.{IntVector, TimeStampMicroTZVector, TimeStampMicroVector, ValueVector, VarCharVector}

import org.apache.comet.CometArrowAllocator

/**
 * Shared logic for date/time field-extraction UDFs (hour/minute/second).
 *
 * Inputs:
 *   - inputs(0): TimeStampMicroTZVector (for TimestampType) or TimeStampMicroVector (for
 *     TimestampNTZType) holding the timestamp column.
 *   - inputs(1): VarCharVector, length-1, holding the session timezone id. Used only for
 *     TimestampType. NTZ ignores it (wall-clock semantics).
 *
 * Output: IntVector of length `numRows` holding the extracted field. Null timestamps produce null
 * output.
 */
abstract class DateTimeFieldUDF extends CometUDF {

  protected def extract(dt: LocalDateTime): Int

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    require(
      inputs.length == 2,
      s"${getClass.getSimpleName} expects 2 inputs, got ${inputs.length}")
    val tsCol = inputs(0)
    val tzVec = inputs(1).asInstanceOf[VarCharVector]
    require(
      tzVec.getValueCount >= 1 && !tzVec.isNull(0),
      s"${getClass.getSimpleName} requires a non-null scalar timezone")
    val zone = ZoneId.of(new String(tzVec.get(0), StandardCharsets.UTF_8))

    val out = new IntVector(s"${getClass.getSimpleName}_result", CometArrowAllocator)
    out.allocateNew(numRows)

    tsCol match {
      case tz: TimeStampMicroTZVector =>
        var i = 0
        while (i < numRows) {
          if (tz.isNull(i)) {
            out.setNull(i)
          } else {
            val micros = tz.get(i)
            val instant = Instant.ofEpochSecond(
              Math.floorDiv(micros, 1000000L),
              Math.floorMod(micros, 1000000L) * 1000L)
            out.set(i, extract(LocalDateTime.ofInstant(instant, zone)))
          }
          i += 1
        }
      case ntz: TimeStampMicroVector =>
        var i = 0
        while (i < numRows) {
          if (ntz.isNull(i)) {
            out.setNull(i)
          } else {
            val micros = ntz.get(i)
            val seconds = Math.floorDiv(micros, 1000000L)
            val nanos = (Math.floorMod(micros, 1000000L) * 1000L).toInt
            val dt = LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC)
            out.set(i, extract(dt))
          }
          i += 1
        }
      case other =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName}: unsupported timestamp vector type: " +
            other.getClass.getName)
    }
    out.setValueCount(numRows)
    out
  }
}
