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

import org.apache.arrow.vector.{IntVector, TimeStampMicroTZVector, TimeStampMicroVector, VarCharVector}
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}

import org.apache.comet.CometArrowAllocator

class DateTimeFieldUDFSuite extends AnyFunSuite {

  private def tzVector(tz: String): VarCharVector = {
    val v = new VarCharVector("tz", CometArrowAllocator)
    v.allocateNew(1)
    v.setSafe(0, tz.getBytes(StandardCharsets.UTF_8))
    v.setValueCount(1)
    v
  }

  private def tsTzVector(tz: String, micros: Array[java.lang.Long]): TimeStampMicroTZVector = {
    val fieldType = new FieldType(true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, tz), null)
    val v = new TimeStampMicroTZVector("ts", fieldType, CometArrowAllocator)
    v.allocateNew(micros.length)
    for (i <- micros.indices) {
      if (micros(i) == null) v.setNull(i) else v.set(i, micros(i))
    }
    v.setValueCount(micros.length)
    v
  }

  private def tsNtzVector(micros: Array[java.lang.Long]): TimeStampMicroVector = {
    val v = new TimeStampMicroVector("ts_ntz", CometArrowAllocator)
    v.allocateNew(micros.length)
    for (i <- micros.indices) {
      if (micros(i) == null) v.setNull(i) else v.set(i, micros(i))
    }
    v.setValueCount(micros.length)
    v
  }

  test("HourUDF on TimestampType in UTC returns the UTC hour") {
    // 2024-06-15 12:34:56 UTC = 1718454896 seconds = 1718454896000000 micros
    val micros = 1718454896000000L
    val ts = tsTzVector("UTC", Array[java.lang.Long](micros))
    val tz = tzVector("UTC")
    val udf = new HourUDF
    val out = udf.evaluate(Array(ts, tz), 1).asInstanceOf[IntVector]
    assert(out.getValueCount == 1)
    assert(out.get(0) == 12)
  }

  test("HourUDF on TimestampType in America/Los_Angeles applies zone shift") {
    val micros = 1718454896000000L // 2024-06-15 12:34:56 UTC = 05:34:56 PDT
    val ts = tsTzVector("UTC", Array[java.lang.Long](micros))
    val tz = tzVector("America/Los_Angeles")
    val udf = new HourUDF
    val out = udf.evaluate(Array(ts, tz), 1).asInstanceOf[IntVector]
    assert(out.get(0) == 5)
  }

  test("HourUDF on TimestampNTZType ignores the timezone arg") {
    // 2024-06-15 12:34:56 (NTZ wall-clock) = 1718454896000000 micros
    val micros = 1718454896000000L
    val ts = tsNtzVector(Array[java.lang.Long](micros))
    val tz = tzVector("Asia/Tokyo") // should NOT shift
    val udf = new HourUDF
    val out = udf.evaluate(Array(ts, tz), 1).asInstanceOf[IntVector]
    assert(out.get(0) == 12)
  }

  test("HourUDF preserves nulls") {
    val ts = tsNtzVector(Array[java.lang.Long](null, 1718454896000000L, null))
    val tz = tzVector("UTC")
    val udf = new HourUDF
    val out = udf.evaluate(Array(ts, tz), 3).asInstanceOf[IntVector]
    assert(out.isNull(0))
    assert(out.get(1) == 12)
    assert(out.isNull(2))
  }

  test("MinuteUDF on TimestampType in UTC") {
    val micros = 1718454896000000L // 2024-06-15 12:34:56 UTC
    val ts = tsTzVector("UTC", Array[java.lang.Long](micros))
    val tz = tzVector("UTC")
    val udf = new MinuteUDF
    val out = udf.evaluate(Array(ts, tz), 1).asInstanceOf[IntVector]
    assert(out.get(0) == 34)
  }

  test("MinuteUDF on TimestampNTZType") {
    val micros = 1718454896000000L // 2024-06-15 12:34:56 NTZ
    val ts = tsNtzVector(Array[java.lang.Long](micros))
    val tz = tzVector("UTC")
    val udf = new MinuteUDF
    val out = udf.evaluate(Array(ts, tz), 1).asInstanceOf[IntVector]
    assert(out.get(0) == 34)
  }
}
