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

package org.apache.comet.codegen

import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{DataType, DateType, LongType}

import org.apache.comet.CometSparkSessionExtensions.isSpark41Plus

/**
 * Unit coverage for the generic `SpecializedGetters.get(ordinal, dataType)` dispatch the kernel
 * bases delegate to. Spark reaches it from `SafeProjection` (for ScalaUDF struct arguments) and
 * from every `CodegenFallback.eval(row)`, including higher-order functions, so its type surface
 * has to match what `CometBatchKernelCodegen.isSupportedDataType` admits at plan time. A type
 * accepted there but missing here throws at execute time, when there is no longer a fallback.
 */
class CometSpecializedGettersDispatchSuite extends AnyFunSuite {

  /** Minimal `CometInternalRow` over one fixed long, to drive the dispatch through `get`. */
  private class StubRow(value: Long, isNull: Boolean = false) extends CometInternalRow {
    override def numFields: Int = 1
    override def isNullAt(ordinal: Int): Boolean = isNull
    override def getLong(ordinal: Int): Long = value
    override def getInt(ordinal: Int): Int = value.toInt
  }

  /** Spark `DataType` for Arrow `Time(NANOSECOND, 64)`, resolved without a 4.1 compile dep. */
  private def timeType: DataType =
    Utils.fromArrowField(
      new Field(
        "t",
        FieldType.nullable(new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
        java.util.Collections.emptyList[Field]()))

  test("get dispatches TimeType through getLong (#5218)") {
    assume(isSpark41Plus, "TimeType requires Spark 4.1+")
    val row = new StubRow(45296000000000L) // 12:34:56
    assert(row.get(0, timeType) === java.lang.Long.valueOf(45296000000000L))
  }

  test("get returns null for a null TimeType slot") {
    assume(isSpark41Plus, "TimeType requires Spark 4.1+")
    val row = new StubRow(45296000000000L, isNull = true)
    assert(row.get(0, timeType) == null)
  }

  test("get still dispatches the long- and int-backed temporal types") {
    // Guards against the TimeType case being inserted ahead of a case it would shadow.
    val row = new StubRow(19723L) // 2024-01-31 as an epoch day
    assert(row.get(0, LongType) === java.lang.Long.valueOf(19723L))
    assert(row.get(0, DateType) === java.lang.Integer.valueOf(19723))
  }
}
