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

package org.apache.comet.serde

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types._

import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, isSpark41Plus}
import org.apache.comet.serde.QueryPlanSerde.supportedDataType

class QueryPlanSerdeSuite extends AnyFunSuite {

  test("supportedDataType matches each caller boundary") {
    val complex = ArrayType(IntegerType)
    val nestedInterval = StructType(
      Seq(
        StructField(
          "i",
          ArrayType(
            MapType(StringType, DayTimeIntervalType(), valueContainsNull = true),
            containsNull = true))))
    val nestedCalendarInterval =
      StructType(Seq(StructField("i", ArrayType(CalendarIntervalType, containsNull = true))))
    val duplicateFields =
      ArrayType(StructType(Seq(StructField("i", IntegerType), StructField("i", IntegerType))))
    val emptyStruct = StructType(Nil)
    val timeTypes = if (isSpark41Plus) Seq(DataType.fromDDL("TIME")) else Seq.empty
    val collatedStrings =
      if (isSpark40Plus) Seq(DataType.fromDDL("STRING COLLATE UTF8_LCASE")) else Seq.empty

    val boundaries: Seq[(String, DataType => Boolean, Seq[DataType], Seq[DataType])] = Seq(
      (
        "expression serde defaults",
        supportedDataType(_),
        Seq(IntegerType, StringType, CalendarIntervalType) ++ timeTypes ++ collatedStrings,
        Seq(complex, YearMonthIntervalType(), DayTimeIntervalType(), emptyStruct)),
      (
        "CometSink",
        supportedDataType(_, allowComplex = true, allowIntervals = true),
        Seq(complex, nestedInterval, nestedCalendarInterval, duplicateFields) ++
          timeTypes ++ collatedStrings,
        Seq(emptyStruct)),
      (
        "CometLocalTableScanExec",
        supportedDataType(
          _,
          allowComplex = true,
          allowIntervals = true,
          allowTimeType = false,
          allowAnyStringType = false),
        Seq(
          IntegerType,
          StringType,
          complex,
          nestedInterval,
          nestedCalendarInterval,
          duplicateFields),
        Seq(emptyStruct) ++ timeTypes ++ collatedStrings),
      (
        "native shuffle",
        supportedDataType(_, allowComplex = true, allowIntervals = true),
        Seq(complex, nestedInterval, nestedCalendarInterval, duplicateFields) ++
          timeTypes ++ collatedStrings,
        Seq(emptyStruct)),
      (
        "JVM columnar shuffle",
        supportedDataType(
          _,
          allowComplex = true,
          allowCalendarInterval = false,
          allowDuplicateStructFieldNames = false),
        Seq(IntegerType, StringType, complex) ++ timeTypes ++ collatedStrings,
        Seq(
          YearMonthIntervalType(),
          DayTimeIntervalType(),
          CalendarIntervalType,
          nestedCalendarInterval,
          duplicateFields,
          emptyStruct)))

    boundaries.foreach { case (name, supports, accepted, rejected) =>
      accepted.foreach(dt => assert(supports(dt), s"$name should accept $dt"))
      rejected.foreach(dt => assert(!supports(dt), s"$name should reject $dt"))
    }
  }
}
