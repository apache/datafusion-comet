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

import org.apache.spark.sql.catalyst.expressions.{ArrayContains, ArrayPosition, AttributeReference, Cast, Divide}
import org.apache.spark.sql.comet.DecimalPrecision
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, isSpark41Plus}
import org.apache.comet.serde.QueryPlanSerde.{canShortCircuitNulls, canSkipEvaluation, supportedDataType, withSharedSubexpressions}

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
          allowAnyStringType = false),
        Seq(
          IntegerType,
          StringType,
          complex,
          nestedInterval,
          nestedCalendarInterval,
          duplicateFields) ++ timeTypes,
        Seq(emptyStruct) ++ collatedStrings),
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

  test("canShortCircuitNulls skips an argument only where the shared subexpressions are known") {
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val value = AttributeReference("i", IntegerType)()
    val cast = Cast(AttributeReference("s", StringType)(), IntegerType)
    val contains = ArrayContains(array, cast)
    val position = ArrayPosition(array, cast)

    // A column is never shared, so it can be skipped anywhere
    assert(canShortCircuitNulls(ArrayContains(array, value)))
    // Something serialized outside an operator's conversion might share the cast
    assert(!canShortCircuitNulls(contains))
    withSharedSubexpressions(Seq(contains)) {
      assert(canShortCircuitNulls(contains))
    }
    withSharedSubexpressions(Seq(contains, position)) {
      assert(!canShortCircuitNulls(contains))
      assert(!canShortCircuitNulls(position))
    }
  }

  test("canShortCircuitNulls follows subexpression elimination except in aggregates") {
    val cast = Cast(AttributeReference("s", StringType)(), IntegerType)
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val contains = ArrayContains(array, cast)
    val shared = Seq(contains, ArrayPosition(array, cast))

    def withSubexpressionElimination(enabled: Boolean)(f: => Unit): Unit = {
      val conf = new SQLConf
      conf.setConf(SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED, enabled)
      SQLConf.withExistingConf(conf) {
        assert(SQLConf.get.subexpressionEliminationEnabled == enabled)
        f
      }
    }

    withSubexpressionElimination(enabled = false) {
      withSharedSubexpressions(shared) {
        assert(canShortCircuitNulls(contains))
      }
      withSharedSubexpressions(shared, alwaysShared = true) {
        assert(!canShortCircuitNulls(contains))
      }
      // Outside an operator's conversion what they share is still unknown
      assert(!canShortCircuitNulls(contains))
    }
  }

  test("canSkipEvaluation recognizes shared subexpressions across decimal promotion") {
    val left = AttributeReference("a", DecimalType(10, 0))()
    val right = AttributeReference("b", DecimalType(10, 0))()
    val cast = Cast(Divide(left, right), IntegerType)
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val contains = ArrayContains(array, cast)
    val position = ArrayPosition(array, cast)
    val promoted = DecimalPrecision.promote(cast)
    assert(!cast.semanticEquals(promoted))

    for (enabled <- Seq(false, true); alwaysShared <- Seq(false, true)) {
      val conf = new SQLConf
      conf.setConf(SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED, enabled)
      SQLConf.withExistingConf(conf) {
        withSharedSubexpressions(Seq(contains, position), alwaysShared) {
          val canSkip = !(enabled || alwaysShared)
          assert(canSkipEvaluation(Seq(cast)) == canSkip)
          assert(canSkipEvaluation(Seq(promoted)) == canSkip)
          assert(canShortCircuitNulls(contains) == canSkip)
          assert(canShortCircuitNulls(DecimalPrecision.promote(contains)) == canSkip)
        }
        withSharedSubexpressions(Seq(contains), alwaysShared) {
          assert(canSkipEvaluation(Seq(cast)))
          assert(canSkipEvaluation(Seq(promoted)))
        }
      }
    }
  }
}
