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

import scala.util.Try

import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types._

import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, isSpark41Plus}
import org.apache.comet.serde.literals.CometLiteral
import org.apache.comet.shims.CometTypeShim

/**
 * Pins `CometLiteral.listLiteralElementSupported` against the arms of
 * `CometLiteral.makeListLiteral`. The gate exists only to keep an element type the encoder has no
 * arm for from reaching it, so a disagreement between the two is not a fallback: the missing arm
 * raises a `MatchError` in the middle of planning, which Spark surfaces as `[INTERNAL_ERROR] The
 * Spark SQL phase planning failed with an internal error`.
 */
class CometLiteralSuite extends CometTestBase with CometTypeShim {

  /**
   * Element types the two sides are compared over. Both halves of the list matter. A type the
   * encoder handles today that drops out of the gate silently costs a native projection; a type
   * the gate admits that the encoder has no arm for is the planning failure above. Add an entry
   * whenever `makeListLiteral` gains an arm, or when a new Spark release adds a type that a
   * folded array literal can carry.
   */
  private val probeElementTypes: Seq[DataType] = Seq(
    NullType,
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DateType,
    TimestampType,
    TimestampNTZType,
    StringType,
    BinaryType,
    DecimalType(10, 2),
    CalendarIntervalType,
    DayTimeIntervalType(),
    YearMonthIntervalType(),
    StructType(Seq(StructField("a", IntegerType))),
    MapType(IntegerType, IntegerType)) ++ variantType.toSeq ++ probeTimeType.toSeq

  /**
   * Spark `DataType` for Arrow `Time(NANOSECOND, 64)`, resolved without a 4.1 compile dep, the
   * way `CometSpecializedGettersDispatchSuite` does it. `convert` has an `isTimeType` arm while
   * `makeListLiteral` has none, so this is exactly the asymmetry the pin exists to catch.
   */
  private def probeTimeType: Option[DataType] =
    if (!isSpark41Plus) None
    else
      Some(
        Utils.fromArrowField(
          new Field(
            "t",
            FieldType.nullable(new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
            java.util.Collections.emptyList[Field]())))

  /**
   * Whether `makeListLiteral` has an arm for `arrayType`'s element type. The probe arrays are
   * empty, so every arm's per-element conversion is skipped and the only thing under test is
   * whether the match falls through to a `MatchError`.
   */
  private def encoderAcceptsElement(array: Array[Any], arrayType: ArrayType): Boolean =
    Try(CometLiteral.makeListLiteral(array, arrayType)).isSuccess

  test("listLiteralElementSupported mirrors makeListLiteral's arms, flat and nested") {
    probeElementTypes.foreach { dt =>
      withClue(s"element type $dt: ") {
        assert(
          CometLiteral.listLiteralElementSupported(dt) ===
            encoderAcceptsElement(Array.empty, ArrayType(dt)))
      }
      // `makeListLiteral` recurses per element rather than on the type, so an empty outer array
      // would never reach the inner type. One empty inner array forces the recursion.
      withClue(s"element type ARRAY<$dt>: ") {
        assert(
          CometLiteral.listLiteralElementSupported(ArrayType(dt)) ===
            encoderAcceptsElement(
              Array[Any](new GenericArrayData(Array.empty[Any])),
              ArrayType(ArrayType(dt))))
      }
    }
  }

  // `makeListLiteral` matches `StringType` as a stable identifier, whose `equals` compares
  // `collationId`, so a collated string has no arm and the gate has to decline it. Loosening
  // either side alone to `_: StringType` would reintroduce the planning failure (gate) or encode
  // the value under the wrong collation (encoder). Built through SQL because `StringType(id)` does
  // not compile against Spark 3.x.
  test("a non-default collation is declined on both sides") {
    assume(isSpark40Plus, "COLLATE requires Spark 4.0")
    val collated = spark.sql("SELECT 'a' COLLATE UTF8_LCASE").schema.head.dataType
    assert(collated !== StringType)
    assert(!CometLiteral.listLiteralElementSupported(collated))
    assert(!encoderAcceptsElement(Array.empty, ArrayType(collated)))
  }
}
