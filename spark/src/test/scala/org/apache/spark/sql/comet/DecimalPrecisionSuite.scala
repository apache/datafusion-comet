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

package org.apache.spark.sql.comet

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, CaseWhen, Cast, CheckOverflow, Coalesce, Divide, Expression, GreaterThan, IsNotNull, Literal, Multiply, Remainder, Subtract}
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType}

class DecimalPrecisionSuite extends SparkFunSuite {

  private val decA = AttributeReference("a", DecimalType(10, 2))()
  private val decB = AttributeReference("b", DecimalType(10, 2))()
  private val wideA = AttributeReference("wa", DecimalType(38, 18))()
  private val wideB = AttributeReference("wb", DecimalType(38, 18))()
  private val intA = AttributeReference("c", IntegerType)()
  private val intB = AttributeReference("d", IntegerType)()

  private def assertUnchanged(expr: Expression): Unit = {
    // Reference equality, not `===`: the point of the guard is that no node is rebuilt.
    val promoted = DecimalPrecision.promote(expr)
    assert(promoted.eq(expr), s"expected promote to leave $expr untouched, got: $promoted")
  }

  test("promote leaves trees that need no wrapper untouched") {
    Seq(
      decA,
      Literal(1),
      Add(intA, intB),
      Multiply(intA, Cast(decA, IntegerType)),
      GreaterThan(decA, decB),
      Cast(Add(intA, intB), DecimalType(20, 4)),
      Coalesce(Seq(decA, decB)),
      CaseWhen(Seq((IsNotNull(decA), decA)), Some(decB)),
      Alias(Add(Cast(decA, LongType), Cast(decB, LongType)), "x")(),
      // Narrow-path decimal arithmetic no longer needs a wrapper (issue #7659).
      Add(decA, decB),
      Subtract(decA, decB),
      Multiply(decA, decB)).foreach(assertUnchanged)
  }

  test("promote wraps decimal arithmetic wherever it appears in the tree") {
    Seq[Expression](
      Add(wideA, wideB),
      Subtract(wideA, wideB),
      Multiply(wideA, wideB),
      Divide(wideA, wideB),
      Remainder(wideA, wideB)).foreach { arithmetic =>
      DecimalPrecision.promote(arithmetic) match {
        case CheckOverflow(child, dt, _) =>
          assert(child.eq(arithmetic))
          assert(dt === arithmetic.dataType)
        case other => fail(s"expected $arithmetic to be wrapped in CheckOverflow, got: $other")
      }

      // The guard has to inspect the whole tree, not just the root: a decimal arithmetic node
      // buried under expressions the rule does not rewrite must still be promoted.
      val buried = Cast(Coalesce(Seq(wideA, arithmetic)), StringType)
      val promoted = DecimalPrecision.promote(buried)
      assert(!promoted.eq(buried))
      assert(promoted.collectFirst { case c: CheckOverflow => c }.isDefined)
    }
  }

  test("promote skips CheckOverflow when the narrow Decimal128 result type is exact") {
    // For + and -, the planner width is max(s1, s2) + max(p1 - s1, p2 - s2):
    // 37 keeps the operation on Decimal128, 38 takes the wide path.
    val narrow = AttributeReference("narrow", DecimalType(37, 18))()
    val wide = AttributeReference("wide", DecimalType(38, 18))()
    val one = AttributeReference("one", DecimalType(1, 0))()
    // For *, the planner width is p1 + p2: 37 is narrow, 38 is wide.
    val eighteen = AttributeReference("eighteen", DecimalType(18, 0))()
    val nineteen = AttributeReference("nineteen", DecimalType(19, 0))()

    Seq[Expression](Add(narrow, one), Subtract(narrow, one), Multiply(eighteen, nineteen))
      .foreach { arithmetic =>
        val promoted = DecimalPrecision.promote(arithmetic)
        assert(promoted.eq(arithmetic), s"expected $arithmetic to stay unwrapped, got $promoted")
      }

    Seq[Expression](Add(wide, one), Subtract(wide, one), Multiply(nineteen, nineteen))
      .foreach { arithmetic =>
        DecimalPrecision.promote(arithmetic) match {
          case CheckOverflow(child, dt, _) =>
            assert(child.eq(arithmetic))
            assert(dt === arithmetic.dataType)
          case other => fail(s"expected $arithmetic to be wrapped, got $other")
        }
      }
  }

  test("promote rewrites only the decimal arithmetic in a mixed tree") {
    val intAdd = Add(intA, intB)
    val decAdd = Add(wideA, wideB)
    val promoted = DecimalPrecision.promote(GreaterThan(Cast(intAdd, DecimalType(20, 2)), decAdd))
    val overflows = promoted.collect { case c: CheckOverflow => c }
    assert(overflows.length === 1)
    assert(overflows.head.child.eq(decAdd))
  }
}
