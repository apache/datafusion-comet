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
  private val intA = AttributeReference("c", IntegerType)()
  private val intB = AttributeReference("d", IntegerType)()

  private def assertUnchanged(expr: Expression): Unit = {
    // Reference equality, not `===`: the point of the guard is that no node is rebuilt.
    val promoted = DecimalPrecision.promote(expr)
    assert(promoted.eq(expr), s"expected promote to leave $expr untouched, got: $promoted")
  }

  test("promote leaves trees without decimal arithmetic untouched") {
    Seq(
      decA,
      Literal(1),
      Add(intA, intB),
      Multiply(intA, Cast(decA, IntegerType)),
      GreaterThan(decA, decB),
      Cast(Add(intA, intB), DecimalType(20, 4)),
      Coalesce(Seq(decA, decB)),
      CaseWhen(Seq((IsNotNull(decA), decA)), Some(decB)),
      Alias(Add(Cast(decA, LongType), Cast(decB, LongType)), "x")()).foreach(assertUnchanged)
  }

  test("promote wraps decimal arithmetic wherever it appears in the tree") {
    Seq[Expression](
      Add(decA, decB),
      Subtract(decA, decB),
      Multiply(decA, decB),
      Divide(decA, decB),
      Remainder(decA, decB)).foreach { arithmetic =>
      DecimalPrecision.promote(arithmetic) match {
        case CheckOverflow(child, dt, _) =>
          assert(child.eq(arithmetic))
          assert(dt === arithmetic.dataType)
        case other => fail(s"expected $arithmetic to be wrapped in CheckOverflow, got: $other")
      }

      // The guard has to inspect the whole tree, not just the root: a decimal arithmetic node
      // buried under expressions the rule does not rewrite must still be promoted.
      val buried = Cast(Coalesce(Seq(decA, arithmetic)), StringType)
      val promoted = DecimalPrecision.promote(buried)
      assert(!promoted.eq(buried))
      assert(promoted.collectFirst { case c: CheckOverflow => c }.isDefined)
    }
  }

  test("promote rewrites only the decimal arithmetic in a mixed tree") {
    val intAdd = Add(intA, intB)
    val decAdd = Add(decA, decB)
    val promoted = DecimalPrecision.promote(GreaterThan(Cast(intAdd, DecimalType(20, 2)), decAdd))
    val overflows = promoted.collect { case c: CheckOverflow => c }
    assert(overflows.length === 1)
    assert(overflows.head.child.eq(decAdd))
  }
}
