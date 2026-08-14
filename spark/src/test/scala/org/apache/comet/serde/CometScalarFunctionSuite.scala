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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{Abs, Cos, Expression, Literal, Unevaluable}
import org.apache.spark.sql.types.{DataType, IntegerType}

import org.apache.comet.{CometExplainInfo, CometSparkSessionExtensions}

/**
 * Synthetic expression whose constructor declares `evalMode`, used to prove class-level detection
 * without depending on Spark-version-specific arithmetic field names.
 */
case class TestEvalModeExpression(child: Expression, evalMode: Boolean)
    extends Expression
    with Unevaluable {
  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = IntegerType
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(child = newChildren.head)
}

/**
 * Synthetic expression whose constructor declares `nullOnOverflow`, matching markers such as
 * Spark's `MakeDecimal`.
 */
case class TestNullOnOverflowExpression(child: Expression, nullOnOverflow: Boolean)
    extends Expression
    with Unevaluable {
  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = IntegerType
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(child = newChildren.head)
}

class CometScalarFunctionSuite extends CometTestBase {

  private def fallbackReasons(expr: Expression): Set[String] = {
    expr.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty)
  }

  private def assertRejectReason(expr: Expression, expectedTokens: String*): Unit = {
    val reasons = fallbackReasons(expr)
    assert(reasons.nonEmpty, s"expected fallback reason on ${expr.nodeName}")
    val joined = reasons.mkString(" ")
    expectedTokens.foreach { token =>
      assert(joined.contains(token), s"expected '$token' in fallback reasons: $reasons")
    }
  }

  test("CometScalarFunction rejects ANSI-sensitive expressions (#5074)") {
    val abs = Abs(Literal(1), failOnError = true)
    val result = CometScalarFunction[Abs]("abs").convert(abs, Seq.empty, binding = true)
    assert(result.isEmpty)
    assertRejectReason(abs, abs.nodeName, "CometScalarFunction", "failOnError")
  }

  test("rejects expressions with failOnError even when false (#5074)") {
    val abs = Abs(Literal(1), failOnError = false)
    val result = CometScalarFunction[Abs]("abs").convert(abs, Seq.empty, binding = true)
    assert(result.isEmpty)
    assertRejectReason(abs, abs.nodeName, "CometScalarFunction", "failOnError")
  }

  test("isAnsiSensitive detects failOnError field") {
    assert(CometScalarFunction.isAnsiSensitive(Abs(Literal(1), failOnError = false)))
    assert(CometScalarFunction.isAnsiSensitive(classOf[Abs]))
    assert(!CometScalarFunction.isAnsiSensitive(Cos(Literal(0.0))))
    assert(!CometScalarFunction.isAnsiSensitive(classOf[Cos]))
  }

  test("isAnsiSensitive detects evalMode field") {
    val ansi = TestEvalModeExpression(Literal(1), evalMode = true)
    val legacy = TestEvalModeExpression(Literal(1), evalMode = false)
    assert(CometScalarFunction.isAnsiSensitive(ansi))
    assert(CometScalarFunction.isAnsiSensitive(legacy))
    assert(CometScalarFunction.isAnsiSensitive(classOf[TestEvalModeExpression]))
  }

  test("isAnsiSensitive detects nullOnOverflow field") {
    val nullOnOverflow = TestNullOnOverflowExpression(Literal(1), nullOnOverflow = true)
    val failOnOverflow = TestNullOnOverflowExpression(Literal(1), nullOnOverflow = false)
    assert(CometScalarFunction.isAnsiSensitive(nullOnOverflow))
    assert(CometScalarFunction.isAnsiSensitive(failOnOverflow))
    assert(CometScalarFunction.isAnsiSensitive(classOf[TestNullOnOverflowExpression]))
  }

  test("rejects expressions with evalMode via plain CometScalarFunction (#5074)") {
    val ansi = TestEvalModeExpression(Literal(1), evalMode = true)
    val legacy = TestEvalModeExpression(Literal(1), evalMode = false)
    assert(
      CometScalarFunction[TestEvalModeExpression]("test")
        .convert(ansi, Seq.empty, binding = true)
        .isEmpty)
    assert(
      CometScalarFunction[TestEvalModeExpression]("test")
        .convert(legacy, Seq.empty, binding = true)
        .isEmpty)
    assertRejectReason(ansi, "CometScalarFunction", "evalMode")
    assertRejectReason(legacy, "CometScalarFunction", "evalMode")
  }

  test("rejects expressions with nullOnOverflow via plain CometScalarFunction (#5074)") {
    val nullOnOverflow = TestNullOnOverflowExpression(Literal(1), nullOnOverflow = true)
    val failOnOverflow = TestNullOnOverflowExpression(Literal(1), nullOnOverflow = false)
    assert(
      CometScalarFunction[TestNullOnOverflowExpression]("test")
        .convert(nullOnOverflow, Seq.empty, binding = true)
        .isEmpty)
    assert(
      CometScalarFunction[TestNullOnOverflowExpression]("test")
        .convert(failOnOverflow, Seq.empty, binding = true)
        .isEmpty)
    assertRejectReason(nullOnOverflow, "CometScalarFunction", "nullOnOverflow")
    assertRejectReason(failOnOverflow, "CometScalarFunction", "nullOnOverflow")
  }

  test("CometScalarFunction allows non-ANSI expressions") {
    val cos = Cos(Literal(0.0))
    val result = CometScalarFunction[Cos]("cos").convert(cos, Seq.empty, binding = true)
    assert(result.isDefined)
    val proto = result.get
    assert(proto.hasScalarFunc)
    assert(proto.getScalarFunc.getFunc === "cos")
    assert(!proto.getScalarFunc.getFailOnError)
    assert(proto.getScalarFunc.getArgsCount === 1)
    assert(!CometSparkSessionExtensions.hasFallbackReason(cos))
  }

  test("no ANSI-sensitive expression is registered with plain CometScalarFunction") {
    val violations = QueryPlanSerde.exprSerdeMap
      .collect {
        case (sparkClass, _: CometScalarFunction[_])
            if CometScalarFunction.isAnsiSensitive(sparkClass) =>
          sparkClass.getName
      }
      .toSeq
      .sorted
    assert(
      violations.isEmpty,
      "ANSI-sensitive expressions use plain CometScalarFunction: " +
        violations.mkString(", "))
  }
}
