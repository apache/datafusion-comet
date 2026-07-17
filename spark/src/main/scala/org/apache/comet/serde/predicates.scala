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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BinaryExpression, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, InSet, IsNaN, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or}
import org.apache.spark.sql.types.BooleanType

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde._

object CometNot extends CometExpressionSerde[Not] {
  override def convert(
      expr: Not,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {

    import ComparisonUtils.hasCollatedOperand

    expr.child match {
      case inner: EqualTo if !hasCollatedOperand(inner.left, inner.right) =>
        createBinaryExpr(
          inner,
          inner.left,
          inner.right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setNeq(binaryExpr))
      case inner: EqualNullSafe if !hasCollatedOperand(inner.left, inner.right) =>
        createBinaryExpr(
          inner,
          inner.left,
          inner.right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setNeqNullSafe(binaryExpr))
      case inner: In if !hasCollatedOperand((inner.value +: inner.list): _*) =>
        ComparisonUtils.in(inner, inner.value, inner.list, inputs, binding, negate = true)
      case _ =>
        // Includes the collated variants of EqualTo / EqualNullSafe / In above: fall through so
        // the child expression's own serde is consulted, which now returns `Unsupported` for
        // non-UTF8_BINARY operands (see `CometEqualTo.getSupportLevel` and siblings). That makes
        // `exprToProtoInternal` return None for the child, which cascades this Not to None and
        // falls the enclosing operator back to Spark — the only way to honour collation-aware
        // (in)equality without a native path.
        createUnaryExpr(
          expr,
          expr.child,
          inputs,
          binding,
          (builder, unaryExpr) => builder.setNot(unaryExpr))
    }
  }
}

object CometAnd extends CometExpressionSerde[And] {
  override def convert(
      expr: And,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Rebalance the (associative) AND chain so deep `a AND b AND ...` predicates produce a
    // shallow proto instead of a left-deep one that overflows protobuf's recursion limit when
    // the plan is re-parsed (see createBalancedBinaryExpr).
    val operands = flattenAssociative(
      expr,
      { case _: And => true; case _ => false },
      { case a: And => (a.left, a.right) })
    createBalancedBinaryExpr(
      expr,
      operands,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setAnd(binaryExpr))
  }
}

object CometOr extends CometExpressionSerde[Or] {
  override def convert(
      expr: Or,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val operands = flattenAssociative(
      expr,
      { case _: Or => true; case _ => false },
      { case o: Or => (o.left, o.right) })
    createBalancedBinaryExpr(
      expr,
      operands,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setOr(binaryExpr))
  }
}

object CometEqualTo extends CollationAwareBinaryPredicate[EqualTo] {
  override def convert(
      expr: EqualTo,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setEq(binaryExpr))
  }
}

object CometEqualNullSafe extends CollationAwareBinaryPredicate[EqualNullSafe] {
  override def convert(
      expr: EqualNullSafe,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setEqNullSafe(binaryExpr))
  }
}

object CometGreaterThan extends CollationAwareBinaryPredicate[GreaterThan] {
  override def convert(
      expr: GreaterThan,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setGt(binaryExpr))
  }
}

object CometGreaterThanOrEqual extends CollationAwareBinaryPredicate[GreaterThanOrEqual] {
  override def convert(
      expr: GreaterThanOrEqual,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setGtEq(binaryExpr))
  }
}

object CometLessThan extends CollationAwareBinaryPredicate[LessThan] {
  override def convert(
      expr: LessThan,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setLt(binaryExpr))
  }
}

object CometLessThanOrEqual extends CollationAwareBinaryPredicate[LessThanOrEqual] {
  override def convert(
      expr: LessThanOrEqual,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setLtEq(binaryExpr))
  }
}

object CometIsNull extends CometExpressionSerde[IsNull] {
  override def convert(
      expr: IsNull,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createUnaryExpr(
      expr,
      expr.child,
      inputs,
      binding,
      (builder, unaryExpr) => builder.setIsNull(unaryExpr))
  }
}

object CometIsNotNull extends CometExpressionSerde[IsNotNull] {
  override def convert(
      expr: IsNotNull,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createUnaryExpr(
      expr,
      expr.child,
      inputs,
      binding,
      (builder, unaryExpr) => builder.setIsNotNull(unaryExpr))
  }
}

object CometIsNaN extends CometExpressionSerde[IsNaN] {
  override def convert(
      expr: IsNaN,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val optExpr = scalarFunctionExprToProtoWithReturnType("isnan", BooleanType, false, childExpr)

    optExprWithFallbackReason(optExpr, expr, expr.child)
  }
}

object CometIn extends CometExpressionSerde[In] with CodegenDispatchFallback {

  override def getSupportLevel(expr: In): SupportLevel =
    ComparisonUtils.collationSupportLevel("In", (expr.value +: expr.list): _*)

  override def getUnsupportedReasons(): Seq[String] =
    Seq(ComparisonUtils.nonDefaultCollationDocReason)

  override def convert(
      expr: In,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    ComparisonUtils.in(expr, expr.value, expr.list, inputs, binding, negate = false)
  }
}

object CometInSet extends CometExpressionSerde[InSet] with CodegenDispatchFallback {

  override def getSupportLevel(expr: InSet): SupportLevel =
    ComparisonUtils.collationSupportLevel("InSet", expr.child)

  override def getUnsupportedReasons(): Seq[String] =
    Seq(ComparisonUtils.nonDefaultCollationDocReason)

  override def convert(
      expr: InSet,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val valueDataType = expr.child.dataType
    val list = expr.hset.map { setVal =>
      Literal(setVal, valueDataType)
    }.toSeq
    // Change `InSet` to `In` expression
    // We do Spark `InSet` optimization in native (DataFusion) side.
    ComparisonUtils.in(expr, expr.child, list, inputs, binding, negate = false)
  }
}

/**
 * Mixin for serdes of binary predicates whose native kernel compares raw bytes: any operand
 * carrying a non-UTF8_BINARY collation is unsupported natively. Mixing in
 * `CodegenDispatchFallback` routes these through the JVM codegen dispatcher (Spark's own
 * `doGenCode`), so the collation-aware evaluation runs inline in the Comet pipeline instead of
 * falling the whole operator back to Spark.
 */
trait CollationAwareBinaryPredicate[T <: BinaryExpression]
    extends CometExpressionSerde[T]
    with CodegenDispatchFallback {
  override def getSupportLevel(expr: T): SupportLevel =
    ComparisonUtils.collationSupportLevel(expr.prettyName, expr.left, expr.right)

  override def getUnsupportedReasons(): Seq[String] =
    Seq(ComparisonUtils.nonDefaultCollationDocReason)
}

object ComparisonUtils {

  // Comet's native equality/ordering/hashing compare raw bytes, so any predicate operand carrying
  // a non-UTF8_BINARY collation (Spark 4+) would produce wrong answers on the native path — e.g.
  // `'a' = 'A'` under `UNICODE_CI` returns true in Spark but false byte-wise. Every binary
  // comparison and `In`/`InSet` serde routes its `getSupportLevel` through here, marking the case
  // `Unsupported`; `CodegenDispatchFallback` then routes those cases through Spark's own
  // `doGenCode` inside the Comet pipeline. `hasNonDefaultStringCollation` walks nested types too,
  // so collated strings inside array/map/struct operands are also caught.
  def nonDefaultCollationReason(exprName: String): String =
    s"$exprName does not support non-UTF8_BINARY collated operands; " +
      "native comparison is byte-wise and cannot honour collation semantics."

  // Doc-friendly variant of `nonDefaultCollationReason` for `getUnsupportedReasons`. The compat
  // guide already scopes the reason to a specific expression, so no per-expr name is needed.
  val nonDefaultCollationDocReason: String =
    "Non-UTF8_BINARY collated operands are routed through the JVM codegen dispatcher " +
      "(Spark's own `doGenCode`) because native comparison is byte-wise."

  def hasCollatedOperand(operands: Expression*): Boolean =
    operands.exists(op => hasNonDefaultStringCollation(op.dataType))

  def collationSupportLevel(exprName: String, operands: Expression*): SupportLevel =
    if (hasCollatedOperand(operands: _*)) {
      Unsupported(Some(nonDefaultCollationReason(exprName)))
    } else {
      Compatible()
    }

  def in(
      expr: Expression,
      value: Expression,
      list: Seq[Expression],
      inputs: Seq[Attribute],
      binding: Boolean,
      negate: Boolean): Option[Expr] = {
    val valueExpr = exprToProtoInternal(value, inputs, binding)
    val listExprs = list.map(exprToProtoInternal(_, inputs, binding))
    if (valueExpr.isDefined && listExprs.forall(_.isDefined)) {
      val builder = ExprOuterClass.In.newBuilder()
      builder.setInValue(valueExpr.get)
      builder.addAllLists(listExprs.map(_.get).asJava)
      builder.setNegated(negate)
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setIn(builder)
          .build())
    } else {
      val allExprs = list ++ Seq(value)
      withFallbackReason(expr, allExprs: _*)
      None
    }
  }
}
