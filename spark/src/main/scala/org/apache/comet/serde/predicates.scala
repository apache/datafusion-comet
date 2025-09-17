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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, InSet, IsNaN, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or}
import org.apache.spark.sql.types.BooleanType

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde._

object CometNot extends CometExpressionSerde[Not] {
  override def convert(
      expr: Not,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {

    expr.child match {
      case expr: EqualTo =>
        createBinaryExpr(
          expr,
          expr.left,
          expr.right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setNeq(binaryExpr))
      case expr: EqualNullSafe =>
        createBinaryExpr(
          expr,
          expr.left,
          expr.right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setNeqNullSafe(binaryExpr))
      case expr: In =>
        ComparisonUtils.in(expr, expr.value, expr.list, inputs, binding, negate = true)
      case _ =>
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
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
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
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setOr(binaryExpr))
  }
}

object CometEqualTo extends CometExpressionSerde[EqualTo] {
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

object CometEqualNullSafe extends CometExpressionSerde[EqualNullSafe] {
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

object CometGreaterThan extends CometExpressionSerde[GreaterThan] {
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

object CometGreaterThanOrEqual extends CometExpressionSerde[GreaterThanOrEqual] {
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

object CometLessThan extends CometExpressionSerde[LessThan] {
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

object CometLessThanOrEqual extends CometExpressionSerde[LessThanOrEqual] {
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
    val optExpr = scalarFunctionExprToProtoWithReturnType("isnan", BooleanType, childExpr)

    optExprWithInfo(optExpr, expr, expr.child)
  }
}

object CometIn extends CometExpressionSerde[In] {
  override def convert(
      expr: In,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    ComparisonUtils.in(expr, expr.value, expr.list, inputs, binding, negate = false)
  }
}

object CometInSet extends CometExpressionSerde[InSet] {
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

object ComparisonUtils {

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
      withInfo(expr, allExprs: _*)
      None
    }
  }
}
