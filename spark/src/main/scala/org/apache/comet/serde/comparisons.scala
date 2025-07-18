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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GreaterThan, GreaterThanOrEqual, In, IsNaN, IsNotNull, IsNull, LessThan, LessThanOrEqual}
import org.apache.spark.sql.types.BooleanType

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, scalarFunctionExprToProtoWithReturnType}

object CometGreaterThan extends CometExpressionSerde with ComparisonBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val greaterThan = expr.asInstanceOf[GreaterThan]

    createBinaryExpr(
      expr,
      greaterThan.left,
      greaterThan.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setGt(binaryExpr))
  }
}

object CometGreaterThanOrEqual extends CometExpressionSerde with ComparisonBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val greaterThanOrEqual = expr.asInstanceOf[GreaterThanOrEqual]

    createBinaryExpr(
      expr,
      greaterThanOrEqual.left,
      greaterThanOrEqual.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setGtEq(binaryExpr))
  }
}

object CometLessThan extends CometExpressionSerde with ComparisonBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val lessThan = expr.asInstanceOf[LessThan]

    createBinaryExpr(
      expr,
      lessThan.left,
      lessThan.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setLt(binaryExpr))
  }
}

object CometLessThanOrEqual extends CometExpressionSerde with ComparisonBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val lessThanOrEqual = expr.asInstanceOf[LessThanOrEqual]

    createBinaryExpr(
      expr,
      lessThanOrEqual.left,
      lessThanOrEqual.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setLtEq(binaryExpr))
  }
}

object CometIsNull extends CometExpressionSerde with ComparisonBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val isNull = expr.asInstanceOf[IsNull]

    createUnaryExpr(
      expr,
      isNull.child,
      inputs,
      binding,
      (builder, unaryExpr) => builder.setIsNull(unaryExpr))
  }
}

object CometIsNotNull extends CometExpressionSerde with ComparisonBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val isNotNull = expr.asInstanceOf[IsNotNull]

    createUnaryExpr(
      expr,
      isNotNull.child,
      inputs,
      binding,
      (builder, unaryExpr) => builder.setIsNotNull(unaryExpr))
  }
}

object CometIsNaN extends CometExpressionSerde with ComparisonBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val isNaN = expr.asInstanceOf[IsNaN]
    val childExpr = exprToProtoInternal(isNaN.child, inputs, binding)
    val optExpr = scalarFunctionExprToProtoWithReturnType("isnan", BooleanType, childExpr)

    optExprWithInfo(optExpr, expr, isNaN.child)
  }
}

object CometIn extends CometExpressionSerde with ComparisonBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val inExpr = expr.asInstanceOf[In]
    in(expr, inExpr.value, inExpr.list, inputs, binding, negate = false)
  }
}

sealed trait ComparisonBase {

  /**
   * Creates a UnaryExpr by calling exprToProtoInternal for the provided child expression and then
   * invokes the supplied function to wrap this UnaryExpr in a top-level Expr.
   *
   * @param child
   *   Spark expression
   * @param inputs
   *   Inputs to the expression
   * @param f
   *   Function that accepts an Expr.Builder and a UnaryExpr and builds the specific top-level
   *   Expr
   * @return
   *   Some(Expr) or None if not supported
   */
  def createUnaryExpr(
      expr: Expression,
      child: Expression,
      inputs: Seq[Attribute],
      binding: Boolean,
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.UnaryExpr) => ExprOuterClass.Expr.Builder)
      : Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(child, inputs, binding) // TODO review
    if (childExpr.isDefined) {
      // create the generic UnaryExpr message
      val inner = ExprOuterClass.UnaryExpr
        .newBuilder()
        .setChild(childExpr.get)
        .build()
      // call the user-supplied function to wrap UnaryExpr in a top-level Expr
      // such as Expr.IsNull or Expr.IsNotNull
      Some(
        f(
          ExprOuterClass.Expr
            .newBuilder(),
          inner).build())
    } else {
      withInfo(expr, child)
      None
    }
  }

  def createBinaryExpr(
      expr: Expression,
      left: Expression,
      right: Expression,
      inputs: Seq[Attribute],
      binding: Boolean,
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.BinaryExpr) => ExprOuterClass.Expr.Builder)
      : Option[ExprOuterClass.Expr] = {
    val leftExpr = exprToProtoInternal(left, inputs, binding)
    val rightExpr = exprToProtoInternal(right, inputs, binding)
    if (leftExpr.isDefined && rightExpr.isDefined) {
      // create the generic BinaryExpr message
      val inner = ExprOuterClass.BinaryExpr
        .newBuilder()
        .setLeft(leftExpr.get)
        .setRight(rightExpr.get)
        .build()
      // call the user-supplied function to wrap BinaryExpr in a top-level Expr
      // such as Expr.And or Expr.Or
      Some(
        f(
          ExprOuterClass.Expr
            .newBuilder(),
          inner).build())
    } else {
      withInfo(expr, left, right)
      None
    }
  }

  // Utility method. Adds explain info if the result of calling exprToProto is None
  def optExprWithInfo(
      optExpr: Option[Expr],
      expr: Expression,
      childExpr: Expression*): Option[Expr] = {
    optExpr match {
      case None =>
        withInfo(expr, childExpr: _*)
        None
      case o => o
    }
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
      withInfo(expr, allExprs: _*)
      None
    }
  }
}
