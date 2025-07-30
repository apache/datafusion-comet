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
import org.apache.comet.serde.QueryPlanSerde.{createBinaryExpr, createUnaryExpr, exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProtoWithReturnType}

object CometGreaterThan extends CometExpressionSerde {
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

object CometGreaterThanOrEqual extends CometExpressionSerde {
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

object CometLessThan extends CometExpressionSerde {
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

object CometLessThanOrEqual extends CometExpressionSerde {
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

object CometIsNull extends CometExpressionSerde {
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

object CometIsNotNull extends CometExpressionSerde {
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

object CometIsNaN extends CometExpressionSerde {
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

object CometIn extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val inExpr = expr.asInstanceOf[In]
    val valueExpr = exprToProtoInternal(inExpr.value, inputs, binding)
    val listExprs = inExpr.list.map(exprToProtoInternal(_, inputs, binding))
    if (valueExpr.isDefined && listExprs.forall(_.isDefined)) {
      val builder = ExprOuterClass.In.newBuilder()
      builder.setInValue(valueExpr.get)
      builder.addAllLists(listExprs.map(_.get).asJava)
      builder.setNegated(false)
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setIn(builder)
          .build())
    } else {
      val allExprs = inExpr.list ++ Seq(inExpr.value)
      withInfo(expr, allExprs: _*)
      None
    }
  }
}
