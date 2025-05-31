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

import org.apache.spark.sql.catalyst.expressions.{Attribute, BitwiseAnd, BitwiseCount, BitwiseGet, BitwiseNot, BitwiseOr, BitwiseXor, Cast, Expression, ShiftLeft, ShiftRight}
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType}

import org.apache.comet.serde.QueryPlanSerde.{createBinaryExpr, createUnaryExpr, exprToProto, optExprWithInfo, scalarFunctionExprToProtoWithReturnType}

object CometBitwiseAdd extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val bitwiseAndExpr = expr.asInstanceOf[BitwiseAnd]
    createBinaryExpr(
      expr,
      bitwiseAndExpr.left,
      bitwiseAndExpr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setBitwiseAnd(binaryExpr))
  }
}

object CometBitwiseNot extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val bitwiseNotExpr = expr.asInstanceOf[BitwiseNot]
    createUnaryExpr(
      expr,
      bitwiseNotExpr.child,
      inputs,
      binding,
      (builder, unaryExpr) => builder.setBitwiseNot(unaryExpr))
  }
}

object CometBitwiseOr extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val bitwiseOrExpr = expr.asInstanceOf[BitwiseOr]
    createBinaryExpr(
      expr,
      bitwiseOrExpr.left,
      bitwiseOrExpr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setBitwiseOr(binaryExpr))
  }
}

object CometBitwiseXor extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val bitwiseXorExpr = expr.asInstanceOf[BitwiseXor]
    createBinaryExpr(
      expr,
      bitwiseXorExpr.left,
      bitwiseXorExpr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setBitwiseXor(binaryExpr))
  }
}

object CometShiftRight extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val shiftRightExpr = expr.asInstanceOf[ShiftRight]
    // DataFusion bitwise shift right expression requires
    // same data type between left and right side
    val rightExpression = if (shiftRightExpr.left.dataType == LongType) {
      Cast(shiftRightExpr.right, LongType)
    } else {
      shiftRightExpr.right
    }

    createBinaryExpr(
      expr,
      shiftRightExpr.left,
      rightExpression,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setBitwiseShiftRight(binaryExpr))
  }
}

object CometShiftLeft extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val shiftLeftLeft = expr.asInstanceOf[ShiftLeft]
    // DataFusion bitwise shift right expression requires
    // same data type between left and right side
    val rightExpression = if (shiftLeftLeft.left.dataType == LongType) {
      Cast(shiftLeftLeft.right, LongType)
    } else {
      shiftLeftLeft.right
    }

    createBinaryExpr(
      expr,
      shiftLeftLeft.left,
      rightExpression,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setBitwiseShiftLeft(binaryExpr))
  }
}

object CometBitwiseGet extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val bitwiseGetExpr = expr.asInstanceOf[BitwiseGet]
    val argProto = exprToProto(bitwiseGetExpr.left, inputs, binding)
    val posProto = exprToProto(bitwiseGetExpr.right, inputs, binding)
    val bitGetScalarExpr =
      scalarFunctionExprToProtoWithReturnType("bit_get", ByteType, argProto, posProto)
    optExprWithInfo(bitGetScalarExpr, expr, expr.children: _*)
  }
}

object CometBitwiseCount extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val bitwiseCountExpr = expr.asInstanceOf[BitwiseCount]
    val childProto = exprToProto(bitwiseCountExpr.child, inputs, binding)
    val bitCountScalarExpr =
      scalarFunctionExprToProtoWithReturnType("bit_count", IntegerType, childProto)
    optExprWithInfo(bitCountScalarExpr, expr, expr.children: _*)
  }
}
