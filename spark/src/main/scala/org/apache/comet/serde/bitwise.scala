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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType}

import org.apache.comet.serde.QueryPlanSerde._

object CometBitwiseAnd extends CometExpressionSerde[BitwiseAnd] {
  override def convert(
      expr: BitwiseAnd,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setBitwiseAnd(binaryExpr))
  }
}

object CometBitwiseNot extends CometExpressionSerde[BitwiseNot] {
  override def convert(
      expr: BitwiseNot,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childProto = exprToProto(expr.child, inputs, binding)
    val bitNotScalarExpr =
      scalarFunctionExprToProto("bit_not", childProto)
    optExprWithInfo(bitNotScalarExpr, expr, expr.children: _*)
  }
}

object CometBitwiseOr extends CometExpressionSerde[BitwiseOr] {
  override def convert(
      expr: BitwiseOr,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setBitwiseOr(binaryExpr))
  }
}

object CometBitwiseXor extends CometExpressionSerde[BitwiseXor] {
  override def convert(
      expr: BitwiseXor,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setBitwiseXor(binaryExpr))
  }
}

object CometShiftRight extends CometExpressionSerde[ShiftRight] {
  override def convert(
      expr: ShiftRight,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // DataFusion bitwise shift right expression requires
    // same data type between left and right side
    val rightExpression = if (expr.left.dataType == LongType) {
      Cast(expr.right, LongType)
    } else {
      expr.right
    }

    createBinaryExpr(
      expr,
      expr.left,
      rightExpression,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setBitwiseShiftRight(binaryExpr))
  }
}

object CometShiftLeft extends CometExpressionSerde[ShiftLeft] {
  override def convert(
      expr: ShiftLeft,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // DataFusion bitwise shift right expression requires
    // same data type between left and right side
    val rightExpression = if (expr.left.dataType == LongType) {
      Cast(expr.right, LongType)
    } else {
      expr.right
    }

    createBinaryExpr(
      expr,
      expr.left,
      rightExpression,
      inputs,
      binding,
      (builder, binaryExpr) => builder.setBitwiseShiftLeft(binaryExpr))
  }
}

object CometBitwiseGet extends CometExpressionSerde[BitwiseGet] {
  override def convert(
      expr: BitwiseGet,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val argProto = exprToProto(expr.left, inputs, binding)
    val posProto = exprToProto(expr.right, inputs, binding)
    val bitGetScalarExpr =
      scalarFunctionExprToProtoWithReturnType("bit_get", ByteType, argProto, posProto)
    optExprWithInfo(bitGetScalarExpr, expr, expr.children: _*)
  }
}

object CometBitwiseCount extends CometExpressionSerde[BitwiseCount] {
  override def convert(
      expr: BitwiseCount,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childProto = exprToProto(expr.child, inputs, binding)
    val bitCountScalarExpr =
      scalarFunctionExprToProtoWithReturnType("bit_count", IntegerType, childProto)
    optExprWithInfo(bitCountScalarExpr, expr, expr.children: _*)
  }
}
