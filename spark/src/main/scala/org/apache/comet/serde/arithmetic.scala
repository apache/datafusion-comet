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

import scala.math.min

import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, Cast, Divide, EqualTo, EvalMode, Expression, If, IntegralDivide, Literal, Multiply, Remainder, Subtract}
import org.apache.spark.sql.types.{ByteType, DataType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.expressions.CometEvalMode
import org.apache.comet.serde.QueryPlanSerde.{castToProto, evalModeToProto, exprToProtoInternal, serializeDataType}
import org.apache.comet.shims.CometEvalModeUtil

trait MathBase {
  def createMathExpression(
      expr: Expression,
      left: Expression,
      right: Expression,
      inputs: Seq[Attribute],
      binding: Boolean,
      dataType: DataType,
      evalMode: EvalMode.Value,
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.MathExpr) => ExprOuterClass.Expr.Builder)
      : Option[ExprOuterClass.Expr] = {
    val leftExpr = exprToProtoInternal(left, inputs, binding)
    val rightExpr = exprToProtoInternal(right, inputs, binding)

    if (leftExpr.isDefined && rightExpr.isDefined) {
      // create the generic MathExpr message
      val builder = ExprOuterClass.MathExpr.newBuilder()
      builder.setLeft(leftExpr.get)
      builder.setRight(rightExpr.get)
      builder.setEvalMode(evalModeToProto(CometEvalModeUtil.fromSparkEvalMode(evalMode)))
      serializeDataType(dataType).foreach { t =>
        builder.setReturnType(t)
      }
      val inner = builder.build()
      // call the user-supplied function to wrap MathExpr in a top-level Expr
      // such as Expr.Add or Expr.Divide
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

  def nullIfWhenPrimitive(expression: Expression): Expression = {
    val zero = Literal.default(expression.dataType)
    expression match {
      case _: Literal if expression != zero => expression
      case _ =>
        If(EqualTo(expression, zero), Literal.create(null, expression.dataType), expression)
    }
  }

  def supportedDataType(dt: DataType): Boolean = dt match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
        _: DoubleType | _: DecimalType =>
      true
    case _ =>
      false
  }

}

object CometAdd extends CometExpressionSerde with MathBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val add = expr.asInstanceOf[Add]
    if (!supportedDataType(add.left.dataType)) {
      withInfo(add, s"Unsupported datatype ${add.left.dataType}")
      return None
    }
    if (add.evalMode == EvalMode.TRY) {
      withInfo(add, s"Eval mode ${add.evalMode} is not supported")
      return None
    }
    createMathExpression(
      expr,
      add.left,
      add.right,
      inputs,
      binding,
      add.dataType,
      add.evalMode,
      (builder, mathExpr) => builder.setAdd(mathExpr))
  }
}

object CometSubtract extends CometExpressionSerde with MathBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val sub = expr.asInstanceOf[Subtract]
    if (!supportedDataType(sub.left.dataType)) {
      withInfo(sub, s"Unsupported datatype ${sub.left.dataType}")
      return None
    }
    if (sub.evalMode == EvalMode.TRY) {
      withInfo(sub, s"Eval mode ${sub.evalMode} is not supported")
      return None
    }
    createMathExpression(
      expr,
      sub.left,
      sub.right,
      inputs,
      binding,
      sub.dataType,
      sub.evalMode,
      (builder, mathExpr) => builder.setSubtract(mathExpr))
  }
}

object CometMultiply extends CometExpressionSerde with MathBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val mul = expr.asInstanceOf[Multiply]
    if (!supportedDataType(mul.left.dataType)) {
      withInfo(mul, s"Unsupported datatype ${mul.left.dataType}")
      return None
    }
    if (mul.evalMode == EvalMode.TRY) {
      withInfo(mul, s"Eval mode ${mul.evalMode} is not supported")
      return None
    }
    createMathExpression(
      expr,
      mul.left,
      mul.right,
      inputs,
      binding,
      mul.dataType,
      mul.evalMode,
      (builder, mathExpr) => builder.setMultiply(mathExpr))
  }
}

object CometDivide extends CometExpressionSerde with MathBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val div = expr.asInstanceOf[Divide]

    // Datafusion now throws an exception for dividing by zero
    // See https://github.com/apache/arrow-datafusion/pull/6792
    // For now, use NullIf to swap zeros with nulls.
    val rightExpr = nullIfWhenPrimitive(div.right)

    if (!supportedDataType(div.left.dataType)) {
      withInfo(div, s"Unsupported datatype ${div.left.dataType}")
      return None
    }
    if (div.evalMode == EvalMode.TRY) {
      withInfo(div, s"Eval mode ${div.evalMode} is not supported")
      return None
    }
    createMathExpression(
      expr,
      div.left,
      rightExpr,
      inputs,
      binding,
      div.dataType,
      div.evalMode,
      (builder, mathExpr) => builder.setDivide(mathExpr))
  }
}

object CometIntegralDivide extends CometExpressionSerde with MathBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val div = expr.asInstanceOf[IntegralDivide]

    if (!supportedDataType(div.left.dataType)) {
      withInfo(div, s"Unsupported datatype ${div.left.dataType}")
      return None
    }
    if (div.evalMode == EvalMode.TRY) {
      withInfo(div, s"Eval mode ${div.evalMode} is not supported")
      return None
    }

    val left =
      if (div.left.dataType.isInstanceOf[DecimalType]) div.left
      else Cast(div.left, DecimalType(19, 0))
    val right =
      if (div.right.dataType.isInstanceOf[DecimalType]) div.right
      else Cast(div.right, DecimalType(19, 0))

    val rightExpr = nullIfWhenPrimitive(right)

    val dataType = (left.dataType, right.dataType) match {
      case (l: DecimalType, r: DecimalType) =>
        // copy from IntegralDivide.resultDecimalType
        val intDig = l.precision - l.scale + r.scale
        DecimalType(min(if (intDig == 0) 1 else intDig, DecimalType.MAX_PRECISION), 0)
      case _ => left.dataType
    }

    val divideExpr = createMathExpression(
      expr,
      left,
      rightExpr,
      inputs,
      binding,
      dataType,
      div.evalMode,
      (builder, mathExpr) => builder.setIntegralDivide(mathExpr))

    if (divideExpr.isDefined) {
      val childExpr = if (dataType.isInstanceOf[DecimalType]) {
        // check overflow for decimal type
        val builder = ExprOuterClass.CheckOverflow.newBuilder()
        builder.setChild(divideExpr.get)
        builder.setFailOnError(div.evalMode == EvalMode.ANSI)
        builder.setDatatype(serializeDataType(dataType).get)
        Some(
          ExprOuterClass.Expr
            .newBuilder()
            .setCheckOverflow(builder)
            .build())
      } else {
        divideExpr
      }

      // cast result to long
      castToProto(expr, None, LongType, childExpr.get, CometEvalMode.LEGACY)
    } else {
      None
    }
  }
}

object CometRemainder extends CometExpressionSerde with MathBase {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val remainder = expr.asInstanceOf[Remainder]
    if (!supportedDataType(remainder.left.dataType)) {
      withInfo(remainder, s"Unsupported datatype ${remainder.left.dataType}")
      return None
    }
    if (remainder.evalMode == EvalMode.TRY) {
      withInfo(remainder, s"Eval mode ${remainder.evalMode} is not supported")
      return None
    }

    createMathExpression(
      expr,
      remainder.left,
      remainder.right,
      inputs,
      binding,
      remainder.dataType,
      remainder.evalMode,
      (builder, mathExpr) => builder.setRemainder(mathExpr))
  }
}
