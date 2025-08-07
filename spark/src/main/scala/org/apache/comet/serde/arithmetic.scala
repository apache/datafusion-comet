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

object CometAdd extends CometExpressionSerde[Add] with MathBase {
  override def convert(
      expr: Add,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!supportedDataType(expr.left.dataType)) {
      withInfo(expr, s"Unsupported datatype ${expr.left.dataType}")
      return None
    }
    createMathExpression(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      expr.dataType,
      expr.evalMode,
      (builder, mathExpr) => builder.setAdd(mathExpr))
  }
}

object CometSubtract extends CometExpressionSerde[Subtract] with MathBase {
  override def convert(
      expr: Subtract,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!supportedDataType(expr.left.dataType)) {
      withInfo(expr, s"Unsupported datatype ${expr.left.dataType}")
      return None
    }
    createMathExpression(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      expr.dataType,
      expr.evalMode,
      (builder, mathExpr) => builder.setSubtract(mathExpr))
  }
}

object CometMultiply extends CometExpressionSerde[Multiply] with MathBase {
  override def convert(
      expr: Multiply,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!supportedDataType(expr.left.dataType)) {
      withInfo(expr, s"Unsupported datatype ${expr.left.dataType}")
      return None
    }
    createMathExpression(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      expr.dataType,
      expr.evalMode,
      (builder, mathExpr) => builder.setMultiply(mathExpr))
  }
}

object CometDivide extends CometExpressionSerde[Divide] with MathBase {
  override def convert(
      expr: Divide,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Datafusion now throws an exception for dividing by zero
    // See https://github.com/apache/arrow-datafusion/pull/6792
    // For now, use NullIf to swap zeros with nulls.
    val rightExpr = nullIfWhenPrimitive(expr.right)
    if (!supportedDataType(expr.left.dataType)) {
      withInfo(expr, s"Unsupported datatype ${expr.left.dataType}")
      return None
    }
    createMathExpression(
      expr,
      expr.left,
      rightExpr,
      inputs,
      binding,
      expr.dataType,
      expr.evalMode,
      (builder, mathExpr) => builder.setDivide(mathExpr))
  }
}

object CometIntegralDivide extends CometExpressionSerde[IntegralDivide] with MathBase {
  override def convert(
      expr: IntegralDivide,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!supportedDataType(expr.left.dataType)) {
      withInfo(expr, s"Unsupported datatype ${expr.left.dataType}")
      return None
    }

//    Precision is set to 19 (max precision for a numerical data type except DecimalType)

    val left =
      if (expr.left.dataType.isInstanceOf[DecimalType]) expr.left
      else Cast(expr.left, DecimalType(19, 0))
    val right =
      if (expr.right.dataType.isInstanceOf[DecimalType]) expr.right
      else Cast(expr.right, DecimalType(19, 0))

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
      expr.evalMode,
      (builder, mathExpr) => builder.setIntegralDivide(mathExpr))

    if (divideExpr.isDefined) {
      val childExpr = if (dataType.isInstanceOf[DecimalType]) {
        // check overflow for decimal type
        val builder = ExprOuterClass.CheckOverflow.newBuilder()
        builder.setChild(divideExpr.get)
        builder.setFailOnError(expr.evalMode == EvalMode.ANSI)
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

object CometRemainder extends CometExpressionSerde[Remainder] with MathBase {
  override def convert(
      expr: Remainder,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!supportedDataType(expr.left.dataType)) {
      withInfo(expr, s"Unsupported datatype ${expr.left.dataType}")
      return None
    }
    if (expr.evalMode == EvalMode.TRY) {
      withInfo(expr, s"Eval mode ${expr.evalMode} is not supported")
      return None
    }

    createMathExpression(
      expr,
      expr.left,
      expr.right,
      inputs,
      binding,
      expr.dataType,
      expr.evalMode,
      (builder, mathExpr) => builder.setRemainder(mathExpr))
  }
}
