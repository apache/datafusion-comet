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

import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, Cast, Divide, EmptyRow, EqualTo, EvalMode, Expression, If, IntegralDivide, Literal, Multiply, Remainder, Round, Subtract, UnaryMinus}
import org.apache.spark.sql.types.{ByteType, DataType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.QueryPlanSerde.{evalModeToProto, exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProtoWithReturnType, serializeDataType}
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
    val rightExpr =
      if (expr.evalMode != EvalMode.ANSI) nullIfWhenPrimitive(expr.right) else expr.right
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

  override def getSupportLevel(expr: IntegralDivide): SupportLevel = {
    if (expr.evalMode == EvalMode.ANSI) {
      Incompatible(Some("ANSI mode is not supported"))
    } else {
      Compatible(None)
    }
  }

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
      CometCast.castToProto(expr, None, LongType, childExpr.get, CometEvalMode.LEGACY)
    } else {
      None
    }
  }
}

object CometRemainder extends CometExpressionSerde[Remainder] with MathBase {

  override def getSupportLevel(expr: Remainder): SupportLevel = {
    if (expr.evalMode == EvalMode.ANSI) {
      Incompatible(Some("ANSI mode is not supported"))
    } else {
      Compatible(None)
    }
  }

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

object CometRound extends CometExpressionSerde[Round] {

  override def getSupportLevel(expr: Round): SupportLevel = {
    if (expr.ansiEnabled) {
      Incompatible(Some("ANSI mode is not supported"))
    } else {
      Compatible(None)
    }
  }

  override def convert(
      r: Round,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // _scale s a constant, copied from Spark's RoundBase because it is a protected val
    val scaleV: Any = r.scale.eval(EmptyRow)
    val _scale: Int = scaleV.asInstanceOf[Int]

    lazy val childExpr = exprToProtoInternal(r.child, inputs, binding)
    r.child.dataType match {
      case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
        withInfo(r, "Decimal type has negative scale")
        None
      case _ if scaleV == null =>
        exprToProtoInternal(Literal(null), inputs, binding)
      case _: ByteType | ShortType | IntegerType | LongType if _scale >= 0 =>
        childExpr // _scale(I.e. decimal place) >= 0 is a no-op for integer types in Spark
      case _: FloatType | DoubleType =>
        // We cannot properly match with the Spark behavior for floating-point numbers.
        // Spark uses BigDecimal for rounding float/double, and BigDecimal fist converts a
        // double to string internally in order to create its own internal representation.
        // The problem is BigDecimal uses java.lang.Double.toString() and it has complicated
        // rounding algorithm. E.g. -5.81855622136895E8 is actually
        // -581855622.13689494132995605468750. Note the 5th fractional digit is 4 instead of
        // 5. Java(Scala)'s toString() rounds it up to -581855622.136895. This makes a
        // difference when rounding at 5th digit, I.e. round(-5.81855622136895E8, 5) should be
        // -5.818556221369E8, instead of -5.8185562213689E8. There is also an example that
        // toString() does NOT round up. 6.1317116247283497E18 is 6131711624728349696. It can
        // be rounded up to 6.13171162472835E18 that still represents the same double number.
        // I.e. 6.13171162472835E18 == 6.1317116247283497E18. However, toString() does not.
        // That results in round(6.1317116247283497E18, -5) == 6.1317116247282995E18 instead
        // of 6.1317116247283999E18.
        withInfo(r, "Comet does not support Spark's BigDecimal rounding")
        None
      case _ =>
        // `scale` must be Int64 type in DataFusion
        val scaleExpr = exprToProtoInternal(Literal(_scale.toLong, LongType), inputs, binding)
        val optExpr =
          scalarFunctionExprToProtoWithReturnType("round", r.dataType, childExpr, scaleExpr)
        optExprWithInfo(optExpr, r, r.child)
    }

  }
}
object CometUnaryMinus extends CometExpressionSerde[UnaryMinus] {

  override def convert(
      expr: UnaryMinus,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    if (childExpr.isDefined) {
      val builder = ExprOuterClass.UnaryMinus.newBuilder()
      builder.setChild(childExpr.get)
      builder.setFailOnError(expr.failOnError)
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setUnaryMinus(builder)
          .build())
    } else {
      withInfo(expr, expr.child)
      None
    }
  }
}
