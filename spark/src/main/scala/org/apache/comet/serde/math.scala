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

import org.apache.spark.sql.catalyst.expressions.{Abs, Atan2, Attribute, Ceil, CheckOverflow, Expression, Floor, Hex, If, LessThanOrEqual, Literal, Log, Log10, Log2, Logarithm, Tan, Unhex}
import org.apache.spark.sql.types.{DecimalType, NumericType}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType, serializeDataType}

object CometAtan2 extends CometExpressionSerde[Atan2] {
  override def convert(
      expr: Atan2,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val leftExpr = exprToProtoInternal(expr.left, inputs, binding)
    val rightExpr = exprToProtoInternal(expr.right, inputs, binding)
    val optExpr = scalarFunctionExprToProto("atan2", leftExpr, rightExpr)
    optExprWithInfo(optExpr, expr, expr.left, expr.right)
  }
}

object CometCeil extends CometExpressionSerde[Ceil] {

  override def getSupportLevel(expr: Ceil): SupportLevel = {
    expr.child.dataType match {
      case _: DecimalType =>
        Incompatible(
          Some(
            "Incorrect results for Decimal type inputs" +
              " (https://github.com/apache/datafusion-comet/issues/1729)"))
      case _ => Compatible()
    }
  }

  override def convert(
      expr: Ceil,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    expr.child.dataType match {
      case t: DecimalType if t.scale == 0 => // zero scale is no-op
        childExpr
      case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
        withInfo(expr, s"Decimal type $t has negative scale")
        None
      case _ =>
        val optExpr =
          scalarFunctionExprToProtoWithReturnType("ceil", expr.dataType, false, childExpr)
        optExprWithInfo(optExpr, expr, expr.child)
    }
  }
}

object CometFloor extends CometExpressionSerde[Floor] {

  override def getSupportLevel(expr: Floor): SupportLevel = {
    expr.child.dataType match {
      case _: DecimalType =>
        Incompatible(
          Some(
            "Incorrect results for Decimal type inputs" +
              " (https://github.com/apache/datafusion-comet/issues/1729)"))
      case _ => Compatible()
    }
  }

  override def convert(
      expr: Floor,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    expr.child.dataType match {
      case t: DecimalType if t.scale == 0 => // zero scale is no-op
        childExpr
      case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
        withInfo(expr, s"Decimal type $t has negative scale")
        None
      case _ =>
        val optExpr =
          scalarFunctionExprToProtoWithReturnType("floor", expr.dataType, false, childExpr)
        optExprWithInfo(optExpr, expr, expr.child)
    }
  }
}

// The expression for `log` functions is defined as null on numbers less than or equal
// to 0. This matches Spark and Hive behavior, where non positive values eval to null
// instead of NaN or -Infinity.
object CometLog extends CometExpressionSerde[Log] with MathExprBase {
  override def convert(
      expr: Log,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(nullIfNegative(expr.child), inputs, binding)
    val optExpr = scalarFunctionExprToProto("ln", childExpr)
    optExprWithInfo(optExpr, expr, expr.child)
  }
}

object CometLog10 extends CometExpressionSerde[Log10] with MathExprBase {
  override def convert(
      expr: Log10,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(nullIfNegative(expr.child), inputs, binding)
    val optExpr = scalarFunctionExprToProto("log10", childExpr)
    optExprWithInfo(optExpr, expr, expr.child)
  }
}

object CometLog2 extends CometExpressionSerde[Log2] with MathExprBase {
  override def convert(
      expr: Log2,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(nullIfNegative(expr.child), inputs, binding)
    val optExpr = scalarFunctionExprToProto("log2", childExpr)
    optExprWithInfo(optExpr, expr, expr.child)

  }
}

object CometLogarithm extends CometExpressionSerde[Logarithm] with MathExprBase {
  override def convert(
      expr: Logarithm,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Spark's Logarithm(left=base, right=value) returns null when result is NaN,
    // which happens when base <= 0 or value <= 0. Apply nullIfNegative to both.
    val leftExpr = exprToProtoInternal(nullIfNegative(expr.left), inputs, binding)
    val rightExpr = exprToProtoInternal(nullIfNegative(expr.right), inputs, binding)
    val optExpr = scalarFunctionExprToProto("log", leftExpr, rightExpr)
    optExprWithInfo(optExpr, expr, expr.left, expr.right)
  }
}

object CometHex extends CometExpressionSerde[Hex] with MathExprBase {
  override def convert(
      expr: Hex,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val optExpr = scalarFunctionExprToProtoWithReturnType("hex", expr.dataType, false, childExpr)
    optExprWithInfo(optExpr, expr, expr.child)
  }
}

object CometUnhex extends CometExpressionSerde[Unhex] with MathExprBase {
  override def convert(
      expr: Unhex,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val failOnErrorExpr = exprToProtoInternal(Literal(expr.failOnError), inputs, binding)

    val optExpr =
      scalarFunctionExprToProtoWithReturnType(
        "unhex",
        expr.dataType,
        false,
        childExpr,
        failOnErrorExpr)
    optExprWithInfo(optExpr, expr, expr.child)
  }
}

object CometAbs extends CometExpressionSerde[Abs] with MathExprBase {

  override def getSupportLevel(expr: Abs): SupportLevel = {
    expr.child.dataType match {
      case _: NumericType =>
        Compatible()
      case _ =>
        // Spark supports NumericType, DayTimeIntervalType, and YearMonthIntervalType
        Unsupported(Some("Only integral, floating-point, and decimal types are supported"))
    }
  }

  override def convert(
      expr: Abs,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val failOnErrorExpr = exprToProtoInternal(Literal(expr.failOnError), inputs, binding)

    val optExpr =
      scalarFunctionExprToProtoWithReturnType(
        "abs",
        expr.dataType,
        false,
        childExpr,
        failOnErrorExpr)
    optExprWithInfo(optExpr, expr, expr.child)
  }
}

object CometTan extends CometExpressionSerde[Tan] {

  override def getSupportLevel(expr: Tan): SupportLevel =
    Incompatible(
      Some(
        "tan(-0.0) produces incorrect result" +
          " (https://github.com/apache/datafusion-comet/issues/1897)"))

  override def convert(
      expr: Tan,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = expr.children.map(exprToProtoInternal(_, inputs, binding))
    val optExpr = scalarFunctionExprToProto("tan", childExpr: _*)
    optExprWithInfo(optExpr, expr, expr.children: _*)
  }
}

sealed trait MathExprBase {
  protected def nullIfNegative(expression: Expression): Expression = {
    val zero = Literal.default(expression.dataType)
    If(LessThanOrEqual(expression, zero), Literal.create(null, expression.dataType), expression)
  }
}

object CometCheckOverflow extends CometExpressionSerde[CheckOverflow] {

  override def getSupportLevel(expr: CheckOverflow): SupportLevel = {
    if (expr.dataType.isInstanceOf[DecimalType]) {
      Compatible()
    } else {
      Unsupported(Some("dataType must be DecimalType"))
    }
  }

  override def convert(
      expr: CheckOverflow,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)

    if (childExpr.isDefined) {
      val builder = ExprOuterClass.CheckOverflow.newBuilder()
      builder.setChild(childExpr.get)
      builder.setFailOnError(!expr.nullOnOverflow)

      // `dataType` must be decimal type
      assert(expr.dataType.isInstanceOf[DecimalType])
      val dataType = serializeDataType(expr.dataType)
      builder.setDatatype(dataType.get)

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCheckOverflow(builder)
          .build())
    } else {
      withInfo(expr, expr.child)
      None
    }
  }
}
