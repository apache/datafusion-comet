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

import org.apache.spark.sql.catalyst.expressions.{Abs, Add, Atan2, Attribute, BRound, Ceil, CheckOverflow, Conv, Expression, Floor, Greatest, Hex, Hypot, Least, Literal, Log, Log10, Log1p, Log2, Logarithm, NaNvl, Pmod, Pow, Signum, Sqrt, UnaryLogExpression, UnaryPositive, Unhex, WidthBucket}
import org.apache.spark.sql.types.{DecimalType, DoubleType, NumericType}

import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType, serializeDataType}

object CometAtan2 extends CometExpressionSerde[Atan2] {
  override def convert(
      expr: Atan2,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Spark adds +0.0 to inputs in order to convert -0.0 to +0.0
    val left = Add(expr.left, Literal.default(expr.left.dataType))
    val right = Add(expr.right, Literal.default(expr.right.dataType))
    val leftExpr = exprToProtoInternal(left, inputs, binding)
    val rightExpr = exprToProtoInternal(right, inputs, binding)
    val optExpr = scalarFunctionExprToProto("atan2", leftExpr, rightExpr)
    optExpr
  }
}

object CometCeil extends CometExpressionSerde[Ceil] {
  override def getSupportLevel(expr: Ceil): SupportLevel = expr.child.dataType match {
    case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
      Unsupported(Some(s"Decimal type $t has negative scale"))
    case _ =>
      Compatible()
  }

  override def convert(
      expr: Ceil,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    expr.child.dataType match {
      case t: DecimalType if t.scale == 0 => // zero scale is no-op
        childExpr
      case _ =>
        val optExpr =
          scalarFunctionExprToProtoWithReturnType("ceil", expr.dataType, false, childExpr)
        optExpr
    }
  }
}

object CometFloor extends CometExpressionSerde[Floor] {
  override def getSupportLevel(expr: Floor): SupportLevel = expr.child.dataType match {
    case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
      Unsupported(Some(s"Decimal type $t has negative scale"))
    case _ =>
      Compatible()
  }

  override def convert(
      expr: Floor,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    expr.child.dataType match {
      case t: DecimalType if t.scale == 0 => // zero scale is no-op
        childExpr
      case _ =>
        val optExpr =
          scalarFunctionExprToProtoWithReturnType("floor", expr.dataType, false, childExpr)
        optExpr
    }
  }
}

sealed abstract class CometUnaryLog[T <: UnaryLogExpression](function: String)
    extends CometExpressionSerde[T] {
  override def convert(
      expr: T,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val guarded = scalarFunctionExprToProtoWithReturnType(
      "spark_null_if_non_positive",
      DoubleType,
      false,
      childExpr)
    scalarFunctionExprToProto(function, guarded)
  }
}

object CometLog extends CometUnaryLog[Log]("ln")

object CometLog10 extends CometUnaryLog[Log10]("log10")

object CometLog2 extends CometUnaryLog[Log2]("log2")

object CometLogarithm extends CometExpressionSerde[Logarithm] {
  override def convert(
      expr: Logarithm,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Uses custom spark_log UDF that returns null when base <= 0 or value <= 0,
    // matching Spark's Logarithm.nullSafeEval behavior.
    val leftExpr = exprToProtoInternal(expr.left, inputs, binding)
    val rightExpr = exprToProtoInternal(expr.right, inputs, binding)
    val optExpr =
      scalarFunctionExprToProtoWithReturnType("spark_log", DoubleType, false, leftExpr, rightExpr)
    optExpr
  }
}

object CometHex extends CometExpressionSerde[Hex] {
  override def convert(
      expr: Hex,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val optExpr = scalarFunctionExprToProtoWithReturnType("hex", expr.dataType, false, childExpr)
    optExpr
  }
}

object CometUnhex extends CometExpressionSerde[Unhex] {
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
    optExpr
  }
}

/**
 * `abs` lowers to the native `abs` kernel for numeric inputs. Interval inputs have no native
 * implementation, so `CodegenDispatchFallback` keeps them in the Comet pipeline by running
 * Spark's own `Abs.doGenCode` in the JVM codegen dispatcher, which matches Spark exactly.
 */
object CometAbs extends CometExpressionSerde[Abs] with CodegenDispatchFallback {

  private val unsupportedReason: String =
    "`INTERVAL YEAR TO MONTH` and `INTERVAL DAY TO SECOND` inputs"

  override def getUnsupportedReasons(): Seq[String] = Seq(unsupportedReason)

  override def getSupportLevel(expr: Abs): SupportLevel = {
    expr.child.dataType match {
      case _: NumericType =>
        Compatible()
      case _ =>
        // Spark supports NumericType, DayTimeIntervalType, and YearMonthIntervalType
        Unsupported(Some(unsupportedReason))
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
    optExpr
  }
}

object CometPow extends CometExpressionSerde[Pow] {

  override def convert(
      expr: Pow,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val leftExpr = exprToProtoInternal(expr.left, inputs, binding)
    val rightExpr = exprToProtoInternal(expr.right, inputs, binding)
    val optExpr = scalarFunctionExprToProto("pow", leftExpr, rightExpr)
    optExpr
  }
}

// Uses a custom spark_sqrt UDF because DataFusion's own `sqrt` errors on negative
// input, while Spark's Sqrt (a plain wrapper around java.lang.Math.sqrt) returns NaN.
// spark_sqrt is a Comet-only name with no DataFusion builtin counterpart, so the
// return type must be set explicitly here to skip the session registry lookup that
// scalarFunctionExprToProto would otherwise trigger (see CometLogarithm/spark_log).
object CometSqrt extends CometExpressionSerde[Sqrt] {
  override def convert(
      expr: Sqrt,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val optExpr =
      scalarFunctionExprToProtoWithReturnType("spark_sqrt", DoubleType, false, childExpr)
    optExpr
  }
}

// Uses a custom spark_signum UDF because DataFusion's own `signum` returns 0.0 for both zeros,
// while Spark's Signum (java.lang.Math.signum) returns the zero it is given, so -0.0 keeps its
// sign. The kernel takes doubles only. Spark's Signum also accepts the two interval types, which
// DataFusion's `signum` failed on at execution, so those fall back. As with CometSqrt, the
// Comet-only name needs its return type set explicitly.
object CometSignum extends CometExpressionSerde[Signum] {

  override def getUnsupportedReasons(): Seq[String] = Seq("Only `DoubleType` is supported")

  override def getSupportLevel(expr: Signum): SupportLevel = expr.child.dataType match {
    case DoubleType => Compatible()
    case other => Unsupported(Some(s"signum does not support input type $other"))
  }

  override def convert(
      expr: Signum,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    scalarFunctionExprToProtoWithReturnType("spark_signum", DoubleType, false, childExpr)
  }
}

object CometCheckOverflow extends CometExpressionSerde[CheckOverflow] {

  override def getUnsupportedReasons(): Seq[String] = Seq("Only `DecimalType` is supported")

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
      None
    }
  }
}

object CometHypot extends CometCodegenDispatch[Hypot]

object CometNaNvl extends CometCodegenDispatch[NaNvl]

object CometBRound extends CometCodegenDispatch[BRound]

object CometConv extends CometCodegenDispatch[Conv]

object CometLog1p extends CometCodegenDispatch[Log1p]

object CometPmod extends CometCodegenDispatch[Pmod]

object CometWidthBucket extends CometCodegenDispatch[WidthBucket]

object CometUnaryPositive extends CometCodegenDispatch[UnaryPositive]

/**
 * `greatest` and `least` order string inputs by raw bytes natively, while Spark orders them under
 * the input collation.
 */
class CometStringOrderingExtremum[T <: Expression](function: String)
    extends CometScalarFunction[T](function)
    with CodegenDispatchFallback {

  override def hasConditionalNativeDefault: Boolean = true

  override def getIncompatibleReasons(): Seq[String] =
    Seq(StringCollationSupport.collationReason(function))

  override def getSupportLevel(expr: T): SupportLevel =
    StringCollationSupport.getSupportLevel(function, expr.children)
}

object CometGreatest extends CometStringOrderingExtremum[Greatest]("greatest")

object CometLeast extends CometStringOrderingExtremum[Least]("least")
