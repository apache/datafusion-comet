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

import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, BinaryArithmetic, Cast, Divide, EmptyRow, EqualTo, EvalMode, Expression, If, IntegralDivide, Literal, Multiply, Remainder, Round, Subtract, UnaryMinus}
import org.apache.spark.sql.types.{ByteType, DataType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType}

import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.QueryPlanSerde.{evalModeToProto, exprToProtoInternal, flattenAssociative, scalarFunctionExprToProtoWithReturnType, serializeDataType}
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
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.MathExpr) => ExprOuterClass.Expr.Builder,
      checkDivideOverflow: Boolean = false): Option[ExprOuterClass.Expr] = {
    val leftExpr = exprToProtoInternal(left, inputs, binding)
    val rightExpr = exprToProtoInternal(right, inputs, binding)

    if (leftExpr.isDefined && rightExpr.isDefined) {
      // create the generic MathExpr message
      val builder = ExprOuterClass.MathExpr.newBuilder()
      builder.setLeft(leftExpr.get)
      builder.setRight(rightExpr.get)
      builder.setEvalMode(evalModeToProto(CometEvalModeUtil.fromSparkEvalMode(evalMode)))
      builder.setCheckDivideOverflow(checkDivideOverflow)
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

  def mathDataTypeSupportLevel(dt: DataType): SupportLevel =
    if (supportedDataType(dt)) {
      Compatible()
    } else {
      Unsupported(Some(s"Unsupported datatype $dt"))
    }

  // Native decimal Add/Subtract/Divide/IntegralDivide/Remainder scale-aligns operands by
  // multiplying by 10^|delta|, which overflows (subtract-with-overflow panic in debug, silent
  // wrap in release) whenever any operand or the result has negative scale. See issue #5013.
  private[comet] val negScaleDecimalArithmeticReason: String =
    "Arithmetic on negative-scale decimal is not supported natively"

  private[comet] def negScaleDecimalRejection(expr: BinaryArithmetic): Option[Unsupported] = {
    def isNegScale(dt: DataType): Boolean = dt match {
      case d: DecimalType => d.scale < 0
      case _ => false
    }
    if (isNegScale(expr.left.dataType) || isNegScale(expr.right.dataType) ||
      isNegScale(expr.dataType)) {
      Some(Unsupported(Some(negScaleDecimalArithmeticReason)))
    } else {
      None
    }
  }

  /**
   * True when an `Add` / `Multiply` chain of `dataType` in `evalMode` can be rebalanced without
   * changing results. Only integral types in LEGACY (wrapping, modular) eval mode are exactly
   * associative, so re-grouping the chain is a no-op on the value. Floating point is not
   * associative (rounding differs by grouping -- Spark's own `ReorderAssociativeOperator`
   * excludes it). ANSI / TRY make integer overflow observable (throw / null), and the grouping
   * changes which intermediate overflows, so those are excluded too. Decimal is excluded because
   * intermediate precision grows per operation.
   */
  def isAssociativeAndRebalanceable(dataType: DataType, evalMode: EvalMode.Value): Boolean =
    evalMode == EvalMode.LEGACY && (dataType match {
      case _: ByteType | _: ShortType | _: IntegerType | _: LongType => true
      case _ => false
    })

  /**
   * Like [[QueryPlanSerde.createBalancedBinaryExpr]] but for `MathExpr`-shaped associative
   * operators (`Add`, `Multiply`): each combined inner node carries the chain's `evalMode` and
   * `returnType`. Rebalances a flattened chain into an `O(log n)`-depth tree so deep `a + b +
   * ...` chains serialize to a shallow proto instead of a left-deep one that overflows protobuf's
   * recursion limit when the plan is re-parsed. Only safe for exactly-associative chains --
   * callers gate via [[isAssociativeAndRebalanceable]]. The flattened leaves all share the
   * chain's type (Spark coerces operands to it, with casts acting as flatten boundaries), so a
   * single `returnType` / `evalMode` is correct for every inner node.
   */
  def createBalancedMathExpr(
      expr: Expression,
      operands: Seq[Expression],
      inputs: Seq[Attribute],
      binding: Boolean,
      dataType: DataType,
      evalMode: EvalMode.Value,
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.MathExpr) => ExprOuterClass.Expr.Builder)
      : Option[ExprOuterClass.Expr] = {
    val protos = operands.map(exprToProtoInternal(_, inputs, binding))
    if (protos.exists(_.isEmpty)) {
      None
    } else {
      val returnType = serializeDataType(dataType)
      val evalModeProto = evalModeToProto(CometEvalModeUtil.fromSparkEvalMode(evalMode))
      val leaves = protos.map(_.get).toIndexedSeq
      def build(slice: IndexedSeq[ExprOuterClass.Expr]): ExprOuterClass.Expr = {
        if (slice.length == 1) slice.head
        else {
          val mid = slice.length / 2
          val mathBuilder = ExprOuterClass.MathExpr
            .newBuilder()
            .setLeft(build(slice.slice(0, mid)))
            .setRight(build(slice.slice(mid, slice.length)))
            .setEvalMode(evalModeProto)
          returnType.foreach(mathBuilder.setReturnType)
          f(ExprOuterClass.Expr.newBuilder(), mathBuilder.build()).build()
        }
      }
      Some(build(leaves))
    }
  }

}

object CometAdd extends CometExpressionSerde[Add] with MathBase {

  override def getSupportLevel(expr: Add): SupportLevel =
    negScaleDecimalRejection(expr).getOrElse(mathDataTypeSupportLevel(expr.left.dataType))

  override def convert(
      expr: Add,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (isAssociativeAndRebalanceable(expr.dataType, expr.evalMode)) {
      // Rebalance deep `a + b + ...` chains (integral + LEGACY = exactly associative) so the
      // proto stays shallow and doesn't overflow protobuf's recursion limit when re-parsed.
      val operands = flattenAssociative(
        expr,
        { case _: Add => true; case _ => false },
        { case a: Add => (a.left, a.right) })
      createBalancedMathExpr(
        expr,
        operands,
        inputs,
        binding,
        expr.dataType,
        expr.evalMode,
        (builder, mathExpr) => builder.setAdd(mathExpr))
    } else {
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
}

object CometSubtract extends CometExpressionSerde[Subtract] with MathBase {

  override def getSupportLevel(expr: Subtract): SupportLevel =
    negScaleDecimalRejection(expr).getOrElse(mathDataTypeSupportLevel(expr.left.dataType))

  override def convert(
      expr: Subtract,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
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

  // No `negScaleDecimalRejection` guard: Multiply doesn't scale-align operands, so negative-scale
  // decimals are safe here. Pinned by a regression test in CometExpressionSuite. See issue #5013.
  override def getSupportLevel(expr: Multiply): SupportLevel =
    mathDataTypeSupportLevel(expr.left.dataType)

  override def convert(
      expr: Multiply,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (isAssociativeAndRebalanceable(expr.dataType, expr.evalMode)) {
      // Rebalance deep `a * b * ...` chains (integral + LEGACY = exactly associative) so the
      // proto stays shallow and doesn't overflow protobuf's recursion limit when re-parsed.
      val operands = flattenAssociative(
        expr,
        { case _: Multiply => true; case _ => false },
        { case m: Multiply => (m.left, m.right) })
      createBalancedMathExpr(
        expr,
        operands,
        inputs,
        binding,
        expr.dataType,
        expr.evalMode,
        (builder, mathExpr) => builder.setMultiply(mathExpr))
    } else {
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
}

object CometDivide extends CometExpressionSerde[Divide] with MathBase {

  override def getSupportLevel(expr: Divide): SupportLevel =
    negScaleDecimalRejection(expr).getOrElse {
      if (expr.dataType.isInstanceOf[DecimalType] &&
        (!expr.left.dataType.isInstanceOf[DecimalType] ||
          !expr.right.dataType.isInstanceOf[DecimalType])) {
        // This is only a sanity check; Spark's type coercion should prevent this case.
        Unsupported(Some("Decimal division with a decimal result requires decimal operands"))
      } else {
        mathDataTypeSupportLevel(expr.left.dataType)
      }
    }

  override def convert(
      expr: Divide,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Datafusion now throws an exception for dividing by zero
    // See https://github.com/apache/arrow-datafusion/pull/6792
    // For now, use NullIf to swap zeros with nulls.
    val rightExpr =
      if (expr.evalMode != EvalMode.ANSI) nullIfWhenPrimitive(expr.right) else expr.right
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

  override def getSupportLevel(expr: IntegralDivide): SupportLevel =
    negScaleDecimalRejection(expr).getOrElse(mathDataTypeSupportLevel(expr.left.dataType))

  override def convert(
      expr: IntegralDivide,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {

//    Precision is set to 19 (max precision for a numerical data type except DecimalType)

    val left =
      if (expr.left.dataType.isInstanceOf[DecimalType]) expr.left
      else Cast(expr.left, DecimalType(19, 0))
    val right =
      if (expr.right.dataType.isInstanceOf[DecimalType]) expr.right
      else Cast(expr.right, DecimalType(19, 0))

    val rightExpr = if (expr.evalMode != EvalMode.ANSI) nullIfWhenPrimitive(right) else right

    val dataType = (left.dataType, rightExpr.dataType) match {
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
      (builder, mathExpr) => builder.setIntegralDivide(mathExpr),
      // Spark only checks integral divide overflow (Long.MinValue div -1) for LONG
      // operands; DECIMAL operands wrap around on the cast to LONG even in ANSI mode
      checkDivideOverflow = expr.checkDivideOverflow)

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

  override def getSupportLevel(expr: Remainder): SupportLevel =
    negScaleDecimalRejection(expr).getOrElse(mathDataTypeSupportLevel(expr.left.dataType))

  override def convert(
      expr: Remainder,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
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

/**
 * `round` lowers to the native `round` kernel for integral and non-negative-scale decimal inputs.
 * The cases below have no native implementation; `CodegenDispatchFallback` keeps them in the
 * Comet pipeline by running Spark's own `RoundBase.doGenCode` in the JVM codegen dispatcher,
 * which matches Spark exactly.
 */
object CometRound extends CometExpressionSerde[Round] with CodegenDispatchFallback {

  private val negativeScaleReason =
    "Negative-scale decimal inputs, which are only creatable with " +
      "spark.sql.legacy.allowNegativeScaleOfDecimal=true"

  private val floatingPointReason =
    "Float and double inputs. Spark rounds them through a BigDecimal built from " +
      "`java.lang.Double.toString()` rather than from the exact binary value, and that " +
      "shortened decimal string can round differently than the value it came from"

  override def getUnsupportedReasons(): Seq[String] =
    Seq(floatingPointReason, negativeScaleReason)

  override def getSupportLevel(expr: Round): SupportLevel = expr.child.dataType match {
    case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
      Unsupported(Some(negativeScaleReason))
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
      Unsupported(Some(floatingPointReason))
    case _ =>
      Compatible()
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
      case _ if scaleV == null =>
        exprToProtoInternal(Literal(null), inputs, binding)
      case _: ByteType | ShortType | IntegerType | LongType if _scale >= 0 =>
        childExpr // _scale(I.e. decimal place) >= 0 is a no-op for integer types in Spark
      case _ =>
        // `scale` must be Int64 type in DataFusion
        val scaleExpr = exprToProtoInternal(Literal(_scale.toLong, LongType), inputs, binding)
        val optExpr =
          scalarFunctionExprToProtoWithReturnType(
            "round",
            r.dataType,
            r.ansiEnabled,
            childExpr,
            scaleExpr)
        optExpr
    }

  }
}
object CometUnaryMinus extends CometExpressionSerde[UnaryMinus] with MathBase {

  override def getSupportLevel(expr: UnaryMinus): SupportLevel =
    mathDataTypeSupportLevel(expr.child.dataType)

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
      None
    }
  }
}
