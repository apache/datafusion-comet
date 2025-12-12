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

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Concat, Expression, InitCap, Length, Like, Literal, Lower, RegExpReplace, RLike, StringLPad, StringRepeat, StringRPad, Substring, Upper}
import org.apache.spark.sql.types.{BinaryType, DataTypes, LongType, StringType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.expressions.{CometCast, CometEvalMode, RegExp}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{createBinaryExpr, exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto}

object CometStringRepeat extends CometExpressionSerde[StringRepeat] {

  override def convert(
      expr: StringRepeat,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val children = expr.children
    val leftCast = Cast(children(0), StringType)
    val rightCast = Cast(children(1), LongType)
    val leftExpr = exprToProtoInternal(leftCast, inputs, binding)
    val rightExpr = exprToProtoInternal(rightCast, inputs, binding)
    val optExpr = scalarFunctionExprToProto("repeat", leftExpr, rightExpr)
    optExprWithInfo(optExpr, expr, leftCast, rightCast)
  }
}

class CometCaseConversionBase[T <: Expression](function: String)
    extends CometScalarFunction[T](function) {

  override def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    if (!CometConf.COMET_CASE_CONVERSION_ENABLED.get()) {
      withInfo(
        expr,
        "Comet is not compatible with Spark for case conversion in " +
          s"locale-specific cases. Set ${CometConf.COMET_CASE_CONVERSION_ENABLED.key}=true " +
          "to enable it anyway.")
      return None
    }
    super.convert(expr, inputs, binding)
  }
}

object CometUpper extends CometCaseConversionBase[Upper]("upper")

object CometLower extends CometCaseConversionBase[Lower]("lower")

object CometLength extends CometScalarFunction[Length]("length") {
  override def getSupportLevel(expr: Length): SupportLevel = expr.child.dataType match {
    case _: BinaryType => Unsupported(Some("Length on BinaryType is not supported"))
    case _ => Compatible()
  }
}

object CometInitCap extends CometScalarFunction[InitCap]("initcap") {

  override def getSupportLevel(expr: InitCap): SupportLevel = {
    // Behavior differs from Spark. One example is that for the input "robert rose-smith", Spark
    // will produce "Robert Rose-smith", but Comet will produce "Robert Rose-Smith".
    // https://github.com/apache/datafusion-comet/issues/1052
    Incompatible(None)
  }

  override def convert(expr: InitCap, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    super.convert(expr, inputs, binding)
  }
}

object CometSubstring extends CometExpressionSerde[Substring] {

  override def convert(
      expr: Substring,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    (expr.pos, expr.len) match {
      case (Literal(pos, _), Literal(len, _)) =>
        exprToProtoInternal(expr.str, inputs, binding) match {
          case Some(strExpr) =>
            val builder = ExprOuterClass.Substring.newBuilder()
            builder.setChild(strExpr)
            builder.setStart(pos.asInstanceOf[Int])
            builder.setLen(len.asInstanceOf[Int])
            Some(ExprOuterClass.Expr.newBuilder().setSubstring(builder).build())
          case None =>
            withInfo(expr, expr.str)
            None
        }
      case _ =>
        withInfo(expr, "Substring pos and len must be literals")
        None
    }
  }
}

object CometConcat extends CometScalarFunction[Concat]("concat") {
  val unsupportedReason = "CONCAT supports only string input parameters"

  override def getSupportLevel(expr: Concat): SupportLevel = {
    if (expr.children.forall(_.dataType == DataTypes.StringType)) {
      Compatible()
    } else {
      Incompatible(Some(unsupportedReason))
    }
  }
}

object CometLike extends CometExpressionSerde[Like] {

  override def convert(expr: Like, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    if (expr.escapeChar == '\\') {
      createBinaryExpr(
        expr,
        expr.left,
        expr.right,
        inputs,
        binding,
        (builder, binaryExpr) => builder.setLike(binaryExpr))
    } else {
      withInfo(expr, s"custom escape character ${expr.escapeChar} not supported in LIKE")
      None
    }
  }
}

object CometRLike extends CometExpressionSerde[RLike] {

  override def convert(expr: RLike, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    expr.right match {
      case Literal(pattern, DataTypes.StringType) =>
        if (!RegExp.isSupportedPattern(pattern.toString) &&
          !CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.get()) {
          withInfo(
            expr,
            s"Regexp pattern $pattern is not compatible with Spark. " +
              s"Set ${CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key}=true " +
              "to allow it anyway.")
          None
        } else {
          createBinaryExpr(
            expr,
            expr.left,
            expr.right,
            inputs,
            binding,
            (builder, binaryExpr) => builder.setRlike(binaryExpr))
        }
      case _ =>
        withInfo(expr, "Only scalar regexp patterns are supported")
        None
    }
  }
}

object CometStringRPad extends CometExpressionSerde[StringRPad] {

  override def getSupportLevel(expr: StringRPad): SupportLevel = {
    if (expr.str.isInstanceOf[Literal]) {
      return Unsupported(Some("Scalar values are not supported for the str argument"))
    }
    if (!expr.pad.isInstanceOf[Literal]) {
      return Unsupported(Some("Only scalar values are supported for the pad argument"))
    }
    Compatible()
  }

  override def convert(
      expr: StringRPad,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {

    scalarFunctionExprToProto(
      "rpad",
      exprToProtoInternal(expr.str, inputs, binding),
      exprToProtoInternal(expr.len, inputs, binding),
      exprToProtoInternal(expr.pad, inputs, binding))
  }
}

object CometStringLPad extends CometExpressionSerde[StringLPad] {

  override def getSupportLevel(expr: StringLPad): SupportLevel = {
    if (expr.str.isInstanceOf[Literal]) {
      return Unsupported(Some("Scalar values are not supported for the str argument"))
    }
    if (!expr.pad.isInstanceOf[Literal]) {
      return Unsupported(Some("Only scalar values are supported for the pad argument"))
    }
    Compatible()
  }

  override def convert(
      expr: StringLPad,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    scalarFunctionExprToProto(
      "lpad",
      exprToProtoInternal(expr.str, inputs, binding),
      exprToProtoInternal(expr.len, inputs, binding),
      exprToProtoInternal(expr.pad, inputs, binding))
  }
}

object CometRegExpReplace extends CometExpressionSerde[RegExpReplace] {
  override def getSupportLevel(expr: RegExpReplace): SupportLevel = {
    if (!RegExp.isSupportedPattern(expr.regexp.toString) &&
      !CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.get()) {
      withInfo(
        expr,
        s"Regexp pattern ${expr.regexp} is not compatible with Spark. " +
          s"Set ${CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key}=true " +
          "to allow it anyway.")
      return Incompatible()
    }
    expr.pos match {
      case Literal(value, DataTypes.IntegerType) if value == 1 => Compatible()
      case _ =>
        Unsupported(Some("Comet only supports regexp_replace with an offset of 1 (no offset)."))
    }
  }

  override def convert(
      expr: RegExpReplace,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val subjectExpr = exprToProtoInternal(expr.subject, inputs, binding)
    val patternExpr = exprToProtoInternal(expr.regexp, inputs, binding)
    val replacementExpr = exprToProtoInternal(expr.rep, inputs, binding)
    // DataFusion's regexp_replace stops at the first match. We need to add the 'g' flag
    // to apply the regex globally to match Spark behavior.
    val flagsExpr = exprToProtoInternal(Literal("g"), inputs, binding)
    val optExpr = scalarFunctionExprToProto(
      "regexp_replace",
      subjectExpr,
      patternExpr,
      replacementExpr,
      flagsExpr)
    optExprWithInfo(optExpr, expr, expr.subject, expr.regexp, expr.rep, expr.pos)
  }
}

trait CommonStringExprs {

  def stringDecode(
      expr: Expression,
      charset: Expression,
      bin: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    charset match {
      case Literal(str, DataTypes.StringType)
          if str.toString.toLowerCase(Locale.ROOT) == "utf-8" =>
        // decode(col, 'utf-8') can be treated as a cast with "try" eval mode that puts nulls
        // for invalid strings.
        // Left child is the binary expression.
        val binExpr = exprToProtoInternal(bin, inputs, binding)
        if (binExpr.isDefined) {
          CometCast.castToProto(expr, None, DataTypes.StringType, binExpr.get, CometEvalMode.TRY)
        } else {
          withInfo(expr, bin)
          None
        }
      case _ =>
        withInfo(expr, "Comet only supports decoding with 'utf-8'.")
        None
    }
  }
}
