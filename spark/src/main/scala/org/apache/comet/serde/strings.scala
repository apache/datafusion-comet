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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Concat, ConcatWs, Expression, If, InitCap, IsNull, Left, Length, Like, Literal, Lower, RegExpReplace, Right, RLike, StringLPad, StringRepeat, StringRPad, StringSplit, Substring, Upper}
import org.apache.spark.sql.types.{BinaryType, DataTypes, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.expressions.{CometCast, CometEvalMode, RegExp}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{createBinaryExpr, exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType}

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

object CometLeft extends CometExpressionSerde[Left] {

  override def convert(expr: Left, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    expr.len match {
      case Literal(lenValue, _) =>
        exprToProtoInternal(expr.str, inputs, binding) match {
          case Some(strExpr) =>
            val builder = ExprOuterClass.Substring.newBuilder()
            builder.setChild(strExpr)
            builder.setStart(1)
            builder.setLen(lenValue.asInstanceOf[Int])
            Some(ExprOuterClass.Expr.newBuilder().setSubstring(builder).build())
          case None =>
            withInfo(expr, expr.str)
            None
        }
      case _ =>
        withInfo(expr, "LEFT len must be a literal")
        None
    }
  }

  override def getSupportLevel(expr: Left): SupportLevel = {
    expr.str.dataType match {
      case _: BinaryType | _: StringType => Compatible()
      case _ => Unsupported(Some(s"LEFT does not support ${expr.str.dataType}"))
    }
  }
}

object CometRight extends CometExpressionSerde[Right] {

  override def convert(expr: Right, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    expr.len match {
      case Literal(lenValue, _) =>
        val lenInt = lenValue.asInstanceOf[Int]
        if (lenInt <= 0) {
          // Match Spark's behavior: If(IsNull(str), NULL, "")
          // This ensures NULL propagation: RIGHT(NULL, 0) -> NULL, RIGHT("hello", 0) -> ""
          val isNullExpr = IsNull(expr.str)
          val nullLiteral = Literal.create(null, StringType)
          val emptyStringLiteral = Literal(UTF8String.EMPTY_UTF8, StringType)
          val ifExpr = If(isNullExpr, nullLiteral, emptyStringLiteral)

          // Serialize the If expression using existing infrastructure
          exprToProtoInternal(ifExpr, inputs, binding)
        } else {
          exprToProtoInternal(expr.str, inputs, binding) match {
            case Some(strExpr) =>
              val builder = ExprOuterClass.Substring.newBuilder()
              builder.setChild(strExpr)
              builder.setStart(-lenInt)
              builder.setLen(lenInt)
              Some(ExprOuterClass.Expr.newBuilder().setSubstring(builder).build())
            case None =>
              withInfo(expr, expr.str)
              None
          }
        }
      case _ =>
        withInfo(expr, "RIGHT len must be a literal")
        None
    }
  }

  override def getSupportLevel(expr: Right): SupportLevel = {
    expr.str.dataType match {
      case _: StringType => Compatible()
      case _ => Unsupported(Some(s"RIGHT does not support ${expr.str.dataType}"))
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

object CometConcatWs extends CometExpressionSerde[ConcatWs] {

  override def convert(expr: ConcatWs, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    expr.children.headOption match {
      // Match Spark behavior: when the separator is NULL, the result of concat_ws is NULL.
      case Some(Literal(null, _)) =>
        val nullLiteral = Literal.create(null, expr.dataType)
        exprToProtoInternal(nullLiteral, inputs, binding)

      case _ if expr.children.forall(_.foldable) =>
        // Fall back to Spark for all-literal args so ConstantFolding can handle it
        withInfo(expr, "all arguments are foldable")
        None

      case _ =>
        // For all other cases, use the generic scalar function implementation.
        CometScalarFunction[ConcatWs]("concat_ws").convert(expr, inputs, binding)
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
          !CometConf.isExprAllowIncompat("regexp")) {
          withInfo(
            expr,
            s"Regexp pattern $pattern is not compatible with Spark. " +
              s"Set ${CometConf.getExprAllowIncompatConfigKey("regexp")}=true " +
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
      !CometConf.isExprAllowIncompat("regexp")) {
      withInfo(
        expr,
        s"Regexp pattern ${expr.regexp} is not compatible with Spark. " +
          s"Set ${CometConf.getExprAllowIncompatConfigKey("regexp")}=true " +
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

/**
 * Serde for StringSplit expression. This is a custom Comet function (not a built-in DataFusion
 * function), so we need to include the return type in the protobuf to avoid DataFusion registry
 * lookup failures.
 */
object CometStringSplit extends CometExpressionSerde[StringSplit] {

  override def getSupportLevel(expr: StringSplit): SupportLevel =
    Incompatible(Some("Regex engine differences between Java and Rust"))

  override def convert(
      expr: StringSplit,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val strExpr = exprToProtoInternal(expr.str, inputs, binding)
    val regexExpr = exprToProtoInternal(expr.regex, inputs, binding)
    val limitExpr = exprToProtoInternal(expr.limit, inputs, binding)
    val optExpr = scalarFunctionExprToProtoWithReturnType(
      "split",
      expr.dataType,
      false,
      strExpr,
      regexExpr,
      limitExpr)
    optExprWithInfo(optExpr, expr, expr.str, expr.regex, expr.limit)
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

  def minutesOfTimeToProto(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val childOpt = expr.children.headOption.orElse {
      withInfo(expr, "MinutesOfTime has no child expression")
      None
    }

    childOpt.flatMap { child =>
      val timeZoneId = {
        val exprClass = expr.getClass
        try {
          val timeZoneIdMethod = exprClass.getMethod("timeZoneId")
          timeZoneIdMethod.invoke(expr).asInstanceOf[Option[String]]
        } catch {
          case _: NoSuchMethodException =>
            try {
              val timeZoneIdField = exprClass.getField("timeZoneId")
              timeZoneIdField.get(expr).asInstanceOf[Option[String]]
            } catch {
              case _: NoSuchFieldException | _: SecurityException => None
            }
        }
      }

      exprToProtoInternal(child, inputs, binding)
        .map { childExpr =>
          val builder = ExprOuterClass.Minute.newBuilder()
          builder.setChild(childExpr)

          val timeZone = timeZoneId.getOrElse("UTC")
          builder.setTimezone(timeZone)

          ExprOuterClass.Expr
            .newBuilder()
            .setMinute(builder)
            .build()
        }
        .orElse {
          withInfo(expr, child)
          None
        }
    }
  }
}
