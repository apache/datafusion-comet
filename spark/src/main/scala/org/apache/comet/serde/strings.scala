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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression, Like, Literal, RLike, StringRPad, Substring}
import org.apache.spark.sql.types.{DataTypes, LongType, StringType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{createBinaryExpr, exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto}

class CometStringExprCastChildren(function: String) extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val castExpr = expr.children.map(Cast(_, StringType))
    val childExpr = castExpr.map(exprToProtoInternal(_, inputs, binding))
    val optExpr = scalarFunctionExprToProto(function, childExpr: _*)
    optExprWithInfo(optExpr, expr, castExpr: _*)
  }
}

object CometAscii extends CometStringExprCastChildren("ascii")

object CometBitLength extends CometStringExprCastChildren("bit_length")

object CometStringInstr extends CometStringExprCastChildren("strpos")

object CometStringRepeat extends CometExpressionSerde {

  override def convert(
      expr: Expression,
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

object CometStringReplace extends CometStringExprCastChildren("replace")

object CometStringTranslate extends CometStringExprCastChildren("translate")

object CometStringTrim extends CometStringExprCastChildren("trim")

object CometStringTrimLeft extends CometStringExprCastChildren("ltrim")

object CometStringTrimRight extends CometStringExprCastChildren("rtrim")

object CometStringTrimBoth extends CometStringExprCastChildren("btrim")

class CometCaseConversionBase(function: String) extends CometStringExprCastChildren(function) {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
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

object CometUpper extends CometCaseConversionBase("upper")

object CometLower extends CometCaseConversionBase("lower")

object CometLength extends CometStringExprCastChildren("length")

object CometInitCap extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (CometConf.COMET_EXEC_INITCAP_ENABLED.get()) {
      val castExpr = Cast(expr.children.head, StringType)
      val childExpr = exprToProtoInternal(castExpr, inputs, binding)
      val optExpr = scalarFunctionExprToProto("initcap", childExpr)
      optExprWithInfo(optExpr, expr, castExpr)
    } else {
      withInfo(
        expr,
        "Comet initCap is not compatible with Spark yet. " +
          "See https://github.com/apache/datafusion-comet/issues/1052 ." +
          s"Set ${CometConf.COMET_EXEC_INITCAP_ENABLED.key}=true to enable it anyway.")
      None
    }

  }
}

object CometChr extends UnaryScalarFuncSerde("char")

object CometConcatWs extends CometStringExprCastChildren("concat_ws")

object CometStringSpace extends UnaryScalarFuncSerde("string_space")

object CometStartsWith extends ScalarFuncSerde("starts_with")

object CometEndsWith extends ScalarFuncSerde("end_with")

object CometContains extends ScalarFuncSerde("contains")

object CometSubstring extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val substring = expr.asInstanceOf[Substring]
    (substring.pos, substring.len) match {
      case (Literal(pos, _), Literal(len, _)) =>
        exprToProtoInternal(substring.str, inputs, binding) match {
          case Some(strExpr) =>
            val builder = ExprOuterClass.Substring.newBuilder()
            builder.setChild(strExpr)
            builder.setStart(pos.asInstanceOf[Int])
            builder.setLen(len.asInstanceOf[Int])
            Some(ExprOuterClass.Expr.newBuilder().setSubstring(builder).build())
          case None =>
            withInfo(expr, substring.str)
            None
        }
      case _ =>
        withInfo(expr, "Substring pos and len must be literals")
        None
    }
  }
}

object CometLike extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val like = expr.asInstanceOf[Like]
    if (like.escapeChar == '\\') {
      createBinaryExpr(
        expr,
        like.left,
        like.right,
        inputs,
        binding,
        (builder, binaryExpr) => builder.setLike(binaryExpr))
    } else {
      withInfo(expr, s"custom escape character ${like.escapeChar} not supported in LIKE")
      None
    }
  }
}

object CometRLike extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val rlike = expr.asInstanceOf[RLike]
    rlike.right match {
      case Literal(pattern, DataTypes.StringType) =>
        val regex = pattern.toString
        if (regex.contains("(?i)") || regex.contains("(?-i)")) {
          withInfo(expr, "Regex flag (?i) and (?-i) are not supported")
          None
        } else {
          createBinaryExpr(
            expr,
            rlike.left,
            rlike.right,
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

object CometOctetLength extends CometStringExprCastChildren("octet_length")

object CometReverse extends CometStringExprCastChildren("reverse")

object CometStringRPad extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val stringRPad = expr.asInstanceOf[StringRPad]
    stringRPad.pad match {
      case Literal(str, DataTypes.StringType) if str.toString == " " =>
        scalarFunctionExprToProto(
          "rpad",
          exprToProtoInternal(stringRPad.str, inputs, binding),
          exprToProtoInternal(stringRPad.len, inputs, binding))
      case _ =>
        withInfo(expr, "StringRPad with non-space characters is not supported")
        None
    }
  }
}
