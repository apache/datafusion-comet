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

import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.{Attribute, BinaryExpression, Cast, Expression, Like, Literal, RLike, StringRPad, StringSpace, Substring}
import org.apache.spark.sql.types.{DataTypes, LongType, StringType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{createBinaryExpr, createUnaryExpr, exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto}

class CometBinaryStringFunction(functionName: String) extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val binaryExpr = expr.asInstanceOf[BinaryExpression]
    scalarFunctionExprToProto(
      functionName,
      exprToProtoInternal(binaryExpr.left, inputs, binding),
      exprToProtoInternal(binaryExpr.right, inputs, binding))
  }
}

object CometStartsWith extends CometBinaryStringFunction("starts_with")
object CometEndsWith extends CometBinaryStringFunction("ends_with")
object CometContains extends CometBinaryStringFunction("contains")

class CometStringExpr(functionName: String) extends CometExpressionSerde {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val cast = expr.children.map(child => Cast(child, StringType))
    val proto: Seq[Option[Expr]] = cast.map(cast => exprToProtoInternal(cast, inputs, binding))
    val optExpr = scalarFunctionExprToProto(functionName, proto: _*)
    optExprWithInfo(optExpr, expr, cast: _*)
  }
}

object CometAscii extends CometStringExpr("ascii")
object CometBitLength extends CometStringExpr("bit_length")
object CometStringInstr extends CometStringExpr("strpos")
object CometStringReplace extends CometStringExpr("replace")
object CometStringTranslate extends CometStringExpr("translate")
object CometLength extends CometStringExpr("length")
object CometOctetLength extends CometStringExpr("octet_length")
object CometReverse extends CometStringExpr("reverse")

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

class CometTrimBase(trimType: String) extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val children = expr.children
    val srcStr = children(0)
    val trimStr = Try(children(1)).toOption
    val srcCast = Cast(srcStr, StringType)
    val srcExpr = exprToProtoInternal(srcCast, inputs, binding)
    if (trimStr.isDefined) {
      val trimCast = Cast(trimStr.get, StringType)
      val trimExpr = exprToProtoInternal(trimCast, inputs, binding)
      val optExpr = scalarFunctionExprToProto(trimType, srcExpr, trimExpr)
      optExprWithInfo(optExpr, expr, srcCast, trimCast)
    } else {
      val optExpr = scalarFunctionExprToProto(trimType, srcExpr)
      optExprWithInfo(optExpr, expr, srcCast)
    }
  }
}

object CometTrim extends CometTrimBase("trim")
object CometStringTrimLeft extends CometTrimBase("ltrim")
object CometStringTrimRight extends CometTrimBase("rtrim")
object CometStringTrimBoth extends CometTrimBase("btrim")

class CometCaseConversionBase(name: String) extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (CometConf.COMET_CASE_CONVERSION_ENABLED.get()) {
      val castExpr = Cast(expr.children.head, StringType)
      val childExpr = exprToProtoInternal(castExpr, inputs, binding)
      val optExpr = scalarFunctionExprToProto(name, childExpr)
      optExprWithInfo(optExpr, expr, castExpr)
    } else {
      withInfo(
        expr,
        "Comet is not compatible with Spark for case conversion in " +
          s"locale-specific cases. Set ${CometConf.COMET_CASE_CONVERSION_ENABLED.key}=true " +
          "to enable it anyway.")
      None
    }
  }
}

object CometUpper extends CometCaseConversionBase("upper")
object CometLower extends CometCaseConversionBase("lower")

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

object CometChr extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val child = expr.children.head
    val childExpr = exprToProtoInternal(child, inputs, binding)
    val optExpr = scalarFunctionExprToProto("chr", childExpr)
    optExprWithInfo(optExpr, expr, child)
  }
}

object CometConcatWs extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    var childExprs: Seq[Expression] = Seq()
    val exprs = expr.children.map(e => {
      val castExpr = Cast(e, StringType)
      childExprs = childExprs :+ castExpr
      exprToProtoInternal(castExpr, inputs, binding)
    })
    val optExpr = scalarFunctionExprToProto("concat_ws", exprs: _*)
    optExprWithInfo(optExpr, expr, childExprs: _*)
  }
}

object CometStringSpace extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    createUnaryExpr(
      expr,
      expr.asInstanceOf[StringSpace].child,
      inputs,
      binding,
      (builder, unaryExpr) => builder.setStringSpace(unaryExpr))
  }
}

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
