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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression}
import org.apache.spark.sql.types.{LongType, StringType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto}

object CometAscii extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val child = expr.children.head
    val castExpr = Cast(child, StringType)
    val childExpr = exprToProtoInternal(castExpr, inputs, binding)
    val optExpr = scalarFunctionExprToProto("ascii", childExpr)
    optExprWithInfo(optExpr, expr, castExpr)
  }
}

object CometBitLength extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val child = expr.children.head
    val castExpr = Cast(child, StringType)
    val childExpr = exprToProtoInternal(castExpr, inputs, binding)
    val optExpr = scalarFunctionExprToProto("bit_length", childExpr)
    optExprWithInfo(optExpr, expr, castExpr)
  }
}

object CometStringInstr extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val children = expr.children
    val leftCast = Cast(children(0), StringType)
    val rightCast = Cast(children(1), StringType)
    val leftExpr = exprToProtoInternal(leftCast, inputs, binding)
    val rightExpr = exprToProtoInternal(rightCast, inputs, binding)
    val optExpr = scalarFunctionExprToProto("strpos", leftExpr, rightExpr)
    optExprWithInfo(optExpr, expr, leftCast, rightCast)
  }
}

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

object CometStringReplace extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val children = expr.children
    val srcCast = Cast(children(0), StringType)
    val searchCast = Cast(children(1), StringType)
    val replaceCast = Cast(children(2), StringType)
    val srcExpr = exprToProtoInternal(srcCast, inputs, binding)
    val searchExpr = exprToProtoInternal(searchCast, inputs, binding)
    val replaceExpr = exprToProtoInternal(replaceCast, inputs, binding)
    val optExpr = scalarFunctionExprToProto("replace", srcExpr, searchExpr, replaceExpr)
    optExprWithInfo(optExpr, expr, srcCast, searchCast, replaceCast)
  }
}

object CometStringTranslate extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val children = expr.children
    val srcCast = Cast(children(0), StringType)
    val matchingCast = Cast(children(1), StringType)
    val replaceCast = Cast(children(2), StringType)
    val srcExpr = exprToProtoInternal(srcCast, inputs, binding)
    val matchingExpr = exprToProtoInternal(matchingCast, inputs, binding)
    val replaceExpr = exprToProtoInternal(replaceCast, inputs, binding)
    val optExpr = scalarFunctionExprToProto("translate", srcExpr, matchingExpr, replaceExpr)
    optExprWithInfo(optExpr, expr, srcCast, matchingCast, replaceCast)
  }
}

object CometTrim extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val children = expr.children
    val srcStr = children(0)
    val trimStr = Try(children(1)).toOption
    CometTrimCommon.trim(expr, srcStr, trimStr, inputs, binding, "trim")
  }
}

object CometStringTrimLeft extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val children = expr.children
    val srcStr = children(0)
    val trimStr = Try(children(1)).toOption
    CometTrimCommon.trim(expr, srcStr, trimStr, inputs, binding, "ltrim")
  }
}

object CometStringTrimRight extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val children = expr.children
    val srcStr = children(0)
    val trimStr = Try(children(1)).toOption
    CometTrimCommon.trim(expr, srcStr, trimStr, inputs, binding, "rtrim")
  }
}

object CometStringTrimBoth extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val children = expr.children
    val srcStr = children(0)
    val trimStr = Try(children(1)).toOption
    CometTrimCommon.trim(expr, srcStr, trimStr, inputs, binding, "btrim")
  }
}

private object CometTrimCommon {
  def trim(
      expr: Expression, // parent expression
      srcStr: Expression,
      trimStr: Option[Expression],
      inputs: Seq[Attribute],
      binding: Boolean,
      trimType: String): Option[Expr] = {
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

object CometUpper extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (CometConf.COMET_CASE_CONVERSION_ENABLED.get()) {
      val castExpr = Cast(expr.children.head, StringType)
      val childExpr = exprToProtoInternal(castExpr, inputs, binding)
      val optExpr = scalarFunctionExprToProto("upper", childExpr)
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

object CometLower extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (CometConf.COMET_CASE_CONVERSION_ENABLED.get()) {
      val castExpr = Cast(expr.children.head, StringType)
      val childExpr = exprToProtoInternal(castExpr, inputs, binding)
      val optExpr = scalarFunctionExprToProto("lower", childExpr)
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

object CometLength extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val castExpr = Cast(expr.children.head, StringType)
    val childExpr = exprToProtoInternal(castExpr, inputs, binding)
    val optExpr = scalarFunctionExprToProto("length", childExpr)
    optExprWithInfo(optExpr, expr, castExpr)
  }
}

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
    val optExpr = scalarFunctionExprToProto("char", childExpr)
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
