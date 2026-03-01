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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.unsafe.types.ByteArray

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProtoWithReturnType}

object CometStaticInvoke extends CometExpressionSerde[StaticInvoke] {

  // With Spark 3.4, CharVarcharCodegenUtils.readSidePadding gets called to pad spaces for
  // char types.
  // See https://github.com/apache/spark/pull/38151
  private val staticInvokeExpressions
      : Map[(String, Class[_]), CometExpressionSerde[StaticInvoke]] =
    Map(
      ("readSidePadding", classOf[CharVarcharCodegenUtils]) -> CometScalarFunction(
        "read_side_padding"),
      ("lpad", classOf[ByteArray]) -> CometBinaryPad("binary_lpad"),
      ("rpad", classOf[ByteArray]) -> CometBinaryPad("binary_rpad"))

  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    staticInvokeExpressions.get((expr.functionName, expr.staticObject)) match {
      case Some(handler) =>
        handler.convert(expr, inputs, binding)
      case None =>
        withInfo(
          expr,
          s"Static invoke expression: ${expr.functionName} is not supported",
          expr.children: _*)
        None
    }
  }
}

/**
 * Handler for ByteArray.lpad/rpad StaticInvoke (Spark 3.2+, via BinaryPad). Maps to Comet's
 * binary_lpad/binary_rpad UDFs.
 */
private case class CometBinaryPad(funcName: String) extends CometExpressionSerde[StaticInvoke] {

  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val str = expr.arguments(0)
    val len = expr.arguments(1)
    val pad = expr.arguments(2)
    if (str.foldable) {
      withInfo(expr, "Scalar values are not supported for the str argument", str)
      return None
    }
    if (!len.foldable) {
      withInfo(expr, "Only scalar values are supported for the len argument", len)
      return None
    }
    if (!pad.foldable) {
      withInfo(expr, "Only scalar values are supported for the pad argument", pad)
      return None
    }
    val strExpr = exprToProtoInternal(str, inputs, binding)
    val lenExpr = exprToProtoInternal(len, inputs, binding)
    val padExpr = exprToProtoInternal(pad, inputs, binding)
    val optExpr = scalarFunctionExprToProtoWithReturnType(
      funcName,
      expr.dataType,
      false,
      strExpr,
      lenExpr,
      padExpr)
    optExprWithInfo(optExpr, expr, expr.arguments: _*)
  }
}
