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

import org.apache.spark.sql.catalyst.expressions.{AesDecrypt, Attribute, Expression, ExpressionImplUtils}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProtoWithReturnType}

private object CometAesDecryptHelper {
  def convertToAesDecryptExpr[T <: Expression](
      expr: T,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val childExpr = expr.children.map(exprToProtoInternal(_, inputs, binding))
    val optExpr = scalarFunctionExprToProtoWithReturnType(
      "aes_decrypt",
      expr.dataType,
      failOnError = false,
      childExpr: _*)
    optExprWithInfo(optExpr, expr, expr.children: _*)
  }
}

object CometAesDecrypt extends CometExpressionSerde[AesDecrypt] {
  override def convert(
      expr: AesDecrypt,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    CometAesDecryptHelper.convertToAesDecryptExpr(expr, inputs, binding)
  }
}

object CometAesDecryptStaticInvoke extends CometExpressionSerde[StaticInvoke] {
  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    CometAesDecryptHelper.convertToAesDecryptExpr(expr, inputs, binding)
  }
}

object CometStaticInvoke extends CometExpressionSerde[StaticInvoke] {

  // With Spark 3.4, CharVarcharCodegenUtils.readSidePadding gets called to pad spaces for
  // char types.
  // See https://github.com/apache/spark/pull/38151
  private val staticInvokeExpressions
      : Map[(String, Class[_]), CometExpressionSerde[StaticInvoke]] =
    Map(
      ("readSidePadding", classOf[CharVarcharCodegenUtils]) -> CometScalarFunction(
        "read_side_padding"),
      ("aesDecrypt", classOf[ExpressionImplUtils]) -> CometAesDecryptStaticInvoke)

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
