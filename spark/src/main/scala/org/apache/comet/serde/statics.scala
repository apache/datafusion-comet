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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Base64, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.types.BooleanType

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto}

object CometStaticInvoke extends CometExpressionSerde[StaticInvoke] {

  // With Spark 3.4, CharVarcharCodegenUtils.readSidePadding gets called to pad spaces for
  // char types.
  // See https://github.com/apache/spark/pull/38151
  private val staticInvokeExpressions
      : Map[(String, Class[_]), CometExpressionSerde[StaticInvoke]] =
    Map(
      ("readSidePadding", classOf[CharVarcharCodegenUtils]) -> CometScalarFunction(
        "read_side_padding"),
      ("encode", classOf[Base64]) -> CometBase64Encode)

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
 * Handler for Base64.encode StaticInvoke (Spark 3.5+, where Base64 is RuntimeReplaceable).
 * Maps to DataFusion's built-in encode(input, 'base64') function.
 */
private object CometBase64Encode extends CometExpressionSerde[StaticInvoke] {

  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // Check if chunked mode is requested (2nd argument, Spark 3.5+)
    expr.arguments match {
      case Seq(_, Literal(true, BooleanType)) =>
        withInfo(expr, "base64 with chunk encoding is not supported")
        return None
      case _ => // OK: either no chunkBase64 param (Spark 3.4) or chunkBase64=false
    }
    val inputExpr = exprToProtoInternal(expr.arguments.head, inputs, binding)
    val encodingExpr = exprToProtoInternal(Literal("base64"), inputs, binding)
    val optExpr = scalarFunctionExprToProto("encode", inputExpr, encodingExpr)
    optExprWithInfo(optExpr, expr, expr.arguments.head)
  }
}
