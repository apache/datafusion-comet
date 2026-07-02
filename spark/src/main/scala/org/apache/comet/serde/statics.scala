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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Base64, ExpressionImplUtils, Literal, StringDecode, TryEval, UrlCodec}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.types.StringType

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithFallbackReason, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType}

object CometStaticInvoke extends CometExpressionSerde[StaticInvoke] {

  // With Spark 3.4, CharVarcharCodegenUtils.readSidePadding gets called to pad spaces for
  // char types.
  // See https://github.com/apache/spark/pull/38151
  private val staticInvokeExpressions
      : Map[(String, Class[_]), CometExpressionSerde[StaticInvoke]] =
    Map(
      ("readSidePadding", classOf[CharVarcharCodegenUtils]) -> CometScalarFunction(
        "read_side_padding"),
      ("isLuhnNumber", classOf[ExpressionImplUtils]) -> CometScalarFunction("luhn_check"),
      ("encode", UrlCodec.getClass) -> CometUrlEncodeStaticInvoke,
      ("decode", UrlCodec.getClass) -> CometUrlDecodeStaticInvoke,
      ("aesEncrypt", classOf[ExpressionImplUtils]) -> CometStaticInvokeCodegenDispatch,
      ("aesDecrypt", classOf[ExpressionImplUtils]) -> CometStaticInvokeCodegenDispatch,
      // Spark 4.0 lowers `decode(bin, charset)` to `StaticInvoke(StringDecode.decode, ...)`
      // carrying the `legacyCharsets` / `legacyErrorAction` flags. Routing through the codegen
      // dispatcher runs Spark's own decoder so both flags are honored. See #4465.
      ("decode", classOf[StringDecode]) -> CometStaticInvokeCodegenDispatch,
      // Spark 3.5+ makes `Base64` RuntimeReplaceable, lowering `base64(bin)` to
      // `StaticInvoke(Base64.encode, Seq(child, chunkBase64), ...)`. On Spark 3.4 the `Base64`
      // node survives and is handled directly (see CometBase64).
      ("encode", classOf[Base64]) -> CometBase64StaticInvoke)

  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    staticInvokeExpressions.get((expr.functionName, expr.staticObject)) match {
      case Some(handler) =>
        handler.convert(expr, inputs, binding)
      case None =>
        withFallbackReason(
          expr,
          s"Static invoke expression: ${expr.functionName} is not supported",
          expr.children: _*)
        None
    }
  }
}

object CometUrlEncodeStaticInvoke extends CometExpressionSerde[StaticInvoke] {
  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.children.head, inputs, binding)
    val optExpr = scalarFunctionExprToProto("url_encode", childExpr)
    optExprWithFallbackReason(optExpr, expr, expr.children: _*)
  }
}

object CometUrlDecodeStaticInvoke extends CometExpressionSerde[StaticInvoke] {
  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val failOnError = expr.children match {
      case Seq(_, Literal(false, _)) => false
      case _ => true
    }
    val funcName = if (failOnError) "url_decode" else "try_url_decode"
    val childExpr = exprToProtoInternal(expr.children.head, inputs, binding)
    val optExpr = scalarFunctionExprToProto(funcName, childExpr)
    optExprWithFallbackReason(optExpr, expr, expr.children: _*)
  }
}

/**
 * Handles `base64(bin)` on Spark 3.5+, where it lowers to `StaticInvoke(Base64.encode, Seq(child,
 * chunkBase64))`. The `chunkBase64` literal carries `spark.sql.chunkBase64String.enabled`
 * (default true) and is passed through to the native function, which honors both modes.
 */
object CometBase64StaticInvoke extends CometExpressionSerde[StaticInvoke] {
  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.arguments.head, inputs, binding)
    val chunkExpr = exprToProtoInternal(expr.arguments(1), inputs, binding)
    val optExpr = scalarFunctionExprToProtoWithReturnType(
      "base64",
      StringType,
      failOnError = false,
      childExpr,
      chunkExpr)
    optExprWithFallbackReason(optExpr, expr, expr.arguments: _*)
  }
}

/** Routes a [[StaticInvoke]] through the JVM codegen dispatcher; used for AES. */
object CometStaticInvokeCodegenDispatch extends CometCodegenDispatch[StaticInvoke]

/** Routes [[TryEval]] through the JVM codegen dispatcher; used for `try_aes_decrypt`. */
object CometTryEval extends CometCodegenDispatch[TryEval]
