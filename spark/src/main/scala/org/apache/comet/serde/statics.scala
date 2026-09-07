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
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.types.StringType

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, scalarFunctionExprToProto, scalarFunctionExprToProtoWithReturnType}

object CometStaticInvoke extends CometExpressionSerde[StaticInvoke] {

  // With Spark 3.4, CharVarcharCodegenUtils.readSidePadding gets called to pad spaces for
  // char types.
  // See https://github.com/apache/spark/pull/38151
  /**
   * Handlers keyed by `(functionName, staticObject class name)`. Class names rather than classes
   * so that Iceberg's system functions, whose classes are not on Comet's compile classpath, can
   * share the map; see [[CometIcebergSystemFunctions]].
   */
  private val staticInvokeExpressions: Map[(String, String), CometExpressionSerde[StaticInvoke]] =
    Map[(String, String), CometExpressionSerde[StaticInvoke]](
      ("readSidePadding", classOf[CharVarcharCodegenUtils].getName) -> CometScalarFunction(
        "read_side_padding"),
      ("isLuhnNumber", classOf[ExpressionImplUtils].getName) -> CometScalarFunction("luhn_check"),
      ("encode", UrlCodec.getClass.getName) -> CometUrlEncodeStaticInvoke,
      ("decode", UrlCodec.getClass.getName) -> CometUrlDecodeStaticInvoke,
      ("aesEncrypt", classOf[ExpressionImplUtils].getName) -> CometStaticInvokeCodegenDispatch,
      ("aesDecrypt", classOf[ExpressionImplUtils].getName) -> CometStaticInvokeCodegenDispatch,
      // Spark 4.0 lowers `decode(bin, charset)` to `StaticInvoke(StringDecode.decode, ...)`
      // carrying the `legacyCharsets` / `legacyErrorAction` flags. Routing through the codegen
      // dispatcher runs Spark's own decoder so both flags are honored. See #4465.
      ("decode", classOf[StringDecode].getName) -> CometStaticInvokeCodegenDispatch,
      // Spark 3.5+ makes `Base64` RuntimeReplaceable, lowering `base64(bin)` to
      // `StaticInvoke(Base64.encode, Seq(child, chunkBase64), ...)`. On Spark 3.4 the `Base64`
      // node survives and is handled directly (see CometBase64).
      ("encode", classOf[Base64].getName) -> CometBase64StaticInvoke) ++
      CometIcebergSystemFunctions.staticInvokeHandlers

  private def handlerFor(expr: StaticInvoke): Option[CometExpressionSerde[StaticInvoke]] =
    staticInvokeExpressions.get((expr.functionName, expr.staticObject.getName))

  /** Every Iceberg system function is named `invoke`, so name the declaring class too. */
  private def noNativePathNote(expr: StaticInvoke): String =
    s"Static invoke expression: ${expr.functionName} has no native implementation and the " +
      s"codegen dispatcher declined it (declared on ${expr.staticObject.getName})"

  /**
   * A `StaticInvoke` outside the allowlist reports `Compatible` and is handled in [[convert]],
   * which routes it through the codegen dispatcher. Deliberately not `Unsupported` +
   * [[CodegenDispatchFallback]]: that would also route a *handler's* `Unsupported` through the
   * dispatcher, and at least one of those is not dispatchable. `CometIcebergTruncate` declines a
   * decimal because Iceberg's `truncate` can return a value wider than the column's declared
   * precision, which Spark nulls only when the row is materialized; the dispatcher writes into an
   * Arrow `Decimal128(precision, scale)` vector just like a native kernel does, so it produces
   * the out-of-range value instead of a null. The mixin's contract ("the case must be something
   * `doGenCode` can compile") does not cover a limit that lives at the Arrow output boundary, so
   * enrollment stays with the individual handlers.
   */
  override def getSupportLevel(expr: StaticInvoke): SupportLevel =
    handlerFor(expr).map(_.getSupportLevel(expr)).getOrElse(Compatible())

  /**
   * `GenerateDocs` only asks the serde registered for the expression class, which is this object,
   * so the per-function handlers' notes have to be collected here or they never reach the
   * compatibility guide.
   */
  override def getUnsupportedReasons(): Seq[String] =
    staticInvokeExpressions.values.toSeq.distinct.flatMap(_.getUnsupportedReasons()).distinct

  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    handlerFor(expr) match {
      case Some(handler) =>
        handler.convert(expr, inputs, binding)
      case None =>
        // Nothing in the allowlist covers this lowering, so run Spark's own implementation inside
        // the Comet pipeline rather than failing the whole operator back to Spark.
        // `StaticInvoke.doGenCode` emits a static method call, so the kernel matches Spark by
        // construction. Spark 4.x keeps lowering more `RuntimeReplaceable` functions this way
        // (`encode`, `is_valid_utf8`, the `TIME` family, ...) and `lpad` / `rpad` on binary has
        // lowered to `StaticInvoke(ByteArray, ...)` since Spark 3.4.
        //
        // The encoder and deserializer trees that make up most `StaticInvoke` usage in typed
        // Dataset operations are unaffected: their arguments are `ObjectType`, which
        // `CometBatchKernelCodegen.isSupportedDataType` rejects, so the dispatcher declines them
        // and they fall back exactly as before.
        CometStaticInvokeCodegenDispatch.convert(expr, inputs, binding).orElse {
          // The dispatcher tags its own reason, but not which static invoke it was.
          withFallbackReason(expr, noNativePathNote(expr))
          None
        }
    }
  }
}

/**
 * Catch-all for `Invoke`, which has no allowlist at all. Spark 4.x lowers a growing number of
 * `RuntimeReplaceable` expressions to evaluator-backed `Invoke` nodes; the Spark 4.x shim
 * reconstructs the handful it recognizes and everything else lands here. `Invoke.doGenCode` emits
 * a method call on the target object, so the dispatcher runs Spark's own implementation inside
 * the Comet pipeline rather than failing the operator back to Spark.
 *
 * As with [[CometStaticInvoke]], the object-typed `Invoke` nodes in encoder / deserializer trees
 * are rejected by `CometBatchKernelCodegen.canHandle` and fall back as before.
 */
object CometInvoke extends CometCodegenDispatch[Invoke]

object CometUrlEncodeStaticInvoke extends CometExpressionSerde[StaticInvoke] {
  override def convert(
      expr: StaticInvoke,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.children.head, inputs, binding)
    val optExpr = scalarFunctionExprToProto("url_encode", childExpr)
    optExpr
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
    optExpr
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
    optExpr
  }
}

/** Routes a [[StaticInvoke]] through the JVM codegen dispatcher; used for AES. */
object CometStaticInvokeCodegenDispatch extends CometCodegenDispatch[StaticInvoke]

/** Routes [[TryEval]] through the JVM codegen dispatcher; used for `try_aes_decrypt`. */
object CometTryEval extends CometCodegenDispatch[TryEval]
