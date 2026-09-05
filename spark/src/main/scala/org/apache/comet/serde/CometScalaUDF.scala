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

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BindReferences, Expression, Literal, RuntimeReplaceable, ScalaUDF}
import org.apache.spark.sql.types.BinaryType

import org.apache.comet.CometConf
import org.apache.comet.CometExplainInfo
import org.apache.comet.CometSparkSessionExtensions.{withCodegenDispatchExpr, withFallbackReason}
import org.apache.comet.codegen.CometBatchKernelCodegen
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Routes scalar `ScalaUDF` (Scala and Java UDFs) through the codegen dispatcher.
 * `ScalaUDF.doGenCode` emits compilable Java that invokes the user function via
 * `ctx.addReferenceObj`; the dispatcher serializes the bound tree, the closure serializer carries
 * the function reference across the wire, and the Janino-compiled kernel invokes it in a tight
 * batch loop.
 *
 * Not covered:
 *   - Aggregate UDFs (`ScalaAggregator`, `TypedImperativeAggregate`, legacy UDAF).
 *   - Table UDFs and generators.
 *   - Python / Pandas UDFs.
 *   - Hive `GenericUDF` / `SimpleUDF`.
 *
 * Gated by [[CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED]]. When disabled, plans containing a
 * `ScalaUDF` fall back to Spark for the enclosing operator.
 *
 * [[emitJvmCodegenDispatch]] exposes the same closure-serialize + dispatcher-proto path to other
 * serdes that want to keep a built-in Spark expression inside the Comet pipeline when no native
 * lowering is viable. See [[CometDateFormat]] for an example.
 */
object CometScalaUDF extends CometExpressionSerde[ScalaUDF] {

  override def getSupportLevel(expr: ScalaUDF): SupportLevel = dispatchSupportLevel(expr)

  override def convert(expr: ScalaUDF, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    emitJvmCodegenDispatch(expr, inputs, binding)

  /**
   * Bind `expr` the way [[emitJvmCodegenDispatch]] will.
   *
   * `RuntimeReplaceable` expressions (e.g. Spark 4's `StructsToJson`) have a `doGenCode` that
   * always throws "Cannot generate code for expression". Catalyst's `ReplaceExpressions` rule
   * normally rewrites them to their `replacement` form before codegen runs. Comet's serde
   * sometimes works with the pre-rewrite form (via shim reconstruction) for matching purposes, so
   * unwrap to the replacement here before binding so the kernel compiles.
   *
   * Binding is against only the `AttributeReference`s the tree actually reads, so ordinals align
   * with the data args shipped alongside the closure. Those attributes are returned too, since
   * [[emitJvmCodegenDispatch]] needs them in the same order to build the data args.
   */
  private def bindForDispatch(expr: Expression): (Expression, Seq[AttributeReference]) = {
    val target = expr match {
      case rr: RuntimeReplaceable => rr.replacement
      case other => other
    }
    val attrs = target.collect { case a: AttributeReference => a }.distinct
    (BindReferences.bindReference(target, AttributeSeq(attrs)), attrs)
  }

  /**
   * `SupportLevel` for a serde whose only path is the codegen dispatcher. `Compatible` when the
   * dispatcher will accept the expression, `Unsupported` (with the same reason
   * [[emitJvmCodegenDispatch]] would have tagged) when it will not.
   *
   * Reporting this from `getSupportLevel` rather than discovering it inside `convert` keeps the
   * serde invariant intact and lets `exprToProtoInternal` handle the decline on its normal
   * `Unsupported` path. Behaviour is unchanged: `CometCodegenDispatch` does not mix in
   * `CodegenDispatchFallback`, so an `Unsupported` result still tags the reason and falls the
   * operator back to Spark, exactly as the `convert`-side decline did.
   *
   * This is a pure predicate -- it records no fallback reason of its own, because the
   * `Unsupported` arm of `exprToProtoInternal` already tags the notes returned here.
   */
  def dispatchSupportLevel(expr: Expression): SupportLevel = {
    val exprName = CometExplainInfo.exprDisplayName(expr)
    if (!CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get()) {
      return Unsupported(
        Some(
          s"$exprName: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false; expression has " +
            "no native path so the plan falls back to Spark"))
    }
    CometBatchKernelCodegen.canHandle(bindForDispatch(expr)._1) match {
      case Some(reason) => Unsupported(Some(s"$exprName: $reason"))
      case None => Compatible()
    }
  }

  /**
   * Bind `expr`, closure-serialize it, and emit a `JvmScalarUdf` proto routed through
   * [[CometScalaUDFCodegen]] so that native execution evaluates the expression inside the
   * Arrow-direct codegen dispatcher. The dispatcher will Janino-compile `expr.doGenCode` into a
   * batch kernel on first invocation per task.
   *
   * Returns `None` (with `withFallbackReason` tagging the reason) when the dispatcher is disabled
   * via [[CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED]] or when
   * [[CometBatchKernelCodegen.canHandle]] refuses the expression tree. Callers should treat
   * `None` as a clean Spark-fallback signal.
   *
   * Serdes that gate on [[dispatchSupportLevel]] have already screened both of those conditions,
   * so for them the checks below are a cheap re-verification. They are kept because several
   * serdes call this directly from `convert` without gating first.
   */
  def emitJvmCodegenDispatch(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val exprName = CometExplainInfo.exprDisplayName(expr)
    if (!CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get()) {
      withFallbackReason(
        expr,
        s"$exprName: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false; expression has " +
          "no native path so the plan falls back to Spark")
      return None
    }

    val (boundExpr, attrs) = bindForDispatch(expr)

    // Gate at plan time. Surface the reason via withFallbackReason rather than crashing Janino
    // at execute.
    CometBatchKernelCodegen.canHandle(boundExpr) match {
      case Some(reason) =>
        withFallbackReason(expr, s"$exprName: $reason")
        return None
      case None =>
    }

    // Serialize via Spark's closure serializer: respects the task context classloader (so user
    // UDF jars are visible) and matches Spark's wire format. The bytes become arg 0 of the
    // JvmScalarUdf proto and self-describe the expression so this works in cluster mode without
    // executor-side driver registry state.
    val serializer = SparkEnv.get.closureSerializer.newInstance()
    val buffer = serializer.serialize(boundExpr)
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    val exprArg = exprToProtoInternal(Literal(bytes, BinaryType), inputs, binding).getOrElse {
      withFallbackReason(
        expr,
        s"$exprName: codegen dispatch: could not serialize closure-serialized bound " +
          "expression payload")
      return None
    }

    val dataArgs = attrs.map { a =>
      exprToProtoInternal(a, inputs, binding).getOrElse {
        withFallbackReason(expr, s"$exprName: codegen dispatch: could not serialize data arg $a")
        return None
      }
    }
    val returnTypeProto = serializeDataType(expr.dataType).getOrElse {
      withFallbackReason(
        expr,
        s"$exprName: codegen dispatch: unsupported return type ${expr.dataType}")
      return None
    }

    val udfBuilder = ExprOuterClass.JvmScalarUdf
      .newBuilder()
      .setClassName(classOf[CometScalaUDFCodegen].getName)
      .addArgs(exprArg)
    dataArgs.foreach(udfBuilder.addArgs)
    udfBuilder
      .setReturnType(returnTypeProto)
      .setReturnNullable(expr.nullable)
    // Dispatch annotation for extended explain. Rolled up per operator by
    // `CometExecRule.rollUpInfoMessages`, which feeds the expression coverage stats and, when
    // `spark.comet.explain.codegen.enabled` is set, a single `[COMET-INFO: JVM codegen dispatcher:
    // ...]` line. Informational only - does not trigger fallback. The marker records that this
    // node itself was dispatched, which the name set alone cannot say once ancestors accumulate
    // their descendants' names.
    expr.setTagValue(CometExplainInfo.DISPATCHED_SELF, ())
    withCodegenDispatchExpr(expr, exprName)
    Some(
      ExprOuterClass.Expr
        .newBuilder()
        .setJvmScalarUdf(udfBuilder.build())
        .build())
  }
}

/**
 * Convenience base for serdes that route a non-ScalaUDF Spark expression through the codegen
 * dispatcher. Delegates `convert` to [[CometScalaUDF.emitJvmCodegenDispatch]], and reports
 * [[CometScalaUDF.dispatchSupportLevel]] so that the two conditions the dispatcher can refuse on
 * -- the global [[CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED]] flag being off, and
 * `CometBatchKernelCodegen.canHandle` rejecting the bound tree -- are reported from
 * `getSupportLevel` instead of surfacing as a `Compatible` serde that then declines in `convert`.
 *
 * When the dispatcher will run the expression this is `Compatible()`: behavior then matches Spark
 * exactly, because the kernel runs Spark's own `doGenCode`.
 */
class CometCodegenDispatch[T <: Expression] extends CometExpressionSerde[T] {
  override def getSupportLevel(expr: T): SupportLevel = CometScalaUDF.dispatchSupportLevel(expr)
  // Intentionally no getCompatibleNotes override: the docs generator emits compat notes under
  // a heading that promises "no additional configuration required". The dispatcher flag is a
  // global concern documented elsewhere; tagging each expression here would contradict the
  // heading. When the flag is off, `getSupportLevel` reports Unsupported with a clear reason
  // that shows up in EXPLAIN, which is the right place for that signal.
  override def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
}
