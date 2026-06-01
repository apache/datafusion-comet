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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BindReferences, Expression, Literal, ScalaUDF}
import org.apache.spark.sql.types.BinaryType

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
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

  override def convert(expr: ScalaUDF, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    emitJvmCodegenDispatch(expr, inputs, binding)

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
   */
  def emitJvmCodegenDispatch(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (!CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get()) {
      withFallbackReason(
        expr,
        s"${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false; expression has no native " +
          "path so the plan falls back to Spark")
      return None
    }

    // Bind against only the AttributeReferences the tree actually reads, so ordinals align with
    // the data args we ship.
    val attrs = expr.collect { case a: AttributeReference => a }.distinct
    val boundExpr = BindReferences.bindReference(expr, AttributeSeq(attrs))

    // Gate at plan time. Surface the reason via withFallbackReason rather than crashing Janino
    // at execute.
    CometBatchKernelCodegen.canHandle(boundExpr) match {
      case Some(reason) =>
        withFallbackReason(expr, reason)
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
    val exprArg = exprToProtoInternal(Literal(bytes, BinaryType), inputs, binding)
      .getOrElse(return None)

    val dataArgs =
      attrs.map(a => exprToProtoInternal(a, inputs, binding).getOrElse(return None))
    val returnTypeProto = serializeDataType(expr.dataType).getOrElse(return None)

    val udfBuilder = ExprOuterClass.JvmScalarUdf
      .newBuilder()
      .setClassName(classOf[CometScalaUDFCodegen].getName)
      .addArgs(exprArg)
    dataArgs.foreach(udfBuilder.addArgs)
    udfBuilder
      .setReturnType(returnTypeProto)
      .setReturnNullable(expr.nullable)
    Some(
      ExprOuterClass.Expr
        .newBuilder()
        .setJvmScalarUdf(udfBuilder.build())
        .build())
  }
}

/**
 * Convenience base for serdes that route a non-ScalaUDF Spark expression through the codegen
 * dispatcher. Delegates `convert` to [[CometScalaUDF.emitJvmCodegenDispatch]] and marks the
 * expression `Compatible()` because the dispatcher runs Spark's own `doGenCode` inside the
 * kernel: behavior matches Spark exactly when [[CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED]] is
 * enabled, and the operator falls back to Spark cleanly when it is not.
 */
class CometCodegenDispatch[T <: Expression] extends CometExpressionSerde[T] {
  override def getSupportLevel(expr: T): SupportLevel = Compatible()
  // Intentionally no getCompatibleNotes override: the docs generator emits compat notes under
  // a heading that promises "no additional configuration required". The dispatcher flag is a
  // global concern documented elsewhere; tagging each expression here would contradict the
  // heading. When the flag is off, `convert` returns None with a clear fallback reason that
  // shows up in EXPLAIN, which is the right place for that signal.
  override def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
}
