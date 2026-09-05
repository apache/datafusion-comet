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

import scala.util.control.NonFatal

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BindReferences, Expression, Literal, RuntimeReplaceable, ScalaUDF}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
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

  /**
   * Marks a subtree as "dispatch this whole thing as one kernel", overriding the normal per-node
   * serde lookup in `QueryPlanSerde.exprToProtoInternal`.
   *
   * Needed when a rewrite builds a tree whose root has a perfectly good native serde but whose
   * children must not be converted independently. `RewriteTypedDatasetMap` is the motivating
   * case: it fuses a typed `Dataset.map` into a `CreateNamedStruct` over N serializer expressions
   * that all share one `Invoke` of the user closure. Letting `CometCreateNamedStruct` convert
   * each field separately would emit N dispatch protos and call the closure N times per row.
   * Tagging the root produces one kernel instead, and subexpression elimination inside it
   * collapses the shared `Invoke` to a single call per row.
   *
   * A tag rather than a Comet-specific `Expression` subclass on purpose: the rewritten plan stays
   * built entirely from stock Spark expressions, so it still executes correctly if the enclosing
   * operator ends up falling back to Spark for an unrelated reason.
   */
  val FORCE_DISPATCH: TreeNodeTag[Unit] = TreeNodeTag[Unit]("comet.forceCodegenDispatch")

  override def convert(expr: ScalaUDF, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    emitJvmCodegenDispatch(expr, inputs, binding)

  /**
   * Bind `expr`, closure-serialize it, and emit a `JvmScalarUdf` proto routed through
   * [[CometScalaUDFCodegen]] so that native execution evaluates the expression inside the
   * Arrow-direct codegen dispatcher. The dispatcher will Janino-compile `expr.doGenCode` into a
   * batch kernel on first invocation per task.
   *
   * Returns `None` (with `withFallbackReason` tagging the reason) when the dispatcher is disabled
   * via [[CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED]], when [[CometBatchKernelCodegen.canHandle]]
   * refuses the expression tree, or when the bound tree cannot be closure-serialized. Callers
   * should treat `None` as a clean Spark-fallback signal; this method never throws.
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

    // `RuntimeReplaceable` expressions (e.g. Spark 4's `StructsToJson`) have a `doGenCode` that
    // always throws "Cannot generate code for expression". Catalyst's `ReplaceExpressions` rule
    // normally rewrites them to their `replacement` form before codegen runs. Comet's serde
    // sometimes works with the pre-rewrite form (via shim reconstruction) for matching purposes,
    // so unwrap to the replacement here before binding so the kernel compiles.
    val target = expr match {
      case rr: RuntimeReplaceable => rr.replacement
      case other => other
    }

    // Bind against only the AttributeReferences the tree actually reads, so ordinals align with
    // the data args we ship.
    val attrs = target.collect { case a: AttributeReference => a }.distinct
    val boundExpr = BindReferences.bindReference(target, AttributeSeq(attrs))

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
    //
    // Guarded because this is the one step in this method that can throw rather than degrade: a
    // tree can hold a reference the closure serializer refuses, such as a `Literal` wrapping a
    // non-serializable evaluator or a UDF closure capturing an open resource. An escape here
    // fails planning, which is a much worse outcome than falling the operator back to Spark.
    // `CometStaticInvoke` / `CometInvoke` route unrecognized nodes here as a catch-all, so the
    // trees reaching this point are arbitrary.
    val bytes =
      try {
        val serializer = SparkEnv.get.closureSerializer.newInstance()
        val buffer = serializer.serialize(boundExpr)
        val serialized = new Array[Byte](buffer.remaining())
        buffer.get(serialized)
        serialized
      } catch {
        // `NonFatal` rather than `NotSerializableException`: Java serialization reports an
        // unserializable object graph as one of several exception types depending on where in
        // the graph it trips, and a custom `writeObject` can throw anything.
        case NonFatal(e) =>
          withFallbackReason(
            expr,
            s"$exprName: codegen dispatch: expression could not be closure-serialized " +
              s"(${e.getClass.getSimpleName}: ${e.getMessage})")
          return None
      }
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
