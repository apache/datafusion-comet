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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BindReferences, Expression, ExpressionSet, If, IsNull, KnownNotNull, Literal, Or, RuntimeReplaceable, ScalaUDF}
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

  override def convert(expr: ScalaUDF, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    emitJvmCodegenDispatch(expr, inputs, binding)

  /**
   * Emits Spark's null guard around a Scala UDF, together with the UDF, as one dispatcher call.
   * Returns `None` when `expr` is not that guard, when an expression in it is disabled, or when
   * the dispatcher cannot take it, and records nothing on `expr` in that case: the caller
   * converts `expr` as usual, which sends the UDF to the dispatcher on its own, and the UDF
   * records its own fallback reason if it cannot go.
   *
   * Spark's `HandleNullInputsForUDF` rule wraps a UDF with a primitive parameter over a nullable
   * input as `if (isnull(a) or isnull(b)) null else f(knownnotnull(a), knownnotnull(b))`, one
   * `IsNull` per such argument. Converted natively, the `If` is a `CASE` that splits the batch by
   * the predicate and merges the two branches back together, which costs more than the call it
   * protects. In the kernel it is a branch per row.
   *
   * Only that shape matches: the predicate must be the one the rule builds from the UDF's guarded
   * arguments, each checked once. The kernel then runs no expression it would not run for the UDF
   * alone, apart from the guard's own `if`, `or` and `isnull`, and it runs Spark's code for all
   * of them. In a filter or join condition, and in the predicate of a conditional,
   * `ReplaceNullWithFalseInPredicate` replaces the null with `false`, so that form matches too.
   */
  def emitNullGuardDispatch(expr: If, inputs: Seq[Attribute], binding: Boolean): Option[Expr] =
    if (isNullGuard(expr) && isEnabled(expr)) {
      tryJvmCodegenDispatch(expr, inputs, binding).toOption
    } else {
      None
    }

  /**
   * Whether converting `guard` natively would pass every `spark.comet.expression.<name>.enabled`
   * check it makes: on the UDF, and on each node of the predicate and the true branch, the
   * guarded arguments included. The dispatcher makes none of these checks, so a guard with a
   * disabled expression stays native, where the conversion records the fallback reason. The UDF's
   * arguments are not checked, because the dispatcher takes them unchecked with or without the
   * guard.
   */
  private def isEnabled(guard: If): Boolean = {
    def disabled(e: Expression): Boolean =
      QueryPlanSerde.exprSerdeMap.get(e.getClass).exists { serde =>
        !CometConf.isExprEnabled(
          serde.asInstanceOf[CometExpressionSerde[Expression]].getExprConfigName(e))
      }
    !disabled(guard.falseValue) && !guard.predicate.exists(disabled) &&
    !guard.trueValue.exists(disabled)
  }

  private def isNullGuard(expr: If): Boolean = expr match {
    case If(predicate, Literal(null, _) | Literal.FalseLiteral, udf: ScalaUDF) =>
      val guarded = udf.inputPrimitives.zip(udf.children).collect {
        case (true, KnownNotNull(arg)) => arg
      }
      // The optimizer drops a repeated `isnull`, as in `f(a, a)`, so check each argument once.
      // `semanticEquals` is false for a nondeterministic argument, so that guard stays native.
      ExpressionSet(guarded).toSeq
        .map(IsNull(_))
        .reduceLeftOption[Expression](Or)
        .exists(_.semanticEquals(predicate))
    case _ => false
  }

  /**
   * Bind `expr`, closure-serialize it, and emit a `JvmScalarUdf` proto routed through
   * [[CometScalaUDFCodegen]] so that native execution evaluates the expression inside the
   * Arrow-direct codegen dispatcher. The dispatcher will Janino-compile `expr.doGenCode` into a
   * batch kernel on first invocation per task.
   *
   * Returns `None` (with `withFallbackReason` tagging the reason) when the dispatcher is disabled
   * via [[CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED]], when the tree calls into code other than
   * Spark's own (see [[CometInvokeTargets]]), when [[CometBatchKernelCodegen.canHandle]] refuses
   * the expression tree, or when the bound tree cannot be closure-serialized. Callers should
   * treat `None` as a clean Spark-fallback signal; this method never throws.
   */
  def emitJvmCodegenDispatch(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] =
    tryJvmCodegenDispatch(expr, inputs, binding) match {
      case Right(proto) => Some(proto)
      case Left(reason) =>
        withFallbackReason(expr, reason)
        None
    }

  /**
   * [[emitJvmCodegenDispatch]] without the fallback tagging: returns the reason the dispatcher
   * cannot take `expr` rather than recording it on `expr`, for a caller that can still convert
   * `expr` another way.
   */
  private def tryJvmCodegenDispatch(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Either[String, Expr] = {
    val exprName = CometExplainInfo.exprDisplayName(expr)
    if (!CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get()) {
      return Left(
        s"$exprName: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false; expression has " +
          "no native path so the plan falls back to Spark")
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

    // The kernel runs every call in the tree, not only the root, so a DataSource V2 function
    // under a dispatched `map(...)` would run in it too. Check the whole tree.
    CometInvokeTargets.declineReason(target) match {
      case Some(reason) =>
        return Left(s"$exprName: codegen dispatch: $reason")
      case None =>
    }

    // Bind against only the AttributeReferences the tree actually reads, so ordinals align with
    // the data args we ship.
    val attrs = target.collect { case a: AttributeReference => a }.distinct
    val boundExpr = BindReferences.bindReference(target, AttributeSeq(attrs))

    // Gate at plan time. Surface the reason as a fallback rather than crashing Janino at execute.
    CometBatchKernelCodegen.canHandle(boundExpr) match {
      case Some(reason) =>
        return Left(s"$exprName: $reason")
      case None =>
    }

    // Serialize via Spark's closure serializer: respects the task context classloader (so user
    // UDF jars are visible) and matches Spark's wire format. The bytes become arg 1 of the
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
          return Left(
            s"$exprName: codegen dispatch: expression could not be closure-serialized " +
              s"(${e.getClass.getSimpleName}: ${e.getMessage})")
      }
    // Arg 0 is a digest of the bytes. The dispatcher finds each batch's kernel by it, so it reads
    // the bytes only to compile on a cache miss instead of copying and hashing them for every
    // batch.
    val exprArgs = Seq(CometScalaUDFCodegen.digest(bytes), bytes).map { payload =>
      exprToProtoInternal(Literal(payload, BinaryType), inputs, binding).getOrElse {
        return Left(
          s"$exprName: codegen dispatch: could not serialize closure-serialized bound " +
            "expression payload")
      }
    }

    val dataArgs = attrs.map { a =>
      exprToProtoInternal(a, inputs, binding).getOrElse {
        return Left(s"$exprName: codegen dispatch: could not serialize data arg $a")
      }
    }
    val returnTypeProto = serializeDataType(expr.dataType).getOrElse {
      return Left(s"$exprName: codegen dispatch: unsupported return type ${expr.dataType}")
    }

    val udfBuilder = ExprOuterClass.JvmScalarUdf
      .newBuilder()
      .setClassName(classOf[CometScalaUDFCodegen].getName)
    exprArgs.foreach(udfBuilder.addArgs)
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
    // The whole subtree under `expr` was bound and closure-serialized into this one kernel, so
    // every expression in it ran in the JVM, not just the root. Naming only the root understates
    // that: for `hypot(abs(b), c)` the dispatched set would hold `hypot` alone, and a test could
    // assert `abs` was native while an `abs` was in fact running inside the kernel. Attribute
    // references and literals are the kernel's inputs rather than work it performed, so they are
    // left out - which also keeps them from appearing in the coverage stats as "expressions".
    target.foreach {
      case _: AttributeReference | _: Literal =>
      case node if !(node eq target) =>
        val _ = withCodegenDispatchExpr(expr, CometExplainInfo.exprDisplayName(node))
      case _ =>
    }
    Right(
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
