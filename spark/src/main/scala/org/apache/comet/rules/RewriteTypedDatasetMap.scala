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

package org.apache.comet.rules

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, AttributeSeq, AttributeSet, BindReferences, BoundReference, CreateNamedStruct, Expression, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.plans.logical.FunctionUtils
import org.apache.spark.sql.execution.{DeserializeToObjectExec, MapElementsExec, ProjectExec, SerializeFromObjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.ObjectType

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.codegen.CometBatchKernelCodegen
import org.apache.comet.serde.CometScalaUDF

/**
 * Collapses the three-operator island that a typed `Dataset.map` produces
 *
 * {{{
 *   SerializeFromObject [serializer...]
 *   +- MapElements <closure>, obj: T
 *      +- DeserializeToObject <deserializer>, obj: T
 *         +- child
 * }}}
 *
 * into a projection over `child` whose expressions are the fused
 * serializer(closure(deserializer(child))) trees. The projection then converts through the
 * ordinary `CometProjectExec` path, and the fused tree routes through the JVM codegen dispatcher,
 * so the whole thing stays inside the Comet pipeline. No proto or native change is involved.
 *
 * '''Why the operators cannot be handled individually.''' `DeserializeToObjectExec` outputs a
 * single `ObjectType` attribute and `SerializeFromObjectExec` consumes one. `ObjectType` is
 * outside `QueryPlanSerde.supportedDataType` and outside
 * `CometBatchKernelCodegen.isSupportedDataType`, because a JVM object reference cannot live in an
 * Arrow vector. Fusing works because `CometBatchKernelCodegen.canHandle` only type-checks the
 * root and the bound references, so the object may exist strictly ''inside'' the tree. See
 * https://github.com/apache/datafusion-comet/issues/5710.
 *
 * '''Why the expression is safe to rebuild.''' Spark already constructs it. This rule mirrors
 * `MapElementsExec.doConsume` (the `Invoke` on the closure literal),
 * `DeserializeToObjectExec.doConsume` and `SerializeFromObjectExec.doConsume`, which whole-stage
 * codegen chains to produce exactly the same fused tree.
 *
 * '''Shape of the output.''' With one output column the projection is a single fused expression.
 * With N > 1 the rule emits two stacked projections: an inner one producing a single
 * `CreateNamedStruct` column, and an outer one extracting the N fields with `GetStructField`. The
 * struct matters for correctness, not tidiness: N separate projection expressions would each
 * carry their own copy of the closure `Invoke` and each become its own dispatch kernel, calling
 * the user closure N times per row where Spark calls it once. The inner struct is tagged
 * `CometScalaUDF.FORCE_DISPATCH` so it compiles into one kernel, and subexpression elimination
 * inside that kernel collapses the shared `Invoke` back to a single call per row.
 *
 * Only `MapElementsExec` is fusable. `MapPartitionsExec`, `FlatMapGroupsExec` and `CoGroupExec`
 * consume iterators or groups rather than rows, so no per-row expression exists for them. A chain
 * of adjacent `MapElementsExec` nodes is fused as a whole, because `ds.map(f).map(g)` leaves both
 * under a single Serialize/Deserialize pair.
 */
object RewriteTypedDatasetMap extends Logging {

  /** Name of the single intermediate struct column when there is more than one output column. */
  private val FUSED_COLUMN = "comet_fused_object"

  def rewrite(plan: SparkPlan): SparkPlan = plan match {
    case serialize: SerializeFromObjectExec =>
      // `ds.map(f).map(g)` leaves two adjacent `MapElements` under one Serialize/Deserialize pair,
      // so walk the whole chain rather than expecting exactly one.
      val chain = mapElementsChain(serialize.child)
      chain.lastOption.map(_.child) match {
        case Some(deserialize: DeserializeToObjectExec) =>
          fuse(serialize, chain, deserialize).getOrElse(plan)
        case _ => plan
      }
    case _ => plan
  }

  /** Consecutive `MapElementsExec` nodes, outermost first. */
  private def mapElementsChain(plan: SparkPlan): Seq[MapElementsExec] = plan match {
    case m: MapElementsExec => m +: mapElementsChain(m.child)
    case _ => Nil
  }

  private def fuse(
      serialize: SerializeFromObjectExec,
      chain: Seq[MapElementsExec],
      deserialize: DeserializeToObjectExec): Option[SparkPlan] = {
    val child = deserialize.child
    val serializer = serialize.serializer
    val objType = chain.head.outputObjectType

    // Every serializer element is an `Alias` for encoder-generated serializers. The outer
    // projection rebuilds them with `withNewChildren`, which needs exactly one child, and that
    // is also what preserves `exprId` / qualifier / metadata across the rewrite.
    if (!serializer.forall(_.isInstanceOf[Alias])) {
      return declineQuietly(
        serialize,
        "serializer contains a non-Alias element: " +
          serializer
            .filterNot(_.isInstanceOf[Alias])
            .map(_.getClass.getSimpleName)
            .mkString(", "))
    }

    // The serializer reads the object through `BoundReference(0, objType)`. Anything else means a
    // shape this rule has not been reasoned about, so leave it alone rather than guess.
    val badRefs = serializer.flatMap(_.collect {
      case b: BoundReference if b.ordinal != 0 || b.dataType != objType => b
    })
    if (badRefs.nonEmpty) {
      return declineQuietly(
        serialize,
        s"serializer reads unexpected bound references: ${badRefs.mkString(", ")}")
    }

    // Compose the chain innermost-first, so `ds.map(f).map(g)` becomes g(f(deserializer)). Each
    // step mirrors `MapElementsExec.doConsume`, including how it picks the specialized
    // `Function1.apply$mc..$sp` name from the operator's own input and output object types.
    val callFunc = chain.reverse.foldLeft(deserialize.deserializer) { (arg, m) =>
      val (funcClass, funcName) = m.func match {
        case _: MapFunction[_, _] => classOf[MapFunction[_, _]] -> "call"
        case _ =>
          FunctionUtils.getFunctionOneName(m.outputObjectType, m.child.output.head.dataType)
      }
      Invoke(
        Literal.create(m.func, ObjectType(funcClass)),
        funcName,
        m.outputObjectType,
        arg :: Nil,
        propagateNull = false)
    }

    // Substitute the closure call for the object the serializer reads. `transform` on an `Alias`
    // preserves `exprId`, qualifier and metadata via `otherCopyArgs`, so the rewritten projection
    // keeps `SerializeFromObjectExec`'s exact output attributes and parents stay valid.
    val fused = serializer.map { ne =>
      ne.transform { case _: BoundReference => callFunc }.asInstanceOf[NamedExpression]
    }

    // A projection may only reference its child's output. The deserializer reads `child.output`
    // and the object reference is gone, so this should hold; check rather than assume.
    val childOutput = AttributeSet(child.output)
    val dangling = fused.flatMap(f => (f.references -- childOutput).toSeq)
    if (dangling.nonEmpty) {
      return declineQuietly(
        serialize,
        s"fused expression references attributes outside the child: ${dangling.mkString(", ")}")
    }

    if (fused.length == 1) {
      // Single output column: the fused expression is already one dispatch kernel, so there is
      // nothing for the struct wrapper to deduplicate.
      val single = fused.head
      val value = single.children.head
      dispatchable(value).map { _ =>
        value.setTagValue(CometScalaUDF.FORCE_DISPATCH, ())
        ProjectExec(fused, child)
      }
    } else {
      // Without subexpression elimination the single kernel would still evaluate the shared
      // `Invoke` once per struct field, so the closure would run N times per row. Spark runs it
      // once; decline rather than change how many times user code executes.
      if (!SQLConf.get.subexpressionEliminationEnabled) {
        return declineQuietly(
          serialize,
          s"${serializer.length} output columns require subexpression elimination to keep the " +
            "closure to one call per row, but " +
            s"${SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key}=false")
      }

      val structExpr =
        CreateNamedStruct(fused.flatMap(ne => Seq(Literal(ne.name), ne.children.head)))
      dispatchable(structExpr).map { _ =>
        structExpr.setTagValue(CometScalaUDF.FORCE_DISPATCH, ())
        val structAlias = Alias(structExpr, FUSED_COLUMN)()
        val inner = ProjectExec(Seq(structAlias), child)
        val structAttr = structAlias.toAttribute
        // `CreateNamedStruct` is never null and copies each field's nullability, so
        // `GetStructField(structAttr, i).nullable` equals the original expression's nullability.
        // The rewritten output attributes therefore match `SerializeFromObjectExec.output` exactly.
        val outer = fused.zipWithIndex.map { case (ne, i) =>
          ne.withNewChildren(Seq(GetStructField(structAttr, i, Some(ne.name))))
            .asInstanceOf[NamedExpression]
        }
        ProjectExec(outer, inner)
      }
    }
  }

  /**
   * Plan-time gate. The rewrite is only worth making if the fused tree can actually reach the
   * dispatcher; otherwise the resulting projection would fall back to Spark anyway, and we would
   * have replaced a whole-stage-fused island with an unfused one. Binds the same way
   * `CometScalaUDF.emitJvmCodegenDispatch` does so `canHandle` sees the tree it will really get.
   */
  private def dispatchable(fusedValue: Expression): Option[Unit] = {
    if (!CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get()) {
      logDebug(
        "RewriteTypedDatasetMap: not rewriting because " +
          s"${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false")
      return None
    }
    // Same binding as `emitJvmCodegenDispatch`, so `canHandle` sees the tree it will really get.
    val attrs = fusedValue.collect { case a: AttributeReference => a }.distinct
    val bound = BindReferences.bindReference(fusedValue, AttributeSeq(attrs))
    CometBatchKernelCodegen.canHandle(bound) match {
      case Some(reason) =>
        logDebug(s"RewriteTypedDatasetMap: not rewriting because $reason")
        None
      case None => Some(())
    }
  }

  /**
   * Leave the sandwich alone and record why on the operator, so `EXPLAIN` shows the reason the
   * typed operation stayed on Spark rather than the bare "not supported" the un-rewritten
   * operators would otherwise produce.
   */
  private def declineQuietly(
      serialize: SerializeFromObjectExec,
      reason: String): Option[SparkPlan] = {
    withFallbackReason(serialize, s"Cannot fuse typed Dataset map: $reason")
    None
  }
}
