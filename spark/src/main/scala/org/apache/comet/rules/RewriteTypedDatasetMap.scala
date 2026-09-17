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
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSet, BoundReference, CreateNamedStruct, Expression, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.plans.logical.FunctionUtils
import org.apache.spark.sql.execution.{DeserializeToObjectExec, MapElementsExec, ProjectExec, SerializeFromObjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.ObjectType

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.{CometScalaUDF, QueryPlanSerde}

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
      chain.lastOption
        .map(_.child)
        .collect { case deserialize: DeserializeToObjectExec => deserialize }
        .flatMap(fuse(serialize, chain, _))
        .getOrElse(plan)
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

    // Cheapest precondition first: a config that cannot change mid-plan.
    if (!CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get()) {
      return decline(
        serialize,
        s"${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false, so there is no dispatcher to " +
          "fuse into")
    }

    // Every serializer element is an `Alias` for encoder-generated serializers -- Spark builds them
    // via `ExpressionEncoder.namedExpressions`, which aliases every field of the flattened
    // `CreateNamedStruct`. The outer projection relies on that: it rebuilds each element with
    // `withNewChildren`, which needs exactly one child, and that is what preserves `exprId` /
    // qualifier / metadata across the rewrite.
    val nonAliases = serializer.filterNot(_.isInstanceOf[Alias])
    if (nonAliases.nonEmpty) {
      return decline(
        serialize,
        "serializer contains a non-Alias element: " +
          nonAliases.map(_.getClass.getSimpleName).mkString(", "))
    }

    // The serializer reads the object through `BoundReference(0, objType)` -- Spark asserts as much
    // in `ExpressionEncoder` ("all serializer expressions must use the same BoundReference") and
    // `ScalaReflection.serializerFor` builds it at ordinal 0. Anything else is a shape this rule
    // has not been reasoned about, so leave it alone rather than guess.
    val badRefs = serializer.flatMap(_.collect {
      case b: BoundReference if b.ordinal != 0 || b.dataType != objType => b
    })
    if (badRefs.nonEmpty) {
      return decline(
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

    // Substitute the closure call for the object the serializer reads. The guard above proved every
    // `BoundReference` here is that object, and the pattern restates it so the substitution cannot
    // outlive its precondition. `transform` on an `Alias` preserves `exprId`, qualifier and
    // metadata via `otherCopyArgs`, so the rewritten projection keeps
    // `SerializeFromObjectExec`'s exact output attributes and parents stay valid.
    val fused = serializer.map { ne =>
      ne.transform {
        case b: BoundReference if b.ordinal == 0 && b.dataType == objType => callFunc
      }.asInstanceOf[NamedExpression]
    }

    // A projection may only reference its child's output. Spark asserts the serializer itself has
    // no free references (`ExpressionEncoder`), and the substituted tree bottoms out in the
    // deserializer, which reads `child.output` -- so this should hold; check rather than assume.
    val dangling = AttributeSet(fused) -- child.outputSet
    if (dangling.nonEmpty) {
      return decline(
        serialize,
        s"fused expression references attributes outside the child: ${dangling.mkString(", ")}")
    }

    // Without subexpression elimination the single kernel would evaluate the shared `Invoke` once
    // per struct field, so the closure would run N times per row. Spark runs it once; decline
    // rather than change how many times user code executes. One output column has nothing to
    // deduplicate, so it does not need CSE.
    if (fused.length > 1 && !SQLConf.get.subexpressionEliminationEnabled) {
      return decline(
        serialize,
        s"${fused.length} output columns require subexpression elimination to keep the closure " +
          s"to one call per row, but ${SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key}=false")
    }

    fused match {
      // One output column is already one kernel; the struct wrapper would have nothing to dedupe.
      case Seq(only) =>
        forceDispatch(only.children.head).map(_ => ProjectExec(fused, child))

      case _ =>
        forceDispatch(
          CreateNamedStruct(fused.flatMap(ne => Seq(Literal(ne.name), ne.children.head)))).map {
          structExpr =>
            val structAlias = Alias(structExpr, FUSED_COLUMN)()
            val inner = ProjectExec(Seq(structAlias), child)
            val structAttr = structAlias.toAttribute
            // `CreateNamedStruct` is never null and copies each field's nullability, so
            // `GetStructField(structAttr, i).nullable` equals the original expression's
            // nullability. The rewritten output attributes therefore match
            // `SerializeFromObjectExec.output` exactly.
            val outer = fused.zipWithIndex.map { case (ne, i) =>
              ne.withNewChildren(Seq(GetStructField(structAttr, i, Some(ne.name))))
                .asInstanceOf[NamedExpression]
            }
            ProjectExec(outer, inner)
        }
    }
  }

  /**
   * Tag `expr` to compile into a single kernel, or `None` when the dispatcher would refuse it.
   *
   * The gate matters: if the fused tree cannot reach the dispatcher, the projection this rule
   * builds would fall back to Spark anyway, and we would have traded a whole-stage-fused island
   * for an unfused one. Delegating to [[CometScalaUDF.canDispatch]] rather than re-deriving the
   * binding is what keeps the prediction identical to what the serde will really do.
   */
  private def forceDispatch[T <: Expression](expr: T): Option[T] =
    CometScalaUDF.canDispatch(expr) match {
      case Some(reason) =>
        logDebug(s"RewriteTypedDatasetMap: not rewriting because $reason")
        None
      case None =>
        expr.setTagValue(QueryPlanSerde.FORCE_DISPATCH, ())
        Some(expr)
    }

  /**
   * Leave the sandwich alone and record why on the operator, so `EXPLAIN` shows the reason the
   * typed operation stayed on Spark rather than the bare "not supported" the un-rewritten
   * operators would otherwise produce.
   */
  private def decline(serialize: SerializeFromObjectExec, reason: String): Option[SparkPlan] = {
    withFallbackReason(serialize, s"Cannot fuse typed Dataset map: $reason")
    None
  }
}
