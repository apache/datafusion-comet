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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeSet, BinaryComparison, Cast, ConditionalExpression, CreateArray, CreateNamedStruct, DateAdd, DateDiff, DateFormatClass, DateSub, EqualNullSafe, Expression, IsNotNull, IsNull, MakeDate, Not, Or, SortOrder}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BaseJoinExec

import org.apache.comet.CometConf
import org.apache.comet.shims.ShimCometWindowGroupLimit

/** Keeps make_date on Spark's row path where native batching can change whether it throws. */
private[rules] object MakeDateEvaluation {
  private val sparkEvaluation = TreeNodeTag[String]("comet.makeDateSparkEvaluation")

  private def enabled(plan: SparkPlan): Boolean =
    CometConf.COMET_EXEC_MAKE_DATE_OPERATOR_FALLBACK_ENABLED.get(plan.conf)

  def reason(plan: SparkPlan): Option[String] =
    if (enabled(plan)) plan.getTagValue(sparkEvaluation) else None

  private def containsMakeDate(expressions: Seq[Expression]): Boolean =
    expressions.exists(_.exists(_.isInstanceOf[MakeDate]))

  private case class ProjectionDemand(boundary: String, deferred: AttributeSet)

  /** References that can be evaluated before the expression without changing skipped inputs. */
  private def eagerReferences(expression: Expression): AttributeSet = expression match {
    case attribute: Attribute => AttributeSet(Seq(attribute))
    case _ =>
      val children = expression match {
        case conditional: ConditionalExpression => conditional.alwaysEvaluatedInputs
        case _: Alias | _: Cast | _: IsNull | _: IsNotNull | _: Not => expression.children
        case _: EqualNullSafe | _: CreateArray | _: CreateNamedStruct => expression.children
        case _: And | _: Or => expression.children.take(1)
        case _: BinaryComparison | _: DateAdd | _: DateSub | _: DateDiff | _: DateFormatClass =>
          expression.children.take(if (expression.children.head.nullable) 1 else 2)
        // Unknown expressions may skip inputs or catch errors raised by their children.
        case _ => Nil
      }
      AttributeSet(children.flatMap(eagerReferences))
  }

  private def copyLogicalLinks(from: SparkPlan, to: SparkPlan): Unit = {
    if (from ne to) {
      // setLogicalLink promotes inherited links to direct links and rewrites descendants.
      // Keep each tag's role and retain the fallback plan's links when the source has none.
      Seq(SparkPlan.LOGICAL_PLAN_TAG, SparkPlan.LOGICAL_PLAN_INHERITED_TAG).foreach { tag =>
        from.getTagValue(tag).foreach(to.setTagValue(tag, _))
      }
    }
  }

  private def nativeBroadcastInput(plan: SparkPlan): Option[SparkPlan] = plan match {
    case _: CometBroadcastExchangeExec => Some(plan)
    case stage: BroadcastQueryStageExec => nativeBroadcastInput(stage.plan).map(_ => stage)
    case reuse: ReusedExchangeExec => nativeBroadcastInput(reuse.child).map(_ => reuse)
    case sink: CometSinkPlaceHolder => nativeBroadcastInput(sink.child)
    case wrapper: CometScanWrapper => nativeBroadcastInput(wrapper.originalPlan)
    case _ => None
  }

  private def restoreBroadcast(plan: SparkPlan, reason: String): SparkPlan =
    nativeBroadcastInput(plan)
      .map { input =>
        // Spark joins need a HashedRelation (or rows for a nested-loop join), not Arrow buffers.
        // The existing transition decodes the cached broadcast and applies its Spark broadcast
        // mode, including through AQE/reuse wrappers, without re-executing the build input.
        val restored = CometColumnarToRowExec(input)
        restored.setTagValue(sparkEvaluation, reason)
        copyLogicalLinks(plan, restored)
        restored
      }
      .getOrElse(plan)

  /**
   * Run before bottom-up conversion and join rewriting. Reasons propagate back to the consuming
   * operator, keeping the row pipeline together instead of inserting an eager Arrow batch between
   * a throwing projection and its lazy consumer. Exchanges materialize their input independently;
   * local sorts and aggregates do so only after their consumer requests input.
   */
  def preserve(plan: SparkPlan): SparkPlan = {
    if (!enabled(plan)) return plan

    def visit(
        node: SparkPlan,
        rowDemand: Option[String],
        projectionDemand: Option[ProjectionDemand]): (SparkPlan, Option[String]) = {
      node match {
        // Materialized stages are independent producers. Their plans have already been guarded.
        case _: QueryStageExec | _: ReusedExchangeExec => return (node, None)
        // These wrappers do not have the child shape of their original Spark operator.
        case wrapper: CometScanWrapper =>
          val (child, childReason) = visit(wrapper.originalPlan, rowDemand, projectionDemand)
          return if (childReason.isDefined) (child, childReason)
          else (wrapper.copy(originalPlan = child), None)
        case sink: CometSinkPlaceHolder =>
          val (child, childReason) = visit(sink.child, rowDemand, projectionDemand)
          return if (childReason.isDefined) (child, childReason)
          else (sink.withNewChildren(Seq(child)), None)
        case _ =>
      }

      val original = node match {
        case scan: CometScanExec =>
          scan.wrapped.copy(
            partitionFilters = scan.partitionFilters,
            dataFilters = scan.dataFilters)
        case scan: CometNativeScanExec => scan.sparkFallback(Nil)
        case native: CometExec => native.originalPlan
        case shuffle: CometShuffleExchangeExec => shuffle.originalPlan
        case broadcast: CometBroadcastExchangeExec => broadcast.originalPlan
        case _ => node
      }
      val exchange = original.isInstanceOf[Exchange]
      val join = original.isInstanceOf[BaseJoinExec]
      val localMaterialization = original match {
        case _: SortExec | _: HashAggregateExec | _: ObjectHashAggregateExec => true
        case _: TakeOrderedAndProjectExec => true
        case _ => false
      }
      val startsLimit = original match {
        case _: CollectLimitExec | _: LocalLimitExec | _: GlobalLimitExec => true
        case topK: TakeOrderedAndProjectExec =>
          SortOrder.orderingSatisfies(topK.child.outputOrdering, topK.sortOrder)
        case _
            if ShimCometWindowGroupLimit.windowGroupLimitClass.exists(_.isInstance(original)) =>
          true
        case _ => false
      }
      val childDemand = original match {
        case _ if exchange => None
        // A join can leave a local sort or aggregate completely unpolled.
        case _ if join || rowDemand.contains("JOIN") => Some("JOIN")
        case _ if startsLimit => Some("LIMIT")
        case _: SampleExec => Some("SAMPLE")
        case _: MapPartitionsExec => Some("a mapPartitions function")
        case _ if localMaterialization => None
        case _ => rowDemand
      }
      val childProjectionDemand = original match {
        // Generated filters can defer deterministic projection outputs until a row is accepted.
        case filter: FilterExec => Some(ProjectionDemand("FILTER", filter.child.outputSet))
        case project: ProjectExec =>
          val deferred = projectionDemand.map(_.deferred).getOrElse(AttributeSet.empty)
          val eager = project.projectList
            .filter(output => !output.deterministic || !deferred.contains(output.toAttribute))
            .flatMap(eagerReferences)
          // ProjectExec forces repeated inputs and nondeterministic outputs. Other inputs can
          // remain inlined in a consumer's conditional, even across intervening aliases.
          val skipped = project.child.outputSet -- project.usedInputs -- AttributeSet(eager)
          if (skipped.isEmpty) {
            None
          } else {
            Some(ProjectionDemand(projectionDemand.map(_.boundary).getOrElse("PROJECT"), skipped))
          }
        case _ if exchange || localMaterialization || join => None
        case _ => projectionDemand
      }
      val visited = node.children.map(visit(_, childDemand, childProjectionDemand))
      val childReason = visited.flatMap(_._2).headOption
      val ownReason = reason(node).orElse {
        if (containsMakeDate(original.expressions)) {
          rowDemand
            .map(boundary => s"make_date requires Spark evaluation below $boundary")
            .orElse(original match {
              case project: ProjectExec =>
                projectionDemand
                  .filter(demand =>
                    project.projectList.exists(output =>
                      output.deterministic && demand.deferred.contains(output.toAttribute) &&
                        containsMakeDate(Seq(output))))
                  .map(demand => s"make_date requires Spark evaluation below ${demand.boundary}")
              // Join keys/conditions and aggregate updates can be skipped inside the operator.
              case _: BaseJoinExec | _: BaseAggregateExec | _: TakeOrderedAndProjectExec =>
                Some(s"make_date requires Spark evaluation in ${original.nodeName}")
              case _ => None
            })
        } else None
      }
      val fallbackReason = ownReason.orElse(if (exchange) None else childReason)
      val children = visited.map { case (child, _) =>
        if (join && fallbackReason.isDefined) restoreBroadcast(child, fallbackReason.get)
        else child
      }
      val rebuilt = node match {
        case _: RowToColumnarExec | _: CometSparkToColumnarExec if childReason.isDefined =>
          children.head
        case _: ColumnarToRowExec | _: CometColumnarToRowExec | _: CometNativeColumnarToRowExec
            if childReason.isDefined && !children.head.supportsColumnar =>
          children.head
        case native: CometExec if fallbackReason.isDefined || children != node.children =>
          native.sparkFallback(children)
        case _ if (original ne node) && (fallbackReason.isDefined || children != node.children) =>
          original.withNewChildren(children)
        case _ => node.withNewChildren(children)
      }
      copyLogicalLinks(node, rebuilt)
      fallbackReason.foreach(rebuilt.setTagValue(sparkEvaluation, _))
      (rebuilt, fallbackReason)
    }

    // Dataset.rdd adds an object-producing root, and its caller may read only a few objects.
    // EliminateSerialization can leave a typed operator, filter, or project at that root.
    def producesObjects(op: SparkPlan): Boolean = op match {
      case _: ObjectProducerExec => true
      case _: FilterExec | _: ProjectExec => producesObjects(op.children.head)
      case _ => false
    }
    val rootDemand = if (producesObjects(plan)) Some("code reading Dataset.rdd") else None
    visit(plan, rootDemand, None)._1
  }
}
