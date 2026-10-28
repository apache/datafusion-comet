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

import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, Attribute, AttributeSet, BinaryComparison, Cast, ConditionalExpression, CreateArray, CreateNamedStruct, DateAdd, DateDiff, DateFormatClass, DateSub, EqualNullSafe, Expression, Greatest, HashExpression, IsNotNull, IsNull, Least, MakeDate, Multiply, Not, Or, SortOrder, Subtract, UnixDate}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}

import org.apache.comet.CometConf
import org.apache.comet.serde.CometMakeDate
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

  private def containsDeferredAggregateDate(expression: Expression): Boolean = expression match {
    // COUNT checks its sole nullable argument on every input row. Filters, other nullable
    // arguments, and a non-nullable argument can let Spark skip evaluating the date.
    case AggregateExpression(Count(Seq(child)), _, _, None, _) if child.nullable => false
    case _: MakeDate => true
    case _ => expression.children.exists(containsDeferredAggregateDate)
  }

  private case class ProjectionDemand(
      boundary: String,
      deferred: AttributeSet,
      // Deferred inputs whose producers need guarding even when they contain no make_date.
      guardedInputs: AttributeSet,
      // Date argument lineage retained until a streamed join can skip its input rows.
      dateInputs: AttributeSet)

  // Attributes may stand for deferred expressions in a lower projection, not materialized values.
  private def harmlessValue(expression: Expression): Boolean =
    expression.references.isEmpty && CometMakeDate.canEvaluateEagerly(expression)

  /** References that a consumer requires without exposing a potentially skipped sibling. */
  private def eagerReferences(expression: Expression): AttributeSet = expression match {
    case attribute: Attribute => AttributeSet(Seq(attribute))
    case _ =>
      val children = expression match {
        case date: MakeDate =>
          val firstNullable = date.children.indexWhere(_.nullable)
          date.children.take(if (firstNullable < 0) date.children.size else firstNullable + 1)
        case conditional: ConditionalExpression =>
          val required = conditional.alwaysEvaluatedInputs
          if (conditional.children.filterNot(required.contains).forall(harmlessValue)) required
          else Nil
        case _: Alias | _: Cast | _: IsNull | _: IsNotNull | _: Not | _: UnixDate =>
          expression.children
        case _: EqualNullSafe | _: CreateArray | _: CreateNamedStruct | _: Greatest | _: Least |
            _: HashExpression[_] =>
          expression.children
        case _: And | _: Or =>
          if (harmlessValue(expression.children(1))) expression.children.take(1) else Nil
        case _: Add | _: Subtract | _: Multiply | _: BinaryComparison | _: DateAdd | _: DateSub |
            _: DateDiff | _: DateFormatClass =>
          val left = expression.children.head
          val right = expression.children(1)
          if (!left.nullable) expression.children
          // A right-hand attribute can inline a throwing expression from an earlier projection.
          // Only self-contained harmless values are safe without tracing that attribute's source.
          else if (harmlessValue(right)) Seq(left)
          else Nil
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
        projectionDemand: Option[ProjectionDemand]): (SparkPlan, Option[String], AttributeSet) = {
      node match {
        // Materialized stages are independent producers. Their plans have already been guarded.
        case _: QueryStageExec | _: ReusedExchangeExec => return (node, None, AttributeSet.empty)
        // These wrappers do not have the child shape of their original Spark operator.
        case wrapper: CometScanWrapper =>
          val (child, childReason, dateOutputs) =
            visit(wrapper.originalPlan, rowDemand, projectionDemand)
          return if (childReason.isDefined) (child, childReason, dateOutputs)
          else (wrapper.copy(originalPlan = child), None, dateOutputs)
        case sink: CometSinkPlaceHolder =>
          val (child, childReason, dateOutputs) = visit(sink.child, rowDemand, projectionDemand)
          return if (childReason.isDefined) (child, childReason, dateOutputs)
          else (sink.withNewChildren(Seq(child)), None, dateOutputs)
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
      val evaluatesDate = containsMakeDate(original.expressions)
      val exchange = original.isInstanceOf[Exchange]
      val join = original.isInstanceOf[BaseJoinExec]
      // Broadcast joins can inline streamed expressions into their consumer. Their build side
      // materializes rows; shuffled hash and sort-merge joins also isolate the streamed input.
      val streamedChildIndex = original match {
        case hash: BroadcastHashJoinExec if hash.supportCodegen =>
          Some(if (hash.buildSide == BuildLeft) 1 else 0)
        case nestedLoop: BroadcastNestedLoopJoinExec if nestedLoop.supportCodegen =>
          Some(if (nestedLoop.buildSide == BuildLeft) 1 else 0)
        case _ => None
      }
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
        case filter: FilterExec =>
          Some(
            ProjectionDemand(
              "FILTER",
              filter.child.outputSet,
              projectionDemand.map(_.guardedInputs).getOrElse(AttributeSet.empty),
              projectionDemand.map(_.dateInputs).getOrElse(AttributeSet.empty)))
        case project: ProjectExec =>
          val deferred = projectionDemand.map(_.deferred).getOrElse(AttributeSet.empty)
          val eager = project.projectList
            .filter(output => !output.deterministic || !deferred.contains(output.toAttribute))
            .flatMap(eagerReferences)
          // ProjectExec forces repeated inputs and nondeterministic outputs. Other inputs can
          // remain inlined in a consumer's conditional, even across intervening aliases.
          val skipped = project.child.outputSet -- project.usedInputs -- AttributeSet(eager)
          val inheritedInputs =
            projectionDemand.map(_.guardedInputs).getOrElse(AttributeSet.empty)
          // A consumer can require the date but still skip another output of this projection.
          val guardedOutputs = inheritedInputs ++
            (if (evaluatesDate) deferred else AttributeSet.empty)
          val directDateInputs = AttributeSet(project.projectList.flatMap { output =>
            output.collect { case date: MakeDate => date }.flatMap(_.references)
          })
          val inputsToGuard =
            directDateInputs ++ AttributeSet(project.projectList.flatMap { output =>
              if ((output.deterministic || rowDemand.isDefined) &&
                guardedOutputs.contains(output.toAttribute)) {
                output.references.toSeq
              } else {
                Nil
              }
            })
          // Keep argument lineage through eager aliases too. A join below this projection can
          // discard an input row before this projection ever requires the argument's value.
          val dateInputs =
            directDateInputs ++ AttributeSet(project.projectList.flatMap { output =>
              if (projectionDemand.exists(_.dateInputs.contains(output.toAttribute))) {
                output.references.toSeq
              } else {
                Nil
              }
            })
          // A lazy row consumer can skip even the date's required arguments on unread rows.
          val guardedChildInputs =
            inputsToGuard.intersect(if (rowDemand.isDefined) project.child.outputSet else skipped)
          if (skipped.isEmpty && guardedChildInputs.isEmpty && dateInputs.isEmpty) {
            None
          } else {
            Some(
              ProjectionDemand(
                projectionDemand.map(_.boundary).getOrElse("PROJECT"),
                skipped,
                guardedChildInputs,
                dateInputs))
          }
        case _ if exchange || localMaterialization => None
        case _ => projectionDemand
      }
      val visited = node.children.zipWithIndex.map { case (child, index) =>
        val demand = if (!join) {
          childProjectionDemand
        } else if (streamedChildIndex.contains(index)) {
          childProjectionDemand.map { demand =>
            // Keys and conditions force their inputs on each visited row. An outer row consumer
            // can still skip that row altogether, so retain its guarded inputs in that case.
            val required = if (rowDemand.isDefined) AttributeSet.empty else original.references
            val streamedOutputs = original.children(index).outputSet
            val forwarded = streamedOutputs -- required
            demand.copy(
              deferred = demand.deferred.intersect(forwarded),
              guardedInputs = (demand.guardedInputs ++ demand.dateInputs).intersect(forwarded),
              // A key is required only on this join's input rows. An earlier join can still
              // discard rows before producing that key, so retain its argument lineage.
              dateInputs = demand.dateInputs.intersect(streamedOutputs))
          }
        } else {
          None
        }
        visit(child, childDemand, demand)
      }
      val childReason = visited.flatMap(_._2).headOption
      val childDateOutputs = AttributeSet(visited.zipWithIndex.flatMap {
        case ((_, _, outputs), index) if !join || streamedChildIndex.contains(index) => outputs
        case _ => AttributeSet.empty
      })
      // Date provenance flows up through aliases while deferred evaluation flows down. This
      // also finds a skipped sibling computed in a projection above the date's producer.
      val dateDerivedOutputs = if (exchange || localMaterialization) {
        AttributeSet.empty
      } else {
        original match {
          case project: ProjectExec =>
            AttributeSet(
              project.projectList
                .filter { output =>
                  containsMakeDate(Seq(output)) || output.references
                    .intersect(childDateOutputs)
                    .nonEmpty
                }
                .map(_.toAttribute))
          case _ => childDateOutputs.intersect(original.outputSet)
        }
      }
      val ownReason = reason(node).orElse {
        // An exchange evaluates its partition keys for every input before a consumer reads rows.
        val rowReason = if (evaluatesDate && !exchange) {
          rowDemand.map(boundary => s"make_date requires Spark evaluation below $boundary")
        } else None
        rowReason.orElse(original match {
          case project: ProjectExec =>
            projectionDemand
              .filter(demand =>
                project.projectList.exists { output =>
                  val computation = output match {
                    case alias: Alias => alias.child
                    case other => other
                  }
                  val deferredDate = output.deterministic &&
                    demand.deferred.contains(output.toAttribute) && containsMakeDate(Seq(output))
                  val deferredComputation = (output.deterministic || rowDemand.isDefined) &&
                    (demand.guardedInputs.contains(output.toAttribute) ||
                      (dateDerivedOutputs.nonEmpty && demand.deferred.contains(
                        output.toAttribute))) &&
                    !CometMakeDate.canEvaluateEagerly(computation)
                  deferredDate || deferredComputation
                })
              .map(demand => s"make_date requires Spark evaluation below ${demand.boundary}")
          case aggregate: BaseAggregateExec
              if aggregate.expressions.exists(containsDeferredAggregateDate) =>
            Some(s"make_date requires Spark evaluation in ${original.nodeName}")
          // Join keys/conditions and top-k outputs can be skipped inside the operator.
          case _: BaseJoinExec | _: TakeOrderedAndProjectExec if evaluatesDate =>
            Some(s"make_date requires Spark evaluation in ${original.nodeName}")
          case _ => None
        })
      }
      val fallbackReason = ownReason.orElse(if (exchange) None else childReason)
      val children = visited.map { case (child, _, _) =>
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
      (rebuilt, fallbackReason, dateDerivedOutputs)
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
