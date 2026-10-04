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

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometExec, CometHashAggregateExec, CometLocalTopKExec, CometNativeColumnarToRowExec, CometPlan, CometSparkToColumnarExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, ColumnarToRowTransition, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.cost.CometCostModel
import org.apache.comet.serde.QueryPlanSerde

/**
 * Reverts a query stage to Spark row-based execution when it has too many columnar-to-row (C2R)
 * transitions. Each C2R indicates Comet could not keep execution columnar and had to fall back.
 * With columnar shuffle enabled, each C2R implies a corresponding R2C round-trip.
 *
 * With `spark.comet.exec.costModel.enabled`, a stage is also reverted when a [[CometCostModel]]
 * estimates that Comet speeds it up by less than `spark.comet.exec.costModel.minSpeedup`.
 *
 * @param wholePlan
 *   visit every stage even under AQE, where Spark normally hands this rule one stage at a time.
 *   Set by the plan-only preview, which holds the whole plan.
 */
case class RevertNativeForTransitionHeavyStages(session: SparkSession, wholePlan: Boolean = false)
    extends Rule[SparkPlan]
    with Logging {

  private def transitionRevertEnabled = CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.get()
  private def maxTransitions = CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.get()
  private def costModelEnabled = CometConf.COMET_EXEC_COST_MODEL_ENABLED.get()

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!transitionRevertEnabled && !costModelEnabled) return plan

    if (session.sessionState.conf.adaptiveExecutionEnabled && !wholePlan) {
      applyForAQE(plan)
    } else {
      applyForNonAQE(plan)
    }
  }

  private def applyForAQE(plan: SparkPlan): SparkPlan = {
    plan match {
      case _: BroadcastExchangeLike => plan
      case exchange: ShuffleExchangeLike =>
        revertStageIfNeeded(exchange.child, Some(exchange))
          .map(reverted => exchange.withNewChildren(Seq(reverted)))
          .getOrElse(plan)
      case _ =>
        // Result stage: its output is collected as rows, so no consumer requires columnar input
        // and the reverted stage needs no trailing R2C.
        revertStageIfNeeded(plan, consumer = None).getOrElse(plan)
    }
  }

  private def applyForNonAQE(plan: SparkPlan): SparkPlan = {
    val withRevertedStages = plan.transformUp { case exchange: ShuffleExchangeLike =>
      revertStageIfNeeded(exchange.child, Some(exchange))
        .map(reverted => exchange.withNewChildren(Seq(reverted)))
        .getOrElse(exchange)
    }
    revertStageIfNeeded(withRevertedStages, consumer = None)
      .getOrElse(withRevertedStages)
  }

  /**
   * Reverts the stage if its C2R count exceeds the threshold or the cost model estimates too
   * small a speedup.
   *
   * @param consumer
   *   the exchange that reads the stage, or None for a result stage
   */
  private def revertStageIfNeeded(
      stagePlan: SparkPlan,
      consumer: Option[ShuffleExchangeLike]): Option[SparkPlan] = {
    val transitionCount = if (transitionRevertEnabled) countTransitions(stagePlan) else 0
    val tooManyTransitions = transitionRevertEnabled && transitionCount > maxTransitions
    if (!tooManyTransitions && !(costModelEnabled && hasCometOperator(stagePlan))) return None

    // Reverting either side of a native aggregate boundary can make one engine consume the
    // other's intermediate state. Typed imperative aggregates such as percentile expose a native
    // array where Spark expects serialized binary; others, including COUNT, must remain in one
    // engine for planner semantics even though their physical buffer types match. Keep both
    // producer and consumer stages native when mixed execution is unsafe across a stage boundary.
    if (hasUnsafeMixedAggregateAtStageBoundary(stagePlan)) return None

    val reverted = revertToSpark(stagePlan)
    bridgeToConsumer(reverted, consumer).flatMap { result =>
      val reason = if (tooManyTransitions) {
        Some(s"Stage reverted: $transitionCount C2R transitions exceed threshold $maxTransitions")
      } else {
        costModelRevertReason(stagePlan, result)
      }
      reason.map { r =>
        withFallbackReason(reverted, r)
        result
      }
    }
  }

  /**
   * Gives a reverted stage the output format its consumer reads, or None if the stage cannot be
   * bridged to it.
   */
  private def bridgeToConsumer(
      reverted: SparkPlan,
      consumer: Option[ShuffleExchangeLike]): Option[SparkPlan] = {
    // A stage that reverts to a bare vectorized scan is columnar, and only yields rows through a
    // transition.
    def rows = if (reverted.supportsColumnar) ColumnarToRowExec(reverted) else reverted
    consumer match {
      case Some(comet: CometShuffleExchangeExec) if comet.shuffleType == CometNativeShuffle =>
        // The native shuffle writer reads Arrow batches, which neither Spark's vectorized scans
        // nor RowToColumnarExec produce.
        if (CometSparkToColumnarExec.isSchemaSupported(reverted.schema, ListBuffer.empty)) {
          Some(CometSparkToColumnarExec(reverted))
        } else {
          None
        }
      case Some(exchange) if exchange.supportsColumnar => Some(RowToColumnarExec(rows))
      case _ => Some(rows)
    }
  }

  /**
   * The reason to revert the stage if the cost model estimates too small a speedup from Comet.
   * Never fails the query: a cost model that throws leaves the stage with Comet.
   */
  private def costModelRevertReason(
      cometStage: SparkPlan,
      sparkStage: SparkPlan): Option[String] = {
    val modelClass = CometConf.COMET_EXEC_COST_MODEL_CLASS.get()
    val minSpeedup = CometConf.COMET_EXEC_COST_MODEL_MIN_SPEEDUP.get()
    try {
      val estimate = CometCostModel.load(modelClass).estimate(cometStage, sparkStage)
      logDebug(
        f"Cost model estimated a speedup of ${estimate.speedup}%.2f (Comet cost " +
          f"${estimate.cometCost}%.3e, Spark cost ${estimate.sparkCost}%.3e) for stage:\n" +
          cometStage.treeString)
      if (estimate.speedup < minSpeedup) {
        Some(
          f"Stage reverted: estimated speedup from Comet of ${estimate.speedup}%.2f is below " +
            s"${CometConf.COMET_EXEC_COST_MODEL_MIN_SPEEDUP.key}=$minSpeedup (set " +
            s"${CometConf.COMET_EXEC_COST_MODEL_ENABLED.key}=false to disable)")
      } else {
        None
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Cost model $modelClass failed; keeping the Comet plan for this stage", e)
        None
    }
  }

  /** Whether this stage has any Comet operator to revert. */
  private def hasCometOperator(plan: SparkPlan): Boolean = plan match {
    case _ if isStageBoundary(plan) => false
    case _: CometPlan => true
    case _ => plan.children.exists(hasCometOperator)
  }

  /**
   * A node that marks the boundary between this stage and an adjacent one.
   */
  private def isStageBoundary(plan: SparkPlan): Boolean = plan match {
    case _: QueryStageExec | _: ShuffleExchangeLike | _: BroadcastExchangeLike => true
    case _ => false
  }

  private def hasUnsafeMixedAggregateAtStageBoundary(stagePlan: SparkPlan): Boolean = {
    def reachesBoundaryBeforeAggregate(plan: SparkPlan): Boolean = plan match {
      case _ if isStageBoundary(plan) => true
      case _: CometHashAggregateExec => false
      case _ => plan.children.exists(reachesBoundaryBeforeAggregate)
    }

    def visit(plan: SparkPlan): Boolean = plan match {
      case _ if isStageBoundary(plan) => false
      case aggregate: CometHashAggregateExec
          if !QueryPlanSerde
            .allAggsSupportNativePartialToSparkFinal(aggregate.aggregateExpressions) ||
            QueryPlanSerde
              .aggsNotSupportingSparkPartialToNativeFinal(aggregate.aggregateExpressions)
              .nonEmpty =>
        val producesBuffer =
          aggregate.modes.exists(mode => mode == Partial || mode == PartialMerge)
        val consumesAcrossBoundary =
          aggregate.modes.exists(mode => mode == Final || mode == PartialMerge) &&
            reachesBoundaryBeforeAggregate(aggregate.child)
        producesBuffer || consumesAcrossBoundary || aggregate.children.exists(visit)
      case _ => plan.children.exists(visit)
    }

    visit(stagePlan)
  }

  /**
   * Like `transformDown`, never descends stage-boundary children.
   */
  private def transformStageDown(plan: SparkPlan)(
      rule: PartialFunction[SparkPlan, SparkPlan]): SparkPlan = {
    val transformed = rule.applyOrElse(plan, identity[SparkPlan])
    val newChildren = transformed.children.map { child =>
      if (isStageBoundary(child)) child else transformStageDown(child)(rule)
    }
    if (newChildren == transformed.children) transformed
    else transformed.withNewChildren(newChildren)
  }

  /** Like `transformUp`, never descends stage-boundary children. */
  private def transformStageUp(plan: SparkPlan)(
      rule: PartialFunction[SparkPlan, SparkPlan]): SparkPlan = {
    val newChildren = plan.children.map { child =>
      if (isStageBoundary(child)) child else transformStageUp(child)(rule)
    }
    val withNewChildren =
      if (newChildren == plan.children) plan else plan.withNewChildren(newChildren)
    rule.applyOrElse(withNewChildren, identity[SparkPlan])
  }

  /** Counts C2R transitions within this stage, stopping at stage boundaries. */
  private[rules] def countTransitions(plan: SparkPlan): Int = {
    var count = 0
    def visit(node: SparkPlan): Unit = node match {
      case _ if isStageBoundary(node) => ()
      case _: ColumnarToRowTransition =>
        count += 1
        node.children.foreach(visit)
      case _ =>
        node.children.foreach(visit)
    }
    visit(plan)
    count
  }

  private[rules] def revertToSpark(plan: SparkPlan): SparkPlan = {
    val stripped = transformStageDown(plan) {
      case CometNativeColumnarToRowExec(child) => child
      case CometColumnarToRowExec(child) => child
      case ColumnarToRowExec(child) => child
      case sparkToColumnar: CometSparkToColumnarExec => sparkToColumnar.child
      case RowToColumnarExec(child) => child
    }
    val reverted = transformStageUp(stripped) {
      // Local candidate selection was inserted by Comet. Only the outer TopK owns
      // the original Spark operator's offset and projection.
      case local: CometLocalTopKExec => local.child
      case cometExec: CometExec =>
        if (cometExec.originalPlan.children.size == cometExec.children.size) {
          cometExec.originalPlan.withNewChildren(cometExec.children)
        } else {
          logWarning(
            "Comet plan and original have different child count for " +
              s"${cometExec.getClass.getSimpleName}, using originalPlan as-is.")
          cometExec.originalPlan
        }
    }
    insertTransitions(reverted)
  }

  private def insertTransitions(plan: SparkPlan): SparkPlan = {
    // transformStageUp never descends into stage-boundary nodes (QueryStageExec, exchanges), so
    // this only needs to bridge row nodes that still have a columnar child within the stage.
    transformStageUp(plan) {
      case node if !node.supportsColumnar =>
        val newChildren = node.children.map { child =>
          if (child.supportsColumnar) ColumnarToRowExec(child) else child
        }
        if (newChildren != node.children) node.withNewChildren(newChildren) else node
    }
  }
}
