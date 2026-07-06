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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometExec, CometNativeColumnarToRowExec, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, ColumnarToRowTransition, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason

/**
 * Reverts a query stage to Spark row-based execution when it has too many columnar-to-row (C2R)
 * transitions. Each C2R indicates Comet could not keep execution columnar and had to fall back.
 * With columnar shuffle enabled, each C2R implies a corresponding R2C round-trip.
 */
case class RevertNativeForTransitionHeavyStages(session: SparkSession)
    extends Rule[SparkPlan]
    with Logging {

  private def enabled = CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.get()
  private def maxTransitions = CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.get()

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!enabled) return plan

    if (session.sessionState.conf.adaptiveExecutionEnabled) {
      applyForAQE(plan)
    } else {
      applyForNonAQE(plan)
    }
  }

  private def applyForAQE(plan: SparkPlan): SparkPlan = {
    plan match {
      case _: BroadcastExchangeLike => plan
      case exchange: ShuffleExchangeLike =>
        revertStageIfNeeded(exchange.child, exchange.supportsColumnar)
          .map(reverted => exchange.withNewChildren(Seq(reverted)))
          .getOrElse(plan)
      case _ =>
        // Result stage: its output is collected as rows, so no consumer requires columnar input
        // and the reverted stage needs no trailing R2C.
        revertStageIfNeeded(plan, outputColumnar = false).getOrElse(plan)
    }
  }

  private def applyForNonAQE(plan: SparkPlan): SparkPlan = {
    val withRevertedStages = plan.transformUp { case exchange: ShuffleExchangeLike =>
      revertStageIfNeeded(exchange.child, exchange.supportsColumnar)
        .map(reverted => exchange.withNewChildren(Seq(reverted)))
        .getOrElse(exchange)
    }
    revertStageIfNeeded(withRevertedStages, outputColumnar = false)
      .getOrElse(withRevertedStages)
  }

  /**
   * Reverts the stage if C2R count exceeds threshold. Wraps in R2C if exchange needs columnar.
   */
  private def revertStageIfNeeded(
      stagePlan: SparkPlan,
      outputColumnar: Boolean): Option[SparkPlan] = {
    val transitionCount = countTransitions(stagePlan)
    if (transitionCount <= maxTransitions) return None

    val reason =
      s"Stage reverted: $transitionCount C2R transitions exceed threshold $maxTransitions"

    val reverted = revertToSpark(stagePlan)
    val result = if (outputColumnar && !reverted.supportsColumnar) {
      RowToColumnarExec(withFallbackReason(reverted, reason))
    } else {
      withFallbackReason(reverted, reason)
    }
    Some(result)
  }

  /**
   * A node that marks the boundary between this stage and an adjacent one.
   */
  private def isStageBoundary(plan: SparkPlan): Boolean = plan match {
    case _: QueryStageExec | _: ShuffleExchangeLike | _: BroadcastExchangeLike => true
    case _ => false
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
    val reverted = transformStageUp(stripped) { case cometExec: CometExec =>
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
