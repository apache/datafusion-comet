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
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{ColumnarToRowExec, ColumnarToRowTransition, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}

import org.apache.comet.CometConf

/**
 * Reverts a query stage to Spark row-based execution when it has too many columnar-to-row (C2R)
 * transitions. Each C2R indicates Comet could not keep execution columnar and had to fall back.
 * With columnar shuffle enabled, each C2R implies a corresponding R2C round-trip.
 *
 */
case class RevertNativeForTransitionHeavyStages(session: SparkSession)
    extends Rule[SparkPlan]
    with Logging {

  private lazy val enabled = CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.get()
  private lazy val maxTransitions = CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.get()

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
        revertStageIfNeeded(plan, outputColumnar = false).getOrElse(plan)
    }
  }

  private def applyForNonAQE(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case exchange: ShuffleExchangeLike =>
        revertStageIfNeeded(exchange.child, exchange.supportsColumnar)
          .map(reverted => exchange.withNewChildren(Seq(reverted)))
          .getOrElse(exchange)
    }
  }

  /** Reverts the stage if C2R count exceeds threshold. Wraps in R2C if exchange needs columnar. */
  private def revertStageIfNeeded(
      stagePlan: SparkPlan,
      outputColumnar: Boolean): Option[SparkPlan] = {
    val transitionCount = countTransitions(stagePlan)
    if (transitionCount <= maxTransitions) return None

    logInfo(
      s"Reverting Comet native execution for stage with $transitionCount C2R transitions " +
        s"(threshold: $maxTransitions).")

    val reverted = revertToSpark(stagePlan)
    val result = if (outputColumnar && !reverted.supportsColumnar) {
      RowToColumnarExec(reverted)
    } else {
      reverted
    }
    Some(result)
  }


  /** Counts C2R transitions within this stage, stopping at stage boundaries. */
  private[rules] def countTransitions(plan: SparkPlan): Int = {
    var count = 0
    def visit(node: SparkPlan): Unit = node match {
      case _: QueryStageExec | _: ShuffleExchangeLike => ()
      case _: ColumnarToRowTransition =>
        count += 1
        node.children.foreach(visit)
      case _ =>
        node.children.foreach(visit)
    }
    visit(plan)
    count
  }

  // Two passes: strip transitions first (they assert child.supportsColumnar in constructors),
  // then revert Comet operators to row-based Spark equivalents.
  private[rules] def revertToSpark(plan: SparkPlan): SparkPlan = {
    val stripped = plan.transformDown {
      case CometNativeColumnarToRowExec(child) => child
      case CometColumnarToRowExec(child) => child
      case ColumnarToRowExec(child) => child
      case sparkToColumnar: CometSparkToColumnarExec => sparkToColumnar.child
      case RowToColumnarExec(child) => child
    }
    stripped.transformUp {
      case cometShuffle: CometShuffleExchangeExec =>
        cometShuffle.originalPlan.withNewChildren(Seq(cometShuffle.child))
      case cometExec: CometExec =>
        if (cometExec.originalPlan.children.size == cometExec.children.size) {
          cometExec.originalPlan.withNewChildren(cometExec.children)
        } else {
          cometExec.originalPlan
        }
    }
  }
}
