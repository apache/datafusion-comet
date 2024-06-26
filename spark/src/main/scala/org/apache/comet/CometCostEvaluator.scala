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

package org.apache.comet

import org.apache.spark.internal.Logging
import org.apache.spark.sql.comet.{CometExec, CometPlan, CometRowToColumnarExec, CometSinkPlaceHolder}
import org.apache.spark.sql.execution.{ColumnarToRowExec, InputAdapter, RowToColumnarExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, Cost, CostEvaluator, QueryStageExec, SimpleCost}

/**
 * The goal of this cost model is to avoid introducing performance regressions in query stages
 * during AQE.
 *
 * This evaluator will be called twice; once for the original Spark plan and once for the Comet
 * plan. Spark will choose the cheapest plan.
 */
class CometCostEvaluator extends CostEvaluator with Logging {

  /** Baseline cost for Spark operator is 1.0 */
  val DEFAULT_SPARK_OPERATOR_COST = 1.0

  /** Relative cost of Comet operator */
  val DEFAULT_COMET_OPERATOR_COST = 0.8

  /** Relative cost of a transition (C2R, R2C) */
  val DEFAULT_TRANSITION_COST = 1.0

  override def evaluateCost(plan: SparkPlan): Cost = {

    // TODO this is a crude prototype where we just penalize transitions, but
    // this can evolve into a true cost model where we have real numbers for the relative
    // performance of Comet operators & expressions versus the Spark versions
    //
    // Some areas to explore
    // - can we use statistics from previous query stage(s)?
    // - transition after filter should be cheaper than transition before filter (such as when
    //   reading from Parquet followed by filter. Comet will filter first then transition)
    def computePlanCost(plan: SparkPlan): Double = {

      // get children even for leaf nodes at query stage edges
      def getChildren(plan: SparkPlan) = plan match {
        case a: AdaptiveSparkPlanExec => Seq(a.inputPlan)
        case qs: QueryStageExec => Seq(qs.plan)
        case p => p.children
      }

      val children = getChildren(plan)
      val childPlanCost = children.map(computePlanCost).sum
      val operatorCost = plan match {
        case _: AdaptiveSparkPlanExec => 0
        case _: CometSinkPlaceHolder => 0
        case _: InputAdapter => 0
        case _: WholeStageCodegenExec => 0
        case RowToColumnarExec(_) => DEFAULT_TRANSITION_COST
        case ColumnarToRowExec(_) => DEFAULT_TRANSITION_COST
        case CometRowToColumnarExec(_) => DEFAULT_TRANSITION_COST
        case _: CometExec => DEFAULT_COMET_OPERATOR_COST
        case _ => DEFAULT_SPARK_OPERATOR_COST
      }

      def isSparkNative(plan: SparkPlan): Boolean = plan match {
        case p: AdaptiveSparkPlanExec => isSparkNative(p.inputPlan)
        case p: QueryStageExec => isSparkNative(p.plan)
        case _: CometPlan => false
        case _ => true
      }

      def isCometNative(plan: SparkPlan): Boolean = plan match {
        case p: AdaptiveSparkPlanExec => isCometNative(p.inputPlan)
        case p: QueryStageExec => isCometNative(p.plan)
        case _: CometPlan => true
        case _ => false
      }

      def isTransition(plan1: SparkPlan, plan2: SparkPlan) = {
        (isSparkNative(plan1) && isCometNative(plan2)) ||
        (isCometNative(plan1) && isSparkNative(plan2))
      }

      val transitionCost = if (children.exists(ch => isTransition(plan, ch))) {
        DEFAULT_TRANSITION_COST
      } else {
        0
      }

      val totalCost = operatorCost + transitionCost + childPlanCost

      logWarning(
        s"total cost is $totalCost ($operatorCost + $transitionCost + $childPlanCost) " +
          s"for ${plan.nodeName}")

      totalCost
    }

    // TODO can we access statistics from previous query stages?
    val estimatedRowCount = 1000
    val cost = (computePlanCost(plan) * estimatedRowCount).toLong

    logWarning(s"Computed cost of $cost for $plan")

    SimpleCost(cost)
  }

}
