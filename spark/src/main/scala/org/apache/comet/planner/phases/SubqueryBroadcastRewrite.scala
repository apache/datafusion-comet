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

package org.apache.comet.planner.phases

import org.apache.spark.internal.Logging
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometNativeColumnarToRowExec, CometNativeExec, CometSubqueryAdaptiveBroadcastExec, CometSubqueryBroadcastExec}
import org.apache.spark.sql.execution.{InSubqueryExec, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.shims.ShimSubqueryBroadcast

/**
 * Post-pass: rewrite DPP subquery broadcasts so that exchange reuse and AQE DPP work correctly
 * after Comet conversion. Runs once over the emitted plan, not at every node of the main
 * transform like the old CometExecRule does.
 *
 * Two cases:
 *
 *   - Non-AQE DPP: a BroadcastExchangeExec on the join side has been converted to
 *     CometBroadcastExchangeExec, but the DPP subquery still references the original
 *     BroadcastExchangeExec. ReuseExchangeAndSubquery (which runs after Comet rules) can't match
 *     them because their types differ. Replace SubqueryBroadcastExec with
 *     CometSubqueryBroadcastExec wrapping the converted broadcast so both sides share the same
 *     type.
 *
 *   - AQE DPP (Spark 3.5+): Spark's PlanAdaptiveDynamicPruningFilters pattern-matches
 *     SubqueryAdaptiveBroadcastExec. When it can't find BroadcastHashJoinExec (Comet replaced it
 *     with CometBroadcastHashJoinExec), it replaces the DPP filter with Literal.TrueLiteral,
 *     disabling DPP. Wrap SABs in CometSubqueryAdaptiveBroadcastExec to hide them from Spark's
 *     rule. CometPlanAdaptiveDynamicPruningFilters later unwraps them with access to the
 *     materialized BroadcastQueryStageExec.
 *
 * Logic copied from CometExecRule.convertSubqueryBroadcasts so CometPlanner can run this rewrite
 * without pulling in the whole old rule. Delete the original when CometExecRule is removed.
 */
object SubqueryBroadcastRewrite extends ShimSubqueryBroadcast with Logging {

  def apply(plan: SparkPlan): SparkPlan = {
    var nonAqeRewrites = 0
    var aqeRewrites = 0
    val out = plan.transformAllExpressions { case inSub: InSubqueryExec =>
      inSub.plan match {
        case sub: SubqueryBroadcastExec =>
          sub.child match {
            case b: BroadcastExchangeExec =>
              val cometChild = b.child match {
                case c2r: CometNativeColumnarToRowExec => c2r.child
                case other => other
              }
              if (cometChild.isInstanceOf[CometNativeExec]) {
                nonAqeRewrites += 1
                val cometBroadcast = CometBroadcastExchangeExec(b, b.output, b.mode, cometChild)
                val cometSub = CometSubqueryBroadcastExec(
                  sub.name,
                  getSubqueryBroadcastExecIndices(sub),
                  sub.buildKeys,
                  cometBroadcast)
                inSub.withNewPlan(cometSub)
              } else {
                inSub
              }
            case _ => inSub
          }
        case sab: SubqueryAdaptiveBroadcastExec if isSpark35Plus =>
          assert(
            sab.buildKeys.nonEmpty,
            s"SubqueryAdaptiveBroadcastExec '${sab.name}' has empty buildKeys")
          aqeRewrites += 1
          val indices = getSubqueryBroadcastIndices(sab)
          val wrapped = CometSubqueryAdaptiveBroadcastExec(
            sab.name,
            indices,
            sab.onlyInBroadcast,
            sab.buildPlan,
            sab.buildKeys,
            sab.child)
          inSub.withNewPlan(wrapped)
        case _: SubqueryAdaptiveBroadcastExec =>
          // Spark 3.4: no injectQueryStageOptimizerRule, leave SAB unwrapped. The
          // CometSpark34AqeDppFallbackRule handles the 3.4-specific path.
          inSub
        case _ => inSub
      }
    }
    if (nonAqeRewrites + aqeRewrites > 0) {
      logDebug(
        s"SubqueryBroadcastRewrite: nonAqeRewrites=$nonAqeRewrites aqeRewrites=$aqeRewrites")
    }
    out
  }
}
