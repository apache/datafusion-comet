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
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometBroadcastHashJoinExec, CometIcebergNativeScanExec, CometSubqueryBroadcastExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, BroadcastQueryStageExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

import org.apache.comet.shims.ShimSubqueryBroadcast

/**
 * Converts SubqueryAdaptiveBroadcastExec (AQE DPP) to CometSubqueryBroadcastExec or
 * SubqueryBroadcastExec inside CometIcebergNativeScanExec's runtimeFilters.
 *
 * Spark's PlanAdaptiveDynamicPruningFilters performs this conversion for BatchScanExec, but
 * CometIcebergNativeScanExec wraps BatchScanExec and hides its runtimeFilters from the plan's
 * expression tree. This rule accesses them directly via originalPlan.runtimeFilters.
 *
 * Registered as postColumnarTransitions (not queryStageOptimizerRule) because CometExecRule runs
 * in preColumnarTransitions and recreates CometIcebergNativeScanExec instances, which would
 * discard earlier modifications. Running after ensures we see the final scan instances.
 *
 * @see
 *   PlanAdaptiveDynamicPruningFilters (Spark's equivalent for visible DPP expressions)
 * @see
 *   CometExecRule.convertSubqueryBroadcasts (non-AQE DPP conversion, PR #4011)
 */
case class CometPlanAdaptiveDynamicPruningFilters(session: SparkSession)
    extends Rule[SparkPlan]
    with AdaptiveSparkPlanHelper
    with ShimSubqueryBroadcast
    with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    // Short-circuit: only process plans containing Iceberg scans with DPP runtime filters.
    val hasIcebergDPP = plan.find {
      case scan: CometIcebergNativeScanExec =>
        scan.originalPlan != null && scan.originalPlan.runtimeFilters.exists {
          case DynamicPruningExpression(_: InSubqueryExec) => true
          case _ => false
        }
      case _ => false
    }.isDefined

    if (!hasIcebergDPP) return plan

    logDebug("Processing plan with Iceberg DPP runtime filters")

    plan.transformUp {
      case scan: CometIcebergNativeScanExec if scan.originalPlan != null =>
        val runtimeFilters = scan.originalPlan.runtimeFilters
        val newFilters = runtimeFilters.map(transformFilter(_, plan))
        if (newFilters != runtimeFilters) {
          val newBatchScan = scan.originalPlan.copy(runtimeFilters = newFilters)
          scan.originalPlan.logicalLink.foreach(newBatchScan.setLogicalLink)
          scan.copy(originalPlan = newBatchScan)
        } else {
          scan
        }
    }
  }

  private def transformFilter(filter: Expression, fullPlan: SparkPlan): Expression = {
    filter.transformUp { case dpe @ DynamicPruningExpression(inSub: InSubqueryExec) =>
      inSub.plan match {
        case sab: SubqueryAdaptiveBroadcastExec =>
          logDebug(s"Converting SubqueryAdaptiveBroadcastExec '${sab.name}'")
          convertSAB(inSub, sab, fullPlan).getOrElse {
            // No matching broadcast join found (e.g., SortMergeJoin, or join optimized away).
            // Spark's PlanAdaptiveDynamicPruningFilters handles onlyInBroadcast=false with an
            // aggregate subquery fallback. We use TrueLiteral for both cases: correct results
            // (scans all partitions), avoids replicating Spark's aggregate planning internals.
            logInfo(s"No matching broadcast join for DPP subquery '${sab.name}', disabling DPP")
            DynamicPruningExpression(Literal.TrueLiteral)
          }
        case _ => dpe
      }
    }
  }

  /**
   * Converts a SubqueryAdaptiveBroadcastExec by finding the matching broadcast join and wiring
   * the subquery to reuse its already-materialized broadcast exchange.
   *
   * The subquery type depends on the actual broadcast exchange type (not the join type):
   *   - CometBroadcastExchangeExec -> CometSubqueryBroadcastExec (decodes Arrow broadcast data)
   *   - BroadcastExchangeExec -> SubqueryBroadcastExec (decodes HashedRelation)
   *
   * CometExecRule converts joins and their broadcast exchanges together, so the join type and
   * broadcast type should always agree. The assert in extractBroadcastChild enforces this.
   */
  private def convertSAB(
      inSub: InSubqueryExec,
      sab: SubqueryAdaptiveBroadcastExec,
      fullPlan: SparkPlan): Option[DynamicPruningExpression] = {
    val buildKeys = sab.buildKeys
    val indices = getSubqueryBroadcastIndices(sab)
    val sabKeyIds: Set[Any] = sab.buildKeys.flatMap(_.references.map(_.exprId)).toSet

    findMatchingBroadcastJoin(sabKeyIds, fullPlan).map {
      case (broadcastChild: SparkPlan, isComet: Boolean) =>
        logDebug(
          s"Matched DPP subquery '${sab.name}' to " +
            s"${if (isComet) "Comet" else "Spark"} broadcast exchange")
        val subquery = if (isComet) {
          CometSubqueryBroadcastExec(sab.name, indices, buildKeys, broadcastChild)
        } else {
          createSubqueryBroadcastExec(sab.name, indices, buildKeys, broadcastChild)
        }
        DynamicPruningExpression(inSub.withNewPlan(subquery))
    }
  }

  /**
   * Finds a broadcast hash join whose build-side keys match the given exprIds. Searches both
   * CometBroadcastHashJoinExec and BroadcastHashJoinExec to handle cases where the join fell back
   * to Spark (e.g., unsupported expression, disabled config).
   */
  private def findMatchingBroadcastJoin(
      sabKeyIds: Set[Any],
      plan: SparkPlan): Option[(SparkPlan, Boolean)] = {

    def extractBroadcastChild(
        buildSide: BuildSide,
        left: SparkPlan,
        right: SparkPlan,
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        isCometJoin: Boolean): Option[(SparkPlan, Boolean)] = {
      val joinBuildKeys = buildSide match {
        case BuildLeft => leftKeys
        case BuildRight => rightKeys
      }
      val joinKeyIds = joinBuildKeys.flatMap(_.references.map(_.exprId)).toSet
      if (sabKeyIds.nonEmpty && sabKeyIds == joinKeyIds) {
        val bc = buildSide match {
          case BuildLeft => left
          case BuildRight => right
        }
        val isCometBroadcast = isCometBroadcastExchange(bc)

        // CometExecRule converts joins and their broadcast exchanges together.
        // A mismatch would cause a ClassCastException (Arrow vs HashedRelation).
        assert(
          isCometJoin == isCometBroadcast,
          s"Join/broadcast type mismatch: join isComet=$isCometJoin, broadcast isComet=" +
            s"$isCometBroadcast. CometExecRule should convert both or neither.")

        Some((bc, isCometBroadcast))
      } else {
        None
      }
    }

    var result: Option[(SparkPlan, Boolean)] = None
    find(plan) {
      case join: CometBroadcastHashJoinExec if result.isEmpty =>
        result = extractBroadcastChild(
          join.buildSide,
          join.left,
          join.right,
          join.leftKeys,
          join.rightKeys,
          isCometJoin = true)
        result.isDefined
      case join: BroadcastHashJoinExec if result.isEmpty =>
        result = extractBroadcastChild(
          join.buildSide,
          join.left,
          join.right,
          join.leftKeys,
          join.rightKeys,
          isCometJoin = false)
        result.isDefined
      case _ => false
    }
    result
  }

  private def isCometBroadcastExchange(plan: SparkPlan): Boolean = plan match {
    case _: CometBroadcastExchangeExec => true
    case BroadcastQueryStageExec(_, _: CometBroadcastExchangeExec, _) => true
    case _ => false
  }
}
