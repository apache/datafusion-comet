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
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometBroadcastHashJoinExec, CometNativeScanExec, CometSubqueryAdaptiveBroadcastExec, CometSubqueryBroadcastExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, BroadcastQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

import org.apache.comet.shims.ShimSubqueryBroadcast

/**
 * Converts CometSubqueryAdaptiveBroadcastExec (wrapped AQE DPP) to CometSubqueryBroadcastExec
 * inside CometNativeScanExec's partitionFilters.
 *
 * CometExecRule wraps SubqueryAdaptiveBroadcastExec in CometSubqueryAdaptiveBroadcastExec during
 * queryStagePreparationRules to prevent Spark's PlanAdaptiveDynamicPruningFilters from replacing
 * DPP with Literal.TrueLiteral (which happens because Spark can't find BroadcastHashJoinExec
 * after Comet replaced it with CometBroadcastHashJoinExec).
 *
 * This rule runs as a queryStageOptimizerRule (after Spark's built-in rules). By this point,
 * broadcast stages are materialized as BroadcastQueryStageExec. We find the matching
 * CometBroadcastHashJoinExec (or BroadcastHashJoinExec for fallback), extract its broadcast
 * child, and create CometSubqueryBroadcastExec for true broadcast reuse.
 *
 * Also handles the dual-filter problem: CometNativeScanExec.partitionFilters and
 * CometScanExec.partitionFilters are separate InSubqueryExec instances. Both must be converted
 * because CometScanExec.dynamicallySelectedPartitions evaluates its own partitionFilters.
 *
 * @see
 *   PlanAdaptiveDynamicPruningFilters (Spark's equivalent for BroadcastHashJoinExec)
 * @see
 *   CometExecRule.convertSubqueryBroadcasts (non-AQE DPP + SAB wrapping)
 */
case object CometPlanAdaptiveDynamicPruningFilters
    extends Rule[SparkPlan]
    with AdaptiveSparkPlanHelper
    with ShimSubqueryBroadcast
    with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan.transformUp {
      case nativeScan: CometNativeScanExec if nativeScan.partitionFilters.exists(hasCometSAB) =>
        logDebug("Converting AQE DPP for CometNativeScanExec")
        convertNativeScanDPP(nativeScan, plan)
    }
  }

  private def convertNativeScanDPP(
      nativeScan: CometNativeScanExec,
      fullPlan: SparkPlan): CometNativeScanExec = {
    val newOuterFilters = nativeScan.partitionFilters.map(f => convertFilter(f, fullPlan))

    // If no filters changed, the scan had no convertible SABs (all fell back to TrueLiteral
    // or were already converted). Return unchanged.
    if (newOuterFilters == nativeScan.partitionFilters) return nativeScan

    // Dual-filter invariant: CometNativeScanExec.partitionFilters and
    // CometScanExec.partitionFilters contain separate InSubqueryExec instances for the same
    // DPP filters. Both must be converted because CometScanExec.dynamicallySelectedPartitions
    // evaluates its own filters via getFilePartitions().
    assert(
      nativeScan.scan != null,
      "CometNativeScanExec with DPP filters must have a non-null CometScanExec")
    val newInnerFilters = nativeScan.scan.partitionFilters.map(f => convertFilter(f, fullPlan))
    val newInnerScan = nativeScan.scan.copy(partitionFilters = newInnerFilters)

    nativeScan.copy(partitionFilters = newOuterFilters, scan = newInnerScan)
  }

  private def convertFilter(filter: Expression, fullPlan: SparkPlan): Expression = {
    filter.transformUp { case dpe @ DynamicPruningExpression(inSub: InSubqueryExec) =>
      extractSABData(inSub) match {
        case Some((name, indices, buildKeys)) =>
          convertSAB(inSub, name, indices, buildKeys, fullPlan).getOrElse {
            logInfo(s"No matching broadcast join for DPP subquery '$name', disabling DPP")
            DynamicPruningExpression(Literal.TrueLiteral)
          }
        case None => dpe
      }
    }
  }

  /**
   * Extracts SAB data from an InSubqueryExec's plan. Handles both:
   *   - CometSubqueryAdaptiveBroadcastExec (outer partitionFilters, wrapped by CometExecRule)
   *   - SubqueryAdaptiveBroadcastExec (inner CometScanExec.partitionFilters, never wrapped
   *     because CometScanExec is @transient and not part of the plan expression tree)
   *
   * Returns (name, indices, buildKeys) if the plan is an SAB variant, None otherwise.
   */
  private def extractSABData(
      inSub: InSubqueryExec): Option[(String, Seq[Int], Seq[Expression])] = {
    def extract(plan: BaseSubqueryExec): Option[(String, Seq[Int], Seq[Expression])] = {
      plan match {
        case csab: CometSubqueryAdaptiveBroadcastExec =>
          Some((csab.name, csab.indices, csab.buildKeys))
        case sab: SubqueryAdaptiveBroadcastExec =>
          Some((sab.name, getSubqueryBroadcastIndices(sab), sab.buildKeys))
        case _ => None
      }
    }
    inSub.plan match {
      case sub: BaseSubqueryExec => extract(sub)
      case ReusedSubqueryExec(sub: BaseSubqueryExec) => extract(sub)
      case _ => None
    }
  }

  /**
   * Converts an SAB (wrapped or unwrapped) by finding the matching broadcast join and wiring the
   * subquery to reuse its already-materialized broadcast exchange.
   *
   * Returns None when no matching broadcast join exists (e.g., SortMergeJoin). Caller should fall
   * back to Literal.TrueLiteral (disabling DPP).
   */
  private def convertSAB(
      inSub: InSubqueryExec,
      name: String,
      indices: Seq[Int],
      buildKeys: Seq[Expression],
      fullPlan: SparkPlan): Option[DynamicPruningExpression] = {
    val sabKeyIds: Set[Any] = buildKeys.flatMap(_.references.map(_.exprId)).toSet
    assert(
      sabKeyIds.nonEmpty,
      s"DPP subquery '$name' has empty buildKeys — " +
        "PlanAdaptiveSubqueries should always populate buildKeys")

    findMatchingBroadcastJoin(sabKeyIds, fullPlan).map { result =>
      val broadcastChild = result._1
      val isComet = result._2
      logDebug(
        s"Matched DPP subquery '$name' to " +
          s"${if (isComet) "Comet" else "Spark"} broadcast: " +
          s"${broadcastChild.getClass.getSimpleName}")

      assert(
        broadcastChild.isInstanceOf[BroadcastQueryStageExec],
        "Expected BroadcastQueryStageExec as broadcast child for DPP reuse, " +
          s"got ${broadcastChild.getClass.getSimpleName}. " +
          "queryStageOptimizerRules should run after broadcast stage materialization.")

      // The stage's plan may be the original exchange or a ReusedExchangeExec (when AQE
      // reuses exchanges across the main plan and scalar subquery plans via shared context).
      // Unwrap ReusedExchangeExec to verify the underlying exchange type matches the join.
      val stageExchange = broadcastChild.asInstanceOf[BroadcastQueryStageExec].plan
      val underlyingExchange = stageExchange match {
        case r: ReusedExchangeExec => r.child
        case other => other
      }
      val isCometExchange = underlyingExchange.isInstanceOf[CometBroadcastExchangeExec]
      val isSparkExchange = underlyingExchange.isInstanceOf[BroadcastExchangeExec]
      assert(
        (isComet && isCometExchange) || (!isComet && isSparkExchange),
        s"Join/broadcast type mismatch: join isComet=$isComet, " +
          s"exchange=${underlyingExchange.getClass.getSimpleName} " +
          s"(via ${stageExchange.getClass.getSimpleName}). " +
          "CometExecRule should convert both or neither.")

      val subquery = if (isComet) {
        CometSubqueryBroadcastExec(name, indices, buildKeys, broadcastChild)
      } else {
        createSubqueryBroadcastExec(name, indices, buildKeys, broadcastChild)
      }
      DynamicPruningExpression(inSub.withNewPlan(subquery))
    }
  }

  /**
   * Finds a broadcast hash join whose build-side keys match the given exprIds. Searches for both
   * CometBroadcastHashJoinExec and BroadcastHashJoinExec to handle cases where the join fell back
   * to Spark (e.g., unsupported expression, disabled Comet BHJ config).
   *
   * Uses AdaptiveSparkPlanHelper.find which traverses through QueryStageExec nodes, ensuring we
   * can see BroadcastQueryStageExec children.
   */
  private def findMatchingBroadcastJoin(
      sabKeyIds: Set[Any],
      plan: SparkPlan): Option[(SparkPlan, Boolean)] = {
    var result: Option[(SparkPlan, Boolean)] = None
    find(plan) {
      case join: CometBroadcastHashJoinExec if result.isEmpty =>
        result = extractBroadcastChild(
          join.buildSide,
          join.left,
          join.right,
          join.leftKeys,
          join.rightKeys,
          isCometJoin = true,
          sabKeyIds)
        result.isDefined
      case join: BroadcastHashJoinExec if result.isEmpty =>
        result = extractBroadcastChild(
          join.buildSide,
          join.left,
          join.right,
          join.leftKeys,
          join.rightKeys,
          isCometJoin = false,
          sabKeyIds)
        result.isDefined
      case _ => false
    }
    result
  }

  private def extractBroadcastChild(
      buildSide: org.apache.spark.sql.catalyst.optimizer.BuildSide,
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      isCometJoin: Boolean,
      sabKeyIds: Set[Any]): Option[(SparkPlan, Boolean)] = {
    val joinBuildKeys = buildSide match {
      case BuildLeft => leftKeys
      case BuildRight => rightKeys
    }
    val joinKeyIds: Set[Any] = joinBuildKeys.flatMap(_.references.map(_.exprId)).toSet
    if (sabKeyIds == joinKeyIds) {
      val bc = buildSide match {
        case BuildLeft => left
        case BuildRight => right
      }
      Some((bc, isCometJoin))
    } else {
      None
    }
  }

  /**
   * Checks if an expression contains an SAB variant (wrapped or unwrapped). The outer
   * CometNativeScanExec.partitionFilters has CometSubqueryAdaptiveBroadcastExec (wrapped by
   * CometExecRule). The inner CometScanExec.partitionFilters may have the original
   * SubqueryAdaptiveBroadcastExec (unwrapped, because CometScanExec is @transient).
   */
  private def hasCometSAB(e: Expression): Boolean =
    e.exists {
      case DynamicPruningExpression(inSub: InSubqueryExec) =>
        extractSABData(inSub).isDefined
      case _ => false
    }
}
