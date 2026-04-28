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
import org.apache.spark.sql.catalyst.expressions.{Alias, BindReferences, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometBroadcastHashJoinExec, CometNativeScanExec, CometSubqueryAdaptiveBroadcastExec, CometSubqueryBroadcastExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, HashJoin}

import org.apache.comet.shims.{ShimPrepareExecutedPlan, ShimSubqueryBroadcast}

/**
 * Converts CometSubqueryAdaptiveBroadcastExec (wrapped AQE DPP) to CometSubqueryBroadcastExec
 * inside CometNativeScanExec's partitionFilters.
 *
 * CometExecRule wraps SubqueryAdaptiveBroadcastExec in CometSubqueryAdaptiveBroadcastExec during
 * queryStagePreparationRules to prevent Spark's PlanAdaptiveDynamicPruningFilters from replacing
 * DPP with Literal.TrueLiteral (which happens because Spark can't find BroadcastHashJoinExec
 * after Comet replaced it with CometBroadcastHashJoinExec).
 *
 * This rule runs as a queryStageOptimizerRule (after Spark's built-in rules). We find the
 * matching CometBroadcastHashJoinExec (or BroadcastHashJoinExec for fallback) and create
 * CometSubqueryBroadcastExec for broadcast reuse.
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
    with ShimPrepareExecutedPlan
    with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    // TODO(#3510): CometNativeScanExec needs special handling because its makeCopy
    // loses @transient scan and expression transformations. Once makeCopy is fixed
    // (or CometScanExec wrapping is removed), replace both cases with a single
    // plan.transformAllExpressions call matching Spark's PlanAdaptiveDynamicPruningFilters.
    plan.transformUp {
      case nativeScan: CometNativeScanExec if nativeScan.partitionFilters.exists(hasCometSAB) =>
        logDebug("Converting AQE DPP for CometNativeScanExec")
        convertNativeScanDPP(nativeScan, plan)
      case p: SparkPlan if !p.isInstanceOf[CometNativeScanExec] && hasWrappedSAB(p) =>
        logDebug(s"Converting AQE DPP for non-Comet node: ${p.nodeName}")
        convertNonCometNodeDPP(p, plan)
    }
  }

  private def convertNativeScanDPP(
      nativeScan: CometNativeScanExec,
      stagePlan: SparkPlan): CometNativeScanExec = {
    val newOuterFilters = nativeScan.partitionFilters.map(f => convertFilter(f, stagePlan))

    if (newOuterFilters == nativeScan.partitionFilters) return nativeScan

    // Dual-filter invariant: CometNativeScanExec.partitionFilters and
    // CometScanExec.partitionFilters are separate InSubqueryExec instances for the
    // same DPP filters. Both must be converted because
    // CometScanExec.dynamicallySelectedPartitions evaluates its own filters.
    assert(
      nativeScan.scan != null,
      "CometNativeScanExec with DPP filters must have a non-null CometScanExec")
    val newInnerFilters = nativeScan.scan.partitionFilters.map(f => convertFilter(f, stagePlan))
    val newInnerScan = nativeScan.scan.copy(partitionFilters = newInnerFilters)

    nativeScan.copy(partitionFilters = newOuterFilters, scan = newInnerScan)
  }

  private def convertFilter(filter: Expression, stagePlan: SparkPlan): Expression = {
    filter.transformUp { case dpe @ DynamicPruningExpression(inSub: InSubqueryExec) =>
      extractSABData(inSub) match {
        case Some(sabData) =>
          convertSAB(inSub, sabData, stagePlan)
        case None => dpe
      }
    }
  }

  /**
   * Extracts SAB data from an InSubqueryExec's plan. Handles both:
   *   - CometSubqueryAdaptiveBroadcastExec (outer partitionFilters, wrapped by CometExecRule)
   *   - SubqueryAdaptiveBroadcastExec (inner CometScanExec.partitionFilters, never wrapped
   *     because CometScanExec is @transient and not part of the plan expression tree)
   */
  private case class SABData(
      name: String,
      indices: Seq[Int],
      onlyInBroadcast: Boolean,
      buildPlan: LogicalPlan,
      buildKeys: Seq[Expression],
      adaptivePlan: SparkPlan)

  private def extractSABData(inSub: InSubqueryExec): Option[SABData] = {
    def extract(plan: BaseSubqueryExec): Option[SABData] = {
      plan match {
        case csab: CometSubqueryAdaptiveBroadcastExec =>
          Some(
            SABData(
              csab.name,
              csab.indices,
              csab.onlyInBroadcast,
              csab.buildPlan,
              csab.buildKeys,
              csab.child))
        case sab: SubqueryAdaptiveBroadcastExec =>
          Some(
            SABData(
              sab.name,
              getSubqueryBroadcastIndices(sab),
              sab.onlyInBroadcast,
              sab.buildPlan,
              sab.buildKeys,
              sab.child))
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
   * Converts an SAB following the same decision tree as Spark's
   * PlanAdaptiveDynamicPruningFilters:
   *
   *   1. exchangeReuseEnabled + matching broadcast join found: Create CometSubqueryBroadcastExec
   *      (or SubqueryBroadcastExec for Spark fallback) wired to the join's broadcast. DPP uses
   *      broadcast reuse via AQE's stageCache.
   *
   * 2. No reusable broadcast + onlyInBroadcast=true: Literal.TrueLiteral. DPP is disabled
   * (correct results, scans all partitions).
   *
   * 3. No reusable broadcast + onlyInBroadcast=false: Aggregate SubqueryExec on the build side
   * (DPP via separate execution, matching Spark's PlanAdaptiveDynamicPruningFilters lines 68-79).
   */
  private def convertSAB(
      inSub: InSubqueryExec,
      sab: SABData,
      stagePlan: SparkPlan): DynamicPruningExpression = {
    val adaptivePlan = sab.adaptivePlan.asInstanceOf[AdaptiveSparkPlanExec]

    val sabKeyIds: Set[Any] = sab.buildKeys.flatMap(_.references.map(_.exprId)).toSet
    assert(
      sabKeyIds.nonEmpty,
      s"DPP subquery '${sab.name}' has empty buildKeys - " +
        "PlanAdaptiveSubqueries should always populate buildKeys")

    // Spark's PlanAdaptiveDynamicPruningFilters is constructed with rootPlan = the
    // current AdaptiveSparkPlanExec (ASPE). Each ASPE (main query and each scalar
    // subquery) gets its own rule instance pointing to itself. The rule searches
    // rootPlan via find() to locate matching broadcast joins.
    //
    // Custom queryStageOptimizerRules (registered via injectQueryStageOptimizerRule)
    // are instantiated once and shared across all ASPEs, so we don't get a per-ASPE
    // rootPlan reference. We approximate Spark's behavior with two searches:
    //
    //   1. stagePlan: the plan arg to apply(), which is the current stage's child
    //      plan. Covers same-stage joins (the common case) and scalar subqueries
    //      where scan+join are under one exchange.
    //   2. context.qe.executedPlan: the main query's ASPE, accessed via the shared
    //      AdaptiveExecutionContext. Covers cross-stage joins in the main query
    //      where a shuffle separates the scan from the broadcast join.
    val rootPlan = adaptivePlan.context.qe.executedPlan

    val matchingJoin = findMatchingBroadcastJoin(sabKeyIds, stagePlan)
      .orElse(findMatchingBroadcastJoin(sabKeyIds, rootPlan))
    val canReuse = conf.exchangeReuseEnabled && matchingJoin.isDefined

    if (canReuse) {
      // Case 1: broadcast reuse. Matches Spark's PlanAdaptiveDynamicPruningFilters
      // lines 44-64: construct a NEW exchange wrapping adaptivePlan.executedPlan,
      // then wrap in a new ASPE. AQE's stageCache ensures the broadcast runs once
      // via ReusedExchangeExec (same canonical form as the join's exchange).
      val (broadcastChild, isComet) = matchingJoin.get
      val buildSidePlan = adaptivePlan.executedPlan
      logDebug(
        s"Matched DPP subquery '${sab.name}' to " +
          s"${if (isComet) "Comet" else "Spark"} broadcast: " +
          s"${broadcastChild.getClass.getSimpleName}")

      // Construct the exchange from buildSidePlan (not from the existing exchange),
      // matching Spark's PlanAdaptiveDynamicPruningFilters lines 44-48. The existing
      // exchange may belong to a different plan context (e.g., the main query) with
      // different attribute IDs than the current SAB's build side (e.g., a scalar
      // subquery). Using the existing exchange's output/mode would cause schema
      // mismatch when CometSubqueryBroadcastExec projects keys by exprId.
      val packedKeys = BindReferences.bindReferences(
        HashJoin.rewriteKeyExpr(sab.buildKeys),
        buildSidePlan.output)
      val mode = HashedRelationBroadcastMode(packedKeys)
      val newExchange = if (isComet) {
        CometBroadcastExchangeExec(buildSidePlan, buildSidePlan.output, mode, buildSidePlan)
      } else {
        BroadcastExchangeExec(mode, buildSidePlan)
      }
      buildSidePlan.logicalLink.foreach(newExchange.setLogicalLink)

      // supportsColumnar must match the exchange. ASPE.getFinalPhysicalPlan
      // applies postStageCreationRules(supportsColumnar) to the final plan.
      // With supportsColumnar=false (the SAB ASPE's default),
      // ApplyColumnarRulesAndInsertTransitions wraps the BroadcastQueryStageExec
      // in ColumnarToRowExec, failing the assertion at ASPE.doExecuteBroadcast
      // that expects BroadcastQueryStageExec as the final plan.
      val newAdaptivePlan = adaptivePlan.copy(
        inputPlan = newExchange,
        supportsColumnar = newExchange.supportsColumnar)
      // ASPE constructor applies queryStagePreparationRules to inputPlan,
      // which clears the logicalLink tag as a side effect. Re-set it so
      // getFinalPhysicalPlan (line 276) can read inputPlan.logicalLink.
      buildSidePlan.logicalLink.foreach(newAdaptivePlan.inputPlan.setLogicalLink)

      val subquery = if (isComet) {
        CometSubqueryBroadcastExec(sab.name, sab.indices, sab.buildKeys, newAdaptivePlan)
      } else {
        createSubqueryBroadcastExec(sab.name, sab.indices, sab.buildKeys, newAdaptivePlan)
      }
      DynamicPruningExpression(inSub.withNewPlan(reuseOrRegisterSubquery(subquery, adaptivePlan)))
    } else if (sab.onlyInBroadcast) {
      // Case 2: no reusable broadcast, and the optimizer says DPP only makes sense
      // with broadcast reuse. Disable DPP (Literal.TrueLiteral).
      logInfo(
        s"No reusable broadcast for DPP subquery '${sab.name}' " +
          "(onlyInBroadcast=true), disabling DPP")
      DynamicPruningExpression(Literal.TrueLiteral)
    } else {
      // Case 3: no reusable broadcast, but the optimizer says DPP is worthwhile
      // even without broadcast reuse. Create an aggregate SubqueryExec on the build
      // side to get distinct partition key values for pruning.
      // Matches Spark's PlanAdaptiveDynamicPruningFilters lines 68-79.
      val aliases =
        sab.indices.map(idx => Alias(sab.buildKeys(idx), sab.buildKeys(idx).toString)())
      val aggregate = Aggregate(aliases, aliases, sab.buildPlan)
      val sparkPlan = shimPrepareExecutedPlan(adaptivePlan, aggregate)
      assert(
        sparkPlan.isInstanceOf[AdaptiveSparkPlanExec],
        "Expected AdaptiveSparkPlanExec from prepareExecutedPlan, " +
          s"got ${sparkPlan.getClass.getSimpleName}")
      val newAdaptivePlan = sparkPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val values = SubqueryExec(sab.name, newAdaptivePlan)
      DynamicPruningExpression(InSubqueryExec(inSub.child, values, inSub.exprId))
    }
  }

  /**
   * Registers a DPP subquery in the shared AdaptiveExecutionContext.subqueryCache for cross-plan
   * deduplication, matching ReuseAdaptiveSubquery's behavior.
   *
   * Our rule runs after Spark's ReuseAdaptiveSubquery (which can't see our subqueries because
   * they don't exist yet when it runs). CometReuseSubquery uses a per-invocation local cache that
   * doesn't span across the main query and scalar subquery plans. Using the shared context cache
   * ensures that identical DPP subqueries across plans are deduplicated.
   */
  private def reuseOrRegisterSubquery(
      subquery: BaseSubqueryExec,
      adaptivePlan: AdaptiveSparkPlanExec): BaseSubqueryExec = {
    if (!conf.subqueryReuseEnabled) return subquery
    val subqueryCache = adaptivePlan.context.subqueryCache
    val cached = subqueryCache.getOrElseUpdate(subquery.canonicalized, subquery)
    if (cached.ne(subquery)) {
      logDebug(s"Reusing cached subquery for '${subquery.name}'")
      ReusedSubqueryExec(cached)
    } else {
      subquery
    }
  }

  /**
   * Finds a broadcast hash join whose build-side keys match the given exprIds. Searches for both
   * CometBroadcastHashJoinExec and BroadcastHashJoinExec to handle cases where the join fell back
   * to Spark.
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

  private def convertNonCometNodeDPP(node: SparkPlan, stagePlan: SparkPlan): SparkPlan = {
    node.transformExpressions {
      case expr if hasCometSAB(expr) =>
        convertFilter(expr, stagePlan)
    }
  }

  /**
   * Checks if a SparkPlan's expressions contain a wrapped CometSubqueryAdaptiveBroadcastExec.
   * Unlike hasCometSAB, this only checks for the wrapped variant. Unwrapped SABs on non-Comet
   * nodes are handled by Spark's own PlanAdaptiveDynamicPruningFilters.
   */
  private def hasWrappedSAB(p: SparkPlan): Boolean =
    p.expressions.exists(_.exists {
      case DynamicPruningExpression(
            InSubqueryExec(_, _: CometSubqueryAdaptiveBroadcastExec, _, _, _, _)) =>
        true
      case _ => false
    })

  /**
   * Checks if an expression contains an SAB variant (wrapped or unwrapped). The outer
   * CometNativeScanExec.partitionFilters has CometSubqueryAdaptiveBroadcastExec (wrapped by
   * CometExecRule). The inner CometScanExec.partitionFilters may have the original
   * SubqueryAdaptiveBroadcastExec (unwrapped, because CometScanExec is
   * @transient).
   */
  private def hasCometSAB(e: Expression): Boolean =
    e.exists {
      case DynamicPruningExpression(inSub: InSubqueryExec) =>
        extractSABData(inSub).isDefined
      case _ => false
    }
}
