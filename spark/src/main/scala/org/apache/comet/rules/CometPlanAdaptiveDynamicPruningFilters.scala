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
import org.apache.spark.sql.catalyst.expressions.{Alias, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometBroadcastHashJoinExec, CometNativeScanExec, CometSubqueryAdaptiveBroadcastExec, CometSubqueryBroadcastExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, BroadcastQueryStageExec}
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
        convertNativeScanDPP(nativeScan)
    }
  }

  private def convertNativeScanDPP(nativeScan: CometNativeScanExec): CometNativeScanExec = {
    val newOuterFilters = nativeScan.partitionFilters.map(f => convertFilter(f))

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
    val newInnerFilters = nativeScan.scan.partitionFilters.map(f => convertFilter(f))
    val newInnerScan = nativeScan.scan.copy(partitionFilters = newInnerFilters)

    nativeScan.copy(partitionFilters = newOuterFilters, scan = newInnerScan)
  }

  private def convertFilter(filter: Expression): Expression = {
    filter.transformUp { case dpe @ DynamicPruningExpression(inSub: InSubqueryExec) =>
      extractSABData(inSub) match {
        case Some(sabData) =>
          convertSAB(inSub, sabData)
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
   * Returns SAB metadata if the plan is an SAB variant, None otherwise.
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
   * (correct results, scans all partitions). Spark does the same: the optimizer decided DPP only
   * makes sense if broadcast reuse is possible, and it isn't.
   *
   * 3. No reusable broadcast + onlyInBroadcast=false: Aggregate SubqueryExec on the build side
   * (DPP via separate execution, matching Spark's PlanAdaptiveDynamicPruningFilters lines 73-83).
   */
  private def convertSAB(inSub: InSubqueryExec, sab: SABData): DynamicPruningExpression = {
    val adaptivePlan = sab.adaptivePlan.asInstanceOf[AdaptiveSparkPlanExec]

    val sabKeyIds: Set[Any] = sab.buildKeys.flatMap(_.references.map(_.exprId)).toSet
    assert(
      sabKeyIds.nonEmpty,
      s"DPP subquery '${sab.name}' has empty buildKeys - " +
        "PlanAdaptiveSubqueries should always populate buildKeys")

    // queryStageOptimizerRules only see the current stage's child plan, but the
    // broadcast join may be in a parent stage (e.g., when a shuffle separates the
    // scan from the join). Matches PlanAdaptiveDynamicPruningFilters(rootPlan)
    // which receives rootPlan as a constructor arg. We get the equivalent via the
    // shared AdaptiveExecutionContext's QueryExecution.executedPlan.
    val rootPlan = adaptivePlan.context.qe.executedPlan

    // scalastyle:off println
    println(s"[CometDPP] convertSAB: name=${sab.name}, onlyInBroadcast=${sab.onlyInBroadcast}")
    println(s"[CometDPP]   sabKeyIds=$sabKeyIds")
    println(s"[CometDPP]   rootPlan class: ${rootPlan.getClass.getSimpleName}")
    // scalastyle:on println

    val matchingJoin = findMatchingBroadcastJoin(sabKeyIds, rootPlan)
    val canReuse = conf.exchangeReuseEnabled && matchingJoin.isDefined

    // scalastyle:off println
    println(
      s"[CometDPP]   matchingJoin=${matchingJoin.map(r => (r._1.getClass.getSimpleName, r._2))}")
    println(s"[CometDPP]   canReuse=$canReuse -> ${if (canReuse) "case 1"
      else if (sab.onlyInBroadcast) "case 2"
      else "case 3"}")
    // scalastyle:on println

    if (canReuse) {
      // Case 1: broadcast reuse.
      val (broadcastChild, isComet) = matchingJoin.get
      logDebug(
        s"Matched DPP subquery '${sab.name}' to " +
          s"${if (isComet) "Comet" else "Spark"} broadcast: " +
          s"${broadcastChild.getClass.getSimpleName}")

      broadcastChild match {
        case stage: BroadcastQueryStageExec =>
          // Broadcast already materialized as a stage (scan and join share a stage,
          // or the broadcast stage was created before this stage). Wire directly.
          val subquery = if (isComet) {
            CometSubqueryBroadcastExec(sab.name, sab.indices, sab.buildKeys, stage)
          } else {
            createSubqueryBroadcastExec(sab.name, sab.indices, sab.buildKeys, stage)
          }
          DynamicPruningExpression(inSub.withNewPlan(subquery))

        case _ =>
          // Broadcast not yet materialized (scan is in a shuffle stage processed
          // before the broadcast stage). Matches Spark's
          // PlanAdaptiveDynamicPruningFilters lines 44-64: construct a NEW exchange
          // wrapping adaptivePlan.executedPlan, then wrap in a new ASPE. AQE's
          // stageCache canonicalization ensures the broadcast runs once (same
          // canonical form as the join's exchange).
          val buildSidePlan = adaptivePlan.executedPlan
          // scalastyle:off println
          println(s"[CometDPP]   broadcast not yet materialized, constructing new exchange")
          println(s"[CometDPP]   buildSidePlan: ${buildSidePlan.getClass.getSimpleName}")
          println(s"[CometDPP]   buildSidePlan.logicalLink: ${buildSidePlan.logicalLink.map(_.getClass.getSimpleName)}")
          // scalastyle:on println
          val newExchange = if (isComet) {
            val cbe = broadcastChild.asInstanceOf[CometBroadcastExchangeExec]
            val newCbe = CometBroadcastExchangeExec(
              cbe.originalPlan, cbe.output, cbe.mode, buildSidePlan)
            buildSidePlan.logicalLink.foreach(newCbe.setLogicalLink)
            newCbe
          } else {
            import org.apache.spark.sql.catalyst.expressions.BindReferences
            import org.apache.spark.sql.execution.joins.{HashedRelationBroadcastMode, HashJoin}
            val packedKeys = BindReferences.bindReferences(
              HashJoin.rewriteKeyExpr(sab.buildKeys), buildSidePlan.output)
            val mode = HashedRelationBroadcastMode(packedKeys)
            val newBe = BroadcastExchangeExec(mode, buildSidePlan)
            buildSidePlan.logicalLink.foreach(newBe.setLogicalLink)
            newBe
          }
          // scalastyle:off println
          println(s"[CometDPP]   newExchange class: ${newExchange.getClass.getSimpleName}")
          println(s"[CometDPP]   newExchange.logicalLink: ${newExchange.logicalLink.map(_.getClass.getSimpleName)}")
          // scalastyle:on println
          // supportsColumnar must match the exchange: ASPE.getFinalPhysicalPlan
          // (line 370-373) applies postStageCreationRules(supportsColumnar) to the
          // final plan. With supportsColumnar=false (the SAB ASPE's default),
          // ApplyColumnarRulesAndInsertTransitions wraps the BroadcastQueryStageExec
          // in ColumnarToRowExec, which fails the assertion at
          // ASPE.doExecuteBroadcast (line 413) that expects BroadcastQueryStageExec.
          val newAdaptivePlan = adaptivePlan.copy(
            inputPlan = newExchange,
            supportsColumnar = newExchange.supportsColumnar)
          // ASPE constructor applies queryStagePreparationRules to inputPlan,
          // which may clear the logicalLink tag as a side effect. Re-set it.
          buildSidePlan.logicalLink.foreach(newAdaptivePlan.inputPlan.setLogicalLink)
          // scalastyle:off println
          println(s"[CometDPP]   newASPE.inputPlan.logicalLink (after re-set): ${newAdaptivePlan.inputPlan.logicalLink.map(_.getClass.getSimpleName)}")
          println(s"[CometDPP]   newASPE.initialPlan class: ${newAdaptivePlan.initialPlan.getClass.getSimpleName}")
          println(s"[CometDPP]   newASPE.initialPlan: ${newAdaptivePlan.initialPlan.treeString}")
          println(s"[CometDPP]   newASPE.executedPlan class: ${newAdaptivePlan.executedPlan.getClass.getSimpleName}")
          println(s"[CometDPP]   newASPE.isSubquery: ${newAdaptivePlan.isSubquery}")
          println(s"[CometDPP]   newASPE.isFinalPlan: ${newAdaptivePlan.isFinalPlan}")
          println(s"[CometDPP]   newASPE.context eq adaptivePlan.context: ${newAdaptivePlan.context eq adaptivePlan.context}")
          val sc = adaptivePlan.context.stageCache
          println(s"[CometDPP]   stageCache size at construction: ${sc.size}")
          sc.foreach { case (key, stage) =>
            println(s"[CometDPP]     cached: ${stage.getClass.getSimpleName}(id=${stage.id}) plan=${stage.plan.getClass.getSimpleName}")
          }
          println(s"[CometDPP]   preprocessingRules: ${newAdaptivePlan.preprocessingRules.map(_.getClass.getSimpleName)}")
          // scalastyle:on println
          val subquery = if (isComet) {
            CometSubqueryBroadcastExec(sab.name, sab.indices, sab.buildKeys, newAdaptivePlan)
          } else {
            createSubqueryBroadcastExec(sab.name, sab.indices, sab.buildKeys, newAdaptivePlan)
          }
          DynamicPruningExpression(inSub.withNewPlan(subquery))
      }
    } else if (sab.onlyInBroadcast) {
      // Case 2: no reusable broadcast, and the optimizer says DPP only makes sense with
      // broadcast reuse. Disable DPP. Spark does the same (Literal.TrueLiteral).
      logInfo(
        s"No reusable broadcast for DPP subquery '${sab.name}' " +
          "(onlyInBroadcast=true), disabling DPP")
      DynamicPruningExpression(Literal.TrueLiteral)
    } else {
      // Case 3: no reusable broadcast, but the optimizer says DPP is worthwhile even
      // without broadcast reuse. Create an aggregate SubqueryExec on the build side to
      // get distinct partition key values for pruning.
      //
      // Matches Spark's PlanAdaptiveDynamicPruningFilters lines 73-83:
      //   val aliases = indices.map(idx => Alias(buildKeys(idx), ...))
      //   val aggregate = Aggregate(aliases, aliases, buildPlan)
      //   val sparkPlan = QueryExecution.prepareExecutedPlan(session, aggregate, context)
      //   val values = SubqueryExec(name, newAdaptivePlan)
      val adaptivePlan = sab.adaptivePlan.asInstanceOf[AdaptiveSparkPlanExec]
      val aliases =
        sab.indices.map(idx => Alias(sab.buildKeys(idx), sab.buildKeys(idx).toString)())
      val aggregate = Aggregate(aliases, aliases, sab.buildPlan)
      val session = adaptivePlan.context.session
      val sparkPlan = QueryExecution.prepareExecutedPlan(session, aggregate, adaptivePlan.context)
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
        // scalastyle:off println
        if (result.isDefined)
          println(s"[CometDPP]   matched CometBHJ, child=${result.get._1.getClass.getSimpleName}")
        // scalastyle:on println
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
        // scalastyle:off println
        if (result.isDefined)
          println(s"[CometDPP]   matched SparkBHJ, child=${result.get._1.getClass.getSimpleName}")
        // scalastyle:on println
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
