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
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus

/**
 * Preserves AQE Dynamic Partition Pruning on Spark < 3.5 by tagging specific nodes to stay
 * Spark-native, so that Spark's PlanAdaptiveDynamicPruningFilters can match them natively.
 *
 * On Spark 3.5+, CometPlanAdaptiveDynamicPruningFilters (queryStageOptimizerRule) rewrites AQE
 * DPP SABs into CometSubqueryBroadcastExec after Spark's own rule has run. Spark 3.4 does not
 * expose injectQueryStageOptimizerRule (SPARK-45785 added it in 3.5), so the rewrite cannot run
 * at the correct time. Rewriting the SAB at queryStagePrepRule time does not work either: AQE
 * rebuilds plan nodes between prep and execution in ways that drop the `@transient` inner scan we
 * would need to update. See the dual-filter handling in
 * CometPlanAdaptiveDynamicPruningFilters.convertNativeScanDPP for why both filters need updating.
 *
 * Instead, on 3.4 we arrange for Spark's PlanAdaptiveDynamicPruningFilters to succeed on its own
 * by keeping the BHJ's build-side exchange (or the peer branch of a self-join SMJ) Spark-native.
 * This rule only writes skip-tags on nodes; it never rewrites expressions or plan structure. Tags
 * survive AQE per-stage re-entry, matching the contract of SKIP_COMET_SHUFFLE_TAG from PR #4010.
 *
 * Registered via injectPreSpark35QueryStagePrepRuleShim before CometScanRule/CometExecRule in
 * CometSparkSessionExtensions, so tags are in place when conversion runs. No-op on Spark 3.5+.
 *
 * Three cases handled:
 *
 *   1. SAB + matching BHJ (non-V1 fact scans: Hive / V2 / V2Filter). The cascade up from a non-V1
 *      scan reaches a CometBroadcastHashJoinExec + CometBroadcastExchangeExec build side; Spark's
 *      class-sensitive sameResult check in PlanAdaptiveDynamicPruningFilters.scala:50-57 fails to
 *      match. Tag the BHJ's build-side BroadcastExchangeExec with SKIP_COMET_BROADCAST_TAG. With
 *      the build exchange Spark-native, Comet's BHJ conversion fails its
 *      forall(_.isInstanceOf[CometNativeExec]) guard and the BHJ stays Spark. Spark's rule then
 *      matches and creates SubqueryBroadcastExec.
 *
 * 2. SAB + matching BHJ on V1. CometScanRule.transformV1Scan already rejects the V1 fact scan via
 * isAqeDynamicPruningFilter; the cascade keeps the BHJ and its BroadcastExchangeExec
 * Spark-native. No tagging needed; this rule is a no-op for V1 BHJ. TPC-DS Q7 on 3.4 stays the
 * same shape it has today, including the Comet acceleration on dim scans below the Spark
 * broadcast.
 *
 * 3. SAB with no matching BHJ (V1 SMJ self-join, SPARK-32509 with
 * AUTO_BROADCASTJOIN_THRESHOLD=-1). The logical Partition-Pruning rule attaches the SAB to only
 * one branch of the self-join. transformV1Scan falls back the SAB-bearing scan; the peer scan (no
 * SAB) Cometizes, producing canonical asymmetry that breaks shuffle exchange reuse. Tag the peer
 * scan with SKIP_COMET_SCAN_TAG and any shuffle whose subtree contains the peer scan with
 * SKIP_COMET_SHUFFLE_TAG. Both branches end up Spark-native with matching canonical forms;
 * Spark's rule replaces the SAB with TrueLiteral, and FileSourceScanExec.doCanonicalize strips it
 * via filterUnusedDynamicPruningExpressions (DataSourceScanExec.scala:731,736), restoring
 * canonical symmetry for reuse.
 *
 * Non-AQE DPP (#4011) is untouched: it produces SubqueryBroadcastExec, not the adaptive variant,
 * and is handled by CometExecRule.convertSubqueryBroadcasts.
 *
 * Known limitations on 3.4:
 *
 *   - Cross-plan scalar-subquery DPP: an SAB in a scalar subquery cannot see a matching BHJ in
 *     the main query. At prep-rule time each AdaptiveSparkPlanExec sees only its own plan. When
 *     the match fails, Spark's own rule falls back to TrueLiteral or aggregate SubqueryExec (same
 *     behavior as Spark-without-Comet on 3.4).
 *   - AQE re-optimization that rebuilds the plan: tags are per-node, so a rebuild drops them, but
 *     this rule re-runs on each prep-rule pass and re-tags from scratch.
 *
 * @see
 *   PlanAdaptiveDynamicPruningFilters (Spark's rule this code arranges to succeed)
 * @see
 *   CometPlanAdaptiveDynamicPruningFilters (Spark 3.5+ equivalent via queryStageOptimizerRule)
 */
case object CometSpark34AqeDppFallbackRule
    extends Rule[SparkPlan]
    with AdaptiveSparkPlanHelper
    with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    // Registered only on Spark < 3.5 via injectPreSpark35QueryStagePrepRuleShim. If the
    // 3.5+ shim mis-registers this rule, fail loud rather than silently disable Comet.
    assert(
      !isSpark35Plus,
      "CometSpark34AqeDppFallbackRule must only be registered on Spark < 3.5; " +
        "see ShimCometSparkSessionExtensions.injectPreSpark35QueryStagePrepRuleShim")

    if (!conf.dynamicPartitionPruningEnabled) return plan

    val sabScans = findSabScans(plan)
    val sbScans = findSubqueryBroadcastScans(plan)
    if (sabScans.isEmpty && sbScans.isEmpty) return plan

    sabScans.foreach { case (scan, sab) =>
      tagForSab(plan, scan, sab)
    }

    // AQE re-optimization path: on subsequent re-optimize cycles, ASPE.preprocessingRules
    // fills the subqueryMap-backed DPP slot with the already-materialized SubqueryBroadcastExec
    // (produced by Spark's PlanAdaptiveDynamicPruningFilters on a previous pass) instead of the
    // original SubqueryAdaptiveBroadcastExec. The freshly-planned BroadcastExchangeExec on the
    // main BHJ's build side is a new instance with no SKIP_COMET_BROADCAST_TAG carried over. If
    // we only tagged via SABs, the next CometExecRule pass would Cometize this BE and the main
    // join's build would no longer canonically match the DPP subquery's Spark BroadcastExchange,
    // losing AQE stageCache broadcast reuse (Spark's DynamicPartitionPruningSuiteV2 SPARK-34637
    // asserts on this reuse). SubqueryBroadcastExec carries the same buildKeys as the original
    // SAB, so we can find the matching BHJ by exprId the same way and tag its build BE again.
    sbScans.foreach { case (scan, sb) =>
      tagForSubqueryBroadcast(plan, scan, sb)
    }

    // This rule only tags; it never rewrites the plan structurally.
    plan
  }

  /**
   * Find every scan whose `partitionFilters` contain a `SubqueryAdaptiveBroadcastExec`.
   *
   * Mirrors the SAB pattern matched by Spark's `PlanAdaptiveDynamicPruningFilters.apply` (lines
   * 41-43):
   * {{{
   *   case DynamicPruningExpression(InSubqueryExec(
   *       value, SubqueryAdaptiveBroadcastExec(...), ...))
   * }}}
   *
   * Returns the scan node itself and the SAB found on it.
   */
  private def findSabScans(plan: SparkPlan): Seq[(SparkPlan, SubqueryAdaptiveBroadcastExec)] = {
    val buf = scala.collection.mutable.ArrayBuffer[(SparkPlan, SubqueryAdaptiveBroadcastExec)]()
    plan.foreach { node =>
      extractFirstSab(node).foreach(sab => buf += ((node, sab)))
    }
    buf.toSeq
  }

  private def extractFirstSab(node: SparkPlan): Option[SubqueryAdaptiveBroadcastExec] = {
    node.expressions
      .flatMap(_.collect {
        case DynamicPruningExpression(inSub: InSubqueryExec)
            if inSub.plan.isInstanceOf[SubqueryAdaptiveBroadcastExec] =>
          inSub.plan.asInstanceOf[SubqueryAdaptiveBroadcastExec]
      })
      .headOption
  }

  /**
   * Find every scan whose DPP `partitionFilters` contain a `SubqueryBroadcastExec` - i.e. a DPP
   * subquery that Spark's `PlanAdaptiveDynamicPruningFilters` has already materialized on a
   * previous AQE pass. See the comment block in `apply` for why we need this in addition to the
   * SAB path.
   *
   * Uses `AdaptiveSparkPlanHelper.foreach`, which descends through `QueryStageExec.plan` and
   * `AdaptiveSparkPlanExec.executedPlan`. By the re-optimize pass where this matters, the fact
   * scan is inside a `ShuffleQueryStageExec` whose `children` is `Seq.empty`, so a plain
   * `plan.foreach` would stop there and miss the scan's `SubqueryBroadcastExec`.
   */
  private def findSubqueryBroadcastScans(
      plan: SparkPlan): Seq[(SparkPlan, SubqueryBroadcastExec)] = {
    val buf = scala.collection.mutable.ArrayBuffer[(SparkPlan, SubqueryBroadcastExec)]()
    foreach(plan) { node =>
      extractFirstSubqueryBroadcast(node).foreach(sb => buf += ((node, sb)))
    }
    buf.toSeq
  }

  private def extractFirstSubqueryBroadcast(node: SparkPlan): Option[SubqueryBroadcastExec] = {
    node.expressions
      .flatMap(_.collect {
        case DynamicPruningExpression(inSub: InSubqueryExec)
            if inSub.plan.isInstanceOf[SubqueryBroadcastExec] =>
          inSub.plan.asInstanceOf[SubqueryBroadcastExec]
      })
      .headOption
  }

  /**
   * Place tags for a single SAB-bearing scan. Behavior depends on whether a matching
   * broadcast-hash join exists in the plan:
   *   - Matching BHJ found: tag its build-side `BroadcastExchangeExec` (case 1 above).
   *   - No matching BHJ: tag peer scans + their shuffles for canonical symmetry (case 3).
   */
  private def tagForSab(
      plan: SparkPlan,
      scan: SparkPlan,
      sab: SubqueryAdaptiveBroadcastExec): Unit = {
    val sabKeyIds: Set[Any] = sab.buildKeys.flatMap(_.references.map(_.exprId)).toSet
    if (sabKeyIds.isEmpty) {
      logWarning(s"SAB '${sab.name}' has empty buildKeys; skipping")
      return
    }

    findMatchingBroadcastJoin(plan, sabKeyIds) match {
      case Some(buildSide) =>
        tagBhjBuildBroadcast(buildSide, sab.name)
      case None =>
        tagPeerScansAndShuffles(plan, scan, sab.name)
    }
  }

  /**
   * Tag the matching BHJ's build-side `BroadcastExchangeExec` for a scan whose DPP filter holds a
   * `SubqueryBroadcastExec` (post-PADPF form). Only the matching-BHJ case applies here: if PADPF
   * already ran on a previous AQE cycle, it would have fallen back to aggregate `SubqueryExec` or
   * `Literal.TrueLiteral` instead of producing a `SubqueryBroadcastExec` when no BHJ matched - so
   * a `SubqueryBroadcastExec` always implies a BHJ with compatible build keys somewhere in the
   * plan. No peer-scan tagging path is needed because the SMJ self-join case (SPARK-32509) never
   * produces a `SubqueryBroadcastExec`.
   */
  private def tagForSubqueryBroadcast(
      plan: SparkPlan,
      scan: SparkPlan,
      sb: SubqueryBroadcastExec): Unit = {
    val keyIds: Set[Any] = sb.buildKeys.flatMap(_.references.map(_.exprId)).toSet
    if (keyIds.isEmpty) {
      logWarning(s"SubqueryBroadcast '${sb.name}' has empty buildKeys; skipping")
      return
    }

    findMatchingBroadcastJoin(plan, keyIds) match {
      case Some(buildSide) =>
        tagBhjBuildBroadcast(buildSide, sb.name + " (via SubqueryBroadcast)")
      case None =>
        // Nothing to tag. Either the BHJ has a different buildKey exprId set (e.g. AQE
        // rewrote attributes across stages) or the matching join isn't in this plan snapshot.
        logDebug(
          s"SubqueryBroadcast '${sb.name}': no matching BHJ on this plan snapshot; no tag placed")
    }
  }

  /**
   * Walk from a BHJ's build-side subtree root to the first `BroadcastExchangeExec` and tag it
   * with `SKIP_COMET_BROADCAST_TAG`. If the subtree root already IS a BroadcastExchangeExec (most
   * common), that's the one. Otherwise we walk until we find one or give up.
   *
   * Tagging the exchange is enough: when it stays Spark-native, `CometBroadcastHashJoinExec`'s
   * conversion guard (`forall(_.isInstanceOf[CometNativeExec])`) fails because a Spark
   * `BroadcastExchangeExec` is not a `CometNativeExec`, so the BHJ stays Spark too. Spark's
   * `PlanAdaptiveDynamicPruningFilters` can then match it via `sameResult`
   * (`PlanAdaptiveDynamicPruningFilters.scala:50-57`) and create a `SubqueryBroadcastExec`.
   */
  private def tagBhjBuildBroadcast(buildSide: SparkPlan, sabName: String): Unit = {
    val found = buildSide.find {
      case _: BroadcastExchangeExec => true
      case _ => false
    }
    found match {
      case Some(be: BroadcastExchangeExec) =>
        be.setTagValue(CometExecRule.SKIP_COMET_BROADCAST_TAG, ())
        logDebug(s"Tagged BroadcastExchangeExec for SAB '$sabName' (BHJ build side)")
      case _ =>
        logWarning(
          s"SAB '$sabName': matched BHJ but could not locate BroadcastExchangeExec on " +
            "build side; skipping")
    }
  }

  /**
   * For SMJ-shaped DPP (SPARK-32509 with AUTO_BROADCASTJOIN_THRESHOLD=-1), there is no BHJ. The
   * logical Partition-Pruning rule attaches the SAB to only one branch of a self-join; the peer
   * branch has no SAB, so `transformV1Scan`'s V1 AQE DPP fallback fires for the SAB-bearing scan
   * only. Peer scans Cometize normally, leading to canonical asymmetry that breaks shuffle
   * exchange reuse (0 `ReusedExchangeExec` instead of 1).
   *
   * Tag every peer scan (same relation as the SAB-bearing scan, different instance) with
   * `SKIP_COMET_SCAN_TAG`, and every `ShuffleExchangeExec` whose subtree contains a peer scan
   * with `SKIP_COMET_SHUFFLE_TAG`. Both branches end up Spark-native with matching canonical
   * forms.
   *
   * For canonical equality the SAB-bearing scan's side is already handled:
   * `FileSourceScanExec.doCanonicalize` strips `DynamicPruningExpression(Literal.TrueLiteral)`
   * via `filterUnusedDynamicPruningExpressions` (`DataSourceScanExec.scala:731,736`), and Spark's
   * rule replaces the SAB with `TrueLiteral` when it can't match.
   */
  private def tagPeerScansAndShuffles(
      plan: SparkPlan,
      sabScan: SparkPlan,
      sabName: String): Unit = {
    val sabRelation = sabScan match {
      case f: FileSourceScanExec => Some(f.relation)
      case _ => None
    }
    if (sabRelation.isEmpty) {
      logDebug(
        s"SAB '$sabName': non-V1 scan with no BHJ match; no peer-tagging heuristic available")
      return
    }
    val sabRel = sabRelation.get

    var taggedScans = 0
    plan.foreach {
      case peer: FileSourceScanExec if (peer ne sabScan) && sameRelation(peer.relation, sabRel) =>
        peer.setTagValue(CometScanRule.SKIP_COMET_SCAN_TAG, ())
        taggedScans += 1
      case _ =>
    }

    var taggedShuffles = 0
    plan.foreach {
      case sh: ShuffleExchangeExec if shuffleSubtreeContainsMatchingScan(sh, sabRel, sabScan) =>
        sh.setTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG, ())
        taggedShuffles += 1
      case _ =>
    }

    logDebug(
      s"SAB '$sabName' (no BHJ match): tagged $taggedScans peer scan(s) + " +
        s"$taggedShuffles shuffle(s)")
  }

  private def sameRelation(a: HadoopFsRelation, b: HadoopFsRelation): Boolean = {
    (a eq b) ||
    (a.location.rootPaths == b.location.rootPaths &&
      a.dataSchema == b.dataSchema &&
      a.partitionSchema == b.partitionSchema)
  }

  private def shuffleSubtreeContainsMatchingScan(
      shuffle: ShuffleExchangeExec,
      sabRelation: HadoopFsRelation,
      sabScan: SparkPlan): Boolean = {
    shuffle.child.exists {
      case scan: FileSourceScanExec =>
        (scan eq sabScan) || sameRelation(scan.relation, sabRelation)
      case _ => false
    }
  }

  /**
   * Mirrors `PlanAdaptiveDynamicPruningFilters.apply` lines 50-57:
   * {{{
   *   find(rootPlan) {
   *     case BroadcastHashJoinExec(_, _, _, BuildLeft,  _, left,  _, _) =>
   *        left.sameResult(exchange)
   *     case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
   *        right.sameResult(exchange)
   *   }
   * }}}
   *
   * Our rule runs BEFORE `CometScanRule`/`CometExecRule`, so the plan is entirely Spark-native at
   * this point. We only match `BroadcastHashJoinExec`. Instead of `sameResult` we match on
   * join-side exprId equality with the SAB's buildKeys; this is semantically equivalent because
   * SAB buildKeys originate from the same logical plan as the join's build-side keys.
   *
   * Returns the BHJ's build-side subtree (the one we want to keep Spark-native), or None if no
   * matching join is found.
   */
  private def findMatchingBroadcastJoin(
      plan: SparkPlan,
      sabKeyIds: Set[Any]): Option[SparkPlan] = {
    var result: Option[SparkPlan] = None
    plan.find {
      case j: BroadcastHashJoinExec =>
        val joinBuildKeys = j.buildSide match {
          case BuildLeft => j.leftKeys
          case BuildRight => j.rightKeys
        }
        val joinKeyIds: Set[Any] = joinBuildKeys.flatMap(_.references.map(_.exprId)).toSet
        if (sabKeyIds == joinKeyIds) {
          result = Some(j.buildSide match {
            case BuildLeft => j.left
            case BuildRight => j.right
          })
          true
        } else {
          false
        }
      case _ => false
    }
    result
  }
}
