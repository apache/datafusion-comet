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
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.planner.tags.CometTags

/**
 * Pre-pass that arranges for Spark's own `PlanAdaptiveDynamicPruningFilters` to succeed on Spark
 * < 3.5 by tagging the right nodes with `SKIP_COMET`. Phase 2 reads the tag and decides
 * `Fallback`, so the tagged nodes stay Spark-native.
 *
 * Absorbs `org.apache.comet.rules.CometSpark34AqeDppFallbackRule` into the planner. The four
 * cases handled there are the four cases handled here; see that file's class docstring for the
 * full rationale (`injectQueryStageOptimizerRule` missing on 3.4, dual-filter problem, AQE
 * preprocessingRules re-optimize, etc.). Differences:
 *
 *   - Writes `CometTags.SKIP_COMET` (one cross-type tag) instead of three node-type-specific
 *     `SKIP_COMET_SCAN_TAG` / `SKIP_COMET_BROADCAST_TAG` / `SKIP_COMET_SHUFFLE_TAG`.
 *   - Runs as a planner pre-pass rather than a `queryStagePrepRule`, so ordering with respect to
 *     Phase 1/2/3 is explicit rather than implicit-via-injection-order.
 *
 * Same node selection logic as the legacy rule. No-op on Spark 3.5+ (the planner / Spark's own
 * rule handle DPP correctly there) and when DPP is disabled.
 *
 * Case-3 (V1 SMJ self-join peer-tagging via `sameRelation`) only applies to `FileSourceScanExec`.
 * V2 Iceberg `BatchScanExec` does not produce this shape because its `runtimeFilters` map
 * differently, and there is no Iceberg analog of `HadoopFsRelation` equality, so the legacy
 * rule's case 3 short-circuits there and so does this one.
 */
object Spark34DppFallbackPrePass extends AdaptiveSparkPlanHelper with Logging {

  private val Reason: String = "AQE DPP on Spark 3.4 requires keeping this node Spark-native"

  def apply(plan: SparkPlan, conf: SQLConf): SparkPlan = {
    if (isSpark35Plus) return plan
    if (!conf.dynamicPartitionPruningEnabled) return plan

    val sabScans = findSabScans(plan)
    val sbScans = findSubqueryBroadcastScans(plan)
    if (sabScans.isEmpty && sbScans.isEmpty) return plan

    sabScans.foreach { case (scan, sab) =>
      tagForSab(plan, scan, sab)
    }
    sbScans.foreach { case (scan, sb) =>
      tagForSubqueryBroadcast(plan, scan, sb)
    }

    plan
  }

  private def setForceFallback(node: SparkPlan): Unit =
    node.setTagValue(CometTags.SKIP_COMET, Reason)

  private def findSabScans(plan: SparkPlan): Seq[(SparkPlan, SubqueryAdaptiveBroadcastExec)] = {
    val buf = scala.collection.mutable.ArrayBuffer[(SparkPlan, SubqueryAdaptiveBroadcastExec)]()
    foreach(plan) { node =>
      extractFirstSab(node).foreach(sab => buf += ((node, sab)))
    }
    buf.toSeq
  }

  private def extractFirstSab(node: SparkPlan): Option[SubqueryAdaptiveBroadcastExec] =
    node.expressions
      .flatMap(_.collect {
        case DynamicPruningExpression(inSub: InSubqueryExec)
            if inSub.plan.isInstanceOf[SubqueryAdaptiveBroadcastExec] =>
          inSub.plan.asInstanceOf[SubqueryAdaptiveBroadcastExec]
      })
      .headOption

  private def findSubqueryBroadcastScans(
      plan: SparkPlan): Seq[(SparkPlan, SubqueryBroadcastExec)] = {
    val buf = scala.collection.mutable.ArrayBuffer[(SparkPlan, SubqueryBroadcastExec)]()
    foreach(plan) { node =>
      extractFirstSubqueryBroadcast(node).foreach(sb => buf += ((node, sb)))
    }
    buf.toSeq
  }

  private def extractFirstSubqueryBroadcast(node: SparkPlan): Option[SubqueryBroadcastExec] =
    node.expressions
      .flatMap(_.collect {
        case DynamicPruningExpression(inSub: InSubqueryExec)
            if inSub.plan.isInstanceOf[SubqueryBroadcastExec] =>
          inSub.plan.asInstanceOf[SubqueryBroadcastExec]
      })
      .headOption

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
      case Some(buildSide) => tagBhjBuildBroadcast(buildSide, sab.name)
      case None => tagPeerScansAndShuffles(plan, scan, sab.name)
    }
  }

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
      case Some(buildSide) => tagBhjBuildBroadcast(buildSide, sb.name)
      case None =>
        logDebug(
          s"SubqueryBroadcast '${sb.name}': no matching BHJ on this plan snapshot; no tag placed")
    }
  }

  private def tagBhjBuildBroadcast(buildSide: SparkPlan, dppName: String): Unit = {
    val found = buildSide.find {
      case _: BroadcastExchangeExec => true
      case _ => false
    }
    found match {
      case Some(be: BroadcastExchangeExec) =>
        setForceFallback(be)
        logDebug(s"Tagged BroadcastExchangeExec for DPP '$dppName' (BHJ build side)")
      case _ =>
        logWarning(
          s"DPP '$dppName': matched BHJ but could not locate BroadcastExchangeExec on " +
            "build side; skipping")
    }
  }

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
    foreach(plan) {
      case peer: FileSourceScanExec if (peer ne sabScan) && sameRelation(peer.relation, sabRel) =>
        setForceFallback(peer)
        taggedScans += 1
      case _ =>
    }

    var taggedShuffles = 0
    foreach(plan) {
      case sh: ShuffleExchangeExec if shuffleSubtreeContainsMatchingScan(sh, sabRel, sabScan) =>
        setForceFallback(sh)
        taggedShuffles += 1
      case _ =>
    }

    logDebug(
      s"SAB '$sabName' (no BHJ match): tagged $taggedScans peer scan(s) + " +
        s"$taggedShuffles shuffle(s)")
  }

  private def sameRelation(a: HadoopFsRelation, b: HadoopFsRelation): Boolean =
    (a eq b) ||
      (a.location.rootPaths == b.location.rootPaths &&
        a.dataSchema == b.dataSchema &&
        a.partitionSchema == b.partitionSchema)

  private def shuffleSubtreeContainsMatchingScan(
      shuffle: ShuffleExchangeExec,
      sabRelation: HadoopFsRelation,
      sabScan: SparkPlan): Boolean =
    find(shuffle.child) {
      case scan: FileSourceScanExec =>
        (scan eq sabScan) || sameRelation(scan.relation, sabRelation)
      case _ => false
    }.isDefined

  private def findMatchingBroadcastJoin(
      plan: SparkPlan,
      sabKeyIds: Set[Any]): Option[SparkPlan] = {
    var result: Option[SparkPlan] = None
    find(plan) {
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
