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
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometNativeExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, V2CommandExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.planner.gates.S2CGate
import org.apache.comet.planner.tags.CometTags
import org.apache.comet.rules.CometExecRule
import org.apache.comet.serde.{CometOperatorSerde, Compatible, Incompatible, Unsupported}

/**
 * Phase 1: predict per node whether it will end up Comet in the final plan, expressed as a
 * `LIKELY_COMET` tag read by Phase 2's demand-aware decisions.
 *
 * Prediction inputs by node category:
 *   - Scan leaves: serde + config gates.
 *   - Shuffle / Broadcast / AQE stage wrappers / already-converted CometNativeExec: structural;
 *     optimistically `true` for exchanges since their real convertibility depends on the children
 *     they end up with, which Phase 3 re-checks at emit time.
 *   - Generic exec operators (Project, Filter, HashAggregate, joins, ...): the operator's own
 *     serde AND every child must be able to provide native input (child `LIKELY_COMET=true` or
 *     S2C-eligible leaf). Mirrors the legacy `CometExecRule` generic case's implicit
 *     `op.children.forall(_.isInstanceOf[CometNativeExec])` guard.
 *
 * Anti-circularity property: no prediction depends on the operator's own DECISION nor on any
 * parent's prediction. That's why this phase is a single post-order walk with no fixed-point
 * iteration. The earlier "in isolation" framing (predict from serde alone, ignore children) was
 * not enough to make Phase 2's demand-aware rules correct: a shuffle between two Spark aggregates
 * needs `LIKELY_COMET=false` on both aggregates to decide Passthrough, and that requires
 * propagating cant-go-native upward from the failing descendant.
 *
 * This phase only mutates tag state on nodes. It does not change the plan tree shape.
 */
object Phase1LikelyComet extends Logging {

  def apply(plan: SparkPlan, conf: SQLConf): SparkPlan = {
    var total = 0
    var likely = 0
    // Post-order walk: children's LIKELY_COMET tags are set before their parents are visited, so
    // `isLikelyComet` can read child tags when refining a parent's prediction (e.g. an exec
    // operator that needs columnar input from its children).
    def visit(node: SparkPlan): Unit = {
      node.children.foreach(visit)
      val verdict = isLikelyComet(node, conf)
      node.setTagValue(CometTags.LIKELY_COMET, verdict)
      total += 1
      if (verdict) likely += 1
    }
    visit(plan)
    logDebug(s"Phase1: total=$total likely=$likely")
    plan
  }

  /**
   * Returns whether `node` would be LIKELY_COMET under the current configuration. Generic exec
   * ops additionally require that each child either already has `LIKELY_COMET=true` or is an
   * S2C-eligible leaf, mirroring the legacy `CometExecRule` generic case which only converts when
   * `op.children.forall(_.isInstanceOf[CometNativeExec])`. External callers must ensure children
   * have been visited first (the planner's `apply` does this via post-order traversal).
   */
  def isLikelyComet(node: SparkPlan, conf: SQLConf): Boolean = node match {
    // Never-convertible control plan nodes.
    case _: AdaptiveSparkPlanExec | _: ExecutedCommandExec | _: V2CommandExec => false

    // CometPlanner supports native_datafusion for V1 Parquet and native Iceberg / CSV for V2.
    // After native_iceberg_compat removal there is a single V1 scan impl gated only by the
    // top-level COMET_NATIVE_SCAN_ENABLED. Phase 3 emission re-applies the full file-format /
    // schema / encryption gates.
    case _: FileSourceScanExec =>
      CometConf.COMET_NATIVE_SCAN_ENABLED.get(conf)
    case _: BatchScanExec =>
      CometConf.COMET_NATIVE_SCAN_ENABLED.get(conf)

    case s: ShuffleExchangeExec =>
      // A shuffle's LIKELY_COMET reflects whether its data source can provide native input.
      // Optimistic-true broke upward propagation: a Spark partial HashAgg below a shuffle could
      // not flag the shuffle as non-likely, which made the final HashAgg above read a stale-true
      // child, which made Phase 2 see `parentLikely=true` on the shuffle and convert it. The
      // legacy rule lived with this and added `revertRedundantColumnarShuffle` (PR #4010) as a
      // post-pass; the planner avoids the conversion in the first place by predicting
      // accurately. Phase 3 still re-checks at emit time. S2C-eligible leaves count because
      // Phase 2 wraps them in `CometSparkToColumnarExec`.
      s.children.exists(c => childCanProvideNativeInput(c, conf))

    case b: BroadcastExchangeExec =>
      // Same shape as shuffle: a broadcast over Spark-only data cannot itself go native, so
      // Phase 1 must report that honestly for parent BHJs' children-OK checks to be correct.
      // Phase 2's BroadcastConsumerIndex still gates conversion on a downstream Comet BHJ
      // wanting this broadcast; this only changes whether the broadcast is a candidate at all.
      b.children.exists(c => childCanProvideNativeInput(c, conf))

    // AQE stage re-entry: a prior CometPlanner pass converted an exchange, AQE materialized it
    // and wrapped it in a query stage. Phase 3 re-emits the stage itself as a Comet-compatible
    // node via `CometExchangeSink` so the parent's protobuf wiring sees it as native.
    case ShuffleQueryStageExec(_, _: CometShuffleExchangeExec, _) => true
    case ShuffleQueryStageExec(_, ReusedExchangeExec(_, _: CometShuffleExchangeExec), _) => true
    case BroadcastQueryStageExec(_, _: CometBroadcastExchangeExec, _) => true
    case BroadcastQueryStageExec(_, ReusedExchangeExec(_, _: CometBroadcastExchangeExec), _) =>
      true

    // An already-emitted CometNativeExec (from a prior AQE pass) stays convertible so parents
    // see it as a native-compatible child.
    case _: CometNativeExec => true

    // Generic exec operators dispatched through the serde map.
    case op =>
      CometExecRule.allExecs.get(op.getClass) match {
        case Some(rawSerde) =>
          val serde = rawSerde.asInstanceOf[CometOperatorSerde[SparkPlan]]
          val selfOk = predictFromSerde(op, serde, conf)
          // Use `serde.dataChildren` so operators with structural Spark wrappers
          // (e.g. DataWritingCommandExec wrapping WriteFilesExec) check the actual data
          // producers, not the wrapper. Mirrors the legacy CometExecRule generic case's
          // `op.children.forall(_.isInstanceOf[CometNativeExec])` guard, with the unwrap moved
          // into the serde.
          selfOk && serde.dataChildren(op).forall(c => childCanProvideNativeInput(c, conf))
        case None =>
          // Fall back: a leaf we don't recognize can't convert; a non-leaf we don't recognize
          // might still act as a passthrough in Phase 2 but is not itself LIKELY_COMET.
          op match {
            case _: LeafExecNode => false
            case _ => false
          }
      }
  }

  private def childCanProvideNativeInput(child: SparkPlan, conf: SQLConf): Boolean =
    child.getTagValue(CometTags.LIKELY_COMET).getOrElse(false) ||
      (child.isInstanceOf[LeafExecNode] && S2CGate.shouldApply(child, conf))

  private def predictFromSerde(
      op: SparkPlan,
      serde: CometOperatorSerde[SparkPlan],
      conf: SQLConf): Boolean = {
    val opName = op.getClass.getSimpleName
    if (!serde.enabledConfig.forall(_.get(conf))) {
      val key = serde.enabledConfig.map(_.key).getOrElse("<none>")
      logDebug(s"Phase1: serde disabled by config node=$opName config=$key")
      // Attach EXTENSION_INFO so the test harness's `getFallbackReasons` can surface this as
      // the visible explain reason. Mirrors legacy CometExecRule.isOperatorEnabled.
      withInfo(
        op,
        s"Native support for operator $opName is disabled. Set $key=true to enable it.")
      return false
    }
    serde.getSupportLevel(op) match {
      case u: Unsupported =>
        logDebug(s"Phase1: serde Unsupported node=$opName reason=$u")
        withInfo(op, u.notes.getOrElse(""))
        false
      case _: Compatible => true
      case i: Incompatible =>
        val allow = CometConf.isOperatorAllowIncompat(opName)
        logDebug(s"Phase1: serde Incompatible node=$opName allow=$allow reason=$i")
        if (allow) {
          true
        } else {
          val incompatConf = CometConf.getOperatorAllowIncompatConfigKey(opName)
          val optionalNotes = i.notes.map(str => s" ($str)").getOrElse("")
          withInfo(
            op,
            s"$opName is not fully compatible with Spark$optionalNotes. " +
              s"To enable it anyway, set $incompatConf=true. ${CometConf.COMPAT_GUIDE}.")
          false
        }
    }
  }
}
