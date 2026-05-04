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

package org.apache.comet.planner

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
import org.apache.comet.planner.tags.CometTags
import org.apache.comet.rules.CometExecRule
import org.apache.comet.serde.{CometOperatorSerde, Compatible, Incompatible, Unsupported}

/**
 * Phase 1: predict whether each node's serde would support it *in isolation*, ignoring child
 * gating. The resulting `LIKELY_COMET` tag is read by Phase 2 to make demand-aware decisions
 * (e.g. shuffle conversion pays off only if at least one side is likelyComet).
 *
 * "In isolation" means: config enabled, own structural / expression / type checks pass, but no
 * consideration of whether children have already been marked convertible. This breaks the
 * circularity that otherwise requires a retry pass (as in the old CometExecRule broadcast
 * handling).
 *
 * The predicate currently wraps `CometOperatorSerde.getSupportLevel` and the `enabledConfig`
 * gate, with `Incompatible` treated per `COMET_EXPR_ALLOW_INCOMPATIBLE_OPERATORS` heuristics.
 *
 * This phase only mutates tag state on nodes. It does not change the plan tree shape.
 */
object Phase1LikelyComet extends Logging {

  def apply(plan: SparkPlan, conf: SQLConf): SparkPlan = {
    var total = 0
    var likely = 0
    plan.foreach { node =>
      val verdict = isLikelyComet(node, conf)
      node.setTagValue(CometTags.LIKELY_COMET, verdict)
      total += 1
      if (verdict) likely += 1
    }
    logDebug(s"Phase1: total=$total likely=$likely")
    plan
  }

  /**
   * Returns whether `node` would be LIKELY_COMET under the current configuration. Exposed so
   * other planner components (e.g. `BroadcastConsumerIndex`) can reason about a hypothetical
   * node's eligibility without walking the whole plan.
   */
  def isLikelyComet(node: SparkPlan, conf: SQLConf): Boolean = node match {
    // Never-convertible control plan nodes.
    case _: AdaptiveSparkPlanExec | _: ExecutedCommandExec | _: V2CommandExec => false

    // CometPlanner supports native_datafusion for V1 Parquet and native Iceberg / CSV for V2.
    // SCAN_AUTO is treated as native_datafusion. SCAN_NATIVE_ICEBERG_COMPAT is not predicted
    // LIKELY_COMET here, so those scans fall back to plain Spark; for that mode, run with
    // COMET_USE_PLANNER=false to use the legacy rule path that still emits CometScanExec.
    // Phase 3 emission re-applies the full file-format / schema / encryption gates.
    case _: FileSourceScanExec =>
      val impl = CometConf.COMET_NATIVE_SCAN_IMPL.get(conf)
      CometConf.COMET_NATIVE_SCAN_ENABLED.get(conf) &&
      (impl == CometConf.SCAN_NATIVE_DATAFUSION || impl == CometConf.SCAN_AUTO)
    case _: BatchScanExec =>
      CometConf.COMET_NATIVE_SCAN_ENABLED.get(conf)

    case _: ShuffleExchangeExec =>
      // Optimistic: Phase 1 can't evaluate the shuffle serde's getSupportLevel accurately
      // because `shuffleSupported` checks `isCometPlan(child)` and children haven't been
      // converted yet. Emit-time guard in Phase 3 re-checks with the actual converted child.
      true

    case _: BroadcastExchangeExec =>
      // Same as shuffle. Broadcast's convertibility also depends on child type / state after
      // conversion. Phase 3 re-checks at emit time.
      true

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
        case Some(serde) =>
          predictFromSerde(op, serde.asInstanceOf[CometOperatorSerde[SparkPlan]], conf)
        case None =>
          // Fall back: a leaf we don't recognize can't convert; a non-leaf we don't recognize
          // might still act as a passthrough in Phase 2 but is not itself LIKELY_COMET.
          op match {
            case _: LeafExecNode => false
            case _ => false
          }
      }
  }

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
