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
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{isCometLoaded, isCometShuffleEnabled}

/**
 * Handles the niche configuration where `spark.comet.exec.enabled=false` but
 * `spark.comet.exec.shuffle.enabled=true`: the user wants Comet's columnar/native shuffle without
 * any other Comet operator conversion.
 *
 * Under the legacy `CometScanRule` + `CometExecRule` pair, this case was covered inside
 * `CometExecRule.apply` with a dedicated `applyCometShuffle` branch that ran when exec was
 * disabled. `CometPlanner` intentionally short-circuits to `convertBlocks` when exec is disabled
 * because its demand-aware Phase 1/2/3 pipeline assumes exec is on (Phase 2's shuffle rule is
 * "convert iff parent or child LIKELY_COMET", which never fires with nothing else convertible).
 * Rather than bake a shuffle-only escape hatch into `CometPlanner`, keep that mode in its own
 * tiny rule so the planner stays focused on the full compile path.
 *
 * Pipeline contract:
 *   - With `COMET_USE_PLANNER=true` (the new path), both `CometPlanner` and this rule run as
 *     `queryStagePrepRule`s and as `preColumnarTransitions`. Their exec-flag gates are disjoint:
 *     `CometPlanner` short-circuits when exec is disabled, this rule short-circuits when exec is
 *     enabled. At most one does real work per invocation.
 *   - With `COMET_USE_PLANNER=false` (the legacy path), this rule is a no-op because
 *     `CometExecRule` already embeds the same logic.
 *
 * How to remove this rule in the future:
 *   1. Drop the `exec=off + shuffle=on` mode from the docs and config. 2. Delete this file. 3. In
 *      `CometSparkSessionExtensions`: remove the `injectQueryStagePrepRule` entry for
 *      `CometShuffleRule`, and replace `CometExecColumnar.preColumnarTransitions`' planner branch
 *      with an identity rule (`new Rule[SparkPlan] { def apply(p) = p }`). 4. Delete
 *      `CometExecRule.SKIP_COMET_SHUFFLE_TAG` if its only remaining reader is
 *      `CometSpark34AqeDppFallbackRule` and Spark 3.4 support has been dropped.
 */
case class CometShuffleRule(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!isCometLoaded(conf)) return plan
    // Legacy rule path covers this mode via CometExecRule; skip to avoid double-wrapping.
    if (!CometConf.COMET_USE_PLANNER.get(conf)) return plan
    // CometPlanner already handled the exec-enabled case, including shuffle conversion in Phase 3.
    if (CometConf.COMET_EXEC_ENABLED.get(conf)) return plan
    if (!isCometShuffleEnabled(conf)) return plan
    applyCometShuffle(plan)
  }

  /**
   * Single-pass wrap of Spark shuffles as `CometShuffleExchangeExec`. No child wiring, no
   * protobuf tree, no logical-link reconciliation: the wrapped shuffle sits under a Spark parent,
   * and Spark's `ApplyColumnarRulesAndInsertTransitions` inserts the columnar-to-row boundaries
   * around it. Verbatim port of `CometExecRule.applyCometShuffle`.
   */
  private def applyCometShuffle(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case s: ShuffleExchangeExec if shouldSkipCometShuffle(s) =>
        s
      case s: ShuffleExchangeExec =>
        CometShuffleExchangeExec.shuffleSupported(s) match {
          case Some(CometNativeShuffle) =>
            // Arrow's native kernels don't support Decimal32 / Decimal64 yet; force Decimal128
            // so the shuffle's input batches match what native execution expects.
            conf.setConfString(CometConf.COMET_USE_DECIMAL_128.key, "true")
            CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)
          case Some(CometColumnarShuffle) =>
            CometShuffleExchangeExec(s, shuffleType = CometColumnarShuffle)
          case None =>
            s
        }
    }
  }

  /**
   * Written by `CometSpark34AqeDppFallbackRule` on Spark < 3.5 and (historically) by the
   * revert-redundant-shuffle post-pass. Honor it so shuffles that should stay Spark-native stay
   * Spark-native.
   */
  private def shouldSkipCometShuffle(s: ShuffleExchangeExec): Boolean =
    s.getTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG).isDefined
}
