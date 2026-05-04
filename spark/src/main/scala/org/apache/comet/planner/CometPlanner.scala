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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{InputFileBlockLength, InputFileBlockStart, InputFileName}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometExec, CometNativeExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isCometLoaded
import org.apache.comet.planner.phases.{NormalizePrePass, Phase1LikelyComet, Phase2Decision, Phase3Emit, SubqueryBroadcastRewrite}
import org.apache.comet.planner.tags.CometTags
import org.apache.comet.rules.RewriteJoin

/**
 * Single-pass compiler from Spark physical plans to Comet-accelerated plans. Replaces the
 * two-rule split of CometScanRule and CometExecRule with a structured pipeline:
 *
 *   1. Pre-pass: NaN/zero normalization on float comparisons, RewriteJoin (SMJ to SHJ/BHJ). 2.
 *      Phase 1 (LIKELY_COMET annotator): predict per node whether serde supports it in isolation.
 *      Also index BroadcastExchangeExec to likely Comet consumers so Phase 2 can judge broadcast
 *      demand without retrying. 3. Phase 2 (DECISION annotator): per-operator demand-aware rules
 *      decide convert / passthrough / fallback. Catches cases like "columnar shuffle with JVM
 *      aggregate on both sides" upstream, replacing the revertRedundantColumnarShuffle post-pass.
 *      4. Phase 3 (emitter): bottom-up transform that builds protobuf and constructs
 *      CometNativeExec subtrees. Sets logical link at construction. Tags emitted roots with
 *      COMET_CONVERTED for AQE short-circuit. 5. Post-pass: SubqueryBroadcast rewrite in a single
 *      expression walk.
 *
 * Idempotency: AQE re-runs the rule on each stage as it materializes. The top-level `apply`
 * checks the root node's `COMET_CONVERTED` tag and short-circuits to `convertBlocks` when set, so
 * we don't re-classify a tree we already emitted.
 *
 * Assertions guard invariants the rule relies on for correctness; if they trip, something further
 * up the pipeline is violating a contract.
 */
case class CometPlanner(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!isCometLoaded(conf)) {
      logDebug("CometPlanner skip: Comet extension not loaded")
      return plan
    }
    assert(
      CometConf.COMET_USE_PLANNER.get(conf),
      s"CometPlanner ran while ${CometConf.COMET_USE_PLANNER.key}=false. The legacy " +
        "CometScanRule + CometExecRule should be the sole rules on this path. Either " +
        "COMET_USE_PLANNER was flipped after session creation or CometPlanner was registered " +
        "by mistake.")

    // Comet exec globally disabled OR root already converted (AQE re-entry): skip phase 1/2/3
    // but still run convertBlocks. AQE re-planning can graft a previously-emitted CometNativeExec
    // subtree (via LogicalQueryStage) into a freshly Spark-planned outer plan; that subtree's
    // top node may have been an interior node (no SerializedPlan) under the prior root. Without
    // a fresh convertBlocks pass it would crash at execution with "should not be executed
    // directly without a serialized plan".
    val skipPhases = !CometConf.COMET_EXEC_ENABLED.get(conf) ||
      plan.getTagValue(CometTags.COMET_CONVERTED).isDefined
    if (skipPhases) {
      return convertBlocks(plan)
    }

    val prepared = prePass(plan)

    // Pre-compute plan-wide flags once so per-node phases do not re-walk the tree. Any
    // input_file_name / input_file_block_start / input_file_block_length reference anywhere in
    // the plan disqualifies all native DataFusion scans; see V1ScanGate.
    val hasInputFileExpressions = prepared.exists(node =>
      node.expressions.exists(_.exists {
        case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
        case _ => false
      }))

    val broadcastConsumers = BroadcastConsumerIndex.build(prepared, conf)

    val context = PlanningContext(
      session = session,
      conf = conf,
      broadcastConsumers = broadcastConsumers,
      hasInputFileExpressions = hasInputFileExpressions)

    val annotated1 = phase1LikelyComet(prepared, context)
    val annotated2 = phase2Decision(annotated1, context)
    val emitted = phase3Emit(annotated2, context)
    val reverted = revertOrphanedBroadcasts(emitted)
    val cleaned = cleanupLogicalLinks(reverted)
    val blocked = convertBlocks(cleaned)
    val finalPlan = postPass(blocked, context)

    val nativeCount = countNative(finalPlan)
    logDebug(s"CometPlanner: planId=${plan.id} nativeExecs=$nativeCount")
    checkPostEmitInvariants(finalPlan)
    finalPlan
  }

  /**
   * Walk the emitted plan top-down and serialize each native-subtree root. A Comet native
   * operator delegates its entire block (itself plus all CometNativeExec descendants) to a single
   * JNI call at execution time. The block root holds the serialized protobuf; children of a
   * serialized root don't carry their own serialized plans because they're embedded in the root's
   * protobuf. Without this step, block roots carry `SerializedPlan(None)` and trip the "should
   * not be executed directly" guard in `CometNativeExec.doExecuteColumnar`.
   *
   * Block boundary rules (copied from `CometExecRule`):
   *   - first CometNativeExec seen is a block root. `convertBlock()` serializes its subtree.
   *   - subsequent CometNativeExec descendants inside that subtree are non-roots.
   *   - hitting a leaf CometNativeExec (e.g. a scan) resets `firstNativeOp` so a sibling subtree
   *     starts a new block.
   *   - hitting a non-CometNativeExec node ALSO resets (e.g. JVM-orchestrated
   *     `CometShuffleExchangeExec` / `CometBroadcastExchangeExec` / `CometCollectLimitExec` after
   *     placeholder unwrap). Without this the native subtree below a shuffle / broadcast never
   *     gets its root serialized.
   *   - `CometNativeWriteExec` resets too: it serializes its own nativeOp on demand, so its
   *     CometNativeExec children start fresh blocks.
   */
  private def convertBlocks(plan: SparkPlan): SparkPlan = {
    var firstNativeOp = true
    val out = plan.transformDown {
      case op: CometNativeExec =>
        val rewritten = if (firstNativeOp) {
          firstNativeOp = false
          op.convertBlock()
        } else {
          op
        }
        if (op.children.isEmpty) {
          firstNativeOp = true
        }
        if (op.getClass.getSimpleName == "CometNativeWriteExec") {
          firstNativeOp = true
        }
        rewritten
      case op =>
        firstNativeOp = true
        op
    }
    out
  }

  /**
   * Recovery pass for the classic "Phase 1 predicts parent is convertible, Phase 2 converts the
   * broadcast child on that prediction, Phase 3 then fails to emit the parent (serde.convert
   * returns None or the BHJ rejects complex join keys)" scenario. The resulting plan is Spark BHJ
   * + CometBroadcastExchange, which crashes at runtime because Spark BHJ calls
   * `buildSide.executeBroadcast` and Comet broadcast exchange is not a `BroadcastExchangeLike`
   * that Spark can read directly.
   *
   * Walk the plan and for any non-native-compatible parent that holds a
   * `CometBroadcastExchangeExec` child, substitute the original Spark `BroadcastExchangeExec`
   * (preserving the already- converted Comet subtree under it). Spark's
   * `ApplyColumnarRulesAndInsertTransitions` inserts `CometColumnarToRow` at the columnar-to-row
   * boundary so execution works.
   *
   * Shuffle doesn't need the equivalent revert because a Spark parent with a Comet columnar
   * shuffle child is handled naturally by Spark's transition insertion.
   */
  private def revertOrphanedBroadcasts(plan: SparkPlan): SparkPlan = {
    if (CometConf.COMET_EXEC_BROADCAST_FORCE_ENABLED.get()) {
      return plan
    }
    var reverted = 0
    val out = plan.transformUp {
      case parent if !isNativeCompatible(parent) && hasCometBroadcastChild(parent) =>
        val newChildren = parent.children.map {
          case c: CometBroadcastExchangeExec =>
            reverted += 1
            BroadcastExchangeExec(c.mode, c.child)
          case other => other
        }
        parent.withNewChildren(newChildren)
      case op => op
    }
    if (reverted > 0) logDebug(s"CometPlanner: reverted $reverted orphaned broadcasts")
    out
  }

  private def isNativeCompatible(node: SparkPlan): Boolean =
    node.isInstanceOf[CometNativeExec] || node.getTagValue(CometTags.NATIVE_OP).isDefined

  private def hasCometBroadcastChild(parent: SparkPlan): Boolean =
    parent.children.exists(_.isInstanceOf[CometBroadcastExchangeExec])

  /**
   * Reconcile each Comet operator's `logicalLink` with its `originalPlan.logicalLink`. For every
   * `CometExec` / `CometShuffleExchangeExec` / `CometBroadcastExchangeExec`:
   *   - if `originalPlan.logicalLink.isDefined`, copy that link onto the Comet operator.
   *   - if `originalPlan.logicalLink.isEmpty`, explicitly unset both `LOGICAL_PLAN_TAG` and
   *     `LOGICAL_PLAN_INHERITED_TAG` on the Comet operator.
   *
   * The unset branch is the load-bearing one. Spark's `SparkPlan.setLogicalLink` recurses into
   * children, writing `LOGICAL_PLAN_INHERITED_TAG` on every descendant that lacks its own
   * `LOGICAL_PLAN_TAG` (recursion stops at descendants that already have a tag of their own).
   * Phase 3 sets logical links bottom-up while emitting, so when the parent join emits and calls
   * `setLogicalLink`, propagation reaches a `CometShuffleExchangeExec` whose source Spark
   * exchange had no logical link of its own. The exchange now carries an inherited link that
   * points at the parent join's logical node rather than its own (stage-boundary) logical node.
   *
   * Why it matters: AQE's `AdaptiveSparkPlanExec.replaceWithQueryStagesInLogicalPlan` walks the
   * current physical plan and, for each materialized query stage, locates a physical match for
   * the stage's logical node via `physicalNode.logicalLink.exists(logicalNode.eq)`. A stale
   * inherited link makes `collectFirst` pick the wrong physical node (typically a Comet operator
   * far above the stage) and that whole subtree becomes a `LogicalQueryStage`. On re-planning via
   * `LogicalQueryStageStrategy`, the captured physical subtree is returned verbatim, so the
   * already-Comet ancestor survives a re-plan that would otherwise produce a fresh Spark plan.
   *
   * Concrete failure mode (regression test: `CometExecSuite."CometBroadcastExchange could be
   * converted to rows using CometColumnarToRow"`): second `df.collect` with
   * `COMET_EXEC_ENABLED=false` is supposed to produce Spark BHJ over a reused materialized
   * `CometBroadcastExchange` query stage. Without this pass, the inner `CometSortMergeJoin` from
   * the first collect survives via `LogicalQueryStage` (the stale inherited link routes the wrap
   * to the SMJ instead of the shuffle stage), and the outer broadcast gets re-planned as a fresh
   * Spark `BroadcastExchange` instead of reusing the Comet one. Mirrors the post-conversion "Set
   * up logical links" pass in legacy `CometExecRule` (rules/CometExecRule.scala). Run after Phase
   * 3 emit, before convertBlocks.
   */
  private def cleanupLogicalLinks(plan: SparkPlan): SparkPlan = {
    var unset = 0
    var set = 0
    val out = plan.transform {
      case op: CometExec =>
        if (op.originalPlan.logicalLink.isEmpty) {
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          unset += 1
        } else {
          op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          set += 1
        }
        op
      case op: CometShuffleExchangeExec =>
        if (op.originalPlan.logicalLink.isEmpty) {
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          unset += 1
        } else {
          op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          set += 1
        }
        op
      case op: CometBroadcastExchangeExec =>
        if (op.originalPlan.logicalLink.isEmpty) {
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          unset += 1
        } else {
          op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          set += 1
        }
        op
    }
    if (set + unset > 0) logDebug(s"CometPlanner: cleanupLogicalLinks set=$set unset=$unset")
    out
  }

  /**
   * Pre-pass: expression-level rewrites that change operator structure. Float NaN/zero
   * normalization around comparison operators (arrow-rs doesn't normalize) and SMJ-to-SHJ/BHJ
   * join rewriting. Both must run before classification because they change the set of node types
   * the later phases see.
   */
  private def prePass(plan: SparkPlan): SparkPlan = {
    val normalized = NormalizePrePass(plan)
    if (CometConf.COMET_REPLACE_SMJ.get()) {
      normalized.transformUp { case p => RewriteJoin.rewrite(p) }
    } else {
      normalized
    }
  }

  /**
   * Phase 1: tag each node with LIKELY_COMET based on serde's expression-sensitive support check.
   * Ignores child gating. Also builds the broadcast consumer index.
   */
  private def phase1LikelyComet(plan: SparkPlan, ctx: PlanningContext): SparkPlan =
    Phase1LikelyComet(plan, ctx.conf)

  /**
   * Phase 2: per-operator demand rules produce a DECISION tag (Convert / Passthrough / Fallback).
   * Top-down so the parent's LIKELY_COMET is known when each node is visited.
   */
  private def phase2Decision(plan: SparkPlan, ctx: PlanningContext): SparkPlan =
    Phase2Decision(plan, ctx)

  /**
   * Phase 3: emit protobuf and construct CometNativeExec subtrees for nodes tagged
   * DECISION=Convert. Wires children; sets logical link and COMET_CONVERTED tag at construction.
   */
  private def phase3Emit(plan: SparkPlan, ctx: PlanningContext): SparkPlan =
    Phase3Emit(ctx.session)(plan)

  /**
   * Post-pass: single expression walk via transformAllExpressions rewriting SubqueryBroadcastExec
   * to CometSubqueryBroadcastExec (non-AQE) and wrapping SAB in
   * CometSubqueryAdaptiveBroadcastExec (AQE 3.5+). Replaces the per-node invocation inside the
   * old CometExecRule's transformUp.
   */
  private def postPass(plan: SparkPlan, ctx: PlanningContext): SparkPlan =
    SubqueryBroadcastRewrite(plan)

  private def countNative(plan: SparkPlan): Int = {
    var n = 0
    plan.foreach {
      case _: CometNativeExec => n += 1
      case _ =>
    }
    n
  }

  /**
   * Post-emission invariants. Any violation means a prior phase produced an inconsistent plan
   * that would either crash at execution or silently produce wrong results. Fail loud during the
   * pre-cutover debugging phase; these become `logWarn` with fallback after stabilization.
   */
  private def checkPostEmitInvariants(plan: SparkPlan): Unit = {
    plan.foreach { node =>
      assert(
        !node.getClass.getName.contains("CometBatchScanExec"),
        "CometBatchScanExec found in emitted plan. CometPlanner should emit " +
          s"CometIcebergNativeScanExec / CometCsvNativeScanExec directly. node=$node")
      assert(
        !node.getClass.getName.endsWith(".CometSinkPlaceHolder") &&
          !node.getClass.getName.endsWith(".CometScanWrapper"),
        s"Placeholder wrapper (${node.getClass.getSimpleName}) survived Phase 3. Every serde " +
          "that returns a wrapper should be unwrapped via NATIVE_OP tag inside runSerde.")
      // A CometNativeExec that is not COMET_CONVERTED shouldn't exist post-Phase-3 because
      // Phase 3 sets the tag at every emission site. If one appears without the tag, some
      // emission path forgot to tag and AQE re-entries will try to re-convert it.
      node match {
        case n: CometNativeExec =>
          assert(
            n.getTagValue(CometTags.COMET_CONVERTED).isDefined,
            s"CometNativeExec missing COMET_CONVERTED tag. node=$n")
        case _ =>
      }
    }
  }
}
