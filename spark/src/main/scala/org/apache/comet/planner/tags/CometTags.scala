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

package org.apache.comet.planner.tags

import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import org.apache.comet.iceberg.CometIcebergNativeScanMetadata
import org.apache.comet.serde.OperatorOuterClass

/**
 * Formal tag vocabulary used by CometPlanner to coordinate across its internal phases and across
 * AQE stage re-entries. Every tag a Comet rule reads or writes should be declared here with a
 * comment describing who sets it and who reads it. This replaces the ad-hoc tag strings that
 * CometScanRule / CometExecRule scatter across companion objects.
 */
object CometTags {

  /**
   * Set by Phase 1 (LIKELY_COMET annotator) on every node. Value `true` means "serde supports
   * this op in isolation" (configs enabled, structural / expression checks pass). Ignores child
   * gating so it can be computed bottom-up without the classification depending on its own
   * descendants' decisions. Read by Phase 2 to judge demand-aware conversion.
   */
  val LIKELY_COMET: TreeNodeTag[Boolean] = TreeNodeTag("comet.likelyComet")

  /**
   * Set by Phase 2 (DECISION annotator) on every node. Encodes whether this node should be
   * converted (`Convert`), kept as a Spark node with Comet-convertible children underneath
   * (`Passthrough`), or fallen back to Spark entirely (`Fallback`). Read by Phase 3 emitter.
   */
  val DECISION: TreeNodeTag[PlannerDecision] = TreeNodeTag("comet.decision")

  /**
   * Set by Phase 3 (emitter) on every emitted CometNativeExec. Marks the subtree as already
   * compiled so that re-entries (especially AQE per-stage re-planning) can skip Phase 1/2/3 via
   * the top-level check in `CometPlanner.apply`. Persists across stage boundaries because
   * TreeNode tags survive makeCopy.
   */
  val COMET_CONVERTED: TreeNodeTag[Unit] = TreeNodeTag("comet.converted")

  /**
   * Set by Phase 2 on a `BatchScanExec` that has been classified as a convertible native Iceberg
   * scan. Carries the pre-extracted metadata (resolved via iceberg-java reflection + catalog
   * properties) so Phase 3 can build the `CometIcebergNativeScanExec` without going through the
   * `CometBatchScanExec` carrier class. Absent on non-Iceberg convertible scans (CSV) and on
   * fall-back scans.
   */
  val ICEBERG_METADATA: TreeNodeTag[CometIcebergNativeScanMetadata] =
    TreeNodeTag("comet.icebergMetadata")

  /**
   * Attached by Phase 3 to any emitted operator that is Comet-compatible but is not itself a
   * `CometNativeExec` (e.g. JVM-orchestrated operators like `CometCollectLimitExec`,
   * `CometBroadcastExchangeExec`, `CometSparkToColumnarExec`). Carries the protobuf `Operator`
   * that a parent's serde needs to wire this node into its own protobuf tree. Replaces the old
   * `CometSinkPlaceHolder` / `CometScanWrapper` nodes that previously carried the same payload as
   * a plan-tree wrapper.
   *
   * `Phase 3` treats a child as native-compatible if it is a `CometNativeExec` OR has this tag.
   */
  val NATIVE_OP: TreeNodeTag[OperatorOuterClass.Operator] = TreeNodeTag("comet.nativeOp")
}

/**
 * Outcome of Phase 2 classification for a single plan node.
 */
sealed trait PlannerDecision

object PlannerDecision {

  /** Emit a CometNativeExec for this node in Phase 3. */
  case object Convert extends PlannerDecision

  /**
   * Wrap this non-Comet leaf in CometSparkToColumnarExec to bridge row-at-a-time data into a
   * Comet-consuming parent. Only used when the parent is LIKELY_COMET so demand exists for the
   * columnar conversion.
   */
  case object ConvertS2C extends PlannerDecision

  /**
   * Keep the Spark node as-is; its children may still convert. Used for plan nodes Comet cannot
   * replace directly (e.g. AdaptiveSparkPlanExec, some V2CommandExec cases).
   */
  case object Passthrough extends PlannerDecision

  /**
   * Node will not convert; record the reasons via `withInfo` for explain output. Any already-
   * converted descendants remain; this node terminates the native subtree at its children.
   */
  case class Fallback(reasons: Set[String]) extends PlannerDecision
}
