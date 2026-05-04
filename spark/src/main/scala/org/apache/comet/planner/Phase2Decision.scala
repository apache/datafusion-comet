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
import org.apache.spark.sql.comet.CometSparkToColumnarExec
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}

import org.apache.comet.CometConf
import org.apache.comet.planner.tags.{CometTags, PlannerDecision}
import org.apache.comet.planner.tags.PlannerDecision.{Convert, ConvertS2C, Fallback, Passthrough}

/**
 * Phase 2: decide per-node whether to convert, pass through, or fall back, using the
 * `LIKELY_COMET` tags Phase 1 attached. Demand-aware rules catch cases where conversion adds
 * overhead without a Comet consumer (PR #4010's columnar-shuffle-between-JVM-aggregates pattern,
 * broadcast-without-Comet-consumer, spark-to-columnar-without-consumer).
 *
 * Walk is top-down so the parent's LIKELY_COMET is known when we visit each child. Per-node
 * rules:
 *
 *   - V1 scan: V1ScanGate decides; Convert on success, Fallback(reasons) with reasons extracted
 *     from the gate.
 *   - V2 scan: V2ScanClassifier decides; Convert on success (Iceberg metadata stashed on tag),
 *     Fallback(reasons) otherwise.
 *   - shuffle: convert iff parent LIKELY_COMET or any child LIKELY_COMET.
 *   - broadcast: convert iff parent LIKELY_COMET (parent is typically a BHJ; output format
 *     incompatible with Spark broadcast).
 *   - spark-to-columnar leaf: convert iff parent LIKELY_COMET.
 *   - generic exec: convert iff LIKELY_COMET and every child LIKELY_COMET.
 *   - otherwise: Passthrough (children may still convert) or Fallback (with reasons).
 *
 * This phase only mutates tag state. Plan shape is untouched. The emitter phase rewrites.
 */
object Phase2Decision extends Logging {

  def apply(plan: SparkPlan, ctx: PlanningContext): SparkPlan = {
    visit(plan, parentLikely = false, ctx)
    plan
  }

  private def visit(node: SparkPlan, parentLikely: Boolean, ctx: PlanningContext): Unit = {
    val selfLikely = likely(node)
    val decision = decide(node, parentLikely, selfLikely, ctx)
    node.setTagValue(CometTags.DECISION, decision)
    assert(
      selfLikely == node.getTagValue(CometTags.LIKELY_COMET).getOrElse(false),
      s"LIKELY_COMET tag out of sync with read value for node=$node")
    node.children.foreach(visit(_, selfLikely, ctx))
  }

  private def likely(node: SparkPlan): Boolean =
    node.getTagValue(CometTags.LIKELY_COMET).getOrElse(false)

  private def decide(
      node: SparkPlan,
      parentLikely: Boolean,
      selfLikely: Boolean,
      ctx: PlanningContext): PlannerDecision = node match {
    case scan: FileSourceScanExec if selfLikely =>
      V1ScanGate.classify(scan, ctx.session, ctx.conf, ctx.hasInputFileExpressions) match {
        case V1ScanClassification.Convertible => Convert
        case V1ScanClassification.NotConvertible(reasons) =>
          logDebug(s"Phase2: V1 gate rejected scan=${scan.id} reasons=$reasons")
          s2cOrFallback(scan, parentLikely, ctx, reasons)
      }

    case _: FileSourceScanExec =>
      s2cOrFallback(node, parentLikely, ctx, Set.empty)

    case scan: BatchScanExec if selfLikely =>
      V2ScanClassifier.classify(scan, ctx.conf) match {
        case V2ScanClassification.IcebergConvertible(metadata) =>
          scan.setTagValue(CometTags.ICEBERG_METADATA, metadata)
          Convert
        case V2ScanClassification.CsvConvertible =>
          Convert
        case V2ScanClassification.NotConvertible(reasons) =>
          logDebug(s"Phase2: V2 classify=NotConvertible scan=${scan.id} reasons=$reasons")
          s2cOrFallback(scan, parentLikely, ctx, reasons)
      }

    case _: BatchScanExec =>
      s2cOrFallback(node, parentLikely, ctx, Set.empty)

    case s: ShuffleExchangeExec =>
      val childLikely = s.children.exists(likely)
      if (selfLikely && (parentLikely || childLikely)) Convert else Passthrough

    case b: BroadcastExchangeExec =>
      val consumed = ctx.broadcastConsumers.isConsumedByCometCandidate(b)
      val forced = CometConf.COMET_EXEC_BROADCAST_FORCE_ENABLED.get(ctx.conf)
      if (selfLikely && (parentLikely || consumed || forced)) Convert else Passthrough

    case _: CometSparkToColumnarExec =>
      if (parentLikely) Convert else Passthrough

    case op if op.isInstanceOf[LeafExecNode] =>
      if (selfLikely) Convert
      else s2cOrFallback(op, parentLikely, ctx, Set.empty)

    case op =>
      // Treat S2C-eligible leaves as effectively convertible for the purpose of parent
      // decisions: Phase 3 will wrap them in CometSparkToColumnarExec when the parent is
      // LIKELY_COMET, so from the parent's perspective they'll present a Comet output.
      // Without this, a Comet-capable parent (e.g. HashAggregate) would fall back just
      // because its BatchScan child isn't natively convertible, even though S2C would
      // bridge the gap. Old CometExecRule side-stepped this by running bottom-up and
      // wrapping S2C leaves before visiting their parents.
      def effectivelyLikely(c: SparkPlan): Boolean =
        likely(c) || (c.isInstanceOf[LeafExecNode] && S2CGate.shouldApply(c, ctx.conf))
      val allChildrenLikely = op.children.nonEmpty && op.children.forall(effectivelyLikely)
      if (selfLikely && allChildrenLikely) Convert
      else if (op.children.exists(effectivelyLikely)) Passthrough
      else Fallback(Set.empty)
  }

  private def s2cOrFallback(
      op: SparkPlan,
      parentLikely: Boolean,
      ctx: PlanningContext,
      reasons: Set[String]): PlannerDecision = {
    if (parentLikely && S2CGate.shouldApply(op, ctx.conf)) {
      ConvertS2C
    } else {
      Fallback(reasons)
    }
  }
}
