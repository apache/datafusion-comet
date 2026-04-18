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

package org.apache.comet

import org.apache.spark.sql.catalyst.trees.{TreeNode, TreeNodeTag}

import org.apache.comet.CometSparkSessionExtensions.withInfo

/**
 * Sticky fallback marker for shuffle / stage nodes.
 *
 * Comet's shuffle-support predicates (e.g. `CometShuffleExchangeExec.columnarShuffleSupported`)
 * run at both initial planning and AQE stage-prep. Some fallback decisions depend on the
 * surrounding plan shape - for example, the presence of a DPP scan below a shuffle. Between the
 * two passes AQE can reshape that subtree (a completed child stage becomes a
 * `ShuffleQueryStageExec`, a `LeafExecNode` whose `children` is empty), so a naive re-evaluation
 * can flip the decision.
 *
 * When a decision is made on the initial-plan pass, the deciding rule records a sticky tag via
 * [[markForFallback]]. On subsequent passes, callers short-circuit via [[isMarkedForFallback]]
 * and preserve the earlier decision instead of re-deriving it from the current plan shape.
 *
 * This tag is kept separate from `CometExplainInfo.EXTENSION_INFO` on purpose: the explain tag
 * accumulates informational reasons (including rolled-up child reasons), many of which are not a
 * full-fallback signal. Treating any presence of explain info as fallback is too coarse and
 * breaks legitimate conversions (e.g. a shuffle tagged "Comet native shuffle not enabled" should
 * still be eligible for columnar shuffle). The fallback tag exists only for decisions that should
 * remain sticky.
 */
object CometFallback {

  val STAGE_FALLBACK_TAG: TreeNodeTag[Set[String]] =
    new TreeNodeTag[Set[String]]("CometStageFallback")

  /**
   * Mark a node so that subsequent shuffle-support re-evaluations fall back to Spark without
   * re-deriving the decision from the (possibly reshaped) subtree. Also records the reason in the
   * usual explain channel so it surfaces in extended explain output.
   */
  def markForFallback[T <: TreeNode[_]](node: T, reason: String): T = {
    val existing = node.getTagValue(STAGE_FALLBACK_TAG).getOrElse(Set.empty[String])
    node.setTagValue(STAGE_FALLBACK_TAG, existing + reason)
    withInfo(node, reason)
    node
  }

  /** True if a prior rule pass marked this node for Spark fallback via [[markForFallback]]. */
  def isMarkedForFallback(node: TreeNode[_]): Boolean =
    node.getTagValue(STAGE_FALLBACK_TAG).exists(_.nonEmpty)
}
