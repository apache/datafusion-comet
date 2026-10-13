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

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{withFallbackReason, withInfo}

/**
 * Adapted from equivalent rule in Apache Gluten.
 *
 * This rule replaces [[SortMergeJoinExec]] with [[ShuffledHashJoinExec]].
 */
object RewriteJoin extends JoinSelectionHelper {

  private def getSmjBuildSide(join: SortMergeJoinExec): Option[BuildSide] = {
    val leftBuildable = canBuildShuffledHashJoinLeft(join.joinType)
    val rightBuildable = canBuildShuffledHashJoinRight(join.joinType)
    if (!leftBuildable && !rightBuildable) {
      return None
    }
    if (!leftBuildable) {
      return Some(BuildRight)
    }
    if (!rightBuildable) {
      return Some(BuildLeft)
    }
    val side = join.logicalLink
      .flatMap {
        case join: Join => Some(getOptimalBuildSide(join))
        case _ => None
      }
      .getOrElse {
        // If smj has no logical link, or its logical link is not a join,
        // then we always choose left as build side.
        BuildLeft
      }
    Some(side)
  }

  private def removeSort(plan: SparkPlan) = plan match {
    case _: SortExec => plan.children.head
    case _ => plan
  }

  /**
   * Adds a local sort under any operator whose child no longer has the ordering the operator
   * requires, using the same check as EnsureRequirements. EnsureRequirements puts no sort above a
   * sort-merge join whose output ordering already satisfies its parent, so when rewrite turns
   * that join into a hash join and removes its sorts, a kept parent such as another sort-merge
   * join on the same key would read unsorted input. Plans that already satisfy their required
   * orderings are returned unchanged.
   */
  def restoreRequiredOrdering(plan: SparkPlan): SparkPlan = plan.transformUp { case p =>
    p.withNewChildren(p.children.zip(p.requiredChildOrdering).map {
      case (child, required) if !SortOrder.orderingSatisfies(child.outputOrdering, required) =>
        SortExec(required, global = false, child = child)
      case (child, _) => child
    })
  }

  /**
   * The largest build side the rewrite converts, or None when the limit is switched off. Unset
   * means Spark's own rule for choosing a shuffled hash join by size: the broadcast threshold
   * times the initial shuffle partition count. A non-positive threshold only disables broadcast
   * joins and says nothing about the hash table an executor can hold, so Spark's default
   * threshold stands in for it.
   */
  private def maxBuildSize(conf: SQLConf): Option[BigInt] =
    CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.get(conf) match {
      case Some(limit) if limit <= 0 => None
      case Some(limit) => Some(BigInt(limit))
      case None =>
        val threshold = conf.autoBroadcastJoinThreshold
        val perPartition =
          if (threshold > 0) threshold else SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.defaultValue.get
        Some(BigInt(perPartition) * conf.numShufflePartitions)
    }

  /**
   * Why the build side is too large to rewrite, or None when it fits. Like Spark's
   * canBuildLocalHashMapBySize, the size must be strictly under the limit. The hash join cannot
   * spill, so a build side without statistics is refused as well. Spark reports an unknown size
   * as Long.MaxValue, which fails the comparison on its own.
   */
  private def buildSideOverLimit(
      smj: SortMergeJoinExec,
      buildSide: BuildSide,
      conf: SQLConf): Option[String] =
    maxBuildSize(conf).flatMap { limit =>
      smj.logicalLink match {
        case Some(join: Join) =>
          val buildSize = buildSide match {
            case BuildLeft => join.left.stats.sizeInBytes
            case BuildRight => join.right.stats.sizeInBytes
          }
          if (buildSize >= limit) {
            Some(
              s"build side size estimate of $buildSize bytes is not under the limit of " +
                s"$limit bytes")
          } else {
            None
          }
        case _ => Some("no statistics are available for the build side")
      }
    }

  def rewrite(plan: SparkPlan, conf: SQLConf): SparkPlan = plan match {
    case smj: SortMergeJoinExec =>
      getSmjBuildSide(smj) match {
        case Some(BuildRight)
            if smj.joinType == LeftSemi || smj.joinType.isInstanceOf[ExistenceJoin] =>
          // LeftSemi https://github.com/apache/datafusion-comet/issues/2667
          // ExistenceJoin https://github.com/apache/datafusion-comet/issues/2697
          withFallbackReason(
            smj,
            "Cannot rewrite SortMergeJoin to HashJoin: " +
              s"BuildRight with ${smj.joinType} is not supported")
          plan
        case Some(buildSide) =>
          buildSideOverLimit(smj, buildSide, conf) match {
            case Some(reason) =>
              // The sort-merge join still runs natively, so this is information for extended
              // explain rather than a fallback reason.
              withInfo(smj, s"Cannot rewrite SortMergeJoin to HashJoin: $reason")
              plan
            case None =>
              ShuffledHashJoinExec(
                smj.leftKeys,
                smj.rightKeys,
                smj.joinType,
                buildSide,
                smj.condition,
                removeSort(smj.left),
                removeSort(smj.right),
                smj.isSkewJoin)
          }
        case _ => plan
      }
    case _ => plan
  }

  def getOptimalBuildSide(join: Join): BuildSide = {
    val leftSize = join.left.stats.sizeInBytes
    val rightSize = join.right.stats.sizeInBytes
    val leftRowCount = join.left.stats.rowCount
    val rightRowCount = join.right.stats.rowCount
    if (leftSize == rightSize && rightRowCount.isDefined && leftRowCount.isDefined) {
      if (rightRowCount.get <= leftRowCount.get) {
        return BuildRight
      }
      return BuildLeft
    }
    if (rightSize <= leftSize) {
      return BuildRight
    }
    BuildLeft
  }
}
