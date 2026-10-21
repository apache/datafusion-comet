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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, JoinStrategyHint, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, LogicalQueryStage}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{isCometLoaded, withFallbackReason, withInfo}
import org.apache.comet.shims.ShimJoinSelection

/**
 * Plans a ShuffledHashJoinExec in place of a SortMergeJoinExec, before EnsureRequirements and
 * RemoveRedundantSorts, so the sorts above the join survive. Adapted from Apache Gluten.
 */
case class CometShuffledHashJoinStrategy(session: SparkSession)
    extends SparkStrategy
    with ShimJoinSelection {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case join: Join if isEnabled(join) => planJoin(join)
    case _ => Nil
  }

  private def sqlConf: SQLConf = session.sessionState.conf

  // Planner strategies run with Comet off and before CometRule, which applies plan-only mode.
  private def isEnabled(join: Join): Boolean =
    CometConf.COMET_FORCE_SHJ.get(sqlConf) &&
      CometConf.COMET_EXEC_ENABLED.get(sqlConf) &&
      !CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.get(sqlConf) &&
      !join.isStreaming &&
      isCometLoaded(sqlConf)

  private def planJoin(join: Join): Seq[SparkPlan] = join match {
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, _, left, right, hint)
        if !isLeftToSpark(join, hint) && canSortMergeJoin(joinType) &&
          RowOrdering.isOrderable(leftKeys) && hashJoinSupportedShim(leftKeys, rightKeys) =>
      buildSide(join) match {
        case Some(BuildRight) if joinType == LeftSemi || joinType.isInstanceOf[ExistenceJoin] =>
          // LeftSemi https://github.com/apache/datafusion-comet/issues/2667
          // ExistenceJoin https://github.com/apache/datafusion-comet/issues/2697
          planBySpark(join) { smj =>
            withFallbackReason(smj, declineReason(s"BuildRight with $joinType is not supported"))
          }
        case Some(side) =>
          buildSideOverLimit(join, side) match {
            case Some(reason) =>
              // Not a fallback: the kept sort-merge join still runs natively.
              planBySpark(join)(smj => withInfo(smj, declineReason(reason)))
            case None =>
              Seq(
                ShuffledHashJoinExec(
                  leftKeys,
                  rightKeys,
                  joinType,
                  side,
                  condition,
                  planLater(left),
                  planLater(right)))
          }
        case None => Nil
      }
    case _ => Nil
  }

  private def isLeftToSpark(join: Join, hint: JoinHint): Boolean =
    isBroadcastStage(join.left) || isBroadcastStage(join.right) ||
      canPlanAsBroadcastHashJoin(join, sqlConf) || hasStrategyHint(hint)

  // canPlanAsBroadcastHashJoin misses a broadcast stage when the adaptive threshold is lower or
  // off, and AQE discards a re-plan that hash joins one.
  private def isBroadcastStage(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, _: BroadcastQueryStageExec) => true
    case _ => false
  }

  // AQE's NO_BROADCAST_HASH and PREFER_SHUFFLE_HASH are outside JoinStrategyHint.strategies.
  private def hasStrategyHint(hint: JoinHint): Boolean =
    Seq(hint.leftHint, hint.rightHint).exists(
      _.exists(_.strategy.exists(JoinStrategyHint.strategies.contains)))

  private def declineReason(reason: String): String =
    s"Cannot rewrite SortMergeJoin to HashJoin: $reason"

  private def planBySpark(join: Join)(
      record: SortMergeJoinExec => SortMergeJoinExec): Seq[SparkPlan] = {
    val planned = session.sessionState.planner.JoinSelection(join)
    planned.flatMap(_.collect { case smj: SortMergeJoinExec => smj }).foreach(record)
    planned
  }

  private def buildSide(join: Join): Option[BuildSide] = {
    val leftBuildable = canBuildShuffledHashJoinLeft(join.joinType)
    val rightBuildable = canBuildShuffledHashJoinRight(join.joinType)
    if (leftBuildable && rightBuildable) {
      Some(optimalBuildSide(join))
    } else if (leftBuildable) {
      Some(BuildLeft)
    } else if (rightBuildable) {
      Some(BuildRight)
    } else {
      None
    }
  }

  private def optimalBuildSide(join: Join): BuildSide = {
    val leftSize = join.left.stats.sizeInBytes
    val rightSize = join.right.stats.sizeInBytes
    val leftRowCount = join.left.stats.rowCount
    val rightRowCount = join.right.stats.rowCount
    if (leftSize == rightSize && rightRowCount.isDefined && leftRowCount.isDefined) {
      if (rightRowCount.get <= leftRowCount.get) BuildRight else BuildLeft
    } else if (rightSize <= leftSize) {
      BuildRight
    } else {
      BuildLeft
    }
  }

  /** The largest build side to hash join, or None when the limit is off. */
  private def maxBuildSize: Option[BigInt] =
    CometConf.COMET_FORCE_SHJ_MAX_BUILD_SIZE.get(sqlConf) match {
      case Some(limit) if limit <= 0 => None
      case Some(limit) => Some(BigInt(limit))
      case None =>
        val threshold = sqlConf.autoBroadcastJoinThreshold
        val perPartition =
          if (threshold > 0) threshold else SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.defaultValue.get
        Some(BigInt(perPartition) * sqlConf.numShufflePartitions)
    }

  /**
   * Why the build side is not strictly under the limit, as in canBuildLocalHashMapBySize. An
   * unknown size is spark.sql.defaultSizeInBytes, so it never fits.
   */
  private def buildSideOverLimit(join: Join, side: BuildSide): Option[String] =
    maxBuildSize.flatMap { limit =>
      val buildSize = side match {
        case BuildLeft => join.left.stats.sizeInBytes
        case BuildRight => join.right.stats.sizeInBytes
      }
      if (buildSize >= limit) {
        Some(
          s"build side size estimate of $buildSize bytes is not under the limit of $limit bytes")
      } else {
        None
      }
    }
}
