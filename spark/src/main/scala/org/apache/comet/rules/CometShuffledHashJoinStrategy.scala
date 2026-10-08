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
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, Join, JoinHint, LogicalPlan, SHUFFLE_HASH, SHUFFLE_MERGE, SHUFFLE_REPLICATE_NL}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, LogicalQueryStage}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{isCometLoaded, withFallbackReason, withInfo}
import org.apache.comet.shims.ShimJoinSelection

/**
 * Plans an equi-join as a ShuffledHashJoinExec where Spark would plan a SortMergeJoinExec, when
 * spark.comet.exec.forceShuffledHashJoin is on. Choosing the join here, before EnsureRequirements
 * and RemoveRedundantSorts run, lets Spark place and keep every sort the plan needs above the
 * join. Planner strategies also run again on every AQE re-plan, where the join's children carry
 * the materialized shuffle sizes.
 *
 * Adapted from the equivalent rule in Apache Gluten.
 */
case class CometShuffledHashJoinStrategy(session: SparkSession)
    extends SparkStrategy
    with ShimJoinSelection {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case join: Join if isEnabled(join) => planJoin(join)
    case _ => Nil
  }

  private def sqlConf: SQLConf = session.sessionState.conf

  // Planner strategies run whether or not Comet is enabled, and before CometRule, so plan-only
  // mode needs its own check here.
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
              // The sort-merge join still runs natively, so this is information for extended
              // explain rather than a fallback reason.
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

  // Broadcasts, including the ones AQE plans once a stage turns out small, and joins with a
  // strategy hint are planned by Spark as usual.
  private def isLeftToSpark(join: Join, hint: JoinHint): Boolean =
    isBroadcastStage(join.left) || isBroadcastStage(join.right) ||
      canPlanAsBroadcastHashJoin(join, sqlConf) || hasStrategyHint(hint)

  private def isBroadcastStage(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, _: BroadcastQueryStageExec) => true
    case _ => false
  }

  // AQE's own hints, such as NO_BROADCAST_HASH and PREFER_SHUFFLE_HASH, do not count.
  private def hasStrategyHint(hint: JoinHint): Boolean =
    Seq(hint.leftHint, hint.rightHint).flatten.flatMap(_.strategy).exists {
      case BROADCAST | SHUFFLE_MERGE | SHUFFLE_HASH | SHUFFLE_REPLICATE_NL => true
      case _ => false
    }

  private def declineReason(reason: String): String =
    s"Cannot rewrite SortMergeJoin to HashJoin: $reason"

  // Lets Spark plan a join this strategy declines and records why on each sort-merge join in
  // the result, so the reason reaches extended explain.
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

  /**
   * The largest build side this strategy plans as a hash join, or None when the limit is switched
   * off. Unset means Spark's own rule for choosing a shuffled hash join by size: the broadcast
   * threshold times the initial shuffle partition count. A non-positive threshold only disables
   * broadcast joins and says nothing about the hash table an executor can hold, so Spark's
   * default threshold stands in for it.
   */
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
   * Why the build side is too large for a hash join, or None when it fits. Like Spark's
   * canBuildLocalHashMapBySize, the size must be strictly under the limit. The hash join cannot
   * spill, so a build side of unknown size, which Spark reports as spark.sql.defaultSizeInBytes
   * (Long.MaxValue by default), is refused by the same comparison.
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
