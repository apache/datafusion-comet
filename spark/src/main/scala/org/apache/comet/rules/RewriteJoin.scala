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

import org.apache.spark.SparkEnv
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo

/**
 * Adapted from equivalent rule in Apache Gluten.
 *
 * This rule replaces [[SortMergeJoinExec]] with [[ShuffledHashJoinExec]] when the build side is
 * small enough to fit in a per-task memory budget. If either side's statistics exceed the budget,
 * the SortMergeJoin is kept to avoid the hash-table OOM that would otherwise result (Comet's
 * native `HashJoinExec` cannot spill its hash table today).
 *
 * The budget is either explicit (`spark.comet.exec.replaceSortMergeJoin.maxBuildSize`) or derived
 * at plan time from `spark.memory.offHeap.size / spark.executor.cores` scaled by `memoryFraction
 * / hashTableOverhead`.
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
   * Compute the maximum build-side `sizeInBytes` (from Spark's logical plan statistics) that we
   * will accept when rewriting a SortMergeJoin to a ShuffledHashJoin. Returns `None` when the
   * size check is disabled (`maxBuildSize = -1`), in which case every SMJ is rewritten regardless
   * of size.
   *
   * When the user-configured `maxBuildSize` is positive, it is used directly. When it is `0`
   * (default), the budget is derived from `spark.memory.offHeap.size / spark.executor.cores`
   * scaled by `memoryFraction / hashTableOverhead`.
   */
  private[rules] def computeMaxBuildSize(): Option[Long] = {
    val configured = CometConf.COMET_REPLACE_SMJ_MAX_BUILD_SIZE.get()
    if (configured == -1L) {
      None
    } else if (configured > 0L) {
      Some(configured)
    } else {
      Some(deriveMaxBuildSize())
    }
  }

  /**
   * Derive a max-build-size from Spark conf: per-task off-heap share * memoryFraction /
   * hashTableOverhead. Falls back to a conservative absolute value if off-heap is disabled or
   * `executor.cores` is missing.
   */
  private def deriveMaxBuildSize(): Long = {
    val memoryFraction = CometConf.COMET_REPLACE_SMJ_MEMORY_FRACTION.get()
    val hashOverhead = CometConf.COMET_REPLACE_SMJ_HASH_TABLE_OVERHEAD.get()

    val sparkConf = Option(SparkEnv.get).map(_.conf)
    val offHeapBytes = sparkConf
      .filter(_.getBoolean("spark.memory.offHeap.enabled", defaultValue = false))
      .map(c => ByteUnit.MiB.toBytes(c.getSizeAsMb("spark.memory.offHeap.size", "0")))
      .getOrElse(0L)
    val executorCores = sparkConf.map(_.getInt("spark.executor.cores", 1)).getOrElse(1).max(1)

    // Fallback when off-heap isn't configured: use a conservative 100 MB cap, matching the
    // previous hardcoded default. Users with on-heap-only deployments should set maxBuildSize
    // explicitly.
    if (offHeapBytes <= 0) {
      100L * 1024L * 1024L
    } else {
      val perTask = offHeapBytes.toDouble / executorCores
      (perTask * memoryFraction / hashOverhead).toLong.max(0L)
    }
  }

  /**
   * True if neither join side's logical `sizeInBytes` exceeds the budget. When the budget is
   * `None` (size check disabled), returns true unconditionally.
   */
  private def withinBudget(
      smj: SortMergeJoinExec,
      buildSide: BuildSide,
      maxBuildSize: Option[Long]): Boolean = maxBuildSize match {
    case None => true
    case Some(cap) =>
      val buildStatsSize = smj.logicalLink match {
        case Some(join: Join) =>
          buildSide match {
            case BuildLeft => join.left.stats.sizeInBytes
            case BuildRight => join.right.stats.sizeInBytes
          }
        case _ =>
          // No logical link: no stats. Fall back to the physical child's sizeInBytes, which
          // for Spark physical plans defaults to a huge number when unknown. If stats are
          // missing we conservatively keep SMJ.
          val physicalSize: BuildSide => BigInt = {
            case BuildLeft =>
              smj.left.logicalLink.map(_.stats.sizeInBytes).getOrElse(BigInt(cap) + 1)
            case BuildRight =>
              smj.right.logicalLink.map(_.stats.sizeInBytes).getOrElse(BigInt(cap) + 1)
          }
          physicalSize(buildSide)
      }
      buildStatsSize <= BigInt(cap)
  }

  def rewrite(plan: SparkPlan): SparkPlan = plan match {
    case smj: SortMergeJoinExec =>
      getSmjBuildSide(smj) match {
        case Some(BuildRight) if smj.joinType == LeftAnti || smj.joinType == LeftSemi =>
          // LeftAnti https://github.com/apache/datafusion-comet/issues/457
          // LeftSemi https://github.com/apache/datafusion-comet/issues/2667
          withInfo(
            smj,
            "Cannot rewrite SortMergeJoin to HashJoin: " +
              s"BuildRight with ${smj.joinType} is not supported")
          plan
        case Some(buildSide) =>
          val maxBuildSize = computeMaxBuildSize()
          if (withinBudget(smj, buildSide, maxBuildSize)) {
            ShuffledHashJoinExec(
              smj.leftKeys,
              smj.rightKeys,
              smj.joinType,
              buildSide,
              smj.condition,
              removeSort(smj.left),
              removeSort(smj.right),
              smj.isSkewJoin)
          } else {
            val (buildSize, cap) = explainBudget(smj, buildSide, maxBuildSize)
            withInfo(
              smj,
              s"Keeping SortMergeJoin: build side stats.sizeInBytes $buildSize > " +
                s"budget $cap bytes. Tune with " +
                s"${CometConf.COMET_REPLACE_SMJ_MAX_BUILD_SIZE.key} or " +
                s"${CometConf.COMET_REPLACE_SMJ_MEMORY_FRACTION.key}.")
            plan
          }
        case _ => plan
      }
    case _ => plan
  }

  /** Build the (buildSize, cap) pair used in the withInfo message. */
  private def explainBudget(
      smj: SortMergeJoinExec,
      buildSide: BuildSide,
      maxBuildSize: Option[Long]): (BigInt, Long) = {
    val buildStatsSize = smj.logicalLink match {
      case Some(join: Join) =>
        buildSide match {
          case BuildLeft => join.left.stats.sizeInBytes
          case BuildRight => join.right.stats.sizeInBytes
        }
      case _ => BigInt(-1)
    }
    (buildStatsSize, maxBuildSize.getOrElse(-1L))
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
