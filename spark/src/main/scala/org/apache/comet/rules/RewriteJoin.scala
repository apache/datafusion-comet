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

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo

/**
 * Adapted from equivalent rule in Apache Gluten.
 *
 * This rule replaces [[SortMergeJoinExec]] with [[ShuffledHashJoinExec]].
 */
object RewriteJoin {

  private def getSmjBuildSide(join: SortMergeJoinExec): BuildSide = {
    join.logicalLink
      .flatMap {
        case join: Join => Some(getOptimalBuildSide(join))
        case _ => None
      }
      .getOrElse {
        // If smj has no logical link, or its logical link is not a join,
        // then we always choose left as build side.
        BuildLeft
      }
  }

  private def removeSort(plan: SparkPlan) = plan match {
    case _: SortExec => plan.children.head
    case _ => plan
  }

  /**
   * Returns true if the build side is small enough to benefit from hash join over sort-merge
   * join. When both sides are large, SMJ's streaming merge on pre-sorted data can outperform hash
   * join's per-task hash table construction.
   */
  private def buildSideSmallEnough(smj: SortMergeJoinExec, buildSide: BuildSide): Boolean = {
    val maxBuildSize = CometConf.COMET_REPLACE_SMJ_MAX_BUILD_SIZE.get()
    if (maxBuildSize <= 0) {
      return true // no limit
    }
    smj.logicalLink match {
      case Some(join: Join) =>
        val buildSize = buildSide match {
          case BuildLeft => join.left.stats.sizeInBytes
          case BuildRight => join.right.stats.sizeInBytes
        }
        buildSize <= maxBuildSize
      case _ =>
        true // no stats available, allow the rewrite
    }
  }

  def rewrite(plan: SparkPlan): SparkPlan = plan match {
    case smj: SortMergeJoinExec =>
      val buildSide = getSmjBuildSide(smj)
      if (!buildSideSmallEnough(smj, buildSide)) {
        withInfo(
          smj,
          "Cannot rewrite SortMergeJoin to HashJoin: " +
            "build side exceeds spark.comet.exec.replaceSortMergeJoin.maxBuildSize")
        plan
      } else {
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
