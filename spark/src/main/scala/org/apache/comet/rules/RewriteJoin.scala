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

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}

import org.apache.comet.CometSparkSessionExtensions.withInfo

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
          ShuffledHashJoinExec(
            smj.leftKeys,
            smj.rightKeys,
            smj.joinType,
            buildSide,
            smj.condition,
            removeSort(smj.left),
            removeSort(smj.right),
            smj.isSkewJoin)
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
