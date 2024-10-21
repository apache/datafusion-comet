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
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftSemi}
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}

/**
 * Adapted from equivalent rule in Apache Gluten.
 *
 * This rule replaces [[SortMergeJoinExec]] with [[ShuffledHashJoinExec]].
 */
object RewriteJoin extends JoinSelectionHelper {

  private def getBuildSide(joinType: JoinType): Option[BuildSide] = {
    if (canBuildShuffledHashJoinRight(joinType)) {
      Some(BuildRight)
    } else if (canBuildShuffledHashJoinLeft(joinType)) {
      Some(BuildLeft)
    } else {
      None
    }
  }

  private def removeSort(plan: SparkPlan) = plan match {
    case _: SortExec => plan.children.head
    case _ => plan
  }

  def rewrite(plan: SparkPlan): SparkPlan = plan match {
    case smj: SortMergeJoinExec =>
      getBuildSide(smj.joinType) match {
        case Some(BuildRight) if smj.joinType == LeftSemi =>
          // TODO this was added as a workaround for TPC-DS q14 hanging and needs
          // further investigation
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
}
