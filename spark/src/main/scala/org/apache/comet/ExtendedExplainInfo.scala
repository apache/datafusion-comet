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

import scala.collection.mutable

import org.apache.spark.sql.ExtendedExplainGenerator
import org.apache.spark.sql.catalyst.trees.{TreeNode, TreeNodeTag}
import org.apache.spark.sql.execution.{InputAdapter, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}

class ExtendedExplainInfo extends ExtendedExplainGenerator {

  override def title: String = "Comet"

  override def generateExtendedInfo(plan: SparkPlan): String = {
    val info = extensionInfo(plan)
    info.distinct.mkString("\n").trim
  }

  private def getActualPlan(node: TreeNode[_]): TreeNode[_] = {
    node match {
      case p: AdaptiveSparkPlanExec => getActualPlan(p.executedPlan)
      case p: InputAdapter => getActualPlan(p.child)
      case p: QueryStageExec => getActualPlan(p.plan)
      case p: WholeStageCodegenExec => getActualPlan(p.child)
      case p => p
    }
  }

  private def extensionInfo(node: TreeNode[_]): mutable.Seq[String] = {
    var info = mutable.Seq[String]()
    val sorted = sortup(node)
    sorted.foreach { p =>
      val all: Array[String] =
        getActualPlan(p).getTagValue(CometExplainInfo.EXTENSION_INFO).getOrElse("").split("\n")
      for (s <- all) {
        info = info :+ s
      }
    }
    info.filter(!_.contentEquals("\n"))
  }

  // get all plan nodes, breadth first traversal, then returned the reversed list so
  // leaf nodes are first
  private def sortup(node: TreeNode[_]): mutable.Queue[TreeNode[_]] = {
    val ordered = new mutable.Queue[TreeNode[_]]()
    val traversed = mutable.Queue[TreeNode[_]](getActualPlan(node))
    while (traversed.nonEmpty) {
      val s = traversed.dequeue()
      ordered += s
      if (s.innerChildren.nonEmpty) {
        s.innerChildren.foreach {
          case c @ (_: TreeNode[_]) => traversed.enqueue(getActualPlan(c))
          case _ =>
        }
      }
      if (s.children.nonEmpty) {
        s.children.foreach {
          case c @ (_: TreeNode[_]) => traversed.enqueue(getActualPlan(c))
          case _ =>
        }
      }
    }
    ordered.reverse
  }
}

object CometExplainInfo {
  val EXTENSION_INFO = new TreeNodeTag[String]("CometExtensionInfo")
}
