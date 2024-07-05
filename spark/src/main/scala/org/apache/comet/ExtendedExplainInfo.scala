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

import org.apache.comet.CometExplainInfo.getActualPlan

class ExtendedExplainInfo extends ExtendedExplainGenerator {

  override def title: String = "Comet"

  override def generateExtendedInfo(plan: SparkPlan): String = {
    if (CometConf.COMET_EXPLAIN_VERBOSE_ENABLED.get()) {
      generateVerboseExtendedInfo(plan)
    } else {
      val info = extensionInfo(plan)
      info.toSeq.sorted.mkString("\n").trim
    }
  }

  private[comet] def extensionInfo(node: TreeNode[_]): Set[String] = {
    var info = mutable.Seq[String]()
    val sorted = sortup(node)
    sorted.foreach { p =>
      val all: Set[String] =
        getActualPlan(p).getTagValue(CometExplainInfo.EXTENSION_INFO).getOrElse(Set.empty[String])
      for (s <- all) {
        info = info :+ s
      }
    }
    info.toSet
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

  // generates the extended info in a verbose manner, printing each node along with the
  // extended information in a tree display
  def generateVerboseExtendedInfo(plan: SparkPlan): String = {
    val outString = new StringBuilder()
    generateTreeString(getActualPlan(plan), 0, Seq(), 0, outString)
    outString.toString()
  }

  // Simplified generateTreeString from Spark TreeNode. Appends explain info to the node if any
  def generateTreeString(
      node: TreeNode[_],
      depth: Int,
      lastChildren: Seq[Boolean],
      indent: Int,
      outString: StringBuilder): Unit = {
    outString.append("   " * indent)
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        outString.append(if (isLast) "   " else ":  ")
      }
      outString.append(if (lastChildren.last) "+- " else ":- ")
    }

    val tagValue = node.getTagValue(CometExplainInfo.EXTENSION_INFO)
    val str = if (tagValue.nonEmpty) {
      s" ${node.nodeName} [COMET: ${tagValue.get.mkString(", ")}]"
    } else {
      node.nodeName
    }
    outString.append(str)
    outString.append("\n")

    val innerChildrenLocal = node.innerChildren
    if (innerChildrenLocal.nonEmpty) {
      innerChildrenLocal.init.foreach {
        case c @ (_: TreeNode[_]) =>
          generateTreeString(
            getActualPlan(c),
            depth + 2,
            lastChildren :+ node.children.isEmpty :+ false,
            indent,
            outString)
        case _ =>
      }
      generateTreeString(
        getActualPlan(innerChildrenLocal.last),
        depth + 2,
        lastChildren :+ node.children.isEmpty :+ true,
        indent,
        outString)
    }
    if (node.children.nonEmpty) {
      node.children.init.foreach {
        case c @ (_: TreeNode[_]) =>
          generateTreeString(
            getActualPlan(c),
            depth + 1,
            lastChildren :+ false,
            indent,
            outString)
        case _ =>
      }
      node.children.last match {
        case c @ (_: TreeNode[_]) =>
          generateTreeString(getActualPlan(c), depth + 1, lastChildren :+ true, indent, outString)
        case _ =>
      }
    }
  }
}

object CometExplainInfo {
  val EXTENSION_INFO = new TreeNodeTag[Set[String]]("CometExtensionInfo")

  def getActualPlan(node: TreeNode[_]): TreeNode[_] = {
    node match {
      case p: AdaptiveSparkPlanExec => getActualPlan(p.executedPlan)
      case p: InputAdapter => getActualPlan(p.child)
      case p: QueryStageExec => getActualPlan(p.plan)
      case p: WholeStageCodegenExec => getActualPlan(p.child)
      case p => p
    }
  }

}
