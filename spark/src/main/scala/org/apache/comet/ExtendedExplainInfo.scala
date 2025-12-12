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
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometPlan, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, InputAdapter, RowToColumnarExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

import org.apache.comet.CometExplainInfo.getActualPlan

class ExtendedExplainInfo extends ExtendedExplainGenerator {

  override def title: String = "Comet"

  def generateExtendedInfo(plan: SparkPlan): String = {
    CometConf.COMET_EXTENDED_EXPLAIN_FORMAT.get() match {
      case CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_VERBOSE =>
        // Generates the extended info in a verbose manner, printing each node along with the
        // extended information in a tree display.
        val planStats = new CometCoverageStats()
        val outString = new StringBuilder()
        generateTreeString(getActualPlan(plan), 0, Seq(), 0, outString, planStats)
        s"${outString.toString()}\n$planStats"
      case CometConf.COMET_EXTENDED_EXPLAIN_FORMAT_FALLBACK =>
        // Generates the extended info as a list of fallback reasons
        getFallbackReasons(plan).mkString("\n").trim
    }
  }

  def getFallbackReasons(plan: SparkPlan): Seq[String] = {
    extensionInfo(plan).toSeq.sorted
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

  // Simplified generateTreeString from Spark TreeNode. Appends explain info to the node if any
  def generateTreeString(
      node: TreeNode[_],
      depth: Int,
      lastChildren: Seq[Boolean],
      indent: Int,
      outString: StringBuilder,
      planStats: CometCoverageStats): Unit = {

    node match {
      case _: AdaptiveSparkPlanExec | _: InputAdapter | _: QueryStageExec |
          _: WholeStageCodegenExec | _: ReusedExchangeExec | _: AQEShuffleReadExec =>
      // ignore
      case _: RowToColumnarExec | _: ColumnarToRowExec | _: CometColumnarToRowExec |
          _: CometSparkToColumnarExec =>
        planStats.transitions += 1
      case _: CometPlan =>
        planStats.cometOperators += 1
      case _ =>
        planStats.sparkOperators += 1
    }

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
            outString,
            planStats)
        case _ =>
      }
      generateTreeString(
        getActualPlan(innerChildrenLocal.last),
        depth + 2,
        lastChildren :+ node.children.isEmpty :+ true,
        indent,
        outString,
        planStats)
    }
    if (node.children.nonEmpty) {
      node.children.init.foreach {
        case c @ (_: TreeNode[_]) =>
          generateTreeString(
            getActualPlan(c),
            depth + 1,
            lastChildren :+ false,
            indent,
            outString,
            planStats)
        case _ =>
      }
      node.children.last match {
        case c @ (_: TreeNode[_]) =>
          generateTreeString(
            getActualPlan(c),
            depth + 1,
            lastChildren :+ true,
            indent,
            outString,
            planStats)
        case _ =>
      }
    }
  }
}

class CometCoverageStats {
  var sparkOperators: Int = 0
  var cometOperators: Int = 0
  var transitions: Int = 0

  override def toString(): String = {
    val eligible = sparkOperators + cometOperators
    val converted =
      if (eligible == 0) 0.0 else cometOperators.toDouble / eligible * 100.0
    s"Comet accelerated $cometOperators out of $eligible " +
      s"eligible operators (${converted.toInt}%). " +
      s"Final plan contains $transitions transitions between Spark and Comet."
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
      case p: ReusedExchangeExec => getActualPlan(p.child)
      case p => p
    }

  }

}
