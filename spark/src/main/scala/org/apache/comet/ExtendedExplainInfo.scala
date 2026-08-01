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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.ExtendedExplainGenerator
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.trees.{TreeNode, TreeNodeTag}
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometNativeColumnarToRowExec, CometPlan, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, InputAdapter, RowToColumnarExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

import org.apache.comet.CometExplainInfo.getActualPlan
import org.apache.comet.annotation.Public

@Public
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
    fallbackReasons(plan).toSeq.sorted
  }

  /**
   * Names of the expressions in `plan` that Comet lowered to native DataFusion expressions,
   * sorted alphabetically. Names are the expression's `prettyName` (the UDF name for a
   * `ScalaUDF`) lowercased, so they match the function names in the expression coverage guide.
   *
   * Structural nodes - attribute references, literals, aliases, bound references - are not
   * reported: they carry no computation and would swamp the interesting names.
   */
  def getNativeExpressions(plan: SparkPlan): Seq[String] = {
    CometCoverageStats.forPlan(plan).nativeExpressions.toSeq.sorted
  }

  /**
   * Names of the expressions in `plan` that Comet kept inside the native pipeline by routing them
   * through the JVM codegen dispatcher (Spark's own `doGenCode` compiled into a batch kernel)
   * rather than lowering them to a native DataFusion expression, sorted alphabetically. See
   * [[getNativeExpressions]] for how names are derived.
   */
  def getCodegenDispatchExpressions(plan: SparkPlan): Seq[String] = {
    CometCoverageStats.forPlan(plan).codegenDispatchExpressions.toSeq.sorted
  }

  private[comet] def fallbackReasons(node: TreeNode[_]): Set[String] = {
    var info = mutable.Seq[String]()
    val sorted = sortup(node)
    sorted.foreach { p =>
      val all: Set[String] =
        getActualPlan(p)
          .getTagValue(CometExplainInfo.FALLBACK_REASONS)
          .getOrElse(Set.empty[String])
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
          _: CometNativeColumnarToRowExec | _: CometSparkToColumnarExec =>
        planStats.transitions += 1
      case _: CometPlan =>
        planStats.cometOperators += 1
      case _ =>
        planStats.sparkOperators += 1
    }

    planStats.recordExpressions(node)

    outString.append("   " * indent)
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        outString.append(if (isLast) "   " else ":  ")
      }
      outString.append(if (lastChildren.last) "+- " else ":- ")
    }

    // Preserve the existing fallback-segment rendering exactly (a defined tag renders
    // `[COMET: ...]`, even when the reason set is empty) and only add the new info segment, so
    // this change does not perturb plans that carry no info message.
    val fallback = node.getTagValue(CometExplainInfo.FALLBACK_REASONS)
    val info = node.getTagValue(CometExplainInfo.EXTENSION_INFO)
    val str = if (fallback.nonEmpty || info.exists(_.nonEmpty)) {
      val sb = new StringBuilder(" ").append(node.nodeName)
      fallback.foreach(v => sb.append(s" [COMET: ${v.mkString(", ")}]"))
      info.filter(_.nonEmpty).foreach(v => sb.append(s" [COMET-INFO: ${v.mkString(", ")}]"))
      sb.toString()
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

  /** Distinct names of expressions lowered to native DataFusion expressions. */
  val nativeExpressions: mutable.Set[String] = mutable.HashSet.empty

  /** Distinct names of expressions routed through the JVM codegen dispatcher. */
  val codegenDispatchExpressions: mutable.Set[String] = mutable.HashSet.empty

  /**
   * Accumulate the expression coverage that `CometExecRule.rollUpInfoMessages` rolled up onto a
   * converted Comet plan node. Nodes that carry no such tags (every Spark operator, and any Comet
   * operator without expressions) contribute nothing.
   */
  private[comet] def recordExpressions(node: TreeNode[_]): Unit = {
    node.getTagValue(CometExplainInfo.NATIVE_EXPRS).foreach(nativeExpressions ++= _)
    node
      .getTagValue(CometExplainInfo.CODEGEN_DISPATCH_EXPRS)
      .foreach(codegenDispatchExpressions ++= _)
  }

  override def toString(): String = {
    val eligible = sparkOperators + cometOperators
    val converted =
      if (eligible == 0) 0.0 else cometOperators.toDouble / eligible * 100.0
    // The same function can be lowered natively for one set of arguments and dispatched for
    // another, so a name can appear in both sets. Count the union rather than adding the two.
    val expressions = (nativeExpressions ++ codegenDispatchExpressions).size
    s"Comet accelerated $cometOperators out of $eligible " +
      s"eligible operators (${converted.toInt}%). " +
      s"Final plan contains $transitions transitions between Spark and Comet. " +
      s"Comet accelerated $expressions expressions " +
      s"(${nativeExpressions.size} native, ${codegenDispatchExpressions.size} codegen dispatch)."
  }
}

object CometCoverageStats {

  /**
   * Compute coverage stats for a plan without generating explain string.
   */
  def forPlan(plan: SparkPlan): CometCoverageStats = {
    val stats = new CometCoverageStats()
    val explainInfo = new ExtendedExplainInfo()
    explainInfo.generateTreeString(
      CometExplainInfo.getActualPlan(plan),
      0,
      Seq(),
      0,
      new StringBuilder(),
      stats)
    stats
  }
}

object CometExplainInfo {
  val FALLBACK_REASONS = new TreeNodeTag[Set[String]]("CometFallbackReasons")
  val EXTENSION_INFO = new TreeNodeTag[Set[String]]("CometExtensionInfo")

  // Expression names the serde routed through the JVM codegen dispatcher. Set on each such
  // expression, then rolled up per operator by `CometExecRule.rollUpInfoMessages` onto the
  // converted Comet plan node (where extended explain reads it for coverage stats, and where it
  // becomes one combined `[COMET-INFO: ...]` when `spark.comet.explain.codegen.enabled` is set).
  val CODEGEN_DISPATCH_EXPRS =
    new TreeNodeTag[Set[String]]("CometCodegenDispatchExprs")

  // Expression names the serde lowered to native DataFusion expressions. The native counterpart
  // of `CODEGEN_DISPATCH_EXPRS`, rolled up the same way, but never rendered in the tree display:
  // it would repeat what the operator names already say.
  val NATIVE_EXPRS = new TreeNodeTag[Set[String]]("CometNativeExprs")

  /**
   * Name used to report `expr` in explain output.
   *
   * `BinaryMathExpression` (Hypot, Pow, ...) overrides `prettyName` to raw uppercase; `ScalaUDF`
   * collapses to `"scalaudf"` for every user UDF. Prefer `ScalaUDF.udfName` when set, then
   * lowercase to normalize.
   */
  def exprDisplayName(expr: Expression): String = {
    val raw = expr match {
      case s: ScalaUDF => s.udfName.getOrElse(s.prettyName)
      case other => other.prettyName
    }
    raw.toLowerCase(Locale.ROOT)
  }

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
