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

package org.apache.comet.serde

import java.util.IdentityHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CallMethodViaReflection, Expression, PlanExpression}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{withFallbackReason, withInfo}
import org.apache.comet.serde.ExprOuterClass.Expr

/**
 * The JVM expression detour (partial project fallback), gated by
 * [[CometConf.COMET_EXEC_JVM_DETOUR_ENABLED]].
 *
 * When an item of a projection list, or a filter condition, has no Comet translation, the whole
 * item is sent to the codegen dispatcher (Spark's own generated code, run over the Arrow batch)
 * instead of falling the operator back to Spark.
 *
 * The detour applies only at the root of an item: the child of a projection item's `Alias` (or
 * the item itself), or the whole filter condition. It never replaces a node below that root. A
 * node that declines conversion can be protecting its ancestors rather than itself: an ancestor's
 * Rust implementation can be Spark-compatible only for the inputs its Rust-implemented children
 * produce, and nothing in the serde API records that dependency (see the discussion on
 * apache/datafusion-comet#4827 and #5574). Detouring a declined node while its ancestors convert
 * to Rust would remove that protection without anything noticing. At the root there are no
 * ancestors in the expression: the projection only outputs the value, and the filter only keeps
 * the rows where it is true. Every node of the tree, conditionals included, runs in Spark's code,
 * so `CASE`, `IF`, `AND`, `OR` and `COALESCE` keep Spark's per-row short-circuit evaluation.
 *
 * A nondeterministic filter condition is not detoured. Spark's whole-stage code for `FilterExec`
 * does not evaluate the condition as one expression: it splits the conjuncts, and evaluates each
 * `IsNotNull` only just before the first conjunct that reads its column. So a nondeterministic
 * conjunct such as `rand(42) < 0.5` runs for rows that a later `IsNotNull` drops, and its state
 * advances for them. The kernel evaluates the condition as one short-circuiting `AND`, which
 * skips those rows, and keeps different rows. A deterministic condition is unaffected by the
 * order. A projection item has no such difference: `ProjectExec` evaluates each item once for
 * every input row, as the kernel does, and each nondeterministic item keeps its own state.
 *
 * The detour is not attempted when the tree contains an expression the user disabled with
 * `spark.comet.expression.<Name>.enabled=false`, because that switch is documented to make the
 * expression fall back to Spark. It is not attempted for a tree with a subquery, whose result is
 * not available when the tree is serialized at planning time. And it is not attempted when the
 * tree can run code that is not Spark's: an expression class from outside Spark's Catalyst
 * packages, such as a Hive UDF, or `reflect` / `java_method`. The dispatcher does not take those
 * through its own serdes (see `CometScalaUDF` and `CometInvokeTargets`), and the detour must not
 * let them in by another way.
 */
object CometJvmDetour {

  /**
   * Convert one root expression: a projection list item or, with `filterCondition`, a filter
   * condition. Tries the usual conversion first and, if that fails, the detour.
   *
   * The failed conversion leaves fallback reasons, info messages (such as a hint to enable a
   * native implementation) and coverage tags in the tree. They do not apply once the whole tree
   * runs in the dispatcher: an operator that falls back for another reason, such as another
   * projection item, must not report them, a hint to enable a native implementation of a node no
   * longer changes how the item runs, and a node that converted to Rust before an ancestor failed
   * now runs in the kernel. So the explain state of the tree is put back as it was before the
   * conversion, and kept from the conversion only if the detour does not happen either. Putting
   * back a snapshot, rather than clearing the tree, matters when a node is shared with an item
   * converted earlier, as `CollapseProject` can leave it: that item's tags on the node stay.
   */
  def exprToProtoOrDetour(
      root: Expression,
      inputs: Seq[Attribute],
      binding: Boolean = true,
      filterCondition: Boolean = false): Option[Expr] =
    if (!CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.get()) {
      // With the detour off this is exactly the conversion the operators did before it existed.
      QueryPlanSerde.exprToProto(root, inputs, binding)
    } else {
      CometExplainInfo.withExpressionFallbackReasonScope {
        val beforeConversion = ExplainState.of(root)
        QueryPlanSerde.exprToProto(root, inputs, binding).orElse {
          val afterConversion = ExplainState.of(root)
          beforeConversion.restore()
          val detoured = detour(root, inputs, binding, filterCondition)
          if (detoured.isEmpty) {
            afterConversion.mergeInto()
          }
          detoured
        }
      }
    }

  /**
   * Run `root` through the codegen dispatcher, after its conversion failed. Returns `None` when
   * the detour is off, when it declines the tree (see [[declineReason]]), or when the dispatcher
   * cannot take the tree; the reasons the conversion recorded are kept in that case, so the
   * operator falls back to Spark as it would without the detour.
   */
  private[comet] def detour(
      root: Expression,
      inputs: Seq[Attribute],
      binding: Boolean,
      filterCondition: Boolean): Option[Expr] = {
    if (!CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.get()) {
      return None
    }
    // The alias only names the output, so the dispatcher takes its child. The proto of a
    // projection item carries no name: `CometAlias` converts to its child's proto too.
    val target = root match {
      case a: Alias => a.child
      case other => other
    }
    declineReason(target, filterCondition) match {
      case Some(reason) =>
        withFallbackReason(root, s"JVM detour not attempted: $reason")
        None
      case None =>
        CometScalaUDF.emitJvmCodegenDispatch(target, inputs, binding).map { proto =>
          withInfo(
            target,
            "Expression evaluated in the JVM by the codegen dispatcher (JVM detour)")
          QueryPlanSerde.attachExprIdAndContext(target, proto)
        }
    }
  }

  /**
   * The config name of the first expression in `tree` that the user disabled with
   * `spark.comet.expression.<Name>.enabled=false`. An expression with a serde uses the serde's
   * config name, which is what its conversion checks. An expression without one uses its class
   * name, which is also the name the conversion of a serde would use by default, so a user can
   * keep any expression out of the detour the same way.
   */
  private[comet] def disabledExpression(tree: Expression): Option[String] =
    tree.collectFirst(Function.unlift { (e: Expression) =>
      val name = QueryPlanSerde.exprSerdeMap.get(e.getClass) match {
        case Some(serde) =>
          serde.asInstanceOf[CometExpressionSerde[Expression]].getExprConfigName(e)
        case None => e.getClass.getSimpleName
      }
      if (CometConf.isExprEnabled(name)) None else Some(name)
    })

  /**
   * Why the detour must not take `target`, or `None`. Checked in this order: a nondeterministic
   * filter condition, an expression the user disabled, a subquery, and code that is not Spark's.
   */
  private[comet] def declineReason(target: Expression, filterCondition: Boolean): Option[String] =
    if (filterCondition && !target.deterministic) {
      Some(
        "the filter condition is nondeterministic, and Spark's filter evaluates its conjuncts " +
          "in an order the dispatcher does not reproduce, so a nondeterministic conjunct would " +
          "run for a different set of rows")
    } else {
      disabledExpression(target)
        .map { name =>
          s"the expression contains $name, which is disabled by " +
            s"${CometConf.getExprEnabledConfigKey(name)}=false"
        }
        .orElse(subqueryReason(target))
        .orElse(nonSparkCodeReason(target))
    }

  /**
   * Why `tree` holds a subquery, or `None`. The dispatcher serializes the tree at planning time,
   * before the subquery has run, so its result would not be available in the kernel.
   * `CometBatchKernelCodegen.canHandle` refuses the same trees; checking here first gives the
   * subquery as the reason, rather than the package check below, since Spark's physical
   * subqueries live outside the Catalyst packages.
   */
  private[comet] def subqueryReason(tree: Expression): Option[String] =
    tree.collectFirst { case e: PlanExpression[_] =>
      s"the expression contains the subquery ${CometExplainInfo.exprDisplayName(e)}, whose " +
        "result is not available when the tree is serialized at planning time"
    }

  /** The package every expression class the detour takes must be in. */
  private val CatalystPackage = "org.apache.spark.sql.catalyst."

  /**
   * Why `tree` can run code that is not Spark's, or `None`. An expression class from outside
   * Spark's Catalyst packages can do anything in its `eval` or generated code: a Hive UDF calls
   * the Hive function, and a third-party extension's expression calls the extension. `reflect`
   * and `java_method` call a Java method the query names. Calls through `Invoke` and
   * `StaticInvoke` are checked by `CometInvokeTargets` when the tree is dispatched.
   */
  private[comet] def nonSparkCodeReason(tree: Expression): Option[String] =
    tree.collectFirst {
      case e if !e.getClass.getName.startsWith(CatalystPackage) =>
        s"${CometExplainInfo.exprDisplayName(e)} is ${e.getClass.getName}, which is not a " +
          "Spark Catalyst expression"
      case e: CallMethodViaReflection =>
        s"${CometExplainInfo.exprDisplayName(e)} calls a Java method named in the query"
    }

  /**
   * The `Set`-valued explain tags a conversion can record on an expression: coverage names,
   * fallback reasons and info messages.
   */
  private val SetTags: Seq[TreeNodeTag[Set[String]]] = Seq(
    CometExplainInfo.NATIVE_EXPRS,
    CometExplainInfo.CODEGEN_DISPATCH_EXPRS,
    CometExplainInfo.FALLBACK_REASONS,
    CometExplainInfo.EXTENSION_INFO)

  /** The explain tags and journal entry of one node, as they were at a point in time. */
  private final case class NodeExplainState(
      node: Expression,
      setTags: Seq[Option[Set[String]]],
      dispatchedSelf: Boolean,
      journalReasons: Set[String])

  /** The explain state of every node of a tree, each instance once. */
  private final class ExplainState(nodes: Seq[NodeExplainState]) {

    /** Put every node back to this state. */
    def restore(): Unit = nodes.foreach { n =>
      SetTags.zip(n.setTags).foreach {
        case (tag, Some(v)) => n.node.setTagValue(tag, v)
        case (tag, None) => n.node.unsetTagValue(tag)
      }
      if (n.dispatchedSelf) {
        n.node.setTagValue(CometExplainInfo.DISPATCHED_SELF, ())
      } else {
        n.node.unsetTagValue(CometExplainInfo.DISPATCHED_SELF)
      }
      CometExplainInfo.setJournalFallbackReasons(n.node, n.journalReasons)
    }

    /** Add this state to each node's current state, so neither loses what it recorded. */
    def mergeInto(): Unit = nodes.foreach { n =>
      SetTags.zip(n.setTags).foreach { case (tag, saved) =>
        saved.foreach(v => n.node.setTagValue(tag, n.node.getTagValue(tag).getOrElse(Set()) ++ v))
      }
      if (n.dispatchedSelf) n.node.setTagValue(CometExplainInfo.DISPATCHED_SELF, ())
      CometExplainInfo.setJournalFallbackReasons(
        n.node,
        CometExplainInfo.currentFallbackReasons(n.node) ++ n.journalReasons)
    }
  }

  private object ExplainState {
    def of(root: Expression): ExplainState = {
      val seen = new IdentityHashMap[Expression, NodeExplainState]()
      root.foreach { e =>
        if (!seen.containsKey(e)) {
          seen.put(
            e,
            NodeExplainState(
              e,
              SetTags.map(e.getTagValue(_)),
              e.getTagValue(CometExplainInfo.DISPATCHED_SELF).isDefined,
              CometExplainInfo.currentFallbackReasons(e)))
        }
      }
      new ExplainState(seen.values().asScala.toSeq)
    }
  }
}
