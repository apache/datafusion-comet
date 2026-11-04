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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CallMethodViaReflection, CodegenObjectFactoryMode, Expression, LeafExpression, PlanExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{CodegenSupport, FilterExec, ProjectExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, withFallbackReason, withInfo}
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
 * so `CASE`, `IF`, `AND`, `OR` and `COALESCE` keep their short-circuit evaluation within a row.
 *
 * What the kernel cannot reproduce is when Spark's whole-stage code evaluates an item at all:
 *
 *   - The kernel evaluates every item for every row of the batch. Spark's whole-stage
 *     `ProjectExec` defers a deterministic item until an operator above it in the stage reads it,
 *     so it never evaluates the item for a row that an operator above drops, such as a filter or
 *     a join in the same stage, or for rows past a `LIMIT`. The kernel can then raise an error
 *     that Spark does not, for an expression that can raise one: ANSI arithmetic, or
 *     `raise_error` / `assert_true` with ANSI on or off. An item that Spark sees as
 *     nondeterministic is evaluated eagerly for every row by Spark too, and each keeps its own
 *     state, so its values match. But Spark decides that from the item's own `deterministic`,
 *     which an `Invoke` computes without its `targetObject`: Spark 4's
 *     `make_valid_utf8(cast(rand(1) AS string))` reports itself deterministic, so Spark defers it
 *     like any deterministic item and its `rand` advances only for the rows Spark evaluates. The
 *     kernel's would advance for every row and give different values, so such an item, one that
 *     reports itself deterministic but contains a nondeterministic node, is not detoured.
 *   - Spark's whole-stage `FilterExec` splits the conjuncts of the condition and evaluates each
 *     `IsNotNull` only just before the first conjunct that reads its column, or at the end if
 *     none does. The kernel evaluates the condition as one short-circuiting `AND`, left to right,
 *     as Spark without whole-stage codegen does. For a deterministic condition only errors differ
 *     (ANSI errors, or `raise_error` / `assert_true`), in both directions. The kernel can skip an
 *     error Spark raises: `y IS NOT NULL AND abs(10 div x) > 0` with a row `(0, NULL)`, where
 *     Spark moves the `IsNotNull` to the end. And it can raise one Spark skips: `abs(10 div
 *     coalesce(x, 0)) > 0 AND x IS NOT NULL` with a NULL `x`, where Spark evaluates the
 *     `IsNotNull` before the first conjunct, which reads `x`. For a nondeterministic condition
 *     the kept rows differ: a conjunct such as `rand(42) < 0.5` runs, and advances its state, for
 *     rows that a later `IsNotNull` drops. So a nondeterministic filter condition is not
 *     detoured.
 *   - Spark's whole-stage `FilterExec` does not eliminate common subexpressions in its predicate,
 *     which could otherwise move a repeated subexpression out of an `AND`'s short circuit.
 *     `FilterExec` without whole-stage codegen does, through `GeneratePredicate`. Spark leaves a
 *     filter out of whole-stage codegen when its condition has a non-leaf `CodegenFallback` (a
 *     higher-order function, say), when it has too many fields, or when whole-stage codegen is
 *     off. So a filter condition is compiled with or without subexpression elimination to match
 *     the form Spark plans for that filter (see [[filterSubexpressionElimination]]). Projection
 *     items keep it within each item where Spark's form for the projection has it (see
 *     [[projectSubexpressionElimination]]). `ProjectExec` also eliminates common subexpressions
 *     across items and evaluates them up front, including ones on the right of an `AND`. The
 *     kernel is compiled per item, so it cannot. So Spark can raise an error that the detour
 *     skips, as Comet's Rust projection does: `x <> 0 AND abs(10 div x) > 1` and `x <> 0 AND
 *     abs(10 div x) > 2` as two items.
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
   * Convert one root expression: a projection list item or, with `filter`, the condition of that
   * filter. Tries the usual conversion first and, if that fails, the detour.
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
      filter: Option[FilterExec] = None,
      project: Option[ProjectExec] = None): Option[Expr] =
    if (!CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.get()) {
      // With the detour off this is exactly the conversion the operators did before it existed.
      QueryPlanSerde.exprToProto(root, inputs, binding)
    } else {
      CometExplainInfo.withExpressionFallbackReasonScope {
        val beforeConversion = ExplainState.of(root)
        QueryPlanSerde.exprToProto(root, inputs, binding).orElse {
          val afterConversion = ExplainState.of(root)
          beforeConversion.restore()
          val detoured = detour(root, inputs, binding, filter, project)
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
      filter: Option[FilterExec],
      project: Option[ProjectExec] = None): Option[Expr] = {
    val filterCondition = filter.isDefined
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
        CometScalaUDF
          .emitJvmCodegenDispatch(
            target,
            inputs,
            binding,
            subexpressionElimination = filter
              .map(filterSubexpressionElimination)
              .orElse(project.map(projectSubexpressionElimination))
              .getOrElse(true))
          .map { proto =>
            withInfo(
              target,
              "Expression evaluated in the JVM by the codegen dispatcher (JVM detour)")
            QueryPlanSerde.attachExprIdAndContext(target, proto)
          }
    }
  }

  /**
   * Whether to eliminate common subexpressions in the kernel for `filter`'s condition, to match
   * the form Spark plans for it:
   *
   *   - In whole-stage code, `FilterExec.doConsume` does not eliminate them. So no.
   *   - Outside whole-stage code, `FilterExec.doExecute` builds the predicate with
   *     `Predicate.create`. With generated code that is `GeneratePredicate.generate(_,
   *     spark.sql.subexpressionElimination.enabled)`, which eliminates them as the kernel does.
   *     So the setting decides.
   *   - With `spark.sql.codegen.factoryMode=NO_CODEGEN`, `Predicate.create` returns an
   *     `InterpretedPredicate`, which evaluates a common subexpression on first use rather than
   *     up front. That evaluates the same subexpressions as no elimination does. So no.
   *
   * This follows what Spark plans. If Spark's whole-stage code falls back to `child.execute()` at
   * run time, because the generated method exceeds `spark.sql.codegen.hugeMethodLimit` or fails
   * to compile with `spark.sql.codegen.fallback` on, `FilterExec` runs outside whole-stage code
   * after all, and its error behavior can differ from the kernel's. That cannot be known at
   * planning time.
   */
  private[comet] def filterSubexpressionElimination(filter: FilterExec): Boolean =
    !runsInWholeStageCodegen(filter) && !noCodegenFactory(filter.conf) &&
      filter.conf.subexpressionEliminationEnabled

  /**
   * Whether to eliminate common subexpressions in the kernel for an item of `project`, to match
   * the form Spark plans for it:
   *
   *   - In whole-stage code, `ProjectExec.doConsume` eliminates them when
   *     `spark.sql.subexpressionElimination.enabled` is on, evaluated up front. So the setting
   *     decides. (Spark does this across items; the kernel can only do it within one.)
   *   - Outside whole-stage code, `ProjectExec` builds an `UnsafeProjection`. With generated code
   *     that is `GenerateUnsafeProjection.generate(_,
   *     spark.sql.subexpressionElimination.enabled)`, which also evaluates them up front. So the
   *     setting decides.
   *   - With `spark.sql.codegen.factoryMode=NO_CODEGEN` and outside whole-stage code, it is an
   *     `InterpretedUnsafeProjection`, which evaluates a common subexpression on first use, as no
   *     elimination does. So no. From Spark 3.5 (SPARK-44236) `NO_CODEGEN` also turns whole-stage
   *     codegen off, so this applies to every projection; in 3.4 a projection that Spark runs in
   *     whole-stage code still eliminates them.
   *
   * The same run-time caveat as for [[filterSubexpressionElimination]] applies.
   */
  private[comet] def projectSubexpressionElimination(project: ProjectExec): Boolean =
    project.conf.subexpressionEliminationEnabled &&
      (runsInWholeStageCodegen(project) || !noCodegenFactory(project.conf))

  /** Whether `spark.sql.codegen.factoryMode` is `NO_CODEGEN`. */
  private def noCodegenFactory(conf: SQLConf): Boolean =
    conf
      .getConfString(SQLConf.CODEGEN_FACTORY_MODE.key)
      .equalsIgnoreCase(CodegenObjectFactoryMode.NO_CODEGEN.toString)

  /**
   * Whether Spark plans `plan` in whole-stage code. This mirrors `CollapseCodegenStages`, which
   * is the same in Spark 3.4.3, 3.5.8, 4.0.1, 4.1.3 and 4.2.1 (`WholeStageCodegenExec.scala`):
   * `spark.sql.codegen.wholeStage` is on (from 3.5, SPARK-44236, also the factory mode is not
   * `NO_CODEGEN`); the operator `supportCodegen`; no non-leaf `CodegenFallback` in its
   * expressions; and neither its output nor its child's exceeds `spark.sql.codegen.maxFields`
   * (`WholeStageCodegenExec.isTooManyFields`).
   */
  private def runsInWholeStageCodegen(plan: SparkPlan with CodegenSupport): Boolean = {
    val conf = plan.conf
    conf.wholeStageEnabled &&
    !(isSpark35Plus && noCodegenFactory(conf)) &&
    plan.supportCodegen &&
    !plan.expressions.exists(_.exists {
      case _: LeafExpression => false
      case _: CodegenFallback => true
      case _ => false
    }) &&
    !WholeStageCodegenExec.isTooManyFields(conf, plan.schema) &&
    !plan.children.exists(child => WholeStageCodegenExec.isTooManyFields(conf, child.schema))
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
   * filter condition, or a projection item that hides a nondeterministic node from Spark; an
   * expression the user disabled; a subquery; and code that is not Spark's.
   */
  private[comet] def declineReason(target: Expression, filterCondition: Boolean): Option[String] =
    // Walks the tree, because `deterministic` is not transitive everywhere: `Invoke` ignores its
    // `targetObject`, which is where Spark 4's `make_valid_utf8` puts its input.
    if (filterCondition && target.exists(!_.deterministic)) {
      Some(
        "the filter condition is nondeterministic, and Spark's filter evaluates its conjuncts " +
          "in an order the dispatcher does not reproduce, so a nondeterministic conjunct would " +
          "run for a different set of rows")
    } else if (!filterCondition && target.deterministic && target.exists(!_.deterministic)) {
      Some(
        "the item contains a nondeterministic expression that Spark does not see, as under an " +
          "Invoke, so Spark's projection may evaluate it for fewer rows than the dispatcher " +
          "would and its values would differ")
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
