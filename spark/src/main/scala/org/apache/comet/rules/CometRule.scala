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

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{ApplyColumnarRulesAndInsertTransitions, BaseSubqueryExec, ColumnarToRowExec, ExecSubqueryExpression, InputAdapter, ReusedSubqueryExec, RowToColumnarExec, SparkPlan, SQLExecution, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange}
import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.isCometLoaded
import org.apache.comet.shims.ShimCometStreaming

object CometRule {

  /**
   * A bounded set of keys, used for plan-only reporting state. Evicts in LRU order once `limit`
   * keys are held, so a long-lived driver retains a fixed amount of reporting state. Same
   * synchronized-`LinkedHashMap` pattern used by `IcebergPlanDataInjector.commonCache`.
   */
  private class BoundedKeySet(limit: Int) {
    private val keys: java.util.Map[String, java.lang.Boolean] =
      java.util.Collections.synchronizedMap(
        new java.util.LinkedHashMap[String, java.lang.Boolean](16, 0.75f, true) {
          override def removeEldestEntry(
              eldest: java.util.Map.Entry[String, java.lang.Boolean]): Boolean = size() > limit
        })

    /** Adds `key`, returning true if it was not already present. */
    def add(key: String): Boolean = keys.put(key, java.lang.Boolean.TRUE) == null
  }

  private val PLAN_ONLY_REPORTED_LIMIT = 1024

  /** `executionId:planFingerprint` keys that plan-only mode has already reported. */
  private val planOnlyReportedPlans = new BoundedKeySet(PLAN_ONLY_REPORTED_LIMIT)

  /**
   * Set on the root of every plan that plan-only mode has reported. Catalyst copies a node's tags
   * onto the node that replaces it (`TreeNode.copyTagsFrom`), so the mark survives the rewrites
   * Spark applies between one application of this rule and the next, which is what lets a later
   * application recognize a plan it has already described.
   */
  private val PLAN_ONLY_REPORTED: TreeNodeTag[Unit] = TreeNodeTag[Unit]("comet.planOnlyReported")

  /**
   * Whether `plan` is the plan of a query stage AQE has just cut, which reaches the columnar rule
   * rooted at the `Exchange` the cut was made at.
   *
   * With AQE off a plan rooted at an `Exchange` is an ordinary plan - `df.repartition(n)`, say -
   * and must still be reported, hence the `aqeEnabled` guard.
   */
  private def isQueryStage(
      plan: SparkPlan,
      queryStagePrep: Boolean,
      aqeEnabled: Boolean): Boolean = {
    aqeEnabled && !queryStagePrep && plan.isInstanceOf[Exchange]
  }

  /**
   * Whether `plan` is an application of this rule to a plan already reported, rather than to a
   * plan the user is asking about. The state is on the plan itself, so this holds whether or not
   * the application carries a SQL execution ID - `df.rdd.count()` and reading `executedPlan`
   * without an action both plan, and in the first case execute AQE stages, with no execution ID
   * set.
   *
   * `AdaptiveSparkPlanExec` is a leaf as far as `exists` is concerned, so the wrapper is matched
   * by name rather than through its contents. A re-optimized plan holds `QueryStageExec` nodes
   * for the stages already materialized. A final plan for a query AQE never had to cut into
   * stages holds neither, and is recognized by the mark left when it was first reported.
   */
  private def isReapplication(plan: SparkPlan): Boolean = {
    plan.exists(p =>
      p.isInstanceOf[QueryStageExec] || p.isInstanceOf[AdaptiveSparkPlanExec] ||
        p.getTagValue(PLAN_ONLY_REPORTED).isDefined)
  }

  /**
   * Whether `plan` is a plan AQE re-optimized down to nothing, which is the one re-optimization
   * result [[isReapplication]] cannot recognize.
   *
   * A re-optimized plan normally holds a `QueryStageExec` for each stage that has materialized.
   * The exception is a stage that materialized empty: `AQEPropagateEmptyRelation` then replaces
   * the stages, and everything above them, with an empty relation, so the plan Spark hands back
   * shares no node with the plan already reported and holds no query stage either. Empty is the
   * only shape that eliminates every stage, and Catalyst records it as a plan whose row count is
   * known to be zero, so the logical link answers the question directly.
   *
   * The check is confined to the query-stage-prep rule, the only one AQE hands a re-optimized
   * plan to. A genuinely empty query - `WHERE false`, an empty local relation - is planned once,
   * reaches the columnar rule instead, and is still reported.
   */
  private def isAdaptiveReplanToNothing(
      plan: SparkPlan,
      queryStagePrep: Boolean,
      aqeEnabled: Boolean): Boolean = {
    aqeEnabled && queryStagePrep && plan.logicalLink.exists(_.maxRows.contains(0L))
  }

  /**
   * Whether plan-only mode should report `plan`, marking it reported if so.
   *
   * Spark applies this rule many times while executing one query, and only some of those
   * applications correspond to a plan the user is asking about. Under AQE one query reaches the
   * rules at least five times:
   *
   *   - the initial plan, through the query-stage-prep rule - the one to report;
   *   - the same plan again as a columnar rule, now wrapped in `AdaptiveSparkPlanExec`;
   *   - each query stage as it is created, again as a columnar rule;
   *   - the re-optimized plan after each stage materializes, through the prep rule;
   *   - the final plan once every stage has materialized, as a columnar rule.
   *
   * Three mechanisms sort those out, because no one of them covers every shape:
   *
   *   - A mark on the plan already reported, so that a later application recognizes it however
   *     Spark rewrote it in between; see [[isReapplication]]. Marking plans individually, rather
   *     than holding one report slot per SQL execution, is what gives the outer query a report of
   *     its own: each scalar subquery and DPP subquery is prepared as a top-level plan in its own
   *     right, and for most of them that happens *before* the outer plan reaches the conversion
   *     rules, so a single slot would be consumed by a subquery and the plan being evaluated
   *     would never be described.
   *   - One re-optimization result carries no trace of the plan it replaced, and is recognized by
   *     what it is rather than by a mark; see [[isAdaptiveReplanToNothing]].
   *   - A subquery referenced from more than one place in the outer plan is prepared once per
   *     reference, as a separate plan each time, differing only in expression IDs; Spark then
   *     collapses them to one `ReusedSubqueryExec`. No mark connects those, so within one SQL
   *     execution the canonicalized plan's hash dedupes them. Canonicalization is what makes the
   *     expression IDs drop out; the raw structural hash sees two different plans.
   *
   * None of this is scoped to a SQL execution ID, which `df.rdd.count()` and reading
   * `executedPlan` without an action both plan - and in the first case execute AQE stages -
   * without.
   *
   * @param queryStagePrep
   *   whether the calling rule instance is registered as a query-stage-prep rule.
   */
  private[comet] def shouldReportPlanOnly(
      executionId: Option[String],
      plan: SparkPlan,
      queryStagePrep: Boolean,
      aqeEnabled: Boolean): Boolean = {
    if (isQueryStage(plan, queryStagePrep, aqeEnabled) || isReapplication(plan)) {
      false
    } else {
      // Mark before deciding. A plan AQE re-optimized to nothing is not worth a report, but the
      // final plan Spark builds from it reaches the columnar rule next and has to be recognized
      // as a plan already dealt with.
      plan.setTagValue(PLAN_ONLY_REPORTED, ())
      // Node tags are not part of a plan's canonical form, so the mark set above does not perturb
      // the key. Without an execution ID there is nothing to scope the state to, and the checks
      // above have already ruled out the repeat applications AQE makes, so report.
      !isAdaptiveReplanToNothing(plan, queryStagePrep, aqeEnabled) &&
      executionId.forall(id => planOnlyReportedPlans.add(s"$id:${plan.canonicalized.hashCode()}"))
    }
  }
}

/**
 * Comet's plan conversion pass: scan conversion followed by operator conversion.
 *
 * The two were previously registered as separate rules, adjacently, in both the columnar and the
 * query-stage-prep paths. Nothing ever ran between them, and neither is useful on its own -
 * [[CometExecRule]] seeds its native chain only from the nodes [[CometScanRule]] produces
 * (`CometScanExec`, `CometBatchScanExec`, `CometContribScanMarker`), so operator conversion
 * against unconverted Spark scans converts nothing. Composing them here makes that ordering an
 * invariant of the code rather than of the registration order, and gives plan-only mode a single
 * place to stand: one short-circuit, and one call to build the plan it reports on.
 *
 * The two rules keep their own classes, files and tests; this only fixes how they are sequenced.
 * `ruleName` in `spark.comet.explain.transformations` output is still each inner rule's own,
 * since this delegates to their `apply`.
 *
 * @param queryStagePrep
 *   true for the instance registered with `injectQueryStagePrepRule`, which under AQE sees the
 *   whole initial plan, and false for the one registered as a columnar rule, which under AQE sees
 *   one query stage at a time. Only plan-only reporting reads this; see
 *   [[CometRule.shouldReportPlanOnly]].
 */
case class CometRule(session: SparkSession, queryStagePrep: Boolean = false)
    extends Rule[SparkPlan] {

  private val scanRule = CometScanRule(session)
  private val execRule = CometExecRule(session)

  override def apply(plan: SparkPlan): SparkPlan = {
    if (planOnlyApplies(plan)) {
      reportPlanOnlyCoverage(plan)
      return plan
    }
    convert(plan)
  }

  /** Scan conversion followed by operator conversion: the plan Comet would execute. */
  private def convert(plan: SparkPlan): SparkPlan = execRule.apply(scanRule.apply(plan))

  /**
   * Whether plan-only mode governs this application.
   *
   * The guards mirror the ones the conversion rules apply to themselves, so that a plan they
   * would have left alone anyway is not diverted into a report. In particular plan-only mode is
   * scoped to `spark.comet.exec.enabled`: with exec disabled there is no operator conversion to
   * describe, and Comet's columnar shuffle should keep being applied as usual.
   */
  private def planOnlyApplies(plan: SparkPlan): Boolean =
    CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.get() &&
      isCometLoaded(conf) &&
      !ShimCometStreaming.isStreamingPlan(plan) &&
      CometConf.COMET_EXEC_ENABLED.get(conf)

  /**
   * If `plan` is one plan-only mode has not already described, build the Comet plan we would have
   * executed and log it. Called from `apply` in plan-only mode; the built plan is discarded.
   *
   * Nothing here may fail the query. Plan-only mode exists so that a workload can be assessed
   * without taking on risk, and the preview rebuilds and rewrites a plan Spark has already
   * prepared, so a plan shape it mishandles has to cost the report rather than the query.
   */
  private def reportPlanOnlyCoverage(plan: SparkPlan): Unit = {
    try {
      val executionId = Option(
        session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      if (CometRule.shouldReportPlanOnly(
          executionId,
          plan,
          queryStagePrep,
          conf.adaptiveExecutionEnabled)) {
        val preview = buildPreview(plan, topLevel = true)
        logWarning(
          s"[Comet plan-only]\n${new ExtendedExplainInfo().generateExtendedInfo(preview)}")
      }
    } catch {
      case NonFatal(e) =>
        logWarning("[Comet plan-only] could not build a coverage report for this query", e)
    }
  }

  /**
   * The plan Comet would have executed for `plan`.
   *
   * Conversion is only the first half of Comet planning. Normally Spark then inserts the columnar
   * transitions and runs Comet's post-columnar rules (see
   * `CometSparkSessionExtensions.CometExecColumnar.postColumnarTransitions`), which can revert
   * whole stages back to Spark and drop redundant transitions. Those steps run here too, so the
   * report describes the plan that would really have executed and counts the transitions that
   * would really have been there, rather than the pre-transition conversion result.
   *
   * `RevertNativeForTransitionHeavyStages` is applied with `applyToAllStages` because the preview
   * holds the whole plan at once, whereas under AQE Spark hands that rule one stage at a time.
   *
   * @param topLevel
   *   false when previewing the plan behind a subquery expression. `ReuseExchangeAndSubquery` is
   *   the last step of Spark's preparation and `QueryExecution.preparations` omits it for a
   *   subquery, so the preview follows suit.
   */
  private def buildPreview(plan: SparkPlan, topLevel: Boolean): SparkPlan = {
    val converted = convert(previewSubqueriesOf(plan))
    val withTransitions =
      ApplyColumnarRulesAndInsertTransitions(Seq.empty, outputsColumnar = false).apply(converted)
    val reverted = RevertNativeForTransitionHeavyStages(session).applyToAllStages(withTransitions)
    val preview = EliminateRedundantTransitions(session).apply(reverted)
    if (topLevel) ReuseExchangeAndSubquery.apply(preview) else preview
  }

  /**
   * `plan` with the plan behind each of its subquery expressions replaced by that plan's own
   * preview.
   *
   * Extended explain walks a node's `innerChildren`, which for a `SparkPlan` are the plans owned
   * by its expressions, and counts their operators towards the report. Normal planning has
   * already converted those plans by the time the outer plan reaches this rule - Spark prepares a
   * scalar subquery through the full preparation sequence, columnar rules included, before
   * substituting it into the outer plan - so leaving them untouched here would report every
   * subquery operator as un-accelerated Spark and understate coverage relative to what Comet
   * really executes.
   *
   * Each subquery is also reported in its own right, because Spark prepares it as a top-level
   * plan of its own; those reports and the counts here therefore describe overlapping sets of
   * operators.
   */
  private def previewSubqueriesOf(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressions { case subquery: ExecSubqueryExpression =>
      subquery.withNewPlan(previewSubquery(subquery.plan))
    }
  }

  private def previewSubquery(subquery: BaseSubqueryExec): BaseSubqueryExec = subquery match {
    // Reuse bookkeeping: the plan to preview is one level further down.
    case reused: ReusedSubqueryExec => reused.copy(child = previewSubquery(reused.child))
    case other =>
      other.withNewChildren(Seq(previewPreparedPlan(other.child))).asInstanceOf[BaseSubqueryExec]
  }

  /**
   * Preview `plan`, which Spark prepared as a plan in its own right.
   *
   * A DPP subquery is the exception to that framing: `PlanDynamicPruningFilters` prepares the
   * build plan and only then wraps it in a `BroadcastExchangeExec`, so the plan that went through
   * the post-columnar rules - and the stage `RevertNativeForTransitionHeavyStages` judged - is
   * the exchange's child, not the exchange. Previewing the exchange instead leaves its child a
   * stage bounded at the top by the exchange, which stops the reversion firing, and the report
   * then counts operators as accelerated that the executed plan runs on Spark. Descend through
   * the wrapper and put it back, so the preview keeps the boundary Spark's preparation used.
   */
  private def previewPreparedPlan(plan: SparkPlan): SparkPlan = plan match {
    case exchange: BroadcastExchangeExec =>
      exchange.withNewChildren(Seq(previewPreparedPlan(exchange.child)))
    case other => buildPreview(stripPreparation(other), topLevel = false)
  }

  /**
   * `plan` with the artifacts of a finished plan preparation removed: whole-stage codegen
   * wrappers and the columnar transitions Spark inserted.
   *
   * A subquery arrives inside the outer plan fully prepared -
   * `ApplyColumnarRulesAndInsertTransitions` and `CollapseCodegenStages` have both run over it -
   * whereas the conversion rules only ever see a plan midway through preparation. A
   * `HashAggregateExec` still wrapped in `WholeStageCodegenExec` is left unconverted, so
   * previewing the prepared form would report a subquery as falling back that Comet in fact
   * accelerates. [[buildPreview]] re-inserts the transitions once conversion is done.
   */
  private def stripPreparation(plan: SparkPlan): SparkPlan = plan.transformUp {
    case WholeStageCodegenExec(child) => child
    case InputAdapter(child) => child
    case ColumnarToRowExec(child) => child
    case RowToColumnarExec(child) => child
  }
}
