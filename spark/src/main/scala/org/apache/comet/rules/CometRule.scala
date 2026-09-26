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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{ApplyColumnarRulesAndInsertTransitions, BaseSubqueryExec, ColumnarToRowExec, ExecSubqueryExpression, InputAdapter, QueryExecution, ReusedSubqueryExec, RowToColumnarExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, InsertAdaptiveSparkPlan, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange}
import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.isCometLoaded
import org.apache.comet.shims.ShimCometStreaming

object CometRule {

  /** Comet's post-columnar rules, shared by `CometColumnar` and the plan-only preview. */
  def postColumnarRules(session: SparkSession, wholePlan: Boolean = false): Seq[Rule[SparkPlan]] =
    Seq(
      RevertNativeForTransitionHeavyStages(session, wholePlan),
      EliminateRedundantTransitions(session))

  /**
   * Canonical hashes of the subquery plans reported for the query this thread is preparing. Spark
   * prepares a query's subqueries synchronously, before the query itself, so the scope is reset
   * when the query's own plan arrives. This works whether or not a SQL execution ID is set.
   */
  private val reportedSubqueries = new ThreadLocal[mutable.Set[Int]] {
    override def initialValue(): mutable.Set[Int] = mutable.Set.empty
  }

  /**
   * Marks the root of a reported plan. Catalyst copies tags onto replacement nodes, so later
   * applications of the rule can recognize the plan however Spark rewrote it in between.
   */
  private val PLAN_ONLY_REPORTED: TreeNodeTag[Unit] = TreeNodeTag[Unit]("comet.planOnlyReported")

  /** Where in Spark's planning the rule is running, read off the call stack. */
  private case class PlanningContext(replanning: Boolean, subquery: Boolean)

  private def planningContext(): PlanningContext = {
    val frames = Thread.currentThread().getStackTrace
    def within(cls: Class[_], method: String): Boolean =
      frames.exists(f => f.getMethodName == method && f.getClassName == cls.getName)
    PlanningContext(
      replanning = within(classOf[AdaptiveSparkPlanExec], "reOptimize"),
      // Subqueries are prepared by `PlanSubqueries` and `PlanDynamicPruningFilters` without AQE,
      // and by `InsertAdaptiveSparkPlan` and `PlanAdaptiveDynamicPruningFilters` with it.
      subquery = within(QueryExecution.getClass, "prepareExecutedPlan") ||
        within(classOf[InsertAdaptiveSparkPlan], "compileSubquery"))
  }

  /**
   * Whether plan-only mode should report `plan`, marking it reported if so.
   *
   * Under AQE the rule sees one query several times: the initial plan (prep rule, the one to
   * report), the same plan wrapped in `AdaptiveSparkPlanExec`, each query stage, each
   * re-optimization and the final plan. Only the first is reported. Each scalar or DPP subquery
   * is prepared as a plan of its own and gets its own report.
   */
  private[comet] def shouldReportPlanOnly(
      plan: SparkPlan,
      queryStagePrep: Boolean,
      aqeEnabled: Boolean): Boolean = {
    // Under AQE the columnar rule sees each new query stage rooted at its Exchange.
    val isQueryStage = aqeEnabled && !queryStagePrep && plan.isInstanceOf[Exchange]
    // Already reported: a re-optimized plan holds query stages, and a final plan carries the
    // mark. `AdaptiveSparkPlanExec` is a leaf to `exists`, so it is matched directly.
    val isReapplication = plan.exists(p =>
      p.isInstanceOf[QueryStageExec] || p.isInstanceOf[AdaptiveSparkPlanExec] ||
        p.getTagValue(PLAN_ONLY_REPORTED).isDefined)
    if (isQueryStage || isReapplication) {
      // Execution is under way, so any subquery prepared from here on belongs to a new scope.
      reportedSubqueries.get().clear()
      false
    } else {
      // Mark even when not reporting, so the final plan built from a re-plan is recognized.
      plan.setTagValue(PLAN_ONLY_REPORTED, ())
      val context = planningContext()
      if (context.replanning) {
        // AQE re-plans a query mid-execution, for example to an empty relation once a stage
        // materializes empty, and the result can share no nodes or stages with the reported plan.
        false
      } else if (context.subquery) {
        // A subquery referenced twice is prepared twice, differing only in expression IDs.
        // Tags are not part of the canonical form.
        reportedSubqueries.get().add(plan.canonicalized.hashCode())
      } else {
        reportedSubqueries.get().clear()
        true
      }
    }
  }
}

/**
 * Comet's plan conversion pass: scan conversion followed by operator conversion.
 *
 * Native scans come only from the nodes [[CometScanRule]] produces (`CometScanExec`,
 * `CometBatchScanExec`, `CometContribScanMarker`), so [[CometExecRule]] must run after it.
 * Running [[CometExecRule]] alone leaves scans on Spark's readers. Composing the two here makes
 * that ordering part of the code instead of the order the rules are registered in, and gives
 * callers that need the whole conversion a single entry point.
 *
 * `spark.comet.explain.transformations` logs each inner rule under its own `ruleName`, since this
 * delegates to their `apply`. Spark's own plan change log sees one rule: query-stage preparation
 * logs this pass as `org.apache.comet.rules.CometRule`, which is the name
 * `spark.sql.planChangeLog.rules` has to match.
 *
 * @param queryStagePrep
 *   true for the `injectQueryStagePrepRule` instance, which sees the whole initial plan under
 *   AQE. Only plan-only reporting reads it.
 */
case class CometRule(session: SparkSession, queryStagePrep: Boolean = false)
    extends Rule[SparkPlan] {

  private val scanRule = CometScanRule(session)
  private val execRule = CometExecRule(session)

  override def apply(plan: SparkPlan): SparkPlan = {
    if (planOnlyApplies(plan)) {
      reportPlanOnlyCoverage(plan)
      plan
    } else {
      convert(plan)
    }
  }

  private def convert(plan: SparkPlan): SparkPlan = execRule.apply(scanRule.apply(plan))

  /** Mirrors the conversion rules' own guards; plan-only is scoped to exec being enabled. */
  private def planOnlyApplies(plan: SparkPlan): Boolean =
    CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.get(conf) &&
      isCometLoaded(conf) &&
      !ShimCometStreaming.isStreamingPlan(plan) &&
      CometConf.COMET_EXEC_ENABLED.get(conf)

  /** Logs the Comet plan for `plan` unless already reported. Never fails the query. */
  private def reportPlanOnlyCoverage(plan: SparkPlan): Unit = {
    try {
      if (CometRule.shouldReportPlanOnly(plan, queryStagePrep, conf.adaptiveExecutionEnabled)) {
        val preview = buildPreview(plan, topLevel = true)
        logWarning(
          s"[Comet plan-only]\n${new ExtendedExplainInfo().generateVerboseInfo(preview)}")
      }
    } catch {
      case NonFatal(e) =>
        logWarning("[Comet plan-only] could not build a coverage report for this query", e)
    }
  }

  /**
   * The plan Comet would execute for `plan`: conversion, columnar transitions, then the
   * post-columnar rules, with stage reversion visiting every stage since this is the whole plan.
   *
   * @param topLevel
   *   false for subquery plans, which Spark prepares without `ReuseExchangeAndSubquery`.
   */
  private def buildPreview(plan: SparkPlan, topLevel: Boolean): SparkPlan = {
    val converted = convert(previewSubqueriesOf(plan))
    val withTransitions =
      ApplyColumnarRulesAndInsertTransitions(Seq.empty, outputsColumnar = false).apply(converted)
    val preview = CometRule
      .postColumnarRules(session, wholePlan = true)
      .foldLeft(withTransitions) { case (p, rule) => rule(p) }
    if (topLevel) ReuseExchangeAndSubquery.apply(preview) else preview
  }

  /**
   * `plan` with each subquery's plan replaced by its preview. Extended explain counts subquery
   * operators, and normal planning has already converted them, so leaving them would understate
   * coverage.
   */
  private def previewSubqueriesOf(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressions { case subquery: ExecSubqueryExpression =>
      subquery.withNewPlan(previewSubquery(subquery.plan))
    }
  }

  private def previewSubquery(subquery: BaseSubqueryExec): BaseSubqueryExec = subquery match {
    case reused: ReusedSubqueryExec => reused.copy(child = previewSubquery(reused.child))
    case other =>
      other.withNewChildren(Seq(previewPreparedPlan(other.child))).asInstanceOf[BaseSubqueryExec]
  }

  /**
   * `PlanDynamicPruningFilters` prepares a DPP build plan before wrapping it in a
   * `BroadcastExchangeExec`, so preview the exchange's child on its own to keep the stage
   * boundary that stage reversion saw.
   */
  private def previewPreparedPlan(plan: SparkPlan): SparkPlan = plan match {
    case exchange: BroadcastExchangeExec =>
      exchange.withNewChildren(Seq(previewPreparedPlan(exchange.child)))
    case other => buildPreview(stripPreparation(other), topLevel = false)
  }

  /**
   * Removes codegen wrappers and transitions from an already-prepared subquery plan, since the
   * conversion rules expect a plan from before those are inserted. [[buildPreview]] re-inserts
   * the transitions.
   */
  private def stripPreparation(plan: SparkPlan): SparkPlan = plan.transformUp {
    case WholeStageCodegenExec(child) => child
    case InputAdapter(child) => child
    case ColumnarToRowExec(child) => child
    case RowToColumnarExec(child) => child
  }
}
