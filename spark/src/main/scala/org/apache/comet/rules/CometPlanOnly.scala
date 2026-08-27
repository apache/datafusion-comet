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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{ApplyColumnarRulesAndInsertTransitions, BaseSubqueryExec, ColumnarToRowExec, CommandResultExec, ExecSubqueryExpression, InputAdapter, QueryExecution, ReusedSubqueryExec, RowToColumnarExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.isCometLoaded

/**
 * Plan-only mode: report the Comet plan Comet would have executed for a query, without offloading
 * any of it to Comet. See `spark.comet.explain.planOnly.enabled`.
 *
 * The conversion rules leave the plan alone while the mode is on, so Spark plans and executes the
 * query exactly as it would with Comet switched off. The report is built afterwards, from the
 * plan Spark actually executed, and thrown away. Comet code therefore cannot reach the query: not
 * by planning it, and not by failing while describing it.
 *
 * Reporting once the query is over, rather than while it is being planned, is what keeps this
 * simple. Spark applies a planner rule many times for one query - once per query stage and once
 * per adaptive re-optimization under AQE, and separately for every subquery it prepares - and
 * telling those applications apart takes a good deal of bookkeeping. A query execution listener
 * fires once per action, holding the finished plan, so there is nothing to tell apart.
 */
object CometPlanOnly extends Logging {

  private val REPORT_PREFIX = "[Comet plan-only]"

  /**
   * Sessions that already have a listener registered. Weakly held so a session that goes away is
   * not kept alive by this, and so a long-lived driver retains no more state than the sessions it
   * is running.
   */
  private val registeredSessions: java.util.Set[SparkSession] =
    java.util.Collections.synchronizedSet(
      java.util.Collections.newSetFromMap(
        new java.util.WeakHashMap[SparkSession, java.lang.Boolean]()))

  /**
   * Registers this session's plan-only listener, if it does not have one yet.
   *
   * Called from `CometExecRule` rather than at session creation so that a session never carries a
   * listener unless plan-only mode is actually used, and so the config can be turned on part way
   * through a session.
   */
  def register(session: SparkSession): Unit = {
    if (registeredSessions.add(session)) {
      session.listenerManager.register(new CometPlanOnlyListener)
      logInfo(s"$REPORT_PREFIX registered a plan-only reporter for this session")
    }
  }

  /**
   * Logs the Comet plan Comet would have executed for `qe`.
   *
   * Nothing here may fail the query, which has finished by this point but whose action would
   * still see an exception thrown from a listener. Plan-only mode exists to let a workload be
   * assessed without taking on risk, so a plan shape the preview mishandles has to cost the
   * report rather than the query.
   */
  private def report(qe: QueryExecution): Unit = {
    val session = qe.sparkSession
    // The listener bus thread has no active session, and the conversion rules read their configs
    // from the active one. Without this the preview would be built from default config values.
    val previous = SparkSession.getActiveSession
    SparkSession.setActiveSession(session)
    try {
      val conf = session.sessionState.conf
      if (CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.get(conf) && isCometLoaded(conf) &&
        CometConf.COMET_EXEC_ENABLED.get(conf) && !isMetadataOnly(qe.executedPlan)) {
        val preview = previewOf(session, qe.executedPlan)
        logWarning(s"$REPORT_PREFIX\n${new ExtendedExplainInfo().generateExtendedInfo(preview)}")
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"$REPORT_PREFIX could not build a coverage report for this query", e)
    } finally {
      previous match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }
    }
  }

  /**
   * Whether `plan` only touches metadata - `CREATE VIEW`, `SHOW TABLES`, `SET`.
   *
   * There is nothing to accelerate in one, and a session runs enough of them that reporting each
   * as 0% would bury the reports worth reading. A command that carries a query below it - `INSERT
   * ... SELECT`, `CREATE TABLE AS SELECT`, a V2 append - has that query as a child and is
   * reported.
   */
  private def isMetadataOnly(plan: SparkPlan): Boolean = plan match {
    case _: ExecutedCommandExec | _: CommandResultExec => true
    case command: V2CommandExec => command.children.isEmpty
    case _ => false
  }

  /**
   * The plan Comet would have executed for `plan`, which Spark has finished preparing and
   * running.
   *
   * Conversion is only the first half of Comet planning. Spark then inserts the columnar
   * transitions and runs Comet's post-columnar rules (see
   * `CometSparkSessionExtensions.CometExecColumnar.postColumnarTransitions`), which can revert
   * whole stages back to Spark and drop redundant transitions. Those steps run here too, so the
   * report describes the plan that would really have executed and counts the transitions that
   * would really have been there.
   *
   * `RevertNativeForTransitionHeavyStages` is applied with `applyToAllStages` because this holds
   * a whole plan, whereas under AQE Spark hands that rule one stage at a time.
   */
  private def previewOf(session: SparkSession, plan: SparkPlan): SparkPlan = {
    val prepared = previewSubqueriesOf(session, stripPreparation(plan))
    val converted = CometExecRule(session)._apply(CometScanRule(session)._apply(prepared))
    val withTransitions =
      ApplyColumnarRulesAndInsertTransitions(Seq.empty, outputsColumnar = false).apply(converted)
    val reverted = RevertNativeForTransitionHeavyStages(session).applyToAllStages(withTransitions)
    EliminateRedundantTransitions(session).apply(reverted)
  }

  /**
   * `plan` as the conversion rules would have seen it, with everything Spark added after them
   * removed: the adaptive wrappers, the whole-stage codegen wrappers, and the columnar
   * transitions.
   *
   * Taking the plan Spark executed and undoing this much of its preparation is what buys the
   * accuracy this mode needs. The alternative, describing the plan as it stood before
   * preparation, describes a plan AQE may have replanned beyond recognition: stages coalesced,
   * joins switched from sort merge to broadcast, an empty side pruned away.
   */
  private def stripPreparation(plan: SparkPlan): SparkPlan = plan match {
    // Under AQE the executed plan is a wrapper holding the plan AQE settled on. Its query stages
    // hold their own plans off to one side, out of `children`, so an ordinary transform would not
    // reach into them.
    case adaptive: AdaptiveSparkPlanExec => stripPreparation(adaptive.executedPlan)
    case stage: QueryStageExec => stripPreparation(stage.plan)
    // A runtime partition-coalescing wrapper over a shuffle stage. It has no counterpart in a plan
    // that has not been through AQE, and the conversion rules judge a shuffle by the exchange, so
    // it goes with the stage it wraps.
    case read: AQEShuffleReadExec => stripPreparation(read.child)
    // `ReuseExchangeAndSubquery` is the last thing Spark's preparation does, after the columnar
    // rules, so in a real Comet run the exchange behind a `ReusedExchangeExec` has already been
    // converted. Here it has not, and the wrapper is a leaf as far as a transform is concerned, so
    // conversion would never reach the subtree while the coverage count - which unwraps the
    // wrapper - still counts every operator in it as Spark. Undo the reuse and let both copies
    // convert, which is what the counts of a real Comet run reflect.
    case reused: ReusedExchangeExec => stripPreparation(reused.child)
    case WholeStageCodegenExec(child) => stripPreparation(child)
    case InputAdapter(child) => stripPreparation(child)
    case ColumnarToRowExec(child) => stripPreparation(child)
    case RowToColumnarExec(child) => stripPreparation(child)
    case other => other.withNewChildren(other.children.map(stripPreparation))
  }

  /**
   * `plan` with the plan behind each of its subquery expressions replaced by that plan's own
   * preview.
   *
   * Extended explain walks a node's `innerChildren`, which for a `SparkPlan` are the plans owned
   * by its expressions, and counts their operators towards the report. Spark prepares a subquery
   * as a plan in its own right and substitutes it into the outer plan, so leaving those plans
   * untouched here would report every subquery operator as un-accelerated Spark and understate
   * coverage against what Comet really executes.
   */
  private def previewSubqueriesOf(session: SparkSession, plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressions { case subquery: ExecSubqueryExpression =>
      subquery.withNewPlan(previewSubquery(session, subquery.plan))
    }
  }

  private def previewSubquery(
      session: SparkSession,
      subquery: BaseSubqueryExec): BaseSubqueryExec =
    subquery match {
      // Reuse bookkeeping: the plan to preview is one level further down.
      case reused: ReusedSubqueryExec =>
        reused.copy(child = previewSubquery(session, reused.child))
      case other =>
        other
          .withNewChildren(Seq(previewSubqueryPlan(session, other.child)))
          .asInstanceOf[BaseSubqueryExec]
    }

  /**
   * Preview the plan behind a subquery, which Spark prepared as a plan in its own right.
   *
   * A dynamic partition pruning subquery is the exception to that framing:
   * `PlanDynamicPruningFilters` prepares the build plan and only then wraps it in a
   * `BroadcastExchangeExec`, so the plan that went through the post-columnar rules - and the
   * stage `RevertNativeForTransitionHeavyStages` judged - is the exchange's child, not the
   * exchange. Previewing the exchange instead would leave its child a stage bounded at the top by
   * the exchange, which stops the reversion firing, and the report would then count operators as
   * accelerated that the executed plan runs on Spark. Descend through the wrapper and put it
   * back, so the preview keeps the boundary Spark's preparation used.
   */
  private def previewSubqueryPlan(session: SparkSession, plan: SparkPlan): SparkPlan =
    plan match {
      case stage: QueryStageExec => previewSubqueryPlan(session, stage.plan)
      case exchange: BroadcastExchangeExec =>
        exchange.withNewChildren(Seq(previewSubqueryPlan(session, exchange.child)))
      case other => previewOf(session, other)
    }

  /** The listener that reports one plan per query. */
  private class CometPlanOnlyListener extends QueryExecutionListener {

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      report(qe)
    }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
      // Report anyway: the query was planned, which is all this mode describes.
      report(qe)
    }
  }
}
