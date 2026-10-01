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

package org.apache.comet.local

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CollectLimitExec, ProjectExec, QueryExecution, RangeExec, SparkPlan}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{isCometLoaded, withInfo}
import org.apache.comet.local.shims.LocalModeSupport
import org.apache.comet.shims.ShimCometStreaming

private[comet] case class CometLocalRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!CometConf.COMET_EXEC_LOCAL_ENABLED.get(conf) || plan.isInstanceOf[CometLocalExec]) {
      return plan
    }
    val reason = LocalModeSupport
      .environmentRejection(session.sparkContext.isLocal, conf.adaptiveExecutionEnabled)
      .orElse {
        if (ShimCometStreaming.isStreamingPlan(plan)) {
          Some("Local execution currently requires a batch query")
        } else if (Thread
            .currentThread()
            .getStackTrace
            .exists(frame =>
              frame.getClassName == QueryExecution.getClass.getName &&
                frame.getMethodName == "prepareExecutedPlan")) {
          // As in CometRule's plan-only reporting, this identifies subquery preparation.
          Some("Local execution currently does not admit subqueries")
        } else if (!CometConf.COMET_ENABLED.get(conf) || !CometConf.COMET_EXEC_ENABLED.get(
            conf)) {
          Some("Local execution requires Comet native execution enabled")
        } else if (CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.get(conf)) {
          Some("Local execution does not run in plan-only mode")
        } else None
      }
    if (reason.isDefined) return withInfo(plan, reason.get)

    // Inspect the complete root, never transform matching descendants of an unsupported query.
    // A root CollectLimit is a result-consumption wrapper (take/head), not a native island.
    val (body, wrap) = plan match {
      case limit: CollectLimitExec if limit.offset == 0 =>
        (limit.child, (p: SparkPlan) => limit.copy(child = p))
      case other => (other, (p: SparkPlan) => p)
    }
    def source(p: SparkPlan): Option[RangeExec] = p match {
      case range: RangeExec => Some(range)
      case project: ProjectExec
          if project.projectList.nonEmpty &&
            project.projectList.forall(e => directColumn(e, project.child)) =>
        source(project.child)
      case _ => None
    }
    source(body) match {
      case Some(range)
          if range.numSlices >= 1 && range.numSlices <= 1024 &&
            body.output.size <= 1024 && CometConf.COMET_BATCH_SIZE.get(conf) >= 1 &&
            CometConf.COMET_BATCH_SIZE.get(conf) <= 65536 && isCometLoaded(conf) =>
        wrap(
          CometLocalExec(
            body.output,
            LocalRangeSpec(
              range.start,
              range.end,
              range.step,
              range.numSlices,
              CometConf.COMET_BATCH_SIZE.get(conf),
              body.output.size)))
      case _ =>
        withInfo(
          plan,
          "Local execution currently supports only range and direct column projections " +
            "(up to 1024 partitions/columns and batch size 65536)")
    }
  }

  private def directColumn(expression: NamedExpression, child: SparkPlan): Boolean =
    expression match {
      case attribute: AttributeReference => child.output.exists(_.exprId == attribute.exprId)
      case alias: Alias =>
        alias.child match {
          case attribute: AttributeReference => child.output.exists(_.exprId == attribute.exprId)
          case _ => false
        }
      case _ => false
    }
}
