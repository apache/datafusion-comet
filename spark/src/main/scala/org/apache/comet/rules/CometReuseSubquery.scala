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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, ReusedSubqueryExec, SparkPlan}

/**
 * Re-applies subquery deduplication after Comet node conversions.
 *
 * Spark's ReuseAdaptiveSubquery runs as a queryStageOptimizerRule before postStageCreationRules,
 * which is where CometScanRule/CometExecRule replace Spark operators with Comet equivalents. The
 * Comet rules copy expressions from the original Spark nodes, which can disrupt subquery reuse
 * that was already applied by Spark's rule. This rule runs after Comet conversions to restore
 * proper deduplication.
 *
 * Uses the same algorithm as Spark's ReuseExchangeAndSubquery (subquery portion): top-down
 * traversal via transformAllExpressionsWithPruning, caching by canonical form.
 *
 * For non-AQE, Spark's ReuseExchangeAndSubquery runs after ApplyColumnarRulesAndInsertTransitions
 * in QueryExecution.preparations and handles reuse correctly without this rule.
 *
 * @see
 *   ReuseExchangeAndSubquery (Spark's non-AQE subquery reuse)
 * @see
 *   ReuseAdaptiveSubquery (Spark's AQE subquery reuse)
 */
case object CometReuseSubquery extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.subqueryReuseEnabled) {
      return plan
    }

    val cache = mutable.Map.empty[SparkPlan, BaseSubqueryExec]

    plan.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
      case sub: ExecSubqueryExpression =>
        val cached = cache.getOrElseUpdate(sub.plan.canonicalized, sub.plan)
        if (cached.ne(sub.plan)) {
          sub.withNewPlan(ReusedSubqueryExec(cached))
        } else {
          sub
        }
    }
  }
}
