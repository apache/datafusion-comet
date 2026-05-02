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

package org.apache.comet.shims

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}

trait ShimCometSparkSessionExtensions {

  protected val EXTENDED_EXPLAIN_PROVIDERS_KEY = "spark.sql.extendedExplainProviders"

  def supportsExtendedExplainInfo(qe: QueryExecution): Boolean = {
    try {
      qe.getClass.getDeclaredMethod(
        "extendedExplainInfo",
        classOf[String => Unit],
        classOf[SparkPlan])
    } catch {
      case _: NoSuchMethodException | _: SecurityException => return false
    }
    true
  }

  // injectQueryStageOptimizerRule not available on Spark 3.4.
  // CometPlanAdaptiveDynamicPruningFilters and CometReuseSubquery are not registered.
  // On 3.4, Spark's PlanAdaptiveDynamicPruningFilters handles SABs directly (converting to
  // TrueLiteral when it can't find BroadcastHashJoinExec, disabling DPP).
  def injectQueryStageOptimizerRuleShim(
      extensions: SparkSessionExtensions,
      rule: Rule[SparkPlan]): Unit = {}

  // Registers a queryStagePrepRule only on Spark < 3.5. The 3.5+ variants no-op this shim.
  // Used by CometSpark34AqeDppFallbackRule, which is a correctness workaround for AQE DPP on
  // Spark 3.4 where injectQueryStageOptimizerRule (and therefore
  // CometPlanAdaptiveDynamicPruningFilters) is unavailable. See
  // CometSpark34AqeDppFallbackRule's class docstring for details.
  def injectPreSpark35QueryStagePrepRuleShim(
      extensions: SparkSessionExtensions,
      rule: Rule[SparkPlan]): Unit = {
    extensions.injectQueryStagePrepRule(_ => rule)
  }
}
