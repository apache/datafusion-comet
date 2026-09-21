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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

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
 */
case class CometRule(session: SparkSession) extends Rule[SparkPlan] {

  private val scanRule = CometScanRule(session)
  private val execRule = CometExecRule(session)

  override def apply(plan: SparkPlan): SparkPlan = execRule.apply(scanRule.apply(plan))
}
