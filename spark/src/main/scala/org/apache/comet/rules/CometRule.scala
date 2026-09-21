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
 * The two were previously registered as separate rules, adjacently, in both the columnar and the
 * query-stage-prep paths. Nothing ever ran between them, and neither is useful on its own -
 * [[CometExecRule]] seeds its native chain only from the nodes [[CometScanRule]] produces
 * (`CometScanExec`, `CometBatchScanExec`, `CometContribScanMarker`), so operator conversion
 * against unconverted Spark scans converts nothing. Composing them here makes that ordering an
 * invariant of the code rather than of the registration order, and gives callers that need the
 * whole conversion - rather than half of it - a single entry point.
 *
 * The two rules keep their own classes, files and tests; this only fixes how they are sequenced.
 * `ruleName` in `spark.comet.explain.transformations` output is still each inner rule's own,
 * since this delegates to their `apply`.
 */
case class CometRule(session: SparkSession) extends Rule[SparkPlan] {

  private val scanRule = CometScanRule(session)
  private val execRule = CometExecRule(session)

  override def apply(plan: SparkPlan): SparkPlan = execRule.apply(scanRule.apply(plan))
}
