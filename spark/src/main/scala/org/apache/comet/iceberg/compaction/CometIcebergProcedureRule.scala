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

package org.apache.comet.iceberg.compaction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Hands Iceberg's `rewrite_data_files` procedure to Comet when `spark.comet.iceberg.compaction
 * .enabled` is on. See [[CometProcedureShim]] for how the procedure is intercepted on each Spark
 * version.
 *
 * Spark optimizes the plan of a `CALL` before it invokes the procedure, so this is an optimizer
 * rule. A resolution rule would be too late, because Spark resolves, binds and invokes the
 * procedure in one pass.
 *
 * The rule runs on every optimized plan, so it looks only at the root, where a `CALL` always is.
 */
case class CometIcebergProcedureRule(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = CometProcedureShim.wrap(session, plan)
}
