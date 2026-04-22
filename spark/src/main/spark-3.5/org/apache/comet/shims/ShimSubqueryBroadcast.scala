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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{InSubqueryExec, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryBroadcastExec}

trait ShimSubqueryBroadcast {

  def getSubqueryBroadcastIndices(sab: SubqueryAdaptiveBroadcastExec): Seq[Int] = {
    Seq(sab.index)
  }

  def getSubqueryBroadcastExecIndices(sub: SubqueryBroadcastExec): Seq[Int] = {
    Seq(sub.index)
  }

  /** Creates a SubqueryBroadcastExec with version-appropriate index parameter. */
  def createSubqueryBroadcastExec(
      name: String,
      indices: Seq[Int],
      buildKeys: Seq[Expression],
      child: SparkPlan): SubqueryBroadcastExec = {
    assert(indices.length == 1, s"Multi-index DPP not supported: indices=$indices")
    SubqueryBroadcastExec(name, indices.head, buildKeys, child)
  }

  // CometPlanAdaptiveDynamicPruningFilters converts all SABs before execution -- to
  // CometSubqueryBroadcastExec (broadcast reuse) or Literal.TrueLiteral (no broadcast).
  // Reaching this method means the rule didn't run, which is a configuration error.
  def resolveSubqueryAdaptiveBroadcast(
      e: InSubqueryExec,
      sab: SubqueryAdaptiveBroadcastExec): Unit = {
    throw new IllegalStateException(
      "SubqueryAdaptiveBroadcastExec should have been converted by " +
        "CometPlanAdaptiveDynamicPruningFilters. This indicates the AQE optimizer rule " +
        "was not registered.")
  }
}
