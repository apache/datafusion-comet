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

package org.apache.comet

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometSubqueryBroadcastExec
import org.apache.spark.sql.execution.{ReusedSubqueryExec, SparkPlan, SubqueryExec}

/**
 * Tests for how [[CometCoverageStats]] categorizes plan nodes into accelerated operators,
 * un-accelerated Spark operators, and transitions.
 */
class CometCoverageStatsSuite extends CometTestBase {

  private def somePlan: SparkPlan =
    spark.range(0, 10).selectExpr("id", "id + 1 as x").queryExecution.executedPlan

  test("ReusedSubquery is not counted as an un-accelerated Spark operator") {
    val reused = ReusedSubqueryExec(SubqueryExec("test", somePlan))
    val stats = CometCoverageStats.forPlan(reused)
    assert(stats.sparkOperators == 0)
    assert(stats.cometOperators == 0)
    assert(stats.transitions == 0)
  }

  test("CometSubqueryBroadcast is counted as an accelerated operator") {
    val child = somePlan
    val base = CometCoverageStats.forPlan(child)
    val subqueryBroadcast =
      CometSubqueryBroadcastExec("dpp", Seq(0), Seq(child.output.head), child)
    val stats = CometCoverageStats.forPlan(subqueryBroadcast)
    assert(stats.cometOperators == base.cometOperators + 1)
    assert(stats.sparkOperators == base.sparkOperators)
    assert(stats.transitions == base.transitions)
  }
}
