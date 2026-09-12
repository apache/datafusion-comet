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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometNativeColumnarToRowExec, CometPlan}
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan, UnionExec}

import org.apache.comet.CometConf

class EliminateRedundantTransitionsSuite extends CometTestBase {

  private def sparkPlanFor(sql: String): SparkPlan = {
    var plan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      plan = spark.sql(sql).queryExecution.executedPlan
    }
    stripAQEPlan(plan)
  }

  private def isCometColumnarToRow(plan: SparkPlan): Boolean = plan match {
    case _: CometColumnarToRowExec => true
    case _: CometNativeColumnarToRowExec => true
    case _ => false
  }

  // The rule decides, per `ColumnarToRowExec`, whether the subtree below it is Comet columnar.
  // That check is memoized across the whole plan, so a Comet branch must not make a sibling
  // Spark-only branch look Comet, or the other way around.
  test("sibling branches are classified independently") {
    withParquetTable((0 until 10).map(i => (i, i.toString)), "tbl") {
      val sparkPlan = sparkPlanFor("SELECT _1 FROM tbl")
      val sparkScan = sparkPlan.collectFirst { case p if p.supportsColumnar => p }
      assume(sparkScan.isDefined, "test requires a columnar Spark scan")
      assert(!sparkScan.get.exists(_.isInstanceOf[CometPlan]))

      val cometPlan = CometExecRule(spark).apply(CometScanRule(spark).apply(sparkPlan))
      val cometScan = cometPlan.collectFirst { case p: CometPlan if p.supportsColumnar => p }
      assume(cometScan.isDefined, "test requires a columnar Comet scan")

      // Two columnar branches producing the same output, only one of which is Comet.
      val sparkTransition = ColumnarToRowExec(sparkScan.get)
      val union = UnionExec(Seq(ColumnarToRowExec(cometScan.get), sparkTransition))

      val result = EliminateRedundantTransitions(spark).apply(union)

      val Seq(cometBranch, sparkBranch) = result.children
      assert(
        isCometColumnarToRow(cometBranch),
        s"Comet branch should use a Comet columnar-to-row transition:\n${result.treeString}")
      assert(
        sparkBranch == sparkTransition,
        s"Spark-only branch should be left untouched:\n${result.treeString}")
    }
  }
}
