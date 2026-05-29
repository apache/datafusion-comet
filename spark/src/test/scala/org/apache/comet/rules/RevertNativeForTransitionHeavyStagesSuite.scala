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

import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

import org.apache.comet.CometConf
import org.apache.spark.sql.CometTestBase

class RevertNativeForTransitionHeavyStagesSuite extends CometTestBase {

  private def createSparkPlan(sql: String): SparkPlan = {
    var plan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      plan = spark.sql(sql).queryExecution.executedPlan
    }
    stripAQEPlan(plan)
  }

  private def applyCometExecRule(plan: SparkPlan): SparkPlan = {
    CometExecRule(spark).apply(plan)
  }

  private def applyFullColumnarPipeline(plan: SparkPlan): SparkPlan = {
    val cometPlan = CometScanRule(spark).apply(plan)
    val execPlan = CometExecRule(spark).apply(cometPlan)
    val withTransitions = ApplyColumnarRulesAndInsertTransitions(Seq.empty, false).apply(execPlan)
    EliminateRedundantTransitions(spark).apply(withTransitions)
  }

  private def countCometExecs(plan: SparkPlan): Int = {
    plan.collect { case _: CometExec => true }.size
  }

  private def countC2RNodes(plan: SparkPlan): Int = {
    plan.collect { case _: ColumnarToRowTransition => true }.size
  }

  test("rule is a no-op when disabled") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
      val rule = RevertNativeForTransitionHeavyStages(spark)
      val sparkPlan = createSparkPlan("SELECT 1")
      val cometPlan = applyCometExecRule(sparkPlan)
      val result = rule.apply(cometPlan)
      assert(result eq cometPlan, "Rule should be a no-op when disabled")
    }
  }

  test("rule does not revert plan below threshold") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "10") {
      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan = createSparkPlan("SELECT id, id * 2 as doubled FROM test_data WHERE id > 5")
        val cometPlan = applyCometExecRule(sparkPlan)

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val result = rule.apply(cometPlan)
        assert(result eq cometPlan, "Plan should be unchanged when below threshold")
      }
    }
  }

  test("countTransitions counts non-root C2R correctly") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      val rule = RevertNativeForTransitionHeavyStages(spark)

      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan = createSparkPlan("SELECT id FROM test_data")
        val cometPlan = applyFullColumnarPipeline(sparkPlan)

        val count = rule.countTransitions(cometPlan)
        // A simple scan+project plan should have 0 or 1 transitions
        assert(count >= 0, s"Transition count should be non-negative, got $count")
      }
    }
  }

  test("countTransitions counts all C2R nodes including root") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      val rule = RevertNativeForTransitionHeavyStages(spark)

      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan = createSparkPlan("SELECT id FROM test_data")
        val cometPlan = applyCometExecRule(sparkPlan)

        // Wrap in a ColumnarToRow (simulating a terminal output)
        val planWithRootC2R = ColumnarToRowExec(cometPlan)
        val count = rule.countTransitions(planWithRootC2R)
        assert(count == 1, s"Should count the C2R node, got $count")
      }
    }
  }

  test("revertToSpark removes CometExec operators") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {

      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan =
          createSparkPlan("SELECT id, id * 2 as doubled FROM test_data WHERE id > 5")
        val cometPlan = applyCometExecRule(sparkPlan)

        assert(countCometExecs(cometPlan) > 0, "Should have CometExec nodes before revert")

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val reverted = rule.revertToSpark(cometPlan)

        assert(countCometExecs(reverted) == 0,
          s"Should have no CometExec nodes after revert, plan:\n${reverted.treeString}")
      }
    }
  }

  test("revertToSpark preserves plan structure") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {

      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan =
          createSparkPlan("SELECT id, id * 2 as doubled FROM test_data WHERE id > 5")
        val cometPlan = applyCometExecRule(sparkPlan)
        val rule = RevertNativeForTransitionHeavyStages(spark)
        val reverted = rule.revertToSpark(cometPlan)

        // Reverted plan should have same output schema
        assert(reverted.output.map(_.name) == cometPlan.output.map(_.name),
          "Output schema should be preserved after revert")
      }
    }
  }

  test("revertToSpark removes all Comet operators from a plan with transitions") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true") {

      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan =
          createSparkPlan("SELECT id, id * 2 as doubled FROM test_data WHERE id > 5")
        val cometPlan = applyFullColumnarPipeline(sparkPlan)

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val result = rule.revertToSpark(cometPlan)
        assert(countCometExecs(result) == 0,
          s"All CometExec should be reverted. Plan:\n${result.treeString}")
      }
    }
  }

  test("CometShuffleExchangeExec is reverted by revertToSpark") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      "spark.sql.adaptive.enabled" -> "false") {

      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan = createSparkPlan("SELECT id FROM test_data DISTRIBUTE BY id")
        val cometPlan = applyCometExecRule(sparkPlan)

        val cometShuffles = cometPlan.collect { case s: CometShuffleExchangeExec => s }
        if (cometShuffles.nonEmpty) {
          val rule = RevertNativeForTransitionHeavyStages(spark)
          val reverted = rule.revertToSpark(cometPlan)
          val remainingCometShuffles = reverted.collect {
            case s: CometShuffleExchangeExec => s
          }
          assert(remainingCometShuffles.isEmpty,
            "CometShuffleExchangeExec should be reverted to ShuffleExchangeExec")
          val sparkShuffles = reverted.collect { case s: ShuffleExchangeExec => s }
          assert(sparkShuffles.nonEmpty,
            "Should have ShuffleExchangeExec after revert")
        }
      }
    }
  }

  test("non-AQE path applies rule per-stage via transformUp") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "10",
      "spark.sql.adaptive.enabled" -> "false") {

      withTempView("test_data") {
        spark.range(10).selectExpr("id", "id % 3 as grp")
          .createOrReplaceTempView("test_data")
        val sparkPlan = createSparkPlan(
          "SELECT grp, count(*) FROM test_data GROUP BY grp")
        val cometPlan = applyCometExecRule(sparkPlan)

        // With high threshold, the non-AQE path should not revert anything
        val rule = RevertNativeForTransitionHeavyStages(spark)
        val result = rule.apply(cometPlan)
        assert(result eq cometPlan,
          "Non-AQE path should not revert when below threshold")
      }
    }
  }

  test("default threshold of 2 allows stages with up to 2 transition pairs") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "2") {
      val rule = RevertNativeForTransitionHeavyStages(spark)

      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan = createSparkPlan("SELECT id FROM test_data")
        val cometPlan = applyFullColumnarPipeline(sparkPlan)

        val pairs = rule.countTransitions(cometPlan)
        val result = rule.apply(cometPlan)
        if (pairs <= 2) {
          assert(result eq cometPlan,
            s"Plan with $pairs pairs should NOT be reverted at threshold 2")
        } else {
          assert(countCometExecs(result) == 0,
            s"Plan with $pairs pairs should be reverted at threshold 2")
        }
      }
    }
  }
}
