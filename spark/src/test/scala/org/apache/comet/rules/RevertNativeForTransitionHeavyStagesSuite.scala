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
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._

import org.apache.comet.CometConf

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

  /**
   * Returns every node that produces a columnar output but consumes a row-based child without a
   * RowToColumnar transition. Such a node is an invalid columnar/row boundary: a columnar parent
   * (e.g. a native CometShuffleExchangeExec) requires columnar input. RowToColumnarExec and
   * CometSparkToColumnarExec are the legitimate row->columnar bridges and are excluded.
   */
  private def invalidColumnarBoundaries(plan: SparkPlan): Seq[SparkPlan] = {
    plan.collect {
      case n
          if n.supportsColumnar && !n.isInstanceOf[RowToColumnarTransition] &&
            n.children.exists(c => !c.supportsColumnar) =>
        n
    }
  }

  test("rule is a no-op when disabled") {
    withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan = createSparkPlan("SELECT id, id * 2 FROM test_data WHERE id > 5")
        val cometPlan = applyCometExecRule(sparkPlan)
        assert(countCometExecs(cometPlan) > 0, "Plan should have CometExec nodes")

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val result = rule.apply(cometPlan)
        assert(result eq cometPlan, "Rule should be a no-op when disabled")
      }
    }
  }

  test("rule does not revert plan below threshold") {
    withSQLConf(
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "10",
      "spark.comet.exec.project.enabled" -> "false") {
      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan =
          createSparkPlan("SELECT id, id * 2 as doubled FROM test_data WHERE id > 5")
        val cometPlan = applyFullColumnarPipeline(sparkPlan)

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val transitions = rule.countTransitions(cometPlan)
        assert(transitions > 0, s"Plan should have transitions, got $transitions")
        assert(transitions <= 10, "Transitions should be below threshold")

        val result = rule.apply(cometPlan)
        assert(result eq cometPlan, "Plan should be unchanged when below threshold")
      }
    }
  }

  test("revertToSpark preserves plan structure") {
    withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {

      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan =
          createSparkPlan("SELECT id, id * 2 as doubled FROM test_data WHERE id > 5")
        val cometPlan = applyCometExecRule(sparkPlan)
        val rule = RevertNativeForTransitionHeavyStages(spark)
        val reverted = rule.revertToSpark(cometPlan)

        // Reverted plan should have same output schema
        assert(
          reverted.output.map(_.name) == cometPlan.output.map(_.name),
          "Output schema should be preserved after revert")
      }
    }
  }

  test("revertToSpark removes all Comet operators from a plan with transitions") {
    withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {

      withTempView("test_data") {
        spark.range(10).toDF("id").createOrReplaceTempView("test_data")
        val sparkPlan =
          createSparkPlan("SELECT id, id * 2 as doubled FROM test_data WHERE id > 5")
        val cometPlan = applyFullColumnarPipeline(sparkPlan)
        assert(countCometExecs(cometPlan) > 0, "Should have CometExec nodes before revert")

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val result = rule.revertToSpark(cometPlan)
        assert(
          countCometExecs(result) == 0,
          s"All CometExec should be reverted. Plan:\n${result.treeString}")
      }
    }
  }

  test("non-AQE path applies rule per-stage via transformUp") {
    withSQLConf(
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "10",
      "spark.sql.adaptive.enabled" -> "false") {

      withTempView("test_data") {
        spark
          .range(10)
          .selectExpr("id", "id % 3 as grp")
          .createOrReplaceTempView("test_data")
        val sparkPlan = createSparkPlan("SELECT grp, count(*) FROM test_data GROUP BY grp")
        val cometPlan = applyCometExecRule(sparkPlan)

        // With high threshold, the non-AQE path should not revert anything
        val rule = RevertNativeForTransitionHeavyStages(spark)
        val result = rule.apply(cometPlan)
        assert(result eq cometPlan, "Non-AQE path should not revert when below threshold")
      }
    }
  }

  test("revert fires with unsupported UDF producing transitions") {
    withParquetTable((0 until 100).map(i => (i, i % 10, s"val_$i")), "tbl") {
      spark.udf.register("identity_udf", (x: Int) => x)
      val query = "SELECT _2, identity_udf(_1), count(*) FROM tbl GROUP BY _2, identity_udf(_1)"

      // Without revert, plan should have transitions due from UDF
      withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
        val df = sql(query)
        df.collect()
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        assert(countC2RNodes(plan) > 0, "UDF should cause C2R transitions")
      }

      // With threshold 0, stage should be reverted
      withSQLConf(
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
        val (_, cometPlan) = checkSparkAnswer(query)
        val executedPlan = stripAQEPlan(cometPlan)
        assert(
          countCometExecs(executedPlan) == 0,
          s"Revert should have removed all CometExec nodes:\n${executedPlan.treeString}")
      }
    }
  }

  test("revert fires and produces correct results when transitions exceed threshold") {
    withParquetTable((0 until 100).map(i => (i, i % 10, s"val_$i")), "tbl") {
      val query = "SELECT _2, count(*), sum(_1) FROM tbl GROUP BY _2"

      // Without revert, plan should have CometExec nodes with transitions
      withSQLConf(
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false",
        "spark.comet.exec.project.enabled" -> "false") {
        val df = sql(query)
        df.collect()
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        assert(countCometExecs(plan) > 0, "Plan without revert should have CometExec nodes")
        assert(countC2RNodes(plan) > 0, "Plan without revert should have C2R transitions")
      }

      // With revert enabled at threshold 0, all CometExec should be removed
      withSQLConf(
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0",
        "spark.comet.exec.project.enabled" -> "false") {
        val (_, cometPlan) = checkSparkAnswer(query)
        val executedPlan = stripAQEPlan(cometPlan)
        assert(
          countCometExecs(executedPlan) == 0,
          s"Revert should have removed all CometExec nodes:\n${executedPlan.treeString}")
      }
    }
  }

  test("revertToSpark must not revert native operators across a shuffle stage boundary") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withParquetTable((0 until 100).map(i => (i, i % 10)), "tbl") {
        // A GROUP BY produces partial-agg -> native shuffle -> final-agg, i.e. two stages.
        val df = sql("SELECT _2, count(*) FROM tbl GROUP BY _2")
        df.collect()
        val cometPlan = stripAQEPlan(df.queryExecution.executedPlan)

        val shuffles = cometPlan.collect { case s: CometShuffleExchangeExec => s }
        assume(shuffles.nonEmpty, "test requires a native CometShuffleExchangeExec")
        assert(
          shuffles.map(s => countCometExecs(s.child)).sum > 0,
          "expected native CometExec operators below the shuffle")
        assert(
          invalidColumnarBoundaries(cometPlan).isEmpty,
          s"precondition: original plan should be valid:\n${cometPlan.treeString}")

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val reverted = rule.revertToSpark(cometPlan)

        val invalid = invalidColumnarBoundaries(reverted)
        assert(
          invalid.isEmpty,
          "revertToSpark produced invalid columnar/row boundaries " +
            s"(${invalid.map(_.nodeName).mkString(", ")}):\n${reverted.treeString}")
      }
    }
  }

  test("non-AQE apply must not produce an invalid plan when the result stage reverts") {
    withSQLConf(
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      // Threshold 0 forces the result stage (above the topmost shuffle) to revert.
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0",
      "spark.sql.adaptive.enabled" -> "false") {
      withParquetTable((0 until 100).map(i => (i, i % 10)), "tbl") {
        var cometPlan: SparkPlan = null
        withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
          val df = sql("SELECT _2, count(*) FROM tbl GROUP BY _2")
          df.collect()
          cometPlan = stripAQEPlan(df.queryExecution.executedPlan)
        }
        assume(
          cometPlan.collect { case s: CometShuffleExchangeExec => s }.nonEmpty,
          "test requires a native CometShuffleExchangeExec")

        val rule = RevertNativeForTransitionHeavyStages(spark)
        val result = rule.apply(cometPlan)

        val invalid = invalidColumnarBoundaries(result)
        assert(
          invalid.isEmpty,
          "rule.apply produced invalid columnar/row boundaries " +
            s"(${invalid.map(_.nodeName).mkString(", ")}):\n${result.treeString}")
      }
    }
  }
}
