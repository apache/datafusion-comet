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
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.internal.SQLConf

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

  private def collectCometAggregates(plan: SparkPlan): Seq[CometHashAggregateExec] = {
    val current = plan match {
      case aggregate: CometHashAggregateExec => Seq(aggregate)
      case _ => Seq.empty
    }
    val descendants = plan match {
      case stage: QueryStageExec => collectCometAggregates(stage.plan)
      case _ => plan.children.flatMap(collectCometAggregates)
    }
    current ++ descendants
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
      val query = "SELECT _2, identity_udf(_1), max(_1) FROM tbl GROUP BY _2, identity_udf(_1)"

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
      val query = "SELECT _2, min(_1), sum(_1) FROM tbl GROUP BY _2"

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

  for (adaptive <- Seq(false, true)) {
    test(s"transition reversion preserves incompatible aggregate buffers with AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
        withParquetTable((0 until 256).map(i => (i % 4, i.toDouble)), "tbl") {
          val (_, plan) =
            checkSparkAnswer("SELECT _1, percentile(_2, 0.5) FROM tbl GROUP BY _1 ORDER BY _1")
          val executedPlan = stripAQEPlan(plan)
          val aggregates = collectCometAggregates(executedPlan)
          assert(aggregates.exists(_.modes == Seq(Partial)), s"$executedPlan")
          assert(aggregates.exists(_.modes == Seq(Final)), s"$executedPlan")
        }
      }
    }
  }

  test("transition reversion finds an incompatible aggregate below another aggregate") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
      withParquetTable((0 until 256).map(i => (i % 4, i.toDouble)), "tbl") {
        val query =
          """SELECT grouping_key, percentile(inner_percentile, 0.5)
            |FROM (
            |  SELECT _1 AS grouping_key, percentile(_2, 0.5) AS inner_percentile
            |  FROM tbl
            |  GROUP BY _1
            |) inner_aggregate
            |GROUP BY grouping_key""".stripMargin
        val (_, plan) = checkSparkAnswer(query)
        val executedPlan = stripAQEPlan(plan)
        val aggregates = collectCometAggregates(executedPlan)
        assert(
          executedPlan.collect { case _: CometShuffleExchangeExec => true }.size == 1,
          s"test requires one exchange below the nested aggregates:\n$executedPlan")
        assert(aggregates.count(_.modes == Seq(Partial)) == 2, s"$executedPlan")
        assert(aggregates.count(_.modes == Seq(Final)) == 2, s"$executedPlan")
      }
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"transition reversion does not split native COUNT stages with AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
        withParquetTable((0 until 256).map(i => (i % 4, i)), "tbl") {
          val (_, plan) = checkSparkAnswer("SELECT _1, count(*) FROM tbl GROUP BY _1 ORDER BY _1")
          val executedPlan = stripAQEPlan(plan)
          val aggregates = collectCometAggregates(executedPlan)
          assert(aggregates.exists(_.modes == Seq(Partial)), s"$executedPlan")
          assert(aggregates.exists(_.modes == Seq(Final)), s"$executedPlan")
        }
      }
    }
  }

  test("transition reversion preserves an incompatible native partial producer") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
      withParquetTable((0 until 256).map(i => (i % 4, i.toDouble)), "tbl") {
        val nativePlan =
          sql("SELECT _1, percentile(_2, 0.5) FROM tbl GROUP BY _1").queryExecution.executedPlan
        val partial = nativePlan
          .collectFirst {
            case aggregate: CometHashAggregateExec if aggregate.modes == Seq(Partial) => aggregate
          }
          .getOrElse(fail(s"expected a native partial aggregate:\n$nativePlan"))
        val producerWithTransition = partial.withNewChildren(
          Seq(CometSparkToColumnarExec(CometNativeColumnarToRowExec(partial.child))))
        val reverter = RevertNativeForTransitionHeavyStages(spark)
        assert(reverter.countTransitions(producerWithTransition) == 1)

        var reverted: SparkPlan = null
        withSQLConf(
          CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
          CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
          reverted = reverter(producerWithTransition)
        }
        assert(reverted eq producerWithTransition)
      }
    }
  }
}
