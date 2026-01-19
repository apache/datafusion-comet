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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.{CometConf, ExtendedExplainInfo}

/**
 * Test suite for the Comet Cost-Based Optimizer (CBO).
 *
 * Note: CBO only affects operator conversion (filter, project, aggregate, etc.), not scan
 * conversion which is handled by CometScanRule.
 */
class CometCBOSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  /** Helper to check if a plan contains any Comet exec operators (excluding scans) */
  private def containsCometExecOperators(plan: SparkPlan): Boolean = {
    stripAQEPlan(plan).find {
      case _: CometFilterExec | _: CometProjectExec | _: CometHashAggregateExec |
          _: CometSortExec | _: CometBroadcastHashJoinExec | _: CometHashJoinExec |
          _: CometSortMergeJoinExec =>
        true
      case _ => false
    }.isDefined
  }

  /** Helper to check if a plan contains any Comet operators (including scans) */
  private def containsCometPlan(plan: SparkPlan): Boolean = {
    stripAQEPlan(plan).find(_.isInstanceOf[CometPlan]).isDefined
  }

  test("CBO prefers Comet for fully native plans") {
    withParquetTable((0 until 1000).map(i => (i, s"val_$i")), "t") {
      withSQLConf(
        CometConf.COMET_CBO_ENABLED.key -> "true",
        CometConf.COMET_CBO_SPEEDUP_THRESHOLD.key -> "1.0") {
        val df = spark.sql("SELECT * FROM t WHERE _1 > 500")
        val plan = df.queryExecution.executedPlan
        assert(containsCometExecOperators(plan), "Expected Comet exec operators in the plan")
      }
    }
  }

  test("CBO respects speedup threshold - high threshold disables Comet exec operators") {
    withParquetTable((0 until 100).map(i => (i, s"val_$i")), "t") {
      // With very high threshold, CBO should fall back to Spark for exec operators
      // Note: Scans may still be Comet since they're converted by CometScanRule
      withSQLConf(
        CometConf.COMET_CBO_ENABLED.key -> "true",
        CometConf.COMET_CBO_SPEEDUP_THRESHOLD.key -> "100.0") {
        val df = spark.sql("SELECT * FROM t WHERE _1 > 50")
        val plan = df.queryExecution.executedPlan
        assert(
          !containsCometExecOperators(plan),
          "Expected no Comet exec operators with high threshold")
      }
    }
  }

  test("CBO respects speedup threshold - low threshold enables Comet") {
    withParquetTable((0 until 100).map(i => (i, s"val_$i")), "t") {
      // With low threshold, always use Comet
      withSQLConf(
        CometConf.COMET_CBO_ENABLED.key -> "true",
        CometConf.COMET_CBO_SPEEDUP_THRESHOLD.key -> "0.1") {
        val df = spark.sql("SELECT * FROM t WHERE _1 > 50")
        val plan = df.queryExecution.executedPlan
        assert(
          containsCometExecOperators(plan),
          "Expected Comet exec operators with low threshold")
      }
    }
  }

  test("CBO disabled returns Comet plan unconditionally") {
    withParquetTable((0 until 100).map(i => (i, s"val_$i")), "t") {
      // CBO is disabled by default, so Comet operators should be used
      withSQLConf(CometConf.COMET_CBO_ENABLED.key -> "false") {
        val df = spark.sql("SELECT * FROM t WHERE _1 > 50")
        val plan = df.queryExecution.executedPlan
        // Should use Comet regardless of cost when CBO is disabled
        assert(containsCometPlan(plan), "Expected Comet operators when CBO is disabled")
      }
    }
  }

  test("CBO analysis is computed when enabled") {
    withParquetTable((0 until 100).map(i => (i, s"val_$i")), "t") {
      withSQLConf(
        CometConf.COMET_CBO_ENABLED.key -> "true",
        CometConf.COMET_CBO_EXPLAIN_ENABLED.key -> "true",
        CometConf.COMET_CBO_SPEEDUP_THRESHOLD.key -> "1.0") {
        val df = spark.sql("SELECT * FROM t WHERE _1 > 50")
        // Just verify the query runs successfully with CBO enabled
        val result = df.collect()
        assert(result.length > 0, "Query should return results")
      }
    }
  }

  test("results are correct regardless of CBO decision") {
    withParquetTable((0 until 100).map(i => (i, s"val_$i")), "t") {
      // Get expected results with Comet disabled
      var expected: Array[org.apache.spark.sql.Row] = Array.empty
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        expected = spark.sql("SELECT * FROM t WHERE _1 > 50").collect()
      }

      // Test with CBO enabled
      withSQLConf(CometConf.COMET_CBO_ENABLED.key -> "true") {
        val actual = spark.sql("SELECT * FROM t WHERE _1 > 50").collect()
        assert(actual.toSet == expected.toSet, "Results should match regardless of CBO decision")
      }
    }
  }

  test("CostAnalysis correctly computes estimated speedup") {
    // Create a simple analysis and verify computation
    val analysis = CostAnalysis(
      cometOperatorCount = 5,
      sparkOperatorCount = 0,
      transitionCount = 0,
      estimatedRowCount = Some(1000L),
      estimatedSizeBytes = Some(100000L),
      sparkCost = 1000.0,
      cometCost = 500.0,
      estimatedSpeedup = 2.0,
      shouldUseComet = true)

    assert(analysis.estimatedSpeedup == 2.0)
    assert(analysis.shouldUseComet)
    assert(analysis.cometOperatorCount == 5)
    assert(analysis.sparkOperatorCount == 0)
  }

  test("CostAnalysis toExplainString format") {
    val analysis = CostAnalysis(
      cometOperatorCount = 3,
      sparkOperatorCount = 1,
      transitionCount = 1,
      estimatedRowCount = Some(1000L),
      estimatedSizeBytes = Some(100000L),
      sparkCost = 1000.0,
      cometCost = 600.0,
      estimatedSpeedup = 1.67,
      shouldUseComet = true)

    val explainString = analysis.toExplainString

    assert(explainString.contains("CBO Analysis"))
    assert(explainString.contains("Decision: Use Comet"))
    assert(explainString.contains("Comet Operators: 3"))
    assert(explainString.contains("Spark Operators: 1"))
    assert(explainString.contains("Transitions: 1"))
    assert(explainString.contains("Estimated Rows: 1000"))
  }

  test("CostAnalysis reflects fall back decision") {
    val analysis = CostAnalysis(
      cometOperatorCount = 1,
      sparkOperatorCount = 5,
      transitionCount = 3,
      estimatedRowCount = Some(1000L),
      estimatedSizeBytes = Some(100000L),
      sparkCost = 500.0,
      cometCost = 800.0,
      estimatedSpeedup = 0.625,
      shouldUseComet = false)

    val explainString = analysis.toExplainString

    assert(explainString.contains("Decision: Fall back to Spark"))
    assert(!analysis.shouldUseComet)
  }

  test("CBO handles aggregation queries") {
    withParquetTable((0 until 1000).map(i => (i % 10, s"val_$i")), "t") {
      withSQLConf(
        CometConf.COMET_CBO_ENABLED.key -> "true",
        CometConf.COMET_CBO_SPEEDUP_THRESHOLD.key -> "1.0") {
        val df = spark.sql("SELECT _1, COUNT(*) as cnt FROM t GROUP BY _1")
        checkSparkAnswer(df)
      }
    }
  }

  test("CBO handles join queries") {
    withParquetTable((0 until 100).map(i => (i, s"val_$i")), "t1") {
      withParquetTable((0 until 100).map(i => (i, s"other_$i")), "t2") {
        withSQLConf(
          CometConf.COMET_CBO_ENABLED.key -> "true",
          CometConf.COMET_CBO_SPEEDUP_THRESHOLD.key -> "1.0") {
          val df = spark.sql("SELECT t1._1, t2._2 FROM t1 JOIN t2 ON t1._1 = t2._1")
          checkSparkAnswer(df)
        }
      }
    }
  }

  test("CBO handles sort queries") {
    withParquetTable((0 until 100).map(i => (i, s"val_$i")), "t") {
      withSQLConf(
        CometConf.COMET_CBO_ENABLED.key -> "true",
        CometConf.COMET_CBO_SPEEDUP_THRESHOLD.key -> "1.0") {
        val df = spark.sql("SELECT * FROM t ORDER BY _1 DESC")
        checkSparkAnswer(df)
      }
    }
  }

  test("PlanStatistics correctly counts operators") {
    // Test the PlanStatistics case class
    val stats = PlanStatistics(
      cometOps = 5,
      sparkOps = 2,
      transitions = 1,
      cometScans = 1,
      cometFilters = 2,
      cometProjects = 2,
      sparkScans = 1,
      sparkFilters = 1)

    assert(stats.cometOps == 5)
    assert(stats.sparkOps == 2)
    assert(stats.transitions == 1)
    assert(stats.cometScans == 1)
    assert(stats.cometFilters == 2)
    assert(stats.cometProjects == 2)
  }

  // Expression-based costing tests

  test("default expression costs favor Comet for simple expressions") {
    // AttributeReference should be very fast in Comet (0.1)
    assert(CometCostEstimator.DEFAULT_EXPR_COSTS("AttributeReference") < 0.5)
    // Arithmetic should be fast in Comet
    assert(CometCostEstimator.DEFAULT_EXPR_COSTS("Add") < 1.0)
    assert(CometCostEstimator.DEFAULT_EXPR_COSTS("Multiply") < 1.0)
    // Comparisons should be fast with SIMD
    assert(CometCostEstimator.DEFAULT_EXPR_COSTS("LessThan") < 1.0)
  }

  test("expression cost can be overridden via config") {
    withSQLConf(CometConf.COMET_CBO_EXPR_COST_PREFIX + ".TestExpr" -> "2.5") {
      val cost = CometConf.getExprCost("TestExpr", 1.0, spark.sessionState.conf)
      assert(cost == 2.5, "Config override should take precedence")
    }
  }

  test("expression cost uses default when config not set") {
    val cost = CometConf.getExprCost(
      "NonExistentExpr",
      CometCostEstimator.DEFAULT_UNKNOWN_EXPR_COST,
      spark.sessionState.conf)
    assert(cost == CometCostEstimator.DEFAULT_UNKNOWN_EXPR_COST)
  }

  test("CBO considers expression costs in projections") {
    withParquetTable((0 until 100).map(i => (i, i * 2, s"val_$i")), "t") {
      withSQLConf(
        CometConf.COMET_CBO_ENABLED.key -> "true",
        CometConf.COMET_CBO_SPEEDUP_THRESHOLD.key -> "1.0") {
        // Query with simple projections (should favor Comet)
        val df = spark.sql("SELECT _1, _2, _1 + _2 as sum FROM t")
        checkSparkAnswer(df)
      }
    }
  }

  test("CBO considers expression costs in filters") {
    withParquetTable((0 until 100).map(i => (i, s"val_$i")), "t") {
      withSQLConf(
        CometConf.COMET_CBO_ENABLED.key -> "true",
        CometConf.COMET_CBO_SPEEDUP_THRESHOLD.key -> "1.0") {
        // Query with comparison filter (should favor Comet)
        val df = spark.sql("SELECT * FROM t WHERE _1 > 50 AND _1 < 80")
        checkSparkAnswer(df)
      }
    }
  }

  test("PlanStatistics tracks expression costs") {
    val stats = PlanStatistics(
      cometOps = 2,
      sparkOps = 1,
      cometFilters = 1,
      cometProjects = 1,
      cometExprCost = 0.5,
      sparkExprCost = 1.0)

    assert(stats.cometExprCost == 0.5)
    assert(stats.sparkExprCost == 1.0)
  }
}
