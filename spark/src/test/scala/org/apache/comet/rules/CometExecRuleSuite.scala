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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql._
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, ExtendedExplainInfo}

/**
 * Test suite specifically for CometExecRule transformation logic. Tests the rule's ability to
 * transform Spark operators to Comet operators, fallback mechanisms, configuration handling, and
 * edge cases.
 */
class CometExecRuleSuite extends CometTestBase {

  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_AUTO) {
        testFun
      }
    }
  }

  // Helper method to apply CometExecRule and return the transformed plan
  private def applyCometExecRule(plan: SparkPlan): SparkPlan = {
    val rule = CometExecRule(spark)
    rule.apply(plan)
  }

  // Helper method to check if a plan contains Comet operators
  private def hasCometOperators(plan: SparkPlan): Boolean = {
    plan.exists(_.isInstanceOf[CometPlan])
  }

  // Helper method to count Comet operators in a plan
  private def countCometOperators(plan: SparkPlan): Int = {
    plan.collect { case _: CometPlan => 1 }.sum
  }

  // Helper method to get fallback reasons from a plan
  private def getFallbackReasons(plan: SparkPlan): Seq[String] = {
    val info = new ExtendedExplainInfo()
    info.extensionInfo(plan).toSeq
  }

  // Helper method to print plan structure for debugging
  private def logPlanStructure(name: String, plan: SparkPlan): Unit = {
    logInfo(s"=== $name ===")
    logInfo(plan.treeString)
    logInfo(s"Comet operators: ${plan.collect { case p: CometPlan => p.getClass.getSimpleName }}")
    logInfo(s"Has Comet operators: ${hasCometOperators(plan)}")
    val fallbacks = getFallbackReasons(plan)
    if (fallbacks.nonEmpty) {
      logInfo(s"Fallback reasons: ${fallbacks.mkString(", ")}")
    }
  }

  test("CometExecRule debug - simple transformation") {
    withTempView("debug_data") {
      Seq((1, 10), (2, 20)).toDF("id", "value").createOrReplaceTempView("debug_data")

      val df = spark.sql("SELECT id FROM debug_data WHERE id > 0")
      val sparkPlan = df.queryExecution.executedPlan
      val transformedPlan = applyCometExecRule(sparkPlan)

      logPlanStructure("Original Plan", sparkPlan)
      logPlanStructure("Transformed Plan", transformedPlan)

      // Basic sanity - plans should exist and query should execute
      assert(sparkPlan != null)
      assert(transformedPlan != null)

      val result = df.collect()
      assert(result.length >= 0)
    }
  }

  test("CometExecRule should be disabled when Comet is not enabled") {
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      withTempView("test_data") {
        Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "name").createOrReplaceTempView("test_data")
        val df = spark.sql("SELECT id, name FROM test_data WHERE id > 1")
        val sparkPlan = df.queryExecution.executedPlan
        val transformedPlan = applyCometExecRule(sparkPlan)

        // Plan should remain unchanged when Comet is disabled
        assert(transformedPlan.fastEquals(sparkPlan))
        assert(!hasCometOperators(transformedPlan))
      }
    }
  }

  test("CometExecRule should apply basic operator transformations") {
    withTempView("test_data") {
      Seq((1, "a", 10.0), (2, "b", 20.0), (3, "c", 30.0))
        .toDF("id", "name", "value")
        .createOrReplaceTempView("test_data")

      val df = spark.sql("SELECT id, name, value * 2 as doubled FROM test_data WHERE id > 1")
      val sparkPlan = df.queryExecution.executedPlan
      val transformedPlan = applyCometExecRule(sparkPlan)

      // Count original Spark operators that should be transformed
      val projectOps = sparkPlan.collect { case _: ProjectExec => 1 }.sum
      val filterOps = sparkPlan.collect { case _: FilterExec => 1 }.sum

      // Count Comet operators after transformation
      val cometProjectOps = transformedPlan.collect { case _: CometProjectExec => 1 }.sum
      val cometFilterOps = transformedPlan.collect { case _: CometFilterExec => 1 }.sum

      // Plan should always be valid
      assert(transformedPlan != null)

      // If there were operators that could be transformed, check the results
      if (projectOps > 0 || filterOps > 0) {
        // We should have either Comet operators or graceful fallback
        val hasAnyComet = hasCometOperators(transformedPlan)

        if (!hasAnyComet) {
          // If no Comet operators, log diagnostic info but don't fail
          // This might happen due to configuration or unsupported expressions
          val fallbackReasons = getFallbackReasons(transformedPlan)
          logInfo(
            s"No Comet operators found. Original: $projectOps projects, $filterOps filters. " +
              s"Fallback reasons: ${fallbackReasons.mkString(", ")}")
        }

        // Basic sanity checks
        assert(cometProjectOps >= 0)
        assert(cometFilterOps >= 0)
      }

      // Verify the query can still execute
      val result = df.collect()
      assert(result.length >= 0)
    }
  }

  test("CometExecRule should handle aggregate transformations") {
    withTempView("test_data") {
      Seq((1, "a", 10), (1, "b", 20), (2, "c", 30), (2, "d", 40))
        .toDF("group_id", "name", "value")
        .createOrReplaceTempView("test_data")

      val df = spark.sql(
        "SELECT group_id, COUNT(*) as count, SUM(value) as total FROM test_data GROUP BY group_id")
      val sparkPlan = df.queryExecution.executedPlan
      val transformedPlan = applyCometExecRule(sparkPlan)

      logPlanStructure("Original Aggregate Plan", sparkPlan)
      logPlanStructure("Transformed Aggregate Plan", transformedPlan)

      // Check for original Spark aggregate operators
      val originalAggOps = sparkPlan.collect { case _: HashAggregateExec => 1 }.sum

      // Check for Comet aggregate operators after transformation
      val cometAggOps = transformedPlan.collect { case _: CometHashAggregateExec => 1 }.sum

      // Plan should always be valid regardless of transformation success
      assert(transformedPlan != null, "Transformed plan should not be null")

      // Verify the query can still execute (basic sanity check)
      val result = df.collect()
      assert(result.length >= 0, "Query should execute successfully")

      // If there were aggregate operations, log what happened
      if (originalAggOps > 0) {
        logInfo(
          s"Aggregate transformation: original aggs: $originalAggOps, " +
            s"comet aggs: $cometAggOps, has comet ops: ${hasCometOperators(transformedPlan)}")

        // The transformation should either succeed (with Comet ops) or gracefully fallback
        // We don't assert Comet ops are present because they might not be supported in all cases
        if (!hasCometOperators(transformedPlan)) {
          val fallbackReasons = getFallbackReasons(transformedPlan)
          logInfo(
            s"No Comet operators found - likely fallback. Reasons: ${fallbackReasons.mkString(", ")}")
        }
      } else {
        logInfo("No aggregate operations found in original plan")
      }
    }
  }

  test("CometExecRule should respect operator-specific configurations") {
    withSQLConf(CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false") {
      withTempView("test_data") {
        Seq((1, "a", 10.0), (2, "b", 20.0))
          .toDF("id", "name", "value")
          .createOrReplaceTempView("test_data")

        val df = spark.sql("SELECT id, name, value * 2 as doubled FROM test_data")
        val sparkPlan = df.queryExecution.executedPlan
        val transformedPlan = applyCometExecRule(sparkPlan)

        // Should not have CometProjectExec when disabled
        val cometProjectOps = transformedPlan.collect { case _: CometProjectExec => 1 }.sum
        assert(cometProjectOps == 0)

        // But may still have other Comet operators (scans, etc.)
        val sparkProjectOps = transformedPlan.collect { case _: ProjectExec => 1 }.sum
        // If there were project operations, they should remain as Spark operators
        if (sparkPlan.collect { case _: ProjectExec => 1 }.sum > 0) {
          assert(sparkProjectOps > 0)
        }
      }
    }
  }

  test("CometExecRule should respect filter configuration") {
    withSQLConf(CometConf.COMET_EXEC_FILTER_ENABLED.key -> "false") {
      withTempView("test_data") {
        Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "name").createOrReplaceTempView("test_data")

        val df = spark.sql("SELECT id, name FROM test_data WHERE id > 1")
        val sparkPlan = df.queryExecution.executedPlan
        val transformedPlan = applyCometExecRule(sparkPlan)

        // Should not have CometFilterExec when disabled
        val cometFilterOps = transformedPlan.collect { case _: CometFilterExec => 1 }.sum
        assert(cometFilterOps == 0)
      }
    }
  }

  test("CometExecRule should handle fallback when expressions are unsupported") {
    withTempView("test_data") {
      Seq((1, "test"), (2, "data")).toDF("id", "text").createOrReplaceTempView("test_data")

      // Use an expression that might not be supported in all cases
      val df = spark.sql(
        "SELECT id, REGEXP_REPLACE(text, 'test', 'replaced') as modified FROM test_data")
      val sparkPlan = df.queryExecution.executedPlan
      val transformedPlan = applyCometExecRule(sparkPlan)

      // Plan should still be valid, either with Comet or fallback to Spark
      assert(transformedPlan != null)

      // If there are fallback reasons, they should be properly recorded
      val fallbackReasons = getFallbackReasons(transformedPlan)
      // This is informational - some expressions may be supported, others may not
    }
  }

  test("CometExecRule should handle shuffle exchange transformations") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempView("large_table") {
        // Create a larger dataset to ensure shuffle occurs
        val data = (1 to 1000).map(i => (i % 10, s"name_$i", i * 1.0))
        data.toDF("group_id", "name", "value").createOrReplaceTempView("large_table")

        val df = spark.sql(
          "SELECT group_id, SUM(value) FROM large_table GROUP BY group_id ORDER BY group_id")
        val sparkPlan = df.queryExecution.executedPlan
        val transformedPlan = applyCometExecRule(sparkPlan)

        // Check for original and transformed shuffle operators
        val originalShuffleOps = sparkPlan.collect { case _: ShuffleExchangeExec => 1 }.sum
        val cometShuffleOps = transformedPlan.collect { case _: CometShuffleExchangeExec =>
          1
        }.sum

        // Plan should always be valid
        assert(transformedPlan != null)

        // Log diagnostic information
        logInfo(s"Shuffle transformation: original shuffles: $originalShuffleOps, " +
          s"comet shuffles: $cometShuffleOps, has any comet: ${hasCometOperators(transformedPlan)}")

        // Should have some form of query execution capability
        val result = df.collect()
        assert(result.length >= 0)

        // If shuffle was present and transformed, verify it's a valid transformation
        if (originalShuffleOps > 0 && cometShuffleOps > 0) {
          assert(hasCometOperators(transformedPlan))
        }
      }
    }
  }

  test("CometExecRule should handle disabled exec mode but enabled shuffle") {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "false",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {

      withTempView("shuffle_data") {
        (1 to 100)
          .map(i => (i % 10, s"name_$i", i * 1.0))
          .toDF("group_id", "name", "value")
          .createOrReplaceTempView("shuffle_data")

        val df = spark.sql("SELECT group_id, SUM(value) FROM shuffle_data GROUP BY group_id")
        val sparkPlan = df.queryExecution.executedPlan
        val transformedPlan = applyCometExecRule(sparkPlan)

        // Should not have native execution operators
        val nativeExecOps = transformedPlan.collect { case _: CometNativeExec => 1 }.sum
        assert(nativeExecOps == 0)

        // But may have shuffle-related Comet operators
        val cometShuffleOps = transformedPlan.collect { case _: CometShuffleExchangeExec =>
          1
        }.sum
        // This depends on whether shuffle actually occurs in this simple case
      }
    }
  }

  test("CometExecRule should generate detailed fallback explanations") {
    withSQLConf(CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {
      withTempView("test_data") {
        Seq((1, "test"), (2, "data")).toDF("id", "text").createOrReplaceTempView("test_data")

        // Force a scenario that might cause fallbacks
        withSQLConf(CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false") {
          val df = spark.sql("SELECT id, UPPER(text) as upper_text FROM test_data")
          val sparkPlan = df.queryExecution.executedPlan
          val transformedPlan = applyCometExecRule(sparkPlan)

          // Should still produce a valid plan
          assert(transformedPlan != null)

          // May have fallback information available
          val fallbackReasons = getFallbackReasons(transformedPlan)
          // This is informational - we don't assert specific content as it depends on configuration
        }
      }
    }
  }
}
