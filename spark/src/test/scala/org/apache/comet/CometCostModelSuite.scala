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

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.CometProjectExec
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

class CometCostModelSuite extends CometTestBase {

  // Fast expressions in Comet (high acceleration factor -> low cost -> preferred)
  // Based on CometCostModel estimates:
  // - length: 9.1x -> cost = 1/9.1 = 0.11 (very low cost, Comet preferred)
  // - reverse: 6.9x -> cost = 1/6.9 = 0.145 (low cost, Comet preferred)

  // Slow expressions in Comet (low acceleration factor -> high cost -> Spark preferred)
  // - trim: 0.4x -> cost = 1/0.4 = 2.5 (high cost, Spark preferred)
  // - ascii: 0.6x -> cost = 1/0.6 = 1.67 (high cost, Spark preferred)

  test("CBO should prefer Comet for fast expressions (length)") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_COST_BASED_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "true") {

      withTempView("test_data") {
        // Create test data
        import testImplicits._
        val df = Seq(
          ("hello world", "test string"),
          ("comet rocks", "another test"),
          ("fast execution", "performance")).toDF("text1", "text2")
        df.createOrReplaceTempView("test_data")

        // Query using length function (fast in Comet: 9.1x acceleration)
        val query = "SELECT length(text1) as len1, length(text2) as len2 FROM test_data"
        val result = sql(query)

        // Execute the query to materialize the plan
        result.collect()

        // Check that Comet is used for the projection (due to low cost)
        val executedPlan = stripAQEPlan(result.queryExecution.executedPlan)
        val hasProjectExec = findProjectExec(executedPlan)

        // With CBO enabled and fast expressions, we should see CometProjectExec
        assert(hasProjectExec.isDefined, "Should have a project operator")
        assert(
          hasProjectExec.get.isInstanceOf[CometProjectExec],
          s"Expected CometProjectExec for fast expression, got ${hasProjectExec.get.getClass.getSimpleName}")
      }
    }
  }

  test("CBO should prefer Spark for slow expressions (trim)") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_COST_BASED_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "true") {

      withTempView("test_data") {
        // Create test data
        import testImplicits._
        val df = Seq(
          ("  hello world  ", "  test string  "),
          ("  comet rocks  ", "  another test  "),
          ("  slow execution  ", "  performance  ")).toDF("text1", "text2")
        df.createOrReplaceTempView("test_data")

        // Query using trim function (slow in Comet: 0.4x acceleration)
        val query = "SELECT trim(text1) as trimmed1, trim(text2) as trimmed2 FROM test_data"
        val result = sql(query)

        // Execute the query to materialize the plan
        result.collect()

        // Check that Spark is used for the projection (due to high cost)
        val executedPlan = stripAQEPlan(result.queryExecution.executedPlan)
        val hasProjectExec = findProjectExec(executedPlan)

        // With CBO enabled and slow expressions, we should see Spark ProjectExec
        assert(hasProjectExec.isDefined, "Should have a project operator")
        assert(
          hasProjectExec.get.isInstanceOf[ProjectExec],
          s"Expected Spark ProjectExec for slow expression, got ${hasProjectExec.get.getClass.getSimpleName}")
      }
    }
  }

  test("Without CBO, Comet should be used regardless of expression cost") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_COST_BASED_OPTIMIZATION_ENABLED.key -> "false", // CBO disabled
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "true") {

      withTempView("test_data") {
        import testImplicits._
        val df = Seq(("  hello world  ", "  test string  ")).toDF("text1", "text2")
        df.createOrReplaceTempView("test_data")

        // Query using trim function (slow in Comet, but CBO is disabled)
        val query = "SELECT trim(text1) as trimmed1 FROM test_data"
        val result = sql(query)

        result.collect()

        val executedPlan = stripAQEPlan(result.queryExecution.executedPlan)
        val hasProjectExec = findProjectExec(executedPlan)

        // Without CBO, Comet should be used even for slow expressions
        assert(hasProjectExec.isDefined, "Should have a project operator")
        assert(
          hasProjectExec.get.isInstanceOf[CometProjectExec],
          s"Expected CometProjectExec when CBO disabled, got ${hasProjectExec.get.getClass.getSimpleName}")
      }
    }
  }

  test("Mixed expressions should use appropriate operators per expression cost") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_COST_BASED_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "true") {

      withTempView("test_data") {
        import testImplicits._
        val df = Seq(("hello world", "test")).toDF("text1", "text2")
        df.createOrReplaceTempView("test_data")

        // Query mixing fast (length: 9.1x) and slow (ascii: 0.6x) expressions
        val query = "SELECT length(text1) as fast_expr, ascii(text1) as slow_expr FROM test_data"
        val result = sql(query)

        result.collect()

        // The overall cost should be average: (9.1 + 0.6) / 2 = 4.85
        // Cost = 1/4.85 = 0.206, which should still prefer Comet
        val executedPlan = stripAQEPlan(result.queryExecution.executedPlan)
        val hasProjectExec = findProjectExec(executedPlan)

        assert(hasProjectExec.isDefined, "Should have a project operator")
        // Mixed expressions with overall positive acceleration should use Comet
        assert(
          hasProjectExec.get.isInstanceOf[CometProjectExec],
          s"Expected CometProjectExec for mixed expressions with positive average, got ${hasProjectExec.get.getClass.getSimpleName}")
      }
    }
  }

  /**
   * Helper method to find ProjectExec or CometProjectExec in the plan tree
   */
  private def findProjectExec(plan: SparkPlan): Option[SparkPlan] = {
    if (plan.isInstanceOf[ProjectExec] || plan.isInstanceOf[CometProjectExec]) {
      Some(plan)
    } else {
      plan.children.flatMap(findProjectExec).headOption
    }
  }
}
