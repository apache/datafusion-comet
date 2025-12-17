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
import org.apache.spark.sql.comet.CometProjectExec
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.SparkPlan

class CometCostModelSuite extends CometTestBase {

  test("CBO should prefer Comet for fast expressions (length)") {
    withSQLConf(CometConf.COMET_COST_BASED_OPTIMIZATION_ENABLED.key -> "true") {
      withTempView("test_data") {
        createSimpleTestData()
        val query = "SELECT length(text1), length(text2) FROM test_data"
        executeAndCheckOperator(
          query,
          classOf[CometProjectExec],
          "Expected CometProjectExec for fast expression")
      }
    }
  }

  test("CBO should prefer Spark for slow expressions (trim)") {
    withSQLConf(CometConf.COMET_COST_BASED_OPTIMIZATION_ENABLED.key -> "true") {
      withTempView("test_data") {
        createPaddedTestData()
        val query = "SELECT trim(text1), trim(text2) FROM test_data"
        executeAndCheckOperator(
          query,
          classOf[ProjectExec],
          "Expected Spark ProjectExec for slow expression")
      }
    }
  }

  /** Create simple test data for string operations using parquet to prevent pushdown */
  private def createSimpleTestData(): Unit = {
    import testImplicits._
    val df = Seq(
      ("hello world", "test string"),
      ("comet rocks", "another test"),
      ("fast execution", "performance")).toDF("text1", "text2")

    // Write to parquet and read back to prevent projection pushdown
    val tempPath = s"${System.getProperty("java.io.tmpdir")}/comet_cost_test_${System.nanoTime()}"
    df.write.mode("overwrite").parquet(tempPath)

    val parquetDf = spark.read.parquet(tempPath).repartition(5)
    parquetDf.createOrReplaceTempView("test_data")
  }

  /** Create padded test data for trim operations using parquet to prevent pushdown */
  private def createPaddedTestData(): Unit = {
    import testImplicits._
    val df = Seq(
      ("  hello world  ", "  test string  "),
      ("  comet rocks  ", "  another test  "),
      ("  slow execution  ", "  performance  ")).toDF("text1", "text2")

    // Write to parquet and read back to prevent projection pushdown
    val tempPath =
      s"${System.getProperty("java.io.tmpdir")}/comet_cost_test_padded_${System.nanoTime()}"
    df.write.mode("overwrite").parquet(tempPath)

    val parquetDf = spark.read.parquet(tempPath).repartition(5)
    parquetDf.createOrReplaceTempView("test_data")
  }

  /** Execute query and check that the expected operator type is used */
  private def executeAndCheckOperator(
      query: String,
      expectedClass: Class[_],
      message: String): Unit = {

    val result = sql(query)
    result.collect() // Materialize the plan

    val executedPlan = stripAQEPlan(result.queryExecution.executedPlan)

    val hasProjectExec = findProjectExec(executedPlan)

    assert(hasProjectExec.isDefined, "Should have a project operator")
    assert(
      expectedClass.isInstance(hasProjectExec.get),
      s"$message, got ${hasProjectExec.get.getClass.getSimpleName}")
  }

  /** Helper method to find ProjectExec or CometProjectExec in the plan tree */
  private def findProjectExec(plan: SparkPlan): Option[SparkPlan] = {
    // More robust recursive search that handles deep nesting
    def searchPlan(node: SparkPlan): Option[SparkPlan] = {
      if (node.isInstanceOf[ProjectExec] || node.isInstanceOf[CometProjectExec]) {
        Some(node)
      } else {
        // Search all children recursively
        for (child <- node.children) {
          searchPlan(child) match {
            case Some(found) => return Some(found)
            case None => // continue searching
          }
        }
        None
      }
    }

    searchPlan(plan)
  }
}
