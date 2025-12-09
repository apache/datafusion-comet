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

import scala.util.Random

import org.apache.spark.sql._
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.CometConf
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

/**
 * Test suite specifically for CometExecRule transformation logic. Tests the rule's ability to
 * transform Spark operators to Comet operators, fallback mechanisms, configuration handling, and
 * edge cases.
 */
class CometExecRuleSuite extends CometTestBase {

  /** Helper method to apply CometExecRule and return the transformed plan */
  private def applyCometExecRule(plan: SparkPlan): SparkPlan = {
    CometExecRule(spark).apply(plan)
  }

  /** Create a test data frame that is used in all tests */
  private def createTestDataFrame = {
    val testSchema = new StructType(
      Array(
        StructField("id", DataTypes.IntegerType, nullable = true),
        StructField("name", DataTypes.StringType, nullable = true)))
    FuzzDataGenerator.generateDataFrame(new Random(42), spark, testSchema, 100, DataGenOptions())
  }

  test("CometExecRule should apply basic operator transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      var df2: DataFrame = null
      var sparkPlan: SparkPlan = null
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        df2 = spark.sql("SELECT id, id * 2 as doubled FROM test_data WHERE id % 2 == 0")
        sparkPlan = df2.queryExecution.executedPlan
      }

      // Count original Spark operators
      val projectOpsOriginal = sparkPlan.collect { case _: ProjectExec => 1 }.sum
      val filterOpsOriginal = sparkPlan.collect { case _: FilterExec => 1 }.sum
      assert(projectOpsOriginal == 1)
      assert(filterOpsOriginal == 1)

      for (cometEnabled <- Seq(true, false)) {
        withSQLConf(
          CometConf.COMET_ENABLED.key -> cometEnabled.toString,
          CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {

          // transform plan
          val transformedPlan = applyCometExecRule(sparkPlan)

          // Count Spark operators after transformation
          val projectOps = transformedPlan.collect { case _: ProjectExec => 1 }.sum
          val filterOps = transformedPlan.collect { case _: FilterExec => 1 }.sum

          // Count Comet operators after transformation
          val cometProjectOps = transformedPlan.collect { case _: CometProjectExec => 1 }.sum
          val cometFilterOps = transformedPlan.collect { case _: CometFilterExec => 1 }.sum

          // Basic sanity checks
          if (cometEnabled) {
            assert(projectOps == 0)
            assert(filterOps == 0)
            assert(cometProjectOps == 1)
            assert(cometFilterOps == 1)
          } else {
            assert(projectOps == 1)
            assert(filterOps == 1)
            assert(cometProjectOps == 0)
            assert(cometFilterOps == 0)
          }

          val result = df2.collect()
          assert(result.length == 55)
        }
      }
    }
  }

}
