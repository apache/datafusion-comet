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
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.CometConf
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

/**
 * Test suite specifically for CometScanRule transformation logic.
 */
class CometScanRuleSuite extends CometTestBase {

  /** Helper method to apply CometExecRule and return the transformed plan */
  private def applyCometScanRule(plan: SparkPlan): SparkPlan = {
    CometScanRule(spark).apply(stripAQEPlan(plan))
  }

  /** Create a test data frame that is used in all tests */
  private def createTestDataFrame = {
    val testSchema = new StructType(
      Array(
        StructField("id", DataTypes.IntegerType, nullable = true),
        StructField("name", DataTypes.StringType, nullable = true)))
    FuzzDataGenerator.generateDataFrame(new Random(42), spark, testSchema, 100, DataGenOptions())
  }

  /** Create a SparkPlan from the specified SQL with Comet disabled */
  private def createSparkPlan(spark: SparkSession, sql: String): SparkPlan = {
    var sparkPlan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val df = spark.sql(sql)
      sparkPlan = df.queryExecution.executedPlan
    }
    sparkPlan
  }

  /** Count the number of the specified operator in the plan */
  private def countOperators(plan: SparkPlan, opClass: Class[_]): Int = {
    stripAQEPlan(plan).collect {
      case stage: QueryStageExec =>
        countOperators(stage.plan, opClass)
      case op if op.getClass.isAssignableFrom(opClass) => 1
    }.sum
  }

  test("CometExecRule should replace scan, but only when Comet is enabled") {
    withTempPath { path =>
      createTestDataFrame.write.parquet(path.toString)
      withTempView("test_data") {
        spark.read.parquet(path.toString).createOrReplaceTempView("test_data")

        val sparkPlan =
          createSparkPlan(spark, "SELECT id, id * 2 as doubled FROM test_data WHERE id % 2 == 0")

        // Count original Spark operators
        assert(countOperators(sparkPlan, classOf[FileSourceScanExec]) == 1)

        for (cometEnabled <- Seq(true, false)) {
          withSQLConf(CometConf.COMET_ENABLED.key -> cometEnabled.toString) {

            val transformedPlan = applyCometScanRule(sparkPlan)

            if (cometEnabled) {
              assert(countOperators(transformedPlan, classOf[FileSourceScanExec]) == 0)
              assert(countOperators(transformedPlan, classOf[CometScanExec]) == 1)
            } else {
              assert(countOperators(transformedPlan, classOf[FileSourceScanExec]) == 1)
              assert(countOperators(transformedPlan, classOf[CometScanExec]) == 0)
            }
          }
        }
      }
    }
  }

}
