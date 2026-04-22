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
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
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
    CometExecRule(spark).apply(stripAQEPlan(plan))
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

  test(
    "CometExecRule should apply basic operator transformations, but only when Comet is enabled") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT id, id * 2 as doubled FROM test_data WHERE id % 2 == 0")

      // Count original Spark operators
      assert(countOperators(sparkPlan, classOf[ProjectExec]) == 1)
      assert(countOperators(sparkPlan, classOf[FilterExec]) == 1)

      for (cometEnabled <- Seq(true, false)) {
        withSQLConf(
          CometConf.COMET_ENABLED.key -> cometEnabled.toString,
          CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {

          val transformedPlan = applyCometExecRule(sparkPlan)

          if (cometEnabled) {
            assert(countOperators(transformedPlan, classOf[ProjectExec]) == 0)
            assert(countOperators(transformedPlan, classOf[FilterExec]) == 0)
            assert(countOperators(transformedPlan, classOf[CometProjectExec]) == 1)
            assert(countOperators(transformedPlan, classOf[CometFilterExec]) == 1)
          } else {
            assert(countOperators(transformedPlan, classOf[ProjectExec]) == 1)
            assert(countOperators(transformedPlan, classOf[FilterExec]) == 1)
            assert(countOperators(transformedPlan, classOf[CometProjectExec]) == 0)
            assert(countOperators(transformedPlan, classOf[CometFilterExec]) == 0)
          }
        }
      }
    }
  }

  test("CometExecRule should apply hash aggregate transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      // Count original Spark operators
      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometHashAggregateExec]) == originalHashAggCount)
      }
    }
  }

  // TODO this test exposes the bug described in
  // https://github.com/apache/datafusion-comet/issues/1389
  ignore("CometExecRule should not allow Comet partial and Spark final hash aggregate") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      // Count original Spark operators
      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // if the final aggregate cannot be converted to Comet, then neither should be
        assert(
          countOperators(transformedPlan, classOf[HashAggregateExec]) == originalHashAggCount)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometExecRule should not allow Spark partial and Comet final hash aggregate") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      // Count original Spark operators
      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        // if the partial aggregate cannot be converted to Comet, then neither should be
        assert(
          countOperators(transformedPlan, classOf[HashAggregateExec]) == originalHashAggCount)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometExecRule should apply broadcast exchange transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(
        spark,
        "SELECT /*+ BROADCAST(b) */ a.id, b.name FROM test_data a JOIN test_data b ON a.id = b.id")

      // Count original Spark operators
      val originalBroadcastExchangeCount =
        countOperators(sparkPlan, classOf[BroadcastExchangeExec])
      assert(originalBroadcastExchangeCount == 1)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[BroadcastExchangeExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometBroadcastExchangeExec]) == originalBroadcastExchangeCount)
      }
    }
  }

  test("CometExecRule should apply shuffle exchange transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT id, COUNT(*) FROM test_data GROUP BY id ORDER BY id")

      // Count original Spark operators
      val originalShuffleExchangeCount = countOperators(sparkPlan, classOf[ShuffleExchangeExec])
      assert(originalShuffleExchangeCount == 2)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometExecRule(sparkPlan)

        assert(countOperators(transformedPlan, classOf[ShuffleExchangeExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometShuffleExchangeExec]) == originalShuffleExchangeCount)
      }
    }
  }

}
