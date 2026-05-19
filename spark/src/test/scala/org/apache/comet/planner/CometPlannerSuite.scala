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

package org.apache.comet.planner

import scala.util.Random

import org.apache.spark.sql._
import org.apache.spark.sql.CometTestBase
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
 * Behavior tests for `CometPlanner`. Mirrors the scenarios covered by `CometScanRuleSuite` and
 * `CometExecRuleSuite` for the legacy two-rule path, asserting equivalent end-state plans under
 * the new three-phase planner. The planner emits different intermediate plan shapes (no
 * `CometScanExec` wrapper, no `CometSinkPlaceHolder`), so assertions target the final operator
 * classes the planner actually produces:
 *
 *   - V1 native scan: `CometNativeScanExec` directly (not `CometScanExec`).
 *   - Project/Filter/HashAggregate/Broadcast/Shuffle: same `Comet*Exec` classes as legacy.
 *
 * Two redundant-shuffle tests at the bottom (`should not emit Comet shuffle between Spark
 * aggregates`, `should emit Comet shuffle between Comet aggregates`) translate the legacy
 * revert-pass tests (`CometExecRuleSuite` lines 270 / 297) into the planner's upstream-avoidance
 * contract: Phase 2's demand-aware rule should produce the correct shuffle decision in one shot
 * without any revert pass. The first of these is a minimal reproducer for the redundant-Comet-
 * shuffle pattern observed in TPC-DS goldens (see PR #4010 for the legacy fix).
 */
class CometPlannerSuite extends CometTestBase {

  /** Apply CometPlanner to a Spark-only plan and return the transformed plan. */
  private def applyCometPlanner(plan: SparkPlan): SparkPlan = {
    CometPlanner(spark).apply(stripAQEPlan(plan))
  }

  private def createTestDataFrame = {
    val testSchema = new StructType(
      Array(
        StructField("id", DataTypes.IntegerType, nullable = true),
        StructField("name", DataTypes.StringType, nullable = true)))
    FuzzDataGenerator.generateDataFrame(new Random(42), spark, testSchema, 100, DataGenOptions())
  }

  /** Build a SparkPlan with Comet disabled so we get the pre-conversion shape to feed in. */
  private def createSparkPlan(spark: SparkSession, sql: String): SparkPlan = {
    var sparkPlan: SparkPlan = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val df = spark.sql(sql)
      sparkPlan = df.queryExecution.executedPlan
    }
    sparkPlan
  }

  private def countOperators(plan: SparkPlan, opClass: Class[_]): Int = {
    stripAQEPlan(plan).collect {
      case stage: QueryStageExec =>
        countOperators(stage.plan, opClass)
      case op if op.getClass.isAssignableFrom(opClass) => 1
    }.sum
  }

  // --- Scan tests (port of CometScanRuleSuite) -----------------------------------------------

  test("CometPlanner should replace FileSourceScanExec, but only when Comet is enabled") {
    withTempPath { path =>
      createTestDataFrame.write.parquet(path.toString)
      withTempView("test_data") {
        spark.read.parquet(path.toString).createOrReplaceTempView("test_data")

        val sparkPlan =
          createSparkPlan(spark, "SELECT id, id * 2 as doubled FROM test_data WHERE id % 2 == 0")

        assert(countOperators(sparkPlan, classOf[FileSourceScanExec]) == 1)

        for (cometEnabled <- Seq(true, false)) {
          withSQLConf(CometConf.COMET_ENABLED.key -> cometEnabled.toString) {
            val transformedPlan = applyCometPlanner(sparkPlan)

            if (cometEnabled) {
              assert(countOperators(transformedPlan, classOf[FileSourceScanExec]) == 0)
              assert(countOperators(transformedPlan, classOf[CometNativeScanExec]) == 1)
            } else {
              assert(countOperators(transformedPlan, classOf[FileSourceScanExec]) == 1)
              assert(countOperators(transformedPlan, classOf[CometNativeScanExec]) == 0)
            }
          }
        }
      }
    }
  }

  test("CometPlanner should fallback to Spark for ShortType when safety check enabled") {
    withTempPath { path =>
      import org.apache.spark.sql.types._
      val unsupportedSchema = new StructType(
        Array(
          StructField("id", DataTypes.IntegerType, nullable = true),
          StructField("value", DataTypes.ShortType, nullable = true),
          StructField("name", DataTypes.StringType, nullable = true)))

      val testData = Seq(Row(1, 1.toShort, "test1"), Row(2, -1.toShort, "test2"))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), unsupportedSchema)
      df.write.parquet(path.toString)

      withTempView("unsupported_data") {
        spark.read.parquet(path.toString).createOrReplaceTempView("unsupported_data")

        val sparkPlan =
          createSparkPlan(spark, "SELECT id, value FROM unsupported_data WHERE id = 1")

        withSQLConf(
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION,
          CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key -> "true") {
          val transformedPlan = applyCometPlanner(sparkPlan)

          assert(countOperators(transformedPlan, classOf[FileSourceScanExec]) == 1)
          assert(countOperators(transformedPlan, classOf[CometNativeScanExec]) == 0)
        }
      }
    }
  }

  // --- Exec tests (port of CometExecRuleSuite) -----------------------------------------------

  test(
    "CometPlanner should apply basic operator transformations, but only when Comet is enabled") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT id, id * 2 as doubled FROM test_data WHERE id % 2 == 0")

      assert(countOperators(sparkPlan, classOf[ProjectExec]) == 1)
      assert(countOperators(sparkPlan, classOf[FilterExec]) == 1)

      for (cometEnabled <- Seq(true, false)) {
        withSQLConf(
          CometConf.COMET_ENABLED.key -> cometEnabled.toString,
          CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {

          val transformedPlan = applyCometPlanner(sparkPlan)

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

  test("CometPlanner should apply hash aggregate transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometPlanner(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometHashAggregateExec]) == originalHashAggCount)
      }
    }
  }

  test("CometPlanner should not allow Spark partial and Comet final hash aggregate") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      val originalHashAggCount = countOperators(sparkPlan, classOf[HashAggregateExec])
      assert(originalHashAggCount == 2)

      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometPlanner(sparkPlan)

        // If the partial aggregate can't go Comet, the final mustn't either, otherwise we end up
        // with a Spark partial feeding a Comet final and the row<->arrow boundary is in the wrong
        // place. This mirrors the legacy CometExecRuleSuite test of the same name.
        assert(
          countOperators(transformedPlan, classOf[HashAggregateExec]) == originalHashAggCount)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)
      }
    }
  }

  test("CometPlanner should apply broadcast exchange transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan = createSparkPlan(
        spark,
        "SELECT /*+ BROADCAST(b) */ a.id, b.name FROM test_data a JOIN test_data b ON a.id = b.id")

      val originalBroadcastExchangeCount =
        countOperators(sparkPlan, classOf[BroadcastExchangeExec])
      assert(originalBroadcastExchangeCount == 1)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometPlanner(sparkPlan)

        assert(countOperators(transformedPlan, classOf[BroadcastExchangeExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometBroadcastExchangeExec]) == originalBroadcastExchangeCount)
      }
    }
  }

  test("CometPlanner should apply shuffle exchange transformations") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT id, COUNT(*) FROM test_data GROUP BY id ORDER BY id")

      val originalShuffleExchangeCount = countOperators(sparkPlan, classOf[ShuffleExchangeExec])
      assert(originalShuffleExchangeCount == 2)

      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometPlanner(sparkPlan)

        assert(countOperators(transformedPlan, classOf[ShuffleExchangeExec]) == 0)
        assert(
          countOperators(
            transformedPlan,
            classOf[CometShuffleExchangeExec]) == originalShuffleExchangeCount)
      }
    }
  }

  // --- Demand-aware shuffle contract (translation of revert-pass tests) ----------------------

  test("CometPlanner should not emit Comet shuffle between Spark aggregates") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      assert(countOperators(sparkPlan, classOf[ShuffleExchangeExec]) == 1)
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)

      // Disable partial aggregate so both aggregates fall back to Spark JVM. The planner's
      // Phase 2 demand-aware rule should emit Passthrough for the shuffle (selfLikely=true but
      // parent/child both have LIKELY_COMET=false), so the shuffle stays Spark with no revert
      // pass needed. Minimal reproducer for the redundant-shuffle pattern that PR #4010 fixed
      // for the legacy rule via revertRedundantColumnarShuffle.
      withSQLConf(
        CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> "false",
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometPlanner(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 2)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 0)

        assert(countOperators(transformedPlan, classOf[CometShuffleExchangeExec]) == 0)
        assert(countOperators(transformedPlan, classOf[ShuffleExchangeExec]) == 1)
      }
    }
  }

  test("CometPlanner should emit Comet shuffle between Comet aggregates") {
    withTempView("test_data") {
      createTestDataFrame.createOrReplaceTempView("test_data")

      val sparkPlan =
        createSparkPlan(spark, "SELECT COUNT(*), SUM(id) FROM test_data GROUP BY (id % 3)")

      assert(countOperators(sparkPlan, classOf[ShuffleExchangeExec]) == 1)
      assert(countOperators(sparkPlan, classOf[HashAggregateExec]) == 2)

      // Default settings: both aggregates convert to Comet, so the shuffle between them has
      // Comet consumers on both sides and must convert too.
      withSQLConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
        val transformedPlan = applyCometPlanner(sparkPlan)

        assert(countOperators(transformedPlan, classOf[HashAggregateExec]) == 0)
        assert(countOperators(transformedPlan, classOf[CometHashAggregateExec]) == 2)

        assert(countOperators(transformedPlan, classOf[ShuffleExchangeExec]) == 0)
        assert(countOperators(transformedPlan, classOf[CometShuffleExchangeExec]) == 1)
      }
    }
  }
}
