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

  test("CometExecRule should replace FileSourceScanExec, but only when Comet is enabled") {
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

  test("CometScanRule should fallback to Spark for ShortType when safety check enabled") {
    withTempPath { path =>
      // Create test data with ShortType which may be from unsigned UINT_8
      import org.apache.spark.sql.types._
      val unsupportedSchema = new StructType(
        Array(
          StructField("id", DataTypes.IntegerType, nullable = true),
          StructField(
            "value",
            DataTypes.ShortType,
            nullable = true
          ), // May be from unsigned UINT_8
          StructField("name", DataTypes.StringType, nullable = true)))

      val testData = Seq(Row(1, 1.toShort, "test1"), Row(2, -1.toShort, "test2"))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), unsupportedSchema)
      df.write.parquet(path.toString)

      withTempView("unsupported_data") {
        spark.read.parquet(path.toString).createOrReplaceTempView("unsupported_data")

        val sparkPlan =
          createSparkPlan(spark, "SELECT id, value FROM unsupported_data WHERE id = 1")

        withSQLConf(CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key -> "true") {
          val transformedPlan = applyCometScanRule(sparkPlan)

          // Should fallback to Spark due to ShortType (may be from unsigned UINT_8)
          assert(countOperators(transformedPlan, classOf[FileSourceScanExec]) == 1)
          assert(countOperators(transformedPlan, classOf[CometScanExec]) == 0)
        }
      }
    }
  }

  test("CometScanRule should fallback to Spark for unsupported _metadata columns") {
    withTempPath { path =>
      createTestDataFrame.write.parquet(path.toString)

      // A temp view's output schema is fixed at creation time (here [id, name]), so
      // `_metadata` cannot resolve through one; query the relation directly.
      for (df <- Seq(
          spark.read.parquet(path.toString).select("id", "_metadata"),
          spark.read.parquet(path.toString).selectExpr("id", "_metadata.row_index"))) {
        var sparkPlan: SparkPlan = null
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          sparkPlan = df.queryExecution.executedPlan
        }
        val transformedPlan = applyCometScanRule(stripAQEPlan(sparkPlan))
        assert(countOperators(transformedPlan, classOf[FileSourceScanExec]) == 1)
        assert(countOperators(transformedPlan, classOf[CometScanExec]) == 0)
      }
    }
  }

  test("CometScanRule should report the metadata-column fallback reason for a V1 scan") {
    // Companion to the test above: that one pins *that* a `_metadata` scan falls back, this one
    // pins the reason the user is shown.
    //
    // It also pins where the guard sits relative to the contrib hook. `transformV1Scan` now offers
    // the scan to `CometScanContrib` before applying any built-in guard, so a contrib that
    // synthesises `_metadata` in its own reader still gets a chance to claim it. On a default
    // build the registry is empty, the hook declines instantly, and the guard below must report
    // exactly what it did before the hook existed.
    withTempPath { path =>
      // One file, so `row_index` is unique across the result and `(id, row_index)` is a total
      // order -- the generated `id`s are not unique (Int.MinValue repeats), so ordering by `id`
      // alone would leave tied rows free to come back in either order.
      createTestDataFrame.repartition(1).write.parquet(path.toString)
      checkSparkAnswerAndFallbackReason(
        spark.read
          .parquet(path.toString)
          // `row_index` is generated per row by the reader, so unlike the file-constant metadata
          // columns (`file_path`, `file_size`, ...) it is not supported natively.
          .selectExpr("id", "_metadata.row_index")
          .orderBy("id", "row_index"),
        // Spark rewrites `_metadata.row_index` into a `_tmp_metadata_row_index` scan column, so
        // that -- not `row_index` -- is the attribute the guard sees and names.
        "Metadata column(s) _tmp_metadata_row_index is not supported")
    }
  }

  test("Lance native scan config defaults to disabled") {
    assert(!CometConf.COMET_LANCE_NATIVE_ENABLED.get())
  }

}
