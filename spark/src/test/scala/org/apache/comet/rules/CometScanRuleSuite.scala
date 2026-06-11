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

import java.util.{Arrays, LinkedHashMap}

import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.Try

import org.apache.spark.sql._
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.CometConf
import org.apache.comet.serde.OperatorOuterClass
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

  test("Lance native scan serde reflects descriptor common fields and split fragments") {
    val serde = loadContribLanceSerde.getOrElse {
      cancel("contrib-lance profile is not enabled")
    }

    val requiredSchema = StructType(
      Seq(
        StructField("id", DataTypes.IntegerType, nullable = false),
        StructField("name", DataTypes.StringType, nullable = true)))
    val projectedSchema = StructType(Seq(StructField("id", DataTypes.IntegerType, false)))
    val descriptor = new FakeLanceNativeScanPlan(requiredSchema, projectedSchema)

    val (common, partitions) =
      serializeFakeLanceDescriptor(serde, descriptor, "fallback-scan", requiredSchema)

    assert(common.getScanId == "scan-123")
    assert(common.getDatasetUri == "s3://bucket/table.lance")
    assert(common.getResolvedVersion == 42L)
    assert(common.getDescriptorVersion == 1)
    assert(common.getBatchSize == 4096)
    assert(common.getNativeScanPlanClass.contains("FakeLanceNativeScanPlan"))
    assert(common.getStorageOptionsMap.get("region") == "us-west-2")
    assert(common.getStorageOptionsMap.get("endpoint") == "http://127.0.0.1:9000")
    assert(common.getRequiredSchemaList.asScala.map(_.getName) == Seq("id", "name"))
    assert(common.getProjectedSchemaList.asScala.map(_.getName) == Seq("id"))
    assert(common.hasFilterSql)
    assert(common.getFilterSql == "id > 10")
    assert(common.hasLimit)
    assert(common.getLimit == 100L)
    assert(common.hasOffset)
    assert(common.getOffset == 5L)

    assert(partitions.length == 2)
    assert(partitions(0).getPartition.getPartitionIndex == 0)
    assert(partitions(0).getPartition.getFragmentIdsList.asScala.map(_.intValue()) == Seq(7, 8))
    assert(partitions(1).getPartition.getPartitionIndex == 1)
    assert(partitions(1).getPartition.getFragmentIdsList.asScala.map(_.intValue()) == Seq(9))
  }

  private def loadContribLanceSerde: Option[AnyRef] =
    Try {
      Class
        .forName("org.apache.comet.serde.operator.CometLanceNativeScan$")
        .getField("MODULE$")
        .get(null)
        .asInstanceOf[AnyRef]
    }.toOption

  private def serializeFakeLanceDescriptor(
      serde: AnyRef,
      descriptor: AnyRef,
      fallbackScanId: String,
      fallbackRequiredSchema: StructType)
      : (OperatorOuterClass.LanceScanCommon, Array[OperatorOuterClass.LanceScan]) = {
    val method = serde.getClass.getMethods
      .find(method =>
        method.getName == "serializeNativePlan" && method.getParameterTypes.length == 3)
      .getOrElse {
        throw new AssertionError("CometLanceNativeScan.serializeNativePlan was not found")
      }

    val serialized = method
      .invoke(serde, descriptor, fallbackScanId, fallbackRequiredSchema)
      .asInstanceOf[Product]
    val commonBytes = serialized.productElement(0).asInstanceOf[Array[Byte]]
    val partitionBytes = serialized.productElement(1).asInstanceOf[Array[Array[Byte]]]

    (
      OperatorOuterClass.LanceScanCommon.parseFrom(commonBytes),
      partitionBytes.map(OperatorOuterClass.LanceScan.parseFrom))
  }

  private class FakeLanceNativeScanPlan(
      requiredSchema: StructType,
      projectedSchema: StructType) {
    private val storageOptions = new LinkedHashMap[String, String]()
    storageOptions.put("region", "us-west-2")
    storageOptions.put("endpoint", "http://127.0.0.1:9000")

    def getDescriptorVersion(): Int = 1
    def getScanId(): String = "scan-123"
    def getDatasetUri(): String = "s3://bucket/table.lance"
    def getResolvedVersion(): Long = 42L
    def getSparkReadSchemaJson(): String = requiredSchema.json
    def getProjectedReadSchemaJson(): String = projectedSchema.json
    def hasPushedFilterSql(): Boolean = true
    def getPushedFilterSql(): String = "id > 10"
    def hasLimit(): Boolean = true
    def getLimit(): Long = 100L
    def hasOffset(): Boolean = true
    def getOffset(): Long = 5L
    def getBatchSize(): Int = 4096
    def getStorageOptions(): java.util.Map[String, String] = storageOptions
    def getSplits(): java.util.List[FakeLanceNativeScanSplit] =
      Arrays.asList(
        new FakeLanceNativeScanSplit(0, Arrays.asList(Int.box(7), Int.box(8))),
        new FakeLanceNativeScanSplit(1, Arrays.asList(Int.box(9))))
  }

  private class FakeLanceNativeScanSplit(splitIndex: Int, fragmentIds: java.util.List[Integer]) {
    def getSplitIndex(): Int = splitIndex
    def getFragmentIds(): java.util.List[Integer] = fragmentIds
  }
}
