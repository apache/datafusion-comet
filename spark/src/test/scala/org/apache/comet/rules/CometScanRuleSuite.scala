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
import org.apache.comet.lance.LanceIntegration
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

  test("Lance native scan config defaults to disabled") {
    assert(!CometConf.COMET_LANCE_NATIVE_ENABLED.get())
  }

  test("LanceIntegration nativeScanPlan reflection handles fallback paths") {
    class WithNativeScanPlan {
      def nativeScanPlan(): String = "native-plan"
    }
    class WithoutNativeScanPlan
    class ThrowingNativeScanPlan {
      def nativeScanPlan(): String = throw new IllegalStateException("boom")
    }

    val cases = Seq(
      ("present method", new WithNativeScanPlan, Some("native-plan")),
      ("missing method", new WithoutNativeScanPlan, None),
      ("throwing method", new ThrowingNativeScanPlan, None))

    cases.foreach { case (name, scan, expected) =>
      assert(
        LanceIntegration.invokeNativeScanPlan(scan).map(_.toString) == expected,
        s"unexpected reflection result for $name")
    }

    val nonLanceScan = new WithNativeScanPlan
    assert(!LanceIntegration.isLanceScan(nonLanceScan))
    assert(LanceIntegration.nativeScanPlan(nonLanceScan).isEmpty)
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
    val storageOptions = new LinkedHashMap[String, String]()
    storageOptions.put("region", "us-west-2")
    storageOptions.put("endpoint", "http://127.0.0.1:9000")

    val descriptor = new FakeLanceNativeScanPlan(
      descriptorVersion = 1,
      scanId = "scan-123",
      datasetUri = "s3://bucket/table.lance",
      resolvedVersion = 42L,
      sparkReadSchemaJson = requiredSchema.json,
      projectedReadSchemaJson = projectedSchema.json,
      pushedFilterSql = Some("id > 10"),
      limit = Some(100L),
      offset = Some(5L),
      batchSize = 4096,
      storageOptions = storageOptions,
      splits = Arrays.asList(
        new FakeLanceNativeScanSplit(0, Arrays.asList(Int.box(7), Int.box(8))),
        new FakeLanceNativeScanSplit(1, Arrays.asList(Int.box(9)))))

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

  test("Lance native scan serde leaves absent optional pushdowns unset") {
    val serde = loadContribLanceSerde.getOrElse {
      cancel("contrib-lance profile is not enabled")
    }

    val schema = StructType(Seq(StructField("id", DataTypes.IntegerType, nullable = true)))
    val descriptor = new FakeLanceNativeScanPlan(
      descriptorVersion = 1,
      scanId = "",
      datasetUri = "/tmp/table.lance",
      resolvedVersion = 3L,
      sparkReadSchemaJson = schema.json,
      projectedReadSchemaJson = schema.json,
      pushedFilterSql = None,
      limit = None,
      offset = None,
      batchSize = 1024,
      storageOptions = new LinkedHashMap[String, String](),
      splits = Arrays.asList(new FakeLanceNativeScanSplit(0, Arrays.asList(Int.box(1)))))

    val (common, partitions) =
      serializeFakeLanceDescriptor(serde, descriptor, "fallback-scan", schema)

    assert(common.getScanId == "fallback-scan")
    assert(!common.hasFilterSql)
    assert(!common.hasLimit)
    assert(!common.hasOffset)
    assert(partitions.length == 1)
    assert(partitions.head.getPartition.getFragmentIdsList.asScala.map(_.intValue()) == Seq(1))
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
      descriptorVersion: Int,
      scanId: String,
      datasetUri: String,
      resolvedVersion: Long,
      sparkReadSchemaJson: String,
      projectedReadSchemaJson: String,
      pushedFilterSql: Option[String],
      limit: Option[Long],
      offset: Option[Long],
      batchSize: Int,
      storageOptions: java.util.Map[String, String],
      splits: java.util.List[FakeLanceNativeScanSplit]) {
    def getDescriptorVersion(): Int = descriptorVersion
    def getScanId(): String = scanId
    def getDatasetUri(): String = datasetUri
    def getResolvedVersion(): Long = resolvedVersion
    def getSparkReadSchemaJson(): String = sparkReadSchemaJson
    def getProjectedReadSchemaJson(): String = projectedReadSchemaJson
    def hasPushedFilterSql(): Boolean = pushedFilterSql.isDefined
    def getPushedFilterSql(): String = pushedFilterSql.get
    def hasLimit(): Boolean = limit.isDefined
    def getLimit(): Long = limit.get
    def hasOffset(): Boolean = offset.isDefined
    def getOffset(): Long = offset.get
    def getBatchSize(): Int = batchSize
    def getStorageOptions(): java.util.Map[String, String] = storageOptions
    def getSplits(): java.util.List[FakeLanceNativeScanSplit] = splits
  }

  private class FakeLanceNativeScanSplit(splitIndex: Int, fragmentIds: java.util.List[Integer]) {
    def getSplitIndex(): Int = splitIndex
    def getFragmentIds(): java.util.List[Integer] = fragmentIds
  }

}
