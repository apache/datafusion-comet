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

import java.time.LocalDateTime

import scala.util.Random

import org.apache.spark.sql._
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.serde.operator.CometNativeScan
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

  private val timestampNtzReadReason =
    "Parquet TIMESTAMP_NTZ data columns require Spark's reader to preserve conversion error timing"

  private def writeDateReadFixture(path: String, rows: Int, badRow: Int): Unit = {
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      spark
        .range(0L, rows.toLong, 1L, 1)
        .selectExpr(
          "id",
          s"date_from_unix_date(CASE WHEN id = $badRow THEN 213503983 ELSE 0 END) AS d")
        .write
        .parquet(path)
      val input = spark.read.parquet(path)
      assert(input.inputFiles.length == 1)
      assert(input.rdd.getNumPartitions == 1)
    }
  }

  private def timestampNtzRead(path: String, filterFirstRow: Boolean = false): DataFrame = {
    val input = spark.read.schema("id BIGINT, d TIMESTAMP_NTZ").parquet(path)
    val filtered = if (filterFirstRow) input.filter("id = 0") else input
    filtered.select("d").limit(1)
  }

  private def checkTimestampNtzRead(
      query: => DataFrame,
      expectedOverflow: Boolean,
      expectedColumnar: Option[Boolean] = None,
      expectedSchemaMismatch: Boolean = false): Unit = {
    // Establish Spark's result or precise error independently before checking the Comet route.
    // A comparison that merely runs the same fallback twice would not pin the consumed rows.
    for (cometEnabled <- Seq(false, true)) {
      withSQLConf(CometConf.COMET_ENABLED.key -> cometEnabled.toString) {
        val df = query
        val initialPlan = df.queryExecution.executedPlan
        if (expectedSchemaMismatch) {
          // Spark 3.x rejects physical DATE as NTZ before any value can be converted.
          val error = intercept[Exception](df.collect())
          val mismatch = causeChain(error).collectFirst {
            case cause: SchemaColumnConvertNotSupportedException => cause
          }
          assert(mismatch.isDefined, error)
          assert(mismatch.get.getColumn == "[d]")
          assert(mismatch.get.getPhysicalType == "INT32")
          assert(mismatch.get.getLogicalType == "timestamp_ntz")
        } else if (expectedOverflow) {
          val error = intercept[Exception](df.collect())
          assert(causeChain(error).exists { cause =>
            cause.getClass == classOf[ArithmeticException] && cause.getMessage == "long overflow"
          })
        } else {
          checkAnswer(df, Seq(Row(LocalDateTime.of(1970, 1, 1, 0, 0))))
        }

        for (plan <- Seq(initialPlan, df.queryExecution.executedPlan)) {
          val scans = collect(plan) { case scan: FileSourceScanExec => scan }
          assert(scans.size == 1, plan)
          assert(collect(plan) { case scan: CometNativeScanExec => scan }.isEmpty, plan)
          expectedColumnar.foreach { expected =>
            assert(scans.head.supportsColumnar == expected, plan)
          }
          val bridges = collect(plan) { case scan: CometSparkToColumnarExec => scan }
          assert(bridges.isEmpty, plan)
          if (cometEnabled) {
            assert(
              scans.head
                .getTagValue(CometExplainInfo.FALLBACK_REASONS)
                .getOrElse(Set.empty[String])
                .contains(timestampNtzReadReason))
          }
        }
      }
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"Parquet NTZ read schema preserves Spark conversion timing (AQE=$adaptive)") {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
        "spark.sql.parquet.datetimeRebaseModeInWrite" -> "CORRECTED",
        "spark.sql.parquet.datetimeRebaseModeInRead" -> "CORRECTED",
        SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
        CometConf.COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
        withTempDir { dir =>
          val late = s"${dir.getCanonicalPath}/late"
          val early = s"${dir.getCanonicalPath}/early"
          val valid = s"${dir.getCanonicalPath}/valid"
          writeDateReadFixture(late, 5001, 5000)
          writeDateReadFixture(early, 2, 1)
          writeDateReadFixture(valid, 5001, -1)

          val cases = Seq(
            ("default batches", late, 4096, 8192, false, false),
            ("inverse batches", late, 8192, 4096, true, false),
            ("equal batches consume overflow", late, 8192, 8192, true, false),
            ("equal batches stop before overflow", late, 4096, 4096, false, false),
            ("valid data", valid, 4096, 8192, false, false),
            ("filter stops before unread overflow", late, 4096, 8192, false, true),
            ("filter cannot hide a decoded overflow", early, 8192, 8192, true, true))
          for {
            (label, path, sparkBatch, cometBatch, overflow, filtered) <- cases
            bridge <- Seq(false, true)
          } {
            withClue(s"$label, bridge=$bridge: ") {
              withSQLConf(
                "spark.sql.parquet.columnarReaderBatchSize" -> sparkBatch.toString,
                CometConf.COMET_BATCH_SIZE.key -> cometBatch.toString,
                CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> bridge.toString) {
                // Spark converts the entire reader batch before applying the filter or LIMIT.
                // Capping to min(Spark, Comet) would incorrectly suppress the inverse error.
                checkTimestampNtzRead(
                  timestampNtzRead(path, filtered),
                  overflow,
                  expectedSchemaMismatch = !isSpark40Plus)
              }
            }
          }
        }
      }
    }

    test(s"Parquet NTZ read schema blocks row and columnar bridge reentry (AQE=$adaptive)") {
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
        "spark.sql.parquet.datetimeRebaseModeInWrite" -> "CORRECTED",
        "spark.sql.parquet.datetimeRebaseModeInRead" -> "CORRECTED",
        "spark.sql.parquet.columnarReaderBatchSize" -> "1",
        CometConf.COMET_BATCH_SIZE.key -> "8192",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
        withTempPath { path =>
          writeDateReadFixture(path.getCanonicalPath, 2, 1)
          for {
            nativeScan <- Seq(false, true)
            wholeStage <- Seq(false, true)
          } {
            withClue(s"nativeScan=$nativeScan, wholeStage=$wholeStage: ") {
              withSQLConf(
                CometConf.COMET_NATIVE_SCAN_ENABLED.key -> nativeScan.toString,
                SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString) {
                // With whole-stage codegen off, the Spark reader can return rows. Repacking
                // those rows into an Arrow batch must not read the overflowing second row.
                checkTimestampNtzRead(
                  timestampNtzRead(path.getCanonicalPath),
                  expectedOverflow = false,
                  expectedColumnar = Some(wholeStage),
                  expectedSchemaMismatch = !isSpark40Plus)
              }
            }
          }
        }
      }
    }
  }

  test("Parquet NTZ scan fallback is scoped to requested data columns") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
      withTempDir { dir =>
        val dataPath = s"${dir.getCanonicalPath}/data"
        val partitionPath = s"${dir.getCanonicalPath}/partitioned"
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          sql(
            "SELECT CAST(0 AS BIGINT) AS id, DATE '1970-01-01' AS d, " +
              "CAST('1970-01-01 00:00:00' AS TIMESTAMP_LTZ) AS ltz, " +
              "CAST('1970-01-01 00:00:00' AS TIMESTAMP_NTZ) AS ntz").write
            .parquet(dataPath)
          spark.read
            .parquet(dataPath)
            .select("id", "ntz")
            .write
            .partitionBy("ntz")
            .parquet(partitionPath)
        }

        // Even a real NTZ file is conservatively protected: its logical schema cannot prove
        // that every selected file has timestamp storage rather than a DATE annotation.
        checkTimestampNtzRead(
          spark.read.parquet(dataPath).select("ntz"),
          expectedOverflow = false)

        val prunedExpected = Seq(
          Row(
            0L,
            java.sql.Date.valueOf("1970-01-01"),
            java.sql.Timestamp.from(java.time.Instant.EPOCH)))
        val partitionExpected = Seq(Row(0L, LocalDateTime.of(1970, 1, 1, 0, 0)))
        for (cometEnabled <- Seq(false, true)) {
          withSQLConf(CometConf.COMET_ENABLED.key -> cometEnabled.toString) {
            // NTZ remains in the relation schema but is absent from the requested file data.
            // DATE and LTZ columns retain their native scan support.
            val pruned = spark.read.parquet(dataPath).select("id", "d", "ltz")
            checkAnswer(pruned, prunedExpected)
            val partitioned = spark.read
              .schema("id BIGINT, ntz TIMESTAMP_NTZ")
              .parquet(partitionPath)
            checkAnswer(partitioned, partitionExpected)
            for (df <- Seq(pruned, partitioned)) {
              val nativeScans = collect(df.queryExecution.executedPlan) {
                case scan: CometNativeScanExec => scan
              }
              assert(nativeScans.size == (if (cometEnabled) 1 else 0))
            }
          }
        }
      }
    }
  }

  test("Parquet NTZ read policy checks top-level scalar types, not nested or partition types") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        var scan: FileSourceScanExec = null
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          spark.range(1).write.parquet(path.getCanonicalPath)
          val df = spark.read.parquet(path.getCanonicalPath)
          val scans = collect(df.queryExecution.executedPlan) { case scan: FileSourceScanExec =>
            scan
          }
          assert(scans.size == 1)
          scan = scans.head
        }
        val requestedNtz = scan.copy(requiredSchema = StructType.fromDDL("v TIMESTAMP_NTZ"))
        assert(
          CometNativeScan
            .timestampNtzReadFallbackReason(requestedNtz)
            .contains(timestampNtzReadReason))
        // Nested conversions use parquet_convert_array rather than the newly checked scalar
        // temporal Cast. This guard must not disable their existing native scan support.
        for (ddl <- Seq(
            "v STRUCT<a: TIMESTAMP_NTZ>",
            "v ARRAY<TIMESTAMP_NTZ>",
            "v MAP<TIMESTAMP_NTZ, INT>",
            "v MAP<INT, TIMESTAMP_NTZ>",
            "v STRUCT<a: ARRAY<STRUCT<b: TIMESTAMP_NTZ>>>",
            "v DATE",
            "v TIMESTAMP_LTZ",
            "v ARRAY<STRUCT<a: TIMESTAMP_LTZ>>")) {
          withClue(s"$ddl: ") {
            val requested = scan.copy(requiredSchema = StructType.fromDDL(ddl))
            assert(CometNativeScan.timestampNtzReadFallbackReason(requested).isEmpty)
          }
        }
        val partitionRelation =
          scan.relation.copy(partitionSchema = StructType.fromDDL("p TIMESTAMP_NTZ"))(spark)
        val partitionOnly = scan.copy(relation = partitionRelation)
        assert(CometNativeScan.timestampNtzReadFallbackReason(partitionOnly).isEmpty)
      }
    }
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

}
