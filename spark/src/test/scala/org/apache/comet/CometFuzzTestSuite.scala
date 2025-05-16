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

import java.io.File
import java.text.SimpleDateFormat

import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.{CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType
import org.apache.spark.sql.types._

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometFuzzTestSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private var filename: String = null

  /**
   * We use Asia/Kathmandu because it has a non-zero number of minutes as the offset, so is an
   * interesting edge case. Also, this timezone tends to be different from the default system
   * timezone.
   *
   * Represents UTC+5:45
   */
  private val defaultTimezone = "Asia/Kathmandu"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tempDir = System.getProperty("java.io.tmpdir")
    filename = s"$tempDir/CometFuzzTestSuite_${System.currentTimeMillis()}.parquet"
    val random = new Random(42)
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {
      val options =
        DataGenOptions(
          generateArray = true,
          generateStruct = true,
          generateNegativeZero = false,
          // override base date due to known issues with experimental scans
          baseDate =
            new SimpleDateFormat("YYYY-MM-DD hh:mm:ss").parse("2024-05-25 12:34:56").getTime)
      ParquetGenerator.makeParquetFile(random, spark, filename, 1000, options)
    }
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteDirectory(new File(filename))
  }

  test("select *") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    val sql = "SELECT * FROM t1"
    if (CometConf.isExperimentalNativeScan) {
      checkSparkAnswerAndOperator(sql)
    } else {
      checkSparkAnswer(sql)
    }
  }

  test("select * with limit") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    val sql = "SELECT * FROM t1 LIMIT 500"
    if (CometConf.isExperimentalNativeScan) {
      checkSparkAnswerAndOperator(sql)
    } else {
      checkSparkAnswer(sql)
    }
  }

  test("order by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      val sql = s"SELECT $col FROM t1 ORDER BY $col"
      // cannot run fully natively due to range partitioning and sort
      val (_, cometPlan) = checkSparkAnswer(sql)
      if (CometConf.isExperimentalNativeScan) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

  test("distribute by single column (complex types)") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    val columns = df.schema.fields.filter(f => isComplexType(f.dataType)).map(_.name)
    for (col <- columns) {
      // DISTRIBUTE BY is equivalent to df.repartition($col)
      val sql = s"SELECT $col FROM t1 DISTRIBUTE BY $col"
      val df = spark.sql(sql)
      df.collect()
      // check for Comet shuffle
      val plan = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      val cometShuffleExchanges = collectCometShuffleExchanges(plan)
      val expectedNumCometShuffles = CometConf.COMET_NATIVE_SCAN_IMPL.get() match {
        case CometConf.SCAN_NATIVE_COMET =>
          // native_comet does not support reading complex types
          0
        case CometConf.SCAN_NATIVE_ICEBERG_COMPAT | CometConf.SCAN_NATIVE_DATAFUSION =>
          CometConf.COMET_SHUFFLE_MODE.get() match {
            case "jvm" =>
              1
            case "native" =>
              // native shuffle does not support complex types as partitioning keys
              0
          }
      }
      assert(cometShuffleExchanges.length == expectedNumCometShuffles)
    }
  }

  test("shuffle") {
    val df = spark.read.parquet(filename)
    val df2 = df.repartition(8, df.col("c0")).sort("c1")
    df2.collect()
    if (CometConf.isExperimentalNativeScan) {
      val cometShuffles = collect(df2.queryExecution.executedPlan) {
        case exec: CometShuffleExchangeExec => exec
      }
      assert(1 == cometShuffles.length)
    }
  }

  test("join") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    df.createOrReplaceTempView("t2")
    for (col <- df.columns) {
      // cannot run fully native due to HashAggregate
      val sql = s"SELECT count(*) FROM t1 JOIN t2 ON t1.$col = t2.$col"
      val (_, cometPlan) = checkSparkAnswer(sql)
      if (CometConf.isExperimentalNativeScan) {
        assert(2 == collectNativeScans(cometPlan).length)
      }
    }
  }

  test("decode") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    // We want to make sure that the schema generator wasn't modified to accidentally omit
    // BinaryType, since then this test would not run any queries and silently pass.
    var testedBinary = false
    for (field <- df.schema.fields if field.dataType == BinaryType) {
      testedBinary = true
      // Intentionally use odd capitalization of 'utf-8' to test normalization.
      val sql = s"SELECT decode(${field.name}, 'utF-8') FROM t1"
      checkSparkAnswerAndOperator(sql)
    }
    assert(testedBinary)
  }

  test("regexp_replace") {
    withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
      val df = spark.read.parquet(filename)
      df.createOrReplaceTempView("t1")
      // We want to make sure that the schema generator wasn't modified to accidentally omit
      // StringType, since then this test would not run any queries and silently pass.
      var testedString = false
      for (field <- df.schema.fields if field.dataType == StringType) {
        testedString = true
        val sql = s"SELECT regexp_replace(${field.name}, 'a', 'b') FROM t1"
        checkSparkAnswerAndOperator(sql)
      }
      assert(testedString)
    }
  }

  test("Parquet temporal types written as INT96") {
    // int96 coercion in DF does not work with nested types yet
    // https://github.com/apache/datafusion/issues/15763
    testParquetTemporalTypes(
      ParquetOutputTimestampType.INT96,
      generateArray = false,
      generateStruct = false)
  }

  test("Parquet temporal types written as TIMESTAMP_MICROS") {
    testParquetTemporalTypes(ParquetOutputTimestampType.TIMESTAMP_MICROS)
  }

  test("Parquet temporal types written as TIMESTAMP_MILLIS") {
    testParquetTemporalTypes(ParquetOutputTimestampType.TIMESTAMP_MILLIS)
  }

  private def testParquetTemporalTypes(
      outputTimestampType: ParquetOutputTimestampType.Value,
      generateArray: Boolean = true,
      generateStruct: Boolean = true): Unit = {

    val options =
      DataGenOptions(
        generateArray = generateArray,
        generateStruct = generateStruct,
        generateNegativeZero = false)

    withTempPath { filename =>
      val random = new Random(42)
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "false",
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> outputTimestampType.toString,
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {
        ParquetGenerator.makeParquetFile(random, spark, filename.toString, 100, options)
      }

      Seq(defaultTimezone, "UTC", "America/Denver").foreach { tz =>
        Seq(true, false).foreach { inferTimestampNtzEnabled =>
          Seq(true, false).foreach { int96TimestampConversion =>
            Seq(true, false).foreach { int96AsTimestamp =>
              withSQLConf(
                CometConf.COMET_ENABLED.key -> "true",
                SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz,
                SQLConf.PARQUET_INT96_AS_TIMESTAMP.key -> int96AsTimestamp.toString,
                SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION.key -> int96TimestampConversion.toString,
                SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key -> inferTimestampNtzEnabled.toString) {

                val df = spark.read.parquet(filename.toString)
                df.createOrReplaceTempView("t1")

                def hasTemporalType(t: DataType): Boolean = t match {
                  case DataTypes.DateType | DataTypes.TimestampType |
                      DataTypes.TimestampNTZType =>
                    true
                  case t: StructType => t.exists(f => hasTemporalType(f.dataType))
                  case t: ArrayType => hasTemporalType(t.elementType)
                  case _ => false
                }

                val columns =
                  df.schema.fields.filter(f => hasTemporalType(f.dataType)).map(_.name)

                for (col <- columns) {
                  checkSparkAnswer(s"SELECT $col FROM t1 ORDER BY $col")
                }
              }
            }
          }
        }
      }
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    Seq("native", "jvm").foreach { shuffleMode =>
      Seq("native_comet", "native_datafusion", "native_iceberg_compat").foreach { scanImpl =>
        super.test(testName + s" ($scanImpl, $shuffleMode shuffle)", testTags: _*) {
          withSQLConf(
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> scanImpl,
            CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> shuffleMode) {
            testFun
          }
        }
      }
    }
  }

  private def collectNativeScans(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) {
      case scan: CometScanExec => scan
      case scan: CometNativeScanExec => scan
    }
  }

  private def collectCometShuffleExchanges(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) {
      case exchange: CometShuffleExchangeExec => exchange
    }
  }

}
