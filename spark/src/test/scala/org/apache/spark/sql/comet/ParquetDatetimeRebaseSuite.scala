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

package org.apache.spark.sql.comet

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkException
import org.apache.spark.sql.{CometTestBase, DataFrame, Dataset, Row}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

// This test checks if Comet reads ancient dates & timestamps that are before 1582, as if they are
// read according to the `LegacyBehaviorPolicy.CORRECTED` mode (i.e., no rebase) in Spark.
abstract class ParquetDatetimeRebaseSuite extends CometTestBase {

  // This is a flag defined in Spark's `org.apache.spark.internal.config.Tests` but is only
  // visible under package `spark`.
  val SPARK_TESTING: String = "spark.testing"

  test("reading ancient dates before 1582") {
    Seq(true, false).foreach { exceptionOnRebase =>
      withSQLConf(
        CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key ->
          exceptionOnRebase.toString) {
        Seq("2_4_5", "2_4_6", "3_2_0").foreach { sparkVersion =>
          val file =
            getResourceParquetFilePath(
              s"test-data/before_1582_date_v$sparkVersion.snappy.parquet")
          val df = spark.read.parquet(file)

          // Parquet file written by 2.4.5 should throw exception for both Spark and Comet
          // For Spark 4.0+, Parquet file written by 2.4.5 should not throw exception
          if ((exceptionOnRebase || sparkVersion == "2_4_5") && (!isSpark40Plus || sparkVersion != "2_4_5") && !CometSparkSessionExtensions
              .usingDataFusionParquetExec(conf)) {
            intercept[SparkException](df.collect())
          } else {
            checkSparkNoRebaseAnswer(df)
          }
        }
      }
    }
  }

  test("reading ancient timestamps before 1582") {
    Seq(true, false).foreach { exceptionOnRebase =>
      withSQLConf(
        CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key ->
          exceptionOnRebase.toString) {
        Seq("2_4_5", "2_4_6", "3_2_0").foreach { sparkVersion =>
          Seq("micros", "millis").foreach { timestampUnit =>
            val file = getResourceParquetFilePath(
              s"test-data/before_1582_timestamp_${timestampUnit}_v${sparkVersion}.snappy.parquet")
            val df = spark.read.parquet(file)

            // Parquet file written by 2.4.5 should throw exception for both Spark and Comet
            // For Spark 4.0+, Parquet file written by 2.4.5 should not throw exception
            if ((exceptionOnRebase || sparkVersion == "2_4_5") && (!isSpark40Plus || sparkVersion != "2_4_5") && !CometSparkSessionExtensions
                .usingDataFusionParquetExec(conf)) {
              intercept[SparkException](df.collect())
            } else {
              checkSparkNoRebaseAnswer(df)
            }
          }
        }
      }
    }
  }

  test("reading ancient int96 timestamps before 1582") {
    Seq(true, false).foreach { exceptionOnRebase =>
      withSQLConf(
        CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key ->
          exceptionOnRebase.toString) {
        Seq("2_4_5", "2_4_6", "3_2_0").foreach { sparkVersion =>
          Seq("dict", "plain").foreach { parquetEncoding =>
            val file = getResourceParquetFilePath(
              s"test-data/before_1582_timestamp_int96_${parquetEncoding}_v${sparkVersion}.snappy.parquet")
            val df = spark.read.parquet(file)

            // Parquet file written by 2.4.5 should throw exception for both Spark and Comet
            // For Spark 4.0+, Parquet file written by 2.4.5 should not throw exception
            if ((exceptionOnRebase || sparkVersion == "2_4_5") && (!isSpark40Plus || sparkVersion != "2_4_5") && !CometSparkSessionExtensions
                .usingDataFusionParquetExec(conf)) {
              intercept[SparkException](df.collect())
            } else {
              checkSparkNoRebaseAnswer(df)
            }
          }
        }
      }
    }
  }

  private def checkSparkNoRebaseAnswer(df: => DataFrame): Unit = {
    var expected: Array[Row] = Array.empty

    withSQLConf(CometConf.COMET_ENABLED.key -> "false", "spark.test.forceNoRebase" -> "true") {

      val previousPropertyValue = Option.apply(System.getProperty(SPARK_TESTING))
      System.setProperty(SPARK_TESTING, "true")

      val dfSpark = Dataset.ofRows(spark, df.logicalPlan)
      expected = dfSpark.collect()

      previousPropertyValue match {
        case Some(v) => System.setProperty(SPARK_TESTING, v)
        case None => System.clearProperty(SPARK_TESTING)
      }
    }

    val dfComet = Dataset.ofRows(spark, df.logicalPlan)
    checkAnswer(dfComet, expected)
  }
}

class ParquetDatetimeRebaseV1Suite extends ParquetDatetimeRebaseSuite {
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*)(withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      testFun
    })(pos)
  }
}

class ParquetDatetimeRebaseV2Suite extends ParquetDatetimeRebaseSuite {
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    // Datasource V2 is not supported by complex readers so force the scan impl back
    // to 'native_comet'
    super.test(testName, testTags: _*)(
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_COMET) {
        testFun
      })(pos)
  }
}
