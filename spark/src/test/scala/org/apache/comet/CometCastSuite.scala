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

import scala.util.Random

import org.apache.spark.sql.{CometTestBase, DataFrame, SaveMode}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DataTypes}

class CometCastSuite extends CometTestBase with AdaptiveSparkPlanHelper {
  import testImplicits._

  private val dataSize = 1000

  // we should eventually add more whitespace chars here as documented in
  // https://docs.oracle.com/javase/8/docs/api/java/lang/Character.html#isWhitespace-char-
  // but this is likely a reasonable starting point for now
  private val whitespaceChars = " \t\r\n"

  private val numericPattern = "0123456789e+-." + whitespaceChars
  private val datePattern = "0123456789/" + whitespaceChars
  private val timestampPattern = "0123456789/:T" + whitespaceChars

  ignore("cast long to short") {
    castTest(generateLongs, DataTypes.ShortType)
  }

  ignore("cast float to bool") {
    castTest(generateFloats, DataTypes.BooleanType)
  }

  ignore("cast float to int") {
    castTest(generateFloats, DataTypes.IntegerType)
  }

  ignore("cast float to string") {
    castTest(generateFloats, DataTypes.StringType)
  }

  test("cast string to bool") {
    val testValues =
      (Seq("TRUE", "True", "true", "FALSE", "False", "false", "1", "0", "", null) ++
        generateStrings("truefalseTRUEFALSEyesno10" + whitespaceChars, 8)).toDF("a")
    castTest(testValues, DataTypes.BooleanType)
  }

  ignore("cast string to byte") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.ByteType)
  }

  ignore("cast string to short") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.ShortType)
  }

  ignore("cast string to int") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.IntegerType)
  }

  ignore("cast string to long") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.LongType)
  }

  ignore("cast string to float") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.FloatType)
  }

  ignore("cast string to double") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.DoubleType)
  }

  ignore("cast string to decimal") {
    val values = generateStrings(numericPattern, 8).toDF("a")
    castTest(values, DataTypes.createDecimalType(10, 2))
    castTest(values, DataTypes.createDecimalType(10, 0))
    castTest(values, DataTypes.createDecimalType(10, -2))
  }

  ignore("cast string to date") {
    castTest(generateStrings(datePattern, 8).toDF("a"), DataTypes.DateType)
  }

  test("cast string to timestamp disabled by default") {
    val values = Seq("2020-01-01T12:34:56.123456", "T2").toDF("a")
    castFallbackTest(
      values.toDF("a"),
      DataTypes.TimestampType,
      "spark.comet.cast.stringToTimestamp is disabled")
  }

  ignore("cast string to timestamp") {
    withSQLConf((CometConf.COMET_CAST_STRING_TO_TIMESTAMP.key, "true")) {
      val values = Seq("2020-01-01T12:34:56.123456", "T2") ++ generateStrings(timestampPattern, 8)
      castTest(values.toDF("a"), DataTypes.TimestampType)
    }
  }

  private def generateFloats(): DataFrame = {
    val r = new Random(0)
    Range(0, dataSize).map(_ => r.nextFloat()).toDF("a")
  }

  private def generateLongs(): DataFrame = {
    val r = new Random(0)
    Range(0, dataSize).map(_ => r.nextLong()).toDF("a")
  }

  private def generateString(r: Random, chars: String, maxLen: Int): String = {
    val len = r.nextInt(maxLen)
    Range(0, len).map(_ => chars.charAt(r.nextInt(chars.length))).mkString
  }

  private def generateStrings(chars: String, maxLen: Int): Seq[String] = {
    val r = new Random(0)
    Range(0, dataSize).map(_ => generateString(r, chars, maxLen))
  }

  private def castFallbackTest(
      input: DataFrame,
      toType: DataType,
      expectedMessage: String): Unit = {
    withTempPath { dir =>
      val data = roundtripParquet(input, dir).coalesce(1)
      data.createOrReplaceTempView("t")

      withSQLConf((SQLConf.ANSI_ENABLED.key, "false")) {
        val df = data.withColumn("converted", col("a").cast(toType))
        df.collect()
        val str =
          new ExtendedExplainInfo().generateExtendedInfo(df.queryExecution.executedPlan)
        assert(str.contains(expectedMessage))
      }
    }
  }

  private def castTest(input: DataFrame, toType: DataType): Unit = {
    withTempPath { dir =>
      val data = roundtripParquet(input, dir).coalesce(1)
      data.createOrReplaceTempView("t")

      withSQLConf((SQLConf.ANSI_ENABLED.key, "false")) {
        // cast() should return null for invalid inputs when ansi mode is disabled
        val df = data.withColumn("converted", col("a").cast(toType))
        checkSparkAnswer(df)

        // try_cast() should always return null for invalid inputs
        val df2 = spark.sql(s"select try_cast(a as ${toType.sql}) from t")
        checkSparkAnswer(df2)
      }

      // with ANSI enabled, we should produce the same exception as Spark
      withSQLConf(
        (SQLConf.ANSI_ENABLED.key, "true"),
        (CometConf.COMET_ANSI_MODE_ENABLED.key, "true")) {

        // cast() should throw exception on invalid inputs when ansi mode is enabled
        val df = data.withColumn("converted", col("a").cast(toType))
        val (expected, actual) = checkSparkThrows(df)

        if (CometSparkSessionExtensions.isSpark34Plus) {
          // We have to workaround https://github.com/apache/datafusion-comet/issues/293 here by
          // removing the "Execution error: " error message prefix that is added by DataFusion
          val cometMessage = actual.getMessage
            .substring("Execution error: ".length)

          assert(expected.getMessage == cometMessage)
        } else {
          // Spark 3.2 and 3.3 have a different error message format so we can't do a direct
          // comparison between Spark and Comet.
          // Spark message is in format `invalid input syntax for type TYPE: VALUE`
          // Comet message is in format `The value 'VALUE' of the type FROM_TYPE cannot be cast to TO_TYPE`
          // We just check that the comet message contains the same invalid value as the Spark message
          val sparkInvalidValue =
            expected.getMessage.substring(expected.getMessage.indexOf(':') + 2)
          assert(actual.getMessage.contains(sparkInvalidValue))
        }

        // try_cast() should always return null for invalid inputs
        val df2 = spark.sql(s"select try_cast(a as ${toType.sql}) from t")
        checkSparkAnswer(df2)
      }
    }
  }

  private def roundtripParquet(df: DataFrame, tempDir: File): DataFrame = {
    val filename = new File(tempDir, s"castTest_${System.currentTimeMillis()}.parquet").toString
    df.write.mode(SaveMode.Overwrite).parquet(filename)
    spark.read.parquet(filename)
  }

}
