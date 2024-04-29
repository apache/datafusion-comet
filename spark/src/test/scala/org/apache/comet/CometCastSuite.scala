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

  test("all valid cast combinations covered") {
    val names = testNames

    def assertTestsExist(fromTypes: Seq[String], toTypes: Seq[String]): Unit = {
      for (fromType <- fromTypes) {
        for (toType <- toTypes) {
          if (fromType != toType) {
            val expectedTestName = s"cast $fromType to $toType"
            if (!names.contains(expectedTestName)) {
              fail(s"Missing test: $expectedTestName")
            }
          }
        }
      }
    }

    // make sure we have tests for all combinations of these basic types
    val basicTypes =
      Seq("bool", "byte", "short", "int", "long", "float", "double", "decimal", "string")
    assertTestsExist(basicTypes, basicTypes)

    // TODO need to add all valid date and timestamp cast combinations
    assertTestsExist(Seq("string"), Seq("date", "timestamp"))
  }

  // CAST from bool

  test("cast bool to bool") {
    castTest(generateBools(), DataTypes.BooleanType)
  }

  ignore("cast bool to byte") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateBools(), DataTypes.ByteType)
  }

  ignore("cast bool to short") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateBools(), DataTypes.ShortType)
  }

  test("cast bool to int") {
    castTest(generateBools(), DataTypes.IntegerType)
  }

  test("cast bool to long") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateBools(), DataTypes.LongType)
  }

  test("cast bool to float") {
    castTest(generateBools(), DataTypes.FloatType)
  }

  test("cast bool to double") {
    castTest(generateBools(), DataTypes.DoubleType)
  }

  ignore("cast bool to decimal") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE] -1117686336 cannot be represented as Decimal(10, 2)
    castTest(generateBools(), DataTypes.createDecimalType(10, 2))
  }

  test("cast bool to string") {
    castTest(generateBools(), DataTypes.StringType)
  }

  // CAST from byte

  test("cast byte to bool") {
    castTest(generateBytes(), DataTypes.BooleanType)
  }

  ignore("cast byte to byte") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateBytes(), DataTypes.ByteType)
  }

  ignore("cast byte to short") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateBytes(), DataTypes.ShortType)
  }

  test("cast byte to int") {
    castTest(generateBytes(), DataTypes.IntegerType)
  }

  test("cast byte to long") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateBytes(), DataTypes.LongType)
  }

  test("cast byte to float") {
    castTest(generateBytes(), DataTypes.FloatType)
  }

  test("cast byte to double") {
    castTest(generateBytes(), DataTypes.DoubleType)
  }

  ignore("cast byte to decimal") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE] -1117686336 cannot be represented as Decimal(10, 2)
    castTest(generateBytes(), DataTypes.createDecimalType(10, 2))
  }

  test("cast byte to string") {
    castTest(generateBytes(), DataTypes.StringType)
  }

  // CAST from short

  test("cast short to bool") {
    castTest(generateShorts(), DataTypes.BooleanType)
  }

  ignore("cast short to byte") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateShorts(), DataTypes.ByteType)
  }

  ignore("cast short to short") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateShorts(), DataTypes.ShortType)
  }

  test("cast short to int") {
    castTest(generateShorts(), DataTypes.IntegerType)
  }

  test("cast short to long") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateShorts(), DataTypes.LongType)
  }

  test("cast short to float") {
    castTest(generateShorts(), DataTypes.FloatType)
  }

  test("cast short to double") {
    castTest(generateShorts(), DataTypes.DoubleType)
  }

  ignore("cast short to decimal") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE] -1117686336 cannot be represented as Decimal(10, 2)
    castTest(generateShorts(), DataTypes.createDecimalType(10, 2))
  }

  test("cast short to string") {
    castTest(generateShorts(), DataTypes.StringType)
  }

  // CAST from integer

  test("cast int to bool") {
    castTest(generateInts(), DataTypes.BooleanType)
  }

  ignore("cast int to byte") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateInts(), DataTypes.ByteType)
  }

  ignore("cast int to short") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateInts(), DataTypes.ShortType)
  }

  test("cast int to int") {
    castTest(generateInts(), DataTypes.IntegerType)
  }

  test("cast int to long") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateInts(), DataTypes.LongType)
  }

  test("cast int to float") {
    castTest(generateInts(), DataTypes.FloatType)
  }

  test("cast int to double") {
    castTest(generateInts(), DataTypes.DoubleType)
  }

  ignore("cast int to decimal") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE] -1117686336 cannot be represented as Decimal(10, 2)
    castTest(generateInts(), DataTypes.createDecimalType(10, 2))
  }

  test("cast int to string") {
    castTest(generateInts(), DataTypes.StringType)
  }

  // CAST from long

  test("cast long to bool") {
    castTest(generateLongs(), DataTypes.BooleanType)
  }

  ignore("cast long to byte") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateLongs(), DataTypes.ByteType)
  }

  ignore("cast long to short") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateLongs(), DataTypes.ShortType)
  }

  ignore("cast long to int") {
    castTest(generateLongs(), DataTypes.IntegerType)
  }

  test("cast long to long") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateLongs(), DataTypes.LongType)
  }

  test("cast long to float") {
    castTest(generateLongs(), DataTypes.FloatType)
  }

  test("cast long to double") {
    castTest(generateLongs(), DataTypes.DoubleType)
  }

  ignore("cast long to decimal") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE] -1117686336 cannot be represented as Decimal(10, 2)
    castTest(generateLongs(), DataTypes.createDecimalType(10, 2))
  }

  test("cast long to string") {
    castTest(generateLongs(), DataTypes.StringType)
  }

  // CAST from float

  ignore("cast float to bool") {
    castTest(generateFloats(), DataTypes.BooleanType)
  }

  ignore("cast float to byte") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateFloats(), DataTypes.ByteType)
  }

  ignore("cast float to short") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateFloats(), DataTypes.ShortType)
  }

  ignore("cast float to int") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateFloats(), DataTypes.IntegerType)
  }

  ignore("cast float to long") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateFloats(), DataTypes.LongType)
  }

  ignore("cast float to float") {
    // fails due to incompatible sort order for 0.0 and -0.0
    castTest(generateFloats(), DataTypes.FloatType)
  }

  ignore("cast float to double") {
    // fails due to incompatible sort order for 0.0 and -0.0
    castTest(generateFloats(), DataTypes.DoubleType)
  }

  ignore("cast float to decimal") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE]
    castTest(generateFloats(), DataTypes.createDecimalType(10, 2))
  }

  ignore("cast float to string") {
    // https://github.com/apache/datafusion-comet/issues/312
    castTest(generateFloats(), DataTypes.StringType)
  }

  // CAST from double

  ignore("cast double to bool") {
    // fails due to incompatible sort order for 0.0 and -0.0
    castTest(generateDoubles(), DataTypes.BooleanType)
  }

  ignore("cast double to byte") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDoubles(), DataTypes.ByteType)
  }

  ignore("cast double to short") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDoubles(), DataTypes.ShortType)
  }

  ignore("cast double to int") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDoubles(), DataTypes.IntegerType)
  }

  ignore("cast double to long") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDoubles(), DataTypes.LongType)
  }

  ignore("cast double to float") {
    castTest(generateDoubles(), DataTypes.FloatType)
  }

  ignore("cast double to double") {
    // fails due to incompatible sort order for 0.0 and -0.0
    castTest(generateDoubles(), DataTypes.DoubleType)
  }

  ignore("cast double to decimal") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE]
    castTest(generateDoubles(), DataTypes.createDecimalType(10, 2))
  }

  ignore("cast double to string") {
    // https://github.com/apache/datafusion-comet/issues/312
    castTest(generateDoubles(), DataTypes.StringType)
  }

  // CAST from decimal

  ignore("cast decimal to bool") {
    // Arrow error: Cast error: Casting from Decimal128(38, 18) to Boolean not supported
    castTest(generateDecimals(), DataTypes.BooleanType)
  }

  ignore("cast decimal to byte") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDecimals(), DataTypes.ByteType)
  }

  ignore("cast decimal to short") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDecimals(), DataTypes.ShortType)
  }

  ignore("cast decimal to int") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDecimals(), DataTypes.IntegerType)
  }

  ignore("cast decimal to long") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDecimals(), DataTypes.LongType)
  }

  test("cast decimal to float") {
    castTest(generateDecimals(), DataTypes.FloatType)
  }

  test("cast decimal to double") {
    castTest(generateDecimals(), DataTypes.DoubleType)
  }

  test("cast decimal to decimal") {
    castTest(generateDecimals(), DataTypes.createDecimalType(10, 2))
  }

  ignore("cast decimal to string") {
    castTest(generateDecimals(), DataTypes.StringType)
  }

  // CAST from string

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
    val values = Seq(
      Float.MaxValue,
      Float.MinPositiveValue,
      Float.MinValue,
      Float.NaN,
      Float.PositiveInfinity,
      Float.NegativeInfinity,
      0.0f,
      -0.0f) ++
      Range(0, dataSize).map(_ => r.nextFloat())
    values.toDF("a")
  }

  private def generateDoubles(): DataFrame = {
    val r = new Random(0)
    val values = Seq(
      Double.MaxValue,
      Double.MinPositiveValue,
      Double.MinValue,
      Double.NaN,
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      0.0d,
      -0.0d) ++
      Range(0, dataSize).map(_ => r.nextDouble())
    values.toDF("a")
  }

  private def generateBools(): DataFrame = {
    Seq(true, false).toDF("a")
  }

  private def generateBytes(): DataFrame = {
    val r = new Random(0)
    val values = Seq(Byte.MinValue, Byte.MaxValue) ++
      Range(0, dataSize).map(_ => r.nextInt().toByte)
    values.toDF("a")
  }

  private def generateShorts(): DataFrame = {
    val r = new Random(0)
    val values = Seq(Short.MinValue, Short.MaxValue) ++
      Range(0, dataSize).map(_ => r.nextInt().toShort)
    values.toDF("a")
  }

  private def generateInts(): DataFrame = {
    val r = new Random(0)
    val values = Seq(Int.MinValue, Int.MaxValue) ++
      Range(0, dataSize).map(_ => r.nextInt())
    values.toDF("a")
  }

  private def generateLongs(): DataFrame = {
    val r = new Random(0)
    val values = Seq(Long.MinValue, Long.MaxValue) ++
      Range(0, dataSize).map(_ => r.nextLong())
    values.toDF("a")
  }

  private def generateDecimals(): DataFrame = {
    Seq(BigDecimal("123456.789"), BigDecimal("-123456.789"), BigDecimal("0.0")).toDF("a")
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
        checkSparkMaybeThrows(df) match {
          case (None, None) =>
          // neither system threw an exception
          case (None, Some(e)) =>
            // Spark succeeded but Comet failed
            throw e
          case (Some(e), None) =>
            // Spark failed but Comet succeeded
            fail(s"Comet should have failed with ${e.getCause.getMessage}")
          case (Some(sparkException), Some(cometException)) =>
            // both systems threw an exception so we make sure they are the same
            val sparkMessage = sparkException.getCause.getMessage
            // We have to workaround https://github.com/apache/datafusion-comet/issues/293 here by
            // removing the "Execution error: " error message prefix that is added by DataFusion
            val cometMessage = cometException.getCause.getMessage
              .replace("Execution error: ", "")
            if (CometSparkSessionExtensions.isSpark34Plus) {
              assert(cometMessage == sparkMessage)
            } else {
              // Spark 3.2 and 3.3 have a different error message format so we can't do a direct
              // comparison between Spark and Comet.
              // Spark message is in format `invalid input syntax for type TYPE: VALUE`
              // Comet message is in format `The value 'VALUE' of the type FROM_TYPE cannot be cast to TO_TYPE`
              // We just check that the comet message contains the same invalid value as the Spark message
              val sparkInvalidValue = sparkMessage.substring(sparkMessage.indexOf(':') + 2)
              assert(cometMessage.contains(sparkInvalidValue))
            }
        }

        // try_cast() should always return null for invalid inputs
        val df2 =
          spark.sql(s"select a, try_cast(a as ${toType.sql}) from t order by a")
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
