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
import org.apache.spark.sql.catalyst.expressions.Cast
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

    def assertTestsExist(fromTypes: Seq[DataType], toTypes: Seq[DataType]): Unit = {
      for (fromType <- fromTypes) {
        for (toType <- toTypes) {
          val expectedTestName = s"cast $fromType to $toType"
          val testExists = names.contains(expectedTestName)
          if (Cast.canCast(fromType, toType)) {
            if (fromType == toType) {
              if (testExists) {
                fail(s"Found redundant test for no-op cast: $expectedTestName")
              }
            } else if (!testExists) {
              fail(s"Missing test: $expectedTestName")
            }
          } else if (testExists) {
            fail(s"Found test for cast that Spark does not support: $expectedTestName")
          }
        }
      }
    }

    // make sure we have tests for all combinations of our supported types
    val supportedTypes =
      Seq(
        DataTypes.BooleanType,
        DataTypes.ByteType,
        DataTypes.ShortType,
        DataTypes.IntegerType,
        DataTypes.LongType,
        DataTypes.FloatType,
        DataTypes.DoubleType,
        DataTypes.createDecimalType(10, 2),
        DataTypes.StringType,
        DataTypes.DateType,
        DataTypes.TimestampType)
    // TODO add DataTypes.TimestampNTZType for Spark 3.4 and later
    assertTestsExist(supportedTypes, supportedTypes)
  }

  // CAST from BooleanType

  ignore("cast BooleanType to ByteType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateBools(), DataTypes.ByteType)
  }

  ignore("cast BooleanType to ShortType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateBools(), DataTypes.ShortType)
  }

  test("cast BooleanType to IntegerType") {
    castTest(generateBools(), DataTypes.IntegerType)
  }

  test("cast BooleanType to LongType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateBools(), DataTypes.LongType)
  }

  test("cast BooleanType to FloatType") {
    castTest(generateBools(), DataTypes.FloatType)
  }

  test("cast BooleanType to DoubleType") {
    castTest(generateBools(), DataTypes.DoubleType)
  }

  ignore("cast BooleanType to DecimalType(10,2)") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE] -1117686336 cannot be represented as Decimal(10, 2)
    castTest(generateBools(), DataTypes.createDecimalType(10, 2))
  }

  test("cast BooleanType to StringType") {
    castTest(generateBools(), DataTypes.StringType)
  }

  ignore("cast BooleanType to TimestampType") {
    // Arrow error: Cast error: Casting from Boolean to Timestamp(Microsecond, Some("UTC")) not supported
    castTest(generateBools(), DataTypes.TimestampType)
  }

  // CAST from ByteType

  test("cast ByteType to BooleanType") {
    castTest(generateBytes(), DataTypes.BooleanType)
  }

  ignore("cast ByteType to ShortType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateBytes(), DataTypes.ShortType)
  }

  test("cast ByteType to IntegerType") {
    castTest(generateBytes(), DataTypes.IntegerType)
  }

  test("cast ByteType to LongType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateBytes(), DataTypes.LongType)
  }

  test("cast ByteType to FloatType") {
    castTest(generateBytes(), DataTypes.FloatType)
  }

  test("cast ByteType to DoubleType") {
    castTest(generateBytes(), DataTypes.DoubleType)
  }

  ignore("cast ByteType to DecimalType(10,2)") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE] -1117686336 cannot be represented as Decimal(10, 2)
    castTest(generateBytes(), DataTypes.createDecimalType(10, 2))
  }

  test("cast ByteType to StringType") {
    castTest(generateBytes(), DataTypes.StringType)
  }

  ignore("cast ByteType to TimestampType") {
    // input: -1, expected: 1969-12-31 15:59:59.0, actual: 1969-12-31 15:59:59.999999
    castTest(generateBytes(), DataTypes.TimestampType)
  }

  // CAST from ShortType

  test("cast ShortType to BooleanType") {
    castTest(generateShorts(), DataTypes.BooleanType)
  }

  ignore("cast ShortType to ByteType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateShorts(), DataTypes.ByteType)
  }

  test("cast ShortType to IntegerType") {
    castTest(generateShorts(), DataTypes.IntegerType)
  }

  test("cast ShortType to LongType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateShorts(), DataTypes.LongType)
  }

  test("cast ShortType to FloatType") {
    castTest(generateShorts(), DataTypes.FloatType)
  }

  test("cast ShortType to DoubleType") {
    castTest(generateShorts(), DataTypes.DoubleType)
  }

  ignore("cast ShortType to DecimalType(10,2)") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE] -1117686336 cannot be represented as Decimal(10, 2)
    castTest(generateShorts(), DataTypes.createDecimalType(10, 2))
  }

  test("cast ShortType to StringType") {
    castTest(generateShorts(), DataTypes.StringType)
  }

  ignore("cast ShortType to TimestampType") {
    // input: -1003, expected: 1969-12-31 15:43:17.0, actual: 1969-12-31 15:59:59.998997
    castTest(generateShorts(), DataTypes.TimestampType)
  }

  // CAST from integer

  test("cast IntegerType to BooleanType") {
    castTest(generateInts(), DataTypes.BooleanType)
  }

  ignore("cast IntegerType to ByteType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateInts(), DataTypes.ByteType)
  }

  ignore("cast IntegerType to ShortType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateInts(), DataTypes.ShortType)
  }

  test("cast IntegerType to LongType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateInts(), DataTypes.LongType)
  }

  test("cast IntegerType to FloatType") {
    castTest(generateInts(), DataTypes.FloatType)
  }

  test("cast IntegerType to DoubleType") {
    castTest(generateInts(), DataTypes.DoubleType)
  }

  ignore("cast IntegerType to DecimalType(10,2)") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE] -1117686336 cannot be represented as Decimal(10, 2)
    castTest(generateInts(), DataTypes.createDecimalType(10, 2))
  }

  test("cast IntegerType to StringType") {
    castTest(generateInts(), DataTypes.StringType)
  }

  ignore("cast IntegerType to TimestampType") {
    // inputL -1000479329, expected: 1938-04-19 01:04:31.0, actual: 1969-12-31 15:43:19.520671
    castTest(generateInts(), DataTypes.TimestampType)
  }

  // CAST from LongType

  test("cast LongType to BooleanType") {
    castTest(generateLongs(), DataTypes.BooleanType)
  }

  ignore("cast LongType to ByteType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateLongs(), DataTypes.ByteType)
  }

  ignore("cast LongType to ShortType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateLongs(), DataTypes.ShortType)
  }

  ignore("cast LongType to IntegerType") {
    castTest(generateLongs(), DataTypes.IntegerType)
  }

  test("cast LongType to FloatType") {
    castTest(generateLongs(), DataTypes.FloatType)
  }

  test("cast LongType to DoubleType") {
    castTest(generateLongs(), DataTypes.DoubleType)
  }

  ignore("cast LongType to DecimalType(10,2)") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE] -1117686336 cannot be represented as Decimal(10, 2)
    castTest(generateLongs(), DataTypes.createDecimalType(10, 2))
  }

  test("cast LongType to StringType") {
    castTest(generateLongs(), DataTypes.StringType)
  }

  ignore("cast LongType to TimestampType") {
    // java.lang.ArithmeticException: long overflow
    castTest(generateLongs(), DataTypes.TimestampType)
  }

  // CAST from FloatType

  ignore("cast FloatType to BooleanType") {
    castTest(generateFloats(), DataTypes.BooleanType)
  }

  ignore("cast FloatType to ByteType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateFloats(), DataTypes.ByteType)
  }

  ignore("cast FloatType to ShortType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateFloats(), DataTypes.ShortType)
  }

  ignore("cast FloatType to IntegerType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateFloats(), DataTypes.IntegerType)
  }

  ignore("cast FloatType to LongType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateFloats(), DataTypes.LongType)
  }

  ignore("cast FloatType to DoubleType") {
    // fails due to incompatible sort order for 0.0 and -0.0
    castTest(generateFloats(), DataTypes.DoubleType)
  }

  ignore("cast FloatType to DecimalType(10,2)") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE]
    castTest(generateFloats(), DataTypes.createDecimalType(10, 2))
  }

  ignore("cast FloatType to StringType") {
    // https://github.com/apache/datafusion-comet/issues/312
    castTest(generateFloats(), DataTypes.StringType)
  }

  ignore("cast FloatType to TimestampType") {
    // https://github.com/apache/datafusion-comet/issues/312
    castTest(generateFloats(), DataTypes.TimestampType)
  }

  // CAST from DoubleType

  ignore("cast DoubleType to BooleanType") {
    // fails due to incompatible sort order for 0.0 and -0.0
    castTest(generateDoubles(), DataTypes.BooleanType)
  }

  ignore("cast DoubleType to ByteType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDoubles(), DataTypes.ByteType)
  }

  ignore("cast DoubleType to ShortType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDoubles(), DataTypes.ShortType)
  }

  ignore("cast DoubleType to IntegerType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDoubles(), DataTypes.IntegerType)
  }

  ignore("cast DoubleType to LongType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDoubles(), DataTypes.LongType)
  }

  ignore("cast DoubleType to FloatType") {
    castTest(generateDoubles(), DataTypes.FloatType)
  }

  ignore("cast DoubleType to DecimalType(10,2)") {
    // Comet should have failed with [NUMERIC_VALUE_OUT_OF_RANGE]
    castTest(generateDoubles(), DataTypes.createDecimalType(10, 2))
  }

  ignore("cast DoubleType to StringType") {
    // https://github.com/apache/datafusion-comet/issues/312
    castTest(generateDoubles(), DataTypes.StringType)
  }

  ignore("cast DoubleType to TimestampType") {
    castTest(generateDoubles(), DataTypes.TimestampType)
  }

  // CAST from DecimalType(10,2)

  ignore("cast DecimalType(10,2) to BooleanType") {
    // Arrow error: Cast error: Casting from Decimal128(38, 18) to Boolean not supported
    castTest(generateDecimals(), DataTypes.BooleanType)
  }

  ignore("cast DecimalType(10,2) to ByteType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDecimals(), DataTypes.ByteType)
  }

  ignore("cast DecimalType(10,2) to ShortType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDecimals(), DataTypes.ShortType)
  }

  ignore("cast DecimalType(10,2) to IntegerType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDecimals(), DataTypes.IntegerType)
  }

  ignore("cast DecimalType(10,2) to LongType") {
    // https://github.com/apache/datafusion-comet/issues/350
    castTest(generateDecimals(), DataTypes.LongType)
  }

  test("cast DecimalType(10,2) to FloatType") {
    castTest(generateDecimals(), DataTypes.FloatType)
  }

  test("cast DecimalType(10,2) to DoubleType") {
    castTest(generateDecimals(), DataTypes.DoubleType)
  }

  ignore("cast DecimalType(10,2) to StringType") {
    castTest(generateDecimals(), DataTypes.StringType)
  }

  ignore("cast DecimalType(10,2) to TimestampType") {
    castTest(generateDecimals(), DataTypes.TimestampType)
  }

  // CAST from StringType

  test("cast StringType to BooleanType") {
    val testValues =
      (Seq("TRUE", "True", "true", "FALSE", "False", "false", "1", "0", "", null) ++
        generateStrings("truefalseTRUEFALSEyesno10" + whitespaceChars, 8)).toDF("a")
    castTest(testValues, DataTypes.BooleanType)
  }

  ignore("cast StringType to ByteType") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.ByteType)
  }

  ignore("cast StringType to ShortType") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.ShortType)
  }

  ignore("cast StringType to IntegerType") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.IntegerType)
  }

  ignore("cast StringType to LongType") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.LongType)
  }

  ignore("cast StringType to FloatType") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.FloatType)
  }

  ignore("cast StringType to DoubleType") {
    castTest(generateStrings(numericPattern, 8).toDF("a"), DataTypes.DoubleType)
  }

  ignore("cast StringType to DecimalType(10,2)") {
    val values = generateStrings(numericPattern, 8).toDF("a")
    castTest(values, DataTypes.createDecimalType(10, 2))
    castTest(values, DataTypes.createDecimalType(10, 0))
    castTest(values, DataTypes.createDecimalType(10, -2))
  }

  ignore("cast StringType to DateType") {
    castTest(generateStrings(datePattern, 8).toDF("a"), DataTypes.DateType)
  }

  test("cast StringType to TimestampType disabled by default") {
    val values = Seq("2020-01-01T12:34:56.123456", "T2").toDF("a")
    castFallbackTest(
      values.toDF("a"),
      DataTypes.TimestampType,
      "spark.comet.cast.stringToTimestamp is disabled")
  }

  ignore("cast StringType to TimestampType") {
    withSQLConf((CometConf.COMET_CAST_STRING_TO_TIMESTAMP.key, "true")) {
      val values = Seq("2020-01-01T12:34:56.123456", "T2") ++ generateStrings(timestampPattern, 8)
      castTest(values.toDF("a"), DataTypes.TimestampType)
    }
  }

  // CAST from date

  ignore("cast DateType to BooleanType") {
    // TODO: implement
  }

  ignore("cast DateType to ByteType") {
    // TODO: implement
  }

  ignore("cast DateType to ShortType") {
    // TODO: implement
  }

  ignore("cast DateType to IntegerType") {
    // TODO: implement
  }

  ignore("cast DateType to LongType") {
    // TODO: implement
  }

  ignore("cast DateType to FloatType") {
    // TODO: implement
  }

  ignore("cast DateType to DoubleType") {
    // TODO: implement
  }

  ignore("cast DateType to DecimalType(10,2)") {
    // TODO: implement
  }

  ignore("cast DateType to StringType") {
    // TODO: implement
  }

  ignore("cast DateType to TimestampType") {
    // TODO: implement
  }

  // CAST from TimestampType

  ignore("cast TimestampType to BooleanType") {
    // TODO: implement
  }

  ignore("cast TimestampType to ByteType") {
    // TODO: implement
  }

  ignore("cast TimestampType to ShortType") {
    // TODO: implement
  }

  ignore("cast TimestampType to IntegerType") {
    // TODO: implement
  }

  ignore("cast TimestampType to LongType") {
    // TODO: implement
  }

  ignore("cast TimestampType to FloatType") {
    // TODO: implement
  }

  ignore("cast TimestampType to DoubleType") {
    // TODO: implement
  }

  ignore("cast TimestampType to DecimalType(10,2)") {
    // TODO: implement
  }

  ignore("cast TimestampType to StringType") {
    // TODO: implement
  }

  ignore("cast TimestampType to DateType") {
    // TODO: implement
  }

  test("cast short to byte") {
    castTest(generateShorts, DataTypes.ByteType)
  }

  test("cast int to byte") {
    castTest(generateInts, DataTypes.ByteType)
  }

  test("cast int to short") {
    castTest(generateInts, DataTypes.ShortType)
  }

  test("cast long to byte") {
    castTest(generateLongs, DataTypes.ByteType)
  }

  test("cast long to short") {
    castTest(generateLongs, DataTypes.ShortType)
  }

  test("cast long to int") {
    castTest(generateLongs, DataTypes.IntegerType)
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
    withNulls(values).toDF("a")
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
    withNulls(values).toDF("a")
  }

  private def generateBools(): DataFrame = {
    withNulls(Seq(true, false)).toDF("a")
  }

  private def generateBytes(): DataFrame = {
    val r = new Random(0)
    val values = Seq(Byte.MinValue, Byte.MaxValue) ++
      Range(0, dataSize).map(_ => r.nextInt().toByte)
    withNulls(values).toDF("a")
  }

  private def generateShorts(): DataFrame = {
    val r = new Random(0)
    val values = Seq(Short.MinValue, Short.MaxValue) ++
      Range(0, dataSize).map(_ => r.nextInt().toShort)
    withNulls(values).toDF("a")
  }

  private def generateInts(): DataFrame = {
    val r = new Random(0)
    val values = Seq(Int.MinValue, Int.MaxValue) ++
      Range(0, dataSize).map(_ => r.nextInt())
    withNulls(values).toDF("a")
  }

  private def generateLongs(): DataFrame = {
    val r = new Random(0)
    val values = Seq(Long.MinValue, Long.MaxValue) ++
      Range(0, dataSize).map(_ => r.nextLong())
    withNulls(values).toDF("a")
  }

  private def generateDecimals(): DataFrame = {
    // TODO improve this
    val values = Seq(BigDecimal("123456.789"), BigDecimal("-123456.789"), BigDecimal("0.0"))
    withNulls(values).toDF("a")
  }

  private def generateString(r: Random, chars: String, maxLen: Int): String = {
    val len = r.nextInt(maxLen)
    Range(0, len).map(_ => chars.charAt(r.nextInt(chars.length))).mkString
  }

  // TODO return DataFrame for consistency with other generators and include null values
  private def generateStrings(chars: String, maxLen: Int): Seq[String] = {
    val r = new Random(0)
    Range(0, dataSize).map(_ => generateString(r, chars, maxLen))
  }

  private def withNulls[T](values: Seq[T]): Seq[Option[T]] = {
    values.map(v => Some(v)) ++ Seq(None)
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
              assert(
                cometMessage.contains(sparkInvalidValue) || cometMessage.contains("overflow"))
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
