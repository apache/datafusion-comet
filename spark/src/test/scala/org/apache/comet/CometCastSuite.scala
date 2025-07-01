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
import scala.util.matching.Regex

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CometTestBase, DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DataTypes, DecimalType, StructField, StructType}

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.expressions.{CometCast, CometEvalMode, Compatible}

class CometCastSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  import testImplicits._

  /** Create a data generator using a fixed seed so that tests are reproducible */
  private val gen = DataGenerator.DEFAULT

  /** Number of random data items to generate in each test */
  private val dataSize = 10000

  // we should eventually add more whitespace chars here as documented in
  // https://docs.oracle.com/javase/8/docs/api/java/lang/Character.html#isWhitespace-char-
  // but this is likely a reasonable starting point for now
  private val whitespaceChars = " \t\r\n"

  /**
   * We use these characters to construct strings that potentially represent valid numbers such as
   * `-12.34d` or `4e7`. Invalid numeric strings will also be generated, such as `+e.-d`.
   */
  private val numericPattern = "0123456789deEf+-." + whitespaceChars

  private val datePattern = "0123456789/" + whitespaceChars

  private val timestampPattern = "0123456789/:T" + whitespaceChars

  lazy val usingParquetExecWithIncompatTypes: Boolean =
    usingDataSourceExecWithIncompatTypes(conf)

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
            } else {
              val testIgnored =
                tags.get(expectedTestName).exists(s => s.contains("org.scalatest.Ignore"))
              CometCast.isSupported(fromType, toType, None, CometEvalMode.LEGACY) match {
                case Compatible(_) =>
                  if (testIgnored) {
                    fail(
                      s"Cast from $fromType to $toType is reported as compatible " +
                        "with Spark but the test is ignored")
                  }
                case _ =>
                  if (!testIgnored) {
                    fail(
                      s"We claim that cast from $fromType to $toType is not compatible " +
                        "with Spark but the test is not ignored")
                  }
              }
            }
          } else if (testExists) {
            fail(s"Found test for cast that Spark does not support: $expectedTestName")
          }
        }
      }
    }

    assertTestsExist(CometCast.supportedTypes, CometCast.supportedTypes)
  }

  // CAST from BooleanType

  test("cast BooleanType to ByteType") {
    castTest(generateBools(), DataTypes.ByteType)
  }

  test("cast BooleanType to ShortType") {
    castTest(generateBools(), DataTypes.ShortType)
  }

  test("cast BooleanType to IntegerType") {
    castTest(generateBools(), DataTypes.IntegerType)
  }

  test("cast BooleanType to LongType") {
    castTest(generateBools(), DataTypes.LongType)
  }

  test("cast BooleanType to FloatType") {
    castTest(generateBools(), DataTypes.FloatType)
  }

  test("cast BooleanType to DoubleType") {
    castTest(generateBools(), DataTypes.DoubleType)
  }

  ignore("cast BooleanType to DecimalType(10,2)") {
    // Arrow error: Cast error: Casting from Boolean to Decimal128(10, 2) not supported
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
    castTest(
      generateBytes(),
      DataTypes.BooleanType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ByteType to ShortType") {
    castTest(
      generateBytes(),
      DataTypes.ShortType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ByteType to IntegerType") {
    castTest(
      generateBytes(),
      DataTypes.IntegerType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ByteType to LongType") {
    castTest(
      generateBytes(),
      DataTypes.LongType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ByteType to FloatType") {
    castTest(
      generateBytes(),
      DataTypes.FloatType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ByteType to DoubleType") {
    castTest(
      generateBytes(),
      DataTypes.DoubleType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ByteType to DecimalType(10,2)") {
    castTest(
      generateBytes(),
      DataTypes.createDecimalType(10, 2),
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ByteType to StringType") {
    castTest(
      generateBytes(),
      DataTypes.StringType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  ignore("cast ByteType to BinaryType") {
    castTest(
      generateBytes(),
      DataTypes.BinaryType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  ignore("cast ByteType to TimestampType") {
    // input: -1, expected: 1969-12-31 15:59:59.0, actual: 1969-12-31 15:59:59.999999
    castTest(
      generateBytes(),
      DataTypes.TimestampType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  // CAST from ShortType

  test("cast ShortType to BooleanType") {
    castTest(
      generateShorts(),
      DataTypes.BooleanType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ShortType to ByteType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(
      generateShorts(),
      DataTypes.ByteType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ShortType to IntegerType") {
    castTest(
      generateShorts(),
      DataTypes.IntegerType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ShortType to LongType") {
    castTest(
      generateShorts(),
      DataTypes.LongType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ShortType to FloatType") {
    castTest(
      generateShorts(),
      DataTypes.FloatType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ShortType to DoubleType") {
    castTest(
      generateShorts(),
      DataTypes.DoubleType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ShortType to DecimalType(10,2)") {
    castTest(
      generateShorts(),
      DataTypes.createDecimalType(10, 2),
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  test("cast ShortType to StringType") {
    castTest(
      generateShorts(),
      DataTypes.StringType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  ignore("cast ShortType to BinaryType") {
    castTest(
      generateShorts(),
      DataTypes.BinaryType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  ignore("cast ShortType to TimestampType") {
    // input: -1003, expected: 1969-12-31 15:43:17.0, actual: 1969-12-31 15:59:59.998997
    castTest(
      generateShorts(),
      DataTypes.TimestampType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes)
  }

  // CAST from integer

  test("cast IntegerType to BooleanType") {
    castTest(generateInts(), DataTypes.BooleanType)
  }

  test("cast IntegerType to ByteType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateInts(), DataTypes.ByteType)
  }

  test("cast IntegerType to ShortType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateInts(), DataTypes.ShortType)
  }

  test("cast IntegerType to LongType") {
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

  ignore("cast IntegerType to BinaryType") {
    castTest(generateInts(), DataTypes.BinaryType)
  }

  ignore("cast IntegerType to TimestampType") {
    // input: -1000479329, expected: 1938-04-19 01:04:31.0, actual: 1969-12-31 15:43:19.520671
    castTest(generateInts(), DataTypes.TimestampType)
  }

  // CAST from LongType

  test("cast LongType to BooleanType") {
    castTest(generateLongs(), DataTypes.BooleanType)
  }

  test("cast LongType to ByteType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateLongs(), DataTypes.ByteType)
  }

  test("cast LongType to ShortType") {
    // https://github.com/apache/datafusion-comet/issues/311
    castTest(generateLongs(), DataTypes.ShortType)
  }

  test("cast LongType to IntegerType") {
    // https://github.com/apache/datafusion-comet/issues/311
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

  ignore("cast LongType to BinaryType") {
    castTest(generateLongs(), DataTypes.BinaryType)
  }

  ignore("cast LongType to TimestampType") {
    // java.lang.ArithmeticException: long overflow
    castTest(generateLongs(), DataTypes.TimestampType)
  }

  // CAST from FloatType

  test("cast FloatType to BooleanType") {
    castTest(generateFloats(), DataTypes.BooleanType)
  }

  test("cast FloatType to ByteType") {
    castTest(generateFloats(), DataTypes.ByteType)
  }

  test("cast FloatType to ShortType") {
    castTest(generateFloats(), DataTypes.ShortType)
  }

  test("cast FloatType to IntegerType") {
    castTest(generateFloats(), DataTypes.IntegerType)
  }

  test("cast FloatType to LongType") {
    castTest(generateFloats(), DataTypes.LongType)
  }

  test("cast FloatType to DoubleType") {
    castTest(generateFloats(), DataTypes.DoubleType)
  }

  ignore("cast FloatType to DecimalType(10,2)") {
    // // https://github.com/apache/datafusion-comet/issues/1371
    castTest(generateFloats(), DataTypes.createDecimalType(10, 2))
  }

  test("cast FloatType to DecimalType(10,2) - allow incompat") {
    withSQLConf(CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
      castTest(generateFloats(), DataTypes.createDecimalType(10, 2))
    }
  }

  test("cast FloatType to StringType") {
    // https://github.com/apache/datafusion-comet/issues/312
    val r = new Random(0)
    val values = Seq(
      Float.MaxValue,
      Float.MinValue,
      Float.NaN,
      Float.PositiveInfinity,
      Float.NegativeInfinity,
      1.0f,
      -1.0f,
      Short.MinValue.toFloat,
      Short.MaxValue.toFloat,
      0.0f) ++
      Range(0, dataSize).map(_ => r.nextFloat())
    castTest(withNulls(values).toDF("a"), DataTypes.StringType)
  }

  ignore("cast FloatType to TimestampType") {
    // java.lang.ArithmeticException: long overflow
    castTest(generateFloats(), DataTypes.TimestampType)
  }

  // CAST from DoubleType

  test("cast DoubleType to BooleanType") {
    castTest(generateDoubles(), DataTypes.BooleanType)
  }

  test("cast DoubleType to ByteType") {
    castTest(generateDoubles(), DataTypes.ByteType)
  }

  test("cast DoubleType to ShortType") {
    castTest(generateDoubles(), DataTypes.ShortType)
  }

  test("cast DoubleType to IntegerType") {
    castTest(generateDoubles(), DataTypes.IntegerType)
  }

  test("cast DoubleType to LongType") {
    castTest(generateDoubles(), DataTypes.LongType)
  }

  test("cast DoubleType to FloatType") {
    castTest(generateDoubles(), DataTypes.FloatType)
  }

  ignore("cast DoubleType to DecimalType(10,2)") {
    // https://github.com/apache/datafusion-comet/issues/1371
    castTest(generateDoubles(), DataTypes.createDecimalType(10, 2))
  }

  test("cast DoubleType to DecimalType(10,2) - allow incompat") {
    withSQLConf(CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
      castTest(generateDoubles(), DataTypes.createDecimalType(10, 2))
    }
  }

  test("cast DoubleType to StringType") {
    // https://github.com/apache/datafusion-comet/issues/312
    val r = new Random(0)
    val values = Seq(
      Double.MaxValue,
      Double.MinValue,
      Double.NaN,
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      0.0d) ++
      Range(0, dataSize).map(_ => r.nextDouble())
    castTest(withNulls(values).toDF("a"), DataTypes.StringType)
  }

  ignore("cast DoubleType to TimestampType") {
    // java.lang.ArithmeticException: long overflow
    castTest(generateDoubles(), DataTypes.TimestampType)
  }

  // CAST from DecimalType(10,2)

  ignore("cast DecimalType(10,2) to BooleanType") {
    // Arrow error: Cast error: Casting from Decimal128(38, 18) to Boolean not supported
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.BooleanType)
  }

  test("cast DecimalType(10,2) to ByteType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.ByteType)
  }

  test("cast DecimalType(10,2) to ShortType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.ShortType)
  }

  test("cast DecimalType(10,2) to IntegerType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.IntegerType)
  }

  test("cast DecimalType(10,2) to LongType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.LongType)
  }

  test("cast DecimalType(10,2) to FloatType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.FloatType)
  }

  test("cast DecimalType(10,2) to DoubleType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.DoubleType)
  }

  test("cast DecimalType(38,18) to ByteType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.ByteType)
  }

  test("cast DecimalType(38,18) to ShortType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.ShortType)
  }

  test("cast DecimalType(38,18) to IntegerType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.IntegerType)
  }

  test("cast DecimalType(38,18) to LongType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.LongType)
  }

  test("cast DecimalType(10,2) to StringType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.StringType)
  }

  ignore("cast DecimalType(10,2) to TimestampType") {
    // input: -123456.789000000000000000, expected: 1969-12-30 05:42:23.211, actual: 1969-12-31 15:59:59.876544
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.TimestampType)
  }

  // CAST from StringType

  test("cast StringType to BooleanType") {
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
    val testValues =
      (Seq("TRUE", "True", "true", "FALSE", "False", "false", "1", "0", "", null) ++
        gen.generateStrings(dataSize, "truefalseTRUEFALSEyesno10" + whitespaceChars, 8)).toDF("a")
    castTest(testValues, DataTypes.BooleanType)
  }

  private val castStringToIntegralInputs: Seq[String] = Seq(
    "",
    ".",
    "+",
    "-",
    "+.",
    "-.",
    "-0",
    "+1",
    "-1",
    ".2",
    "-.2",
    "1e1",
    "1.1d",
    "1.1f",
    Byte.MinValue.toString,
    (Byte.MinValue.toShort - 1).toString,
    Byte.MaxValue.toString,
    (Byte.MaxValue.toShort + 1).toString,
    Short.MinValue.toString,
    (Short.MinValue.toInt - 1).toString,
    Short.MaxValue.toString,
    (Short.MaxValue.toInt + 1).toString,
    Int.MinValue.toString,
    (Int.MinValue.toLong - 1).toString,
    Int.MaxValue.toString,
    (Int.MaxValue.toLong + 1).toString,
    Long.MinValue.toString,
    Long.MaxValue.toString,
    "-9223372036854775809", // Long.MinValue -1
    "9223372036854775808" // Long.MaxValue + 1
  )

  test("cast StringType to ByteType") {
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
    // test with hand-picked values
    castTest(castStringToIntegralInputs.toDF("a"), DataTypes.ByteType)
    // fuzz test
    castTest(gen.generateStrings(dataSize, numericPattern, 4).toDF("a"), DataTypes.ByteType)
  }

  test("cast StringType to ShortType") {
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
    // test with hand-picked values
    castTest(castStringToIntegralInputs.toDF("a"), DataTypes.ShortType)
    // fuzz test
    castTest(gen.generateStrings(dataSize, numericPattern, 5).toDF("a"), DataTypes.ShortType)
  }

  test("cast StringType to IntegerType") {
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
    // test with hand-picked values
    castTest(castStringToIntegralInputs.toDF("a"), DataTypes.IntegerType)
    // fuzz test
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.IntegerType)
  }

  test("cast StringType to LongType") {
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
    // test with hand-picked values
    castTest(castStringToIntegralInputs.toDF("a"), DataTypes.LongType)
    // fuzz test
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.LongType)
  }

  ignore("cast StringType to FloatType") {
    // https://github.com/apache/datafusion-comet/issues/326
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.FloatType)
  }

  test("cast StringType to FloatType (partial support)") {
    withSQLConf(
      CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "false") {
      castTest(
        gen.generateStrings(dataSize, "0123456789.", 8).toDF("a"),
        DataTypes.FloatType,
        testAnsi = false)
    }
  }

  ignore("cast StringType to DoubleType") {
    // https://github.com/apache/datafusion-comet/issues/326
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.DoubleType)
  }

  test("cast StringType to DoubleType (partial support)") {
    withSQLConf(
      CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "false") {
      castTest(
        gen.generateStrings(dataSize, "0123456789.", 8).toDF("a"),
        DataTypes.DoubleType,
        testAnsi = false)
    }
  }

  ignore("cast StringType to DecimalType(10,2)") {
    // https://github.com/apache/datafusion-comet/issues/325
    val values = gen.generateStrings(dataSize, numericPattern, 8).toDF("a")
    castTest(values, DataTypes.createDecimalType(10, 2))
  }

  test("cast StringType to DecimalType(10,2) (partial support)") {
    withSQLConf(
      CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "false") {
      val values = gen
        .generateStrings(dataSize, "0123456789.", 8)
        .filter(_.exists(_.isDigit))
        .toDF("a")
      castTest(values, DataTypes.createDecimalType(10, 2), testAnsi = false)
    }
  }

  test("cast StringType to BinaryType") {
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.BinaryType)
  }

  test("cast StringType to DateType") {
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
    val validDates = Seq(
      "262142-01-01",
      "262142-01-01 ",
      "262142-01-01T ",
      "262142-01-01T 123123123",
      "-262143-12-31",
      "-262143-12-31 ",
      "-262143-12-31T",
      "-262143-12-31T ",
      "-262143-12-31T 123123123",
      "2020",
      "2020-1",
      "2020-1-1",
      "2020-01",
      "2020-01-01",
      "2020-1-01 ",
      "2020-01-1",
      "02020-01-01",
      "2020-01-01T",
      "2020-10-01T  1221213",
      "002020-01-01  ",
      "0002020-01-01  123344",
      "-3638-5")
    val invalidDates = Seq(
      "0",
      "202",
      "3/",
      "3/3/",
      "3/3/2020",
      "3#3#2020",
      "2020-010-01",
      "2020-10-010",
      "2020-10-010T",
      "--262143-12-31",
      "--262143-12-31T 1234 ",
      "abc-def-ghi",
      "abc-def-ghi jkl",
      "2020-mar-20",
      "not_a_date",
      "T2",
      "\t\n3938\n8",
      "8701\t",
      "\n8757",
      "7593\t\t\t",
      "\t9374 \n ",
      "\n 9850 \t",
      "\r\n\t9840",
      "\t9629\n",
      "\r\n 9629 \r\n",
      "\r\n 962 \r\n",
      "\r\n 62 \r\n")

    // due to limitations of NaiveDate we only support years between 262143 BC and 262142 AD"
    val unsupportedYearPattern: Regex = "^\\s*[0-9]{5,}".r
    val fuzzDates = gen
      .generateStrings(dataSize, datePattern, 8)
      .filterNot(str => unsupportedYearPattern.findFirstMatchIn(str).isDefined)
    castTest((validDates ++ invalidDates ++ fuzzDates).toDF("a"), DataTypes.DateType)
  }

  test("cast StringType to TimestampType disabled by default") {
    withSQLConf((SQLConf.SESSION_LOCAL_TIMEZONE.key, "UTC")) {
      val values = Seq("2020-01-01T12:34:56.123456", "T2").toDF("a")
      castFallbackTest(
        values.toDF("a"),
        DataTypes.TimestampType,
        "Not all valid formats are supported")
    }
  }

  ignore("cast StringType to TimestampType") {
    // https://github.com/apache/datafusion-comet/issues/328
    withSQLConf((CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key, "true")) {
      val values = Seq("2020-01-01T12:34:56.123456", "T2") ++ gen.generateStrings(
        dataSize,
        timestampPattern,
        8)
      castTest(values.toDF("a"), DataTypes.TimestampType)
    }
  }

  test("cast StringType to TimestampType disabled for non-UTC timezone") {
    withSQLConf((SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Denver")) {
      val values = Seq("2020-01-01T12:34:56.123456", "T2").toDF("a")
      castFallbackTest(
        values.toDF("a"),
        DataTypes.TimestampType,
        "Cast will use UTC instead of Some(America/Denver)")
    }
  }

  test("cast StringType to TimestampType - subset of supported values") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu") {
      val values = Seq(
        "2020",
        "2020-01",
        "2020-01-01",
        "2020-01-01T12",
        "2020-01-01T12:34",
        "2020-01-01T12:34:56",
        "2020-01-01T12:34:56.123456",
        "T2",
        "-9?",
        "0100",
        "0100-01",
        "0100-01-01",
        "0100-01-01T12",
        "0100-01-01T12:34",
        "0100-01-01T12:34:56",
        "0100-01-01T12:34:56.123456",
        "10000",
        "10000-01",
        "10000-01-01",
        "10000-01-01T12",
        "10000-01-01T12:34",
        "10000-01-01T12:34:56",
        "10000-01-01T12:34:56.123456")
      castTimestampTest(values.toDF("a"), DataTypes.TimestampType)
    }

    // test for invalid inputs
    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
      CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
      val values = Seq("-9?", "1-", "0.5")
      castTimestampTest(values.toDF("a"), DataTypes.TimestampType)
    }
  }

  // CAST from BinaryType

  ignore("cast BinaryType to StringType") {
    // https://github.com/apache/datafusion-comet/issues/377
    castTest(generateBinary(), DataTypes.StringType)
  }

  test("cast BinaryType to StringType - valid UTF-8 inputs") {
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.StringType)
  }

  // CAST from DateType

  ignore("cast DateType to BooleanType") {
    // Arrow error: Cast error: Casting from Date32 to Boolean not supported
    castTest(generateDates(), DataTypes.BooleanType)
  }

  ignore("cast DateType to ByteType") {
    // Arrow error: Cast error: Casting from Date32 to Int8 not supported
    castTest(generateDates(), DataTypes.ByteType)
  }

  ignore("cast DateType to ShortType") {
    // Arrow error: Cast error: Casting from Date32 to Int16 not supported
    castTest(generateDates(), DataTypes.ShortType)
  }

  ignore("cast DateType to IntegerType") {
    // input: 2345-01-01, expected: null, actual: 3789391
    castTest(generateDates(), DataTypes.IntegerType)
  }

  ignore("cast DateType to LongType") {
    // input: 2024-01-01, expected: null, actual: 19723
    castTest(generateDates(), DataTypes.LongType)
  }

  ignore("cast DateType to FloatType") {
    // Arrow error: Cast error: Casting from Date32 to Float32 not supported
    castTest(generateDates(), DataTypes.FloatType)
  }

  ignore("cast DateType to DoubleType") {
    // Arrow error: Cast error: Casting from Date32 to Float64 not supported
    castTest(generateDates(), DataTypes.DoubleType)
  }

  ignore("cast DateType to DecimalType(10,2)") {
    // Arrow error: Cast error: Casting from Date32 to Decimal128(10, 2) not supported
    castTest(generateDates(), DataTypes.createDecimalType(10, 2))
  }

  test("cast DateType to StringType") {
    castTest(generateDates(), DataTypes.StringType)
  }

  ignore("cast DateType to TimestampType") {
    // Arrow error: Cast error: Casting from Date32 to Timestamp(Microsecond, Some("UTC")) not supported
    castTest(generateDates(), DataTypes.TimestampType)
  }

  // CAST from TimestampType

  ignore("cast TimestampType to BooleanType") {
    // Arrow error: Cast error: Casting from Timestamp(Microsecond, Some("America/Los_Angeles")) to Boolean not supported
    castTest(generateTimestamps(), DataTypes.BooleanType)
  }

  ignore("cast TimestampType to ByteType") {
    // https://github.com/apache/datafusion-comet/issues/352
    // input: 2023-12-31 10:00:00.0, expected: 32, actual: null
    castTest(generateTimestamps(), DataTypes.ByteType)
  }

  ignore("cast TimestampType to ShortType") {
    // https://github.com/apache/datafusion-comet/issues/352
    // input: 2023-12-31 10:00:00.0, expected: -21472, actual: null]
    castTest(generateTimestamps(), DataTypes.ShortType)
  }

  ignore("cast TimestampType to IntegerType") {
    // https://github.com/apache/datafusion-comet/issues/352
    // input: 2023-12-31 10:00:00.0, expected: 1704045600, actual: null]
    castTest(generateTimestamps(), DataTypes.IntegerType)
  }

  test("cast TimestampType to LongType") {
    castTest(generateTimestampsExtended(), DataTypes.LongType)
  }

  ignore("cast TimestampType to FloatType") {
    // https://github.com/apache/datafusion-comet/issues/352
    // input: 2023-12-31 10:00:00.0, expected: 1.7040456E9, actual: 1.7040456E15
    castTest(generateTimestamps(), DataTypes.FloatType)
  }

  ignore("cast TimestampType to DoubleType") {
    // https://github.com/apache/datafusion-comet/issues/352
    // input: 2023-12-31 10:00:00.0, expected: 1.7040456E9, actual: 1.7040456E15
    castTest(generateTimestamps(), DataTypes.DoubleType)
  }

  ignore("cast TimestampType to DecimalType(10,2)") {
    // https://github.com/apache/datafusion-comet/issues/1280
    // Native cast invoked for unsupported cast from Timestamp(Microsecond, Some("Etc/UTC")) to Decimal128(10, 2)
    castTest(generateTimestamps(), DataTypes.createDecimalType(10, 2))
  }

  test("cast TimestampType to StringType") {
    castTest(generateTimestamps(), DataTypes.StringType)
  }

  test("cast TimestampType to DateType") {
    castTest(generateTimestamps(), DataTypes.DateType)
  }

  // Complex Types

  test("cast StructType to StringType") {
    // https://github.com/apache/datafusion-comet/issues/1441
    assume(!usingDataSourceExec)
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          // primitives
          checkSparkAnswerAndOperator(
            "SELECT CAST(struct(_1, _2, _3, _4, _5, _6, _7, _8) as string) FROM tbl")
          checkSparkAnswerAndOperator("SELECT CAST(struct(_9, _10, _11, _12) as string) FROM tbl")
          // decimals
          // TODO add _16 when https://github.com/apache/datafusion-comet/issues/1068 is resolved
          checkSparkAnswerAndOperator("SELECT CAST(struct(_15, _17) as string) FROM tbl")
          // dates & timestamps
          checkSparkAnswerAndOperator("SELECT CAST(struct(_18, _19, _20) as string) FROM tbl")
          // named struct
          checkSparkAnswerAndOperator(
            "SELECT CAST(named_struct('a', _1, 'b', _2) as string) FROM tbl")
          // nested struct
          checkSparkAnswerAndOperator(
            "SELECT CAST(named_struct('a', named_struct('b', _1, 'c', _2)) as string) FROM tbl")
        }
      }
    }
  }

  test("cast StructType to StructType") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          checkSparkAnswerAndOperator(
            "SELECT CAST(CASE WHEN _1 THEN struct(_1, _2, _3, _4) ELSE null END as " +
              "struct<_1:string, _2:string, _3:string, _4:string>) FROM tbl")
        }
      }
    }
  }

  test("cast StructType to StructType with different names") {
    withTable("tab1") {
      sql("""
           |CREATE TABLE tab1 (s struct<a: string, b: string>)
           |USING parquet
         """.stripMargin)
      sql("INSERT INTO TABLE tab1 SELECT named_struct('col1','1','col2','2')")
      if (usingDataSourceExec) {
        checkSparkAnswerAndOperator(
          "SELECT CAST(s AS struct<field1:string, field2:string>) AS new_struct FROM tab1")
      } else {
        // Should just fall back to Spark since non-DataSourceExec scan does not support nested types.
        checkSparkAnswer(
          "SELECT CAST(s AS struct<field1:string, field2:string>) AS new_struct FROM tab1")
      }
    }
  }

  test("cast between decimals with different precision and scale") {
    val rowData = Seq(
      Row(BigDecimal("12345.6789")),
      Row(BigDecimal("9876.5432")),
      Row(BigDecimal("123.4567")))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(rowData),
      StructType(Seq(StructField("a", DataTypes.createDecimalType(10, 4)))))

    castTest(df, DecimalType(6, 2))
  }

  test("cast between decimals with higher precision than source") {
    // cast between Decimal(10, 2) to Decimal(10,4)
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.createDecimalType(10, 4))
  }

  test("cast between decimals with negative precision") {
    // cast to negative scale
    checkSparkMaybeThrows(
      spark.sql("select a, cast(a as DECIMAL(10,-4)) from t order by a")) match {
      case (expected, actual) =>
        assert(expected.contains("PARSE_SYNTAX_ERROR") === actual.contains("PARSE_SYNTAX_ERROR"))
    }
  }

  test("cast between decimals with zero precision") {
    // cast between Decimal(10, 2) to Decimal(10,0)
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.createDecimalType(10, 0))
  }

  private def generateFloats(): DataFrame = {
    withNulls(gen.generateFloats(dataSize)).toDF("a")
  }

  private def generateDoubles(): DataFrame = {
    withNulls(gen.generateDoubles(dataSize)).toDF("a")
  }

  private def generateBools(): DataFrame = {
    withNulls(Seq(true, false)).toDF("a")
  }

  private def generateBytes(): DataFrame = {
    withNulls(gen.generateBytes(dataSize)).toDF("a")
  }

  private def generateShorts(): DataFrame = {
    withNulls(gen.generateShorts(dataSize)).toDF("a")
  }

  private def generateInts(): DataFrame = {
    withNulls(gen.generateInts(dataSize)).toDF("a")
  }

  private def generateLongs(): DataFrame = {
    withNulls(gen.generateLongs(dataSize)).toDF("a")
  }

  private def generateDecimalsPrecision10Scale2(): DataFrame = {
    val values = Seq(
      BigDecimal("-99999999.999"),
      BigDecimal("-123456.789"),
      BigDecimal("-32768.678"),
      // Short Min
      BigDecimal("-32767.123"),
      BigDecimal("-128.12312"),
      // Byte Min
      BigDecimal("-127.123"),
      BigDecimal("0.0"),
      // Byte Max
      BigDecimal("127.123"),
      BigDecimal("128.12312"),
      BigDecimal("32767.122"),
      // Short Max
      BigDecimal("32768.678"),
      BigDecimal("123456.789"),
      BigDecimal("99999999.999"))
    withNulls(values).toDF("b").withColumn("a", col("b").cast(DecimalType(10, 2))).drop("b")
  }

  private def generateDecimalsPrecision38Scale18(): DataFrame = {
    val values = Seq(
      BigDecimal("-99999999999999999999.999999999999"),
      BigDecimal("-9223372036854775808.234567"),
      // Long Min
      BigDecimal("-9223372036854775807.123123"),
      BigDecimal("-2147483648.123123123"),
      // Int Min
      BigDecimal("-2147483647.123123123"),
      BigDecimal("-123456.789"),
      BigDecimal("0.00000000000"),
      BigDecimal("123456.789"),
      // Int Max
      BigDecimal("2147483647.123123123"),
      BigDecimal("2147483648.123123123"),
      BigDecimal("9223372036854775807.123123"),
      // Long Max
      BigDecimal("9223372036854775808.234567"),
      BigDecimal("99999999999999999999.999999999999"))
    withNulls(values).toDF("a")
  }

  private def generateDates(): DataFrame = {
    val values = Seq("2024-01-01", "999-01-01", "12345-01-01")
    withNulls(values).toDF("b").withColumn("a", col("b").cast(DataTypes.DateType)).drop("b")
  }

  // Extended values are Timestamps that are outside dates supported chrono::DateTime and
  // therefore not supported by operations using it.
  private def generateTimestampsExtended(): DataFrame = {
    val values = Seq("290000-12-31T01:00:00+02:00")
    generateTimestamps().unionByName(
      values.toDF("str").select(col("str").cast(DataTypes.TimestampType).as("a")))
  }

  private def generateTimestamps(): DataFrame = {
    val values =
      Seq(
        "2024-01-01T12:34:56.123456",
        "2024-01-01T01:00:00Z",
        "9999-12-31T01:00:00-02:00",
        "2024-12-31T01:00:00+02:00")
    withNulls(values)
      .toDF("str")
      .withColumn("a", col("str").cast(DataTypes.TimestampType))
      .drop("str")
  }

  private def generateBinary(): DataFrame = {
    val r = new Random(0)
    val bytes = new Array[Byte](8)
    val values: Seq[Array[Byte]] = Range(0, dataSize).map(_ => {
      r.nextBytes(bytes)
      bytes.clone()
    })
    values.toDF("a")
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

  private def castTimestampTest(input: DataFrame, toType: DataType) = {
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
    }
  }

  private def castTest(
      input: DataFrame,
      toType: DataType,
      hasIncompatibleType: Boolean = false,
      testAnsi: Boolean = true): Unit = {

    withTempPath { dir =>
      val data = roundtripParquet(input, dir).coalesce(1)
      data.createOrReplaceTempView("t")

      withSQLConf((SQLConf.ANSI_ENABLED.key, "false")) {
        // cast() should return null for invalid inputs when ansi mode is disabled
        val df = spark.sql(s"select a, cast(a as ${toType.sql}) from t order by a")
        if (hasIncompatibleType) {
          checkSparkAnswer(df)
        } else {
          checkSparkAnswerAndOperator(df)
        }

        // try_cast() should always return null for invalid inputs
        val df2 =
          spark.sql(s"select a, try_cast(a as ${toType.sql}) from t order by a")
        if (hasIncompatibleType) {
          checkSparkAnswer(df2)
        } else {
          checkSparkAnswerAndOperator(df2)
        }
      }

      if (testAnsi) {
        // with ANSI enabled, we should produce the same exception as Spark
        withSQLConf((SQLConf.ANSI_ENABLED.key, "true")) {

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
              val sparkMessage =
                if (sparkException.getCause != null) sparkException.getCause.getMessage
                else sparkException.getMessage
              val cometMessage =
                if (cometException.getCause != null) cometException.getCause.getMessage
                else cometException.getMessage
              // this if branch should only check decimal to decimal cast and errors when output precision, scale causes overflow.
              if (df.schema("a").dataType.typeName.contains("decimal") && toType.typeName
                  .contains("decimal") && sparkMessage.contains("cannot be represented as")) {
                assert(cometMessage.contains("too large to store"))
              } else {
                if (CometSparkSessionExtensions.isSpark40Plus) {
                  // for Spark 4 we expect to sparkException carries the message
                  assert(
                    sparkException.getMessage
                      .replace(".WITH_SUGGESTION] ", "]")
                      .startsWith(cometMessage))
                } else {
                  // for Spark 3.4 we expect to reproduce the error message exactly
                  assert(cometMessage == sparkMessage)
                }
              }
          }

          // try_cast() should always return null for invalid inputs
          val df2 =
            spark.sql(s"select a, try_cast(a as ${toType.sql}) from t order by a")
          if (hasIncompatibleType) {
            checkSparkAnswer(df2)
          } else {
            checkSparkAnswerAndOperator(df2)
          }

        }
      }
    }
  }

  private def roundtripParquet(df: DataFrame, tempDir: File): DataFrame = {
    val filename = new File(tempDir, s"castTest_${System.currentTimeMillis()}.parquet").toString
    df.write.mode(SaveMode.Overwrite).parquet(filename)
    spark.read.parquet(filename)
  }

}
