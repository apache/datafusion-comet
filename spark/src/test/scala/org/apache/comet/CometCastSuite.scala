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

import scala.collection.mutable.ListBuffer
import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CometTestBase, DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DataTypes, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import org.apache.comet.CometSparkSessionExtensions.isSpark41Plus
import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.rules.CometScanTypeChecker
import org.apache.comet.serde.{Compatible, Incompatible}

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
    hasUnsignedSmallIntSafetyCheck(conf)

  // Timezone list to check temporal type casts
  private val representativeTimezones = Seq(
    // UTC
    "UTC",
    // North America
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
    "America/Sao_Paulo", // South America, UTC-3 (no DST in winter)
    // Europe
    "Europe/London",
    "Europe/Paris",
    "Europe/Berlin",
    // Africa
    "Africa/Cairo", // UTC+2, no DST
    "Africa/Johannesburg", // UTC+2, no DST
    // Middle East
    "Asia/Dubai", // UTC+4, no DST
    // Asia/Tehran omitted: IANA tzdata for 1977-1979 Iran has been revised multiple times
    // (UTC+4 vs UTC+3:30 as standard for parts of that period), causing JDK and chrono-tz
    // to disagree on historical dates. Use Asia/Dubai for UTC+4 coverage.
    // Asia
    "Asia/Tokyo",
    "Asia/Shanghai",
    // Asia/Singapore omitted: changed UTC+7:30 -> UTC+8 in 1982; test dates go back to 1970
    // and JDK tzdata versions may disagree with chrono-tz on the historical offset.
    "Asia/Kolkata", // UTC+5:30 (half-hour offset)
    "Asia/Kathmandu", // UTC+5:45 (quarter-hour offset)
    // Oceania
    "Australia/Sydney",
    "Pacific/Auckland",
    "Pacific/Chatham" // UTC+12:45 (quarter-hour offset)
  )

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

  test("cast BooleanType to DecimalType(10,2)") {
    castTest(generateBools(), DataTypes.createDecimalType(10, 2))
  }

  test("cast BooleanType to DecimalType(14,4)") {
    castTest(generateBools(), DataTypes.createDecimalType(14, 4))
  }

  test("cast BooleanType to DecimalType(30,0)") {
    castTest(generateBools(), DataTypes.createDecimalType(30, 0))
  }

  test("cast BooleanType to StringType") {
    castTest(generateBools(), DataTypes.StringType)
  }

  test("cast BooleanType to TimestampType") {
    // Spark does not support ANSI or Try mode for Boolean to Timestamp casts
    castTest(generateBools(), DataTypes.TimestampType, testAnsi = false, testTry = false)
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

  test("cast ByteType to BinaryType") {
    //    Spark does not support ANSI or Try mode
    castTest(
      generateBytes(),
      DataTypes.BinaryType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes,
      testAnsi = false,
      testTry = false)
  }

  test("cast ByteType to TimestampType") {
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        castTest(
          generateBytes(),
          DataTypes.TimestampType,
          hasIncompatibleType = usingParquetExecWithIncompatTypes)
      }
    }
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

  test("cast ShortType to BinaryType") {
//    Spark does not support ANSI or Try mode
    castTest(
      generateShorts(),
      DataTypes.BinaryType,
      hasIncompatibleType = usingParquetExecWithIncompatTypes,
      testAnsi = false,
      testTry = false)
  }

  test("cast ShortType to TimestampType") {
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        castTest(
          generateShorts(),
          DataTypes.TimestampType,
          hasIncompatibleType = usingParquetExecWithIncompatTypes)
      }
    }
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

  test("cast IntegerType to DecimalType(10,2)") {
    castTest(generateInts(), DataTypes.createDecimalType(10, 2))
  }

  test("cast IntegerType to DecimalType(10,2) overflow check") {
    val intToDecimal10OverflowValues =
      Seq(Int.MinValue, -100000000, -100000001, 100000000, 100000001, Int.MaxValue).toDF("a")
    castTest(intToDecimal10OverflowValues, DataTypes.createDecimalType(10, 2))
  }

  test("cast IntegerType to DecimalType check arbitrary scale and precision") {
    Seq(DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE, 0, 10, 15)
      .combinations(2)
      .map({ c =>
        castTest(generateInts(), DataTypes.createDecimalType(c.head, c.last))
      })
  }

  test("cast IntegerType to StringType") {
    castTest(generateInts(), DataTypes.StringType)
  }

  test("cast IntegerType to BinaryType") {
    //    Spark does not support ANSI or Try mode
    castTest(generateInts(), DataTypes.BinaryType, testAnsi = false, testTry = false)
  }

  test("cast IntegerType to TimestampType") {
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        castTest(generateInts(), DataTypes.TimestampType)
      }
    }
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

  test("cast LongType to DecimalType(10,2)") {
    castTest(generateLongs(), DataTypes.createDecimalType(10, 2))
  }

  test("cast LongType to StringType") {
    castTest(generateLongs(), DataTypes.StringType)
  }

  test("cast LongType to BinaryType") {
    //    Spark does not support ANSI or Try mode
    castTest(generateLongs(), DataTypes.BinaryType, testAnsi = false, testTry = false)
  }

  test("cast LongType to TimestampType") {
    // Cast back to long avoids java.sql.Timestamp overflow during collect() for extreme values
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        withTable("t1") {
          generateLongs().write.saveAsTable("t1")
          val df = spark.sql("select a, cast(cast(a as timestamp) as long) from t1")
          checkSparkAnswerAndOperator(df)
        }
      }
    }
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
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[Cast]) -> "true") {
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
      -0.0f,
      0.0f) ++
      Range(0, dataSize).map(_ => r.nextFloat())
    castTest(withNulls(values).toDF("a"), DataTypes.StringType)
  }

  test("cast FloatType to TimestampType") {
    assume(!isSpark41Plus, "https://github.com/apache/datafusion-comet/issues/4098")
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        // Use useDFDiff to avoid collect() which fails on extreme timestamp values
        castTest(generateFloats(), DataTypes.TimestampType, useDataFrameDiff = true)
      }
    }
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
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[Cast]) -> "true") {
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
      1.0d,
      -1.0d,
      Int.MinValue.toDouble,
      Int.MaxValue.toDouble,
      -0.0d,
      0.0d) ++
      Range(0, dataSize).map(_ => r.nextDouble())
    castTest(withNulls(values).toDF("a"), DataTypes.StringType)
  }

  test("cast DoubleType to TimestampType") {
    assume(!isSpark41Plus, "https://github.com/apache/datafusion-comet/issues/4098")
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        // Use useDFDiff to avoid collect() which fails on extreme timestamp values
        castTest(generateDoubles(), DataTypes.TimestampType, useDataFrameDiff = true)
      }
    }
  }

  // CAST from DecimalType(10,2)

  test("cast DecimalType(10,2) to BooleanType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.BooleanType)
  }

  test("cast DecimalType(10,2) to ByteType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.ByteType)
  }

  test("cast DecimalType(10,2) to ShortType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.ShortType)
    castTest(
      generateDecimalsPrecision10Scale2(Seq(BigDecimal("-96833550.07"))),
      DataTypes.ShortType)
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

  // CAST from DecimalType(15,5): fractional truncation for int/long; int overflow possible

  test("cast DecimalType(15,5) to IntegerType") {
    castTest(generateDecimalsPrecision15Scale5(), DataTypes.IntegerType)
  }

  test("cast DecimalType(15,5) to LongType") {
    castTest(generateDecimalsPrecision15Scale5(), DataTypes.LongType)
  }

  test("cast DecimalType(15,5) to BooleanType") {
    castTest(generateDecimalsPrecision15Scale5(), DataTypes.BooleanType)
  }

  // CAST from DecimalType(20,0): large integers with no fractional part; long overflow possible

  test("cast DecimalType(20,0) to IntegerType") {
    castTest(generateDecimalsPrecision20Scale0(), DataTypes.IntegerType)
  }

  test("cast DecimalType(20,0) to LongType") {
    castTest(generateDecimalsPrecision20Scale0(), DataTypes.LongType)
  }

  test("cast DecimalType(20,0) to BooleanType") {
    castTest(generateDecimalsPrecision20Scale0(), DataTypes.BooleanType)
  }

  test("cast DecimalType(38,18) to ByteType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.ByteType)
  }

  test("cast DecimalType(38,18) to ShortType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.ShortType)
    castTest(
      generateDecimalsPrecision38Scale18(Seq(BigDecimal("-99999999999999999999.07"))),
      DataTypes.ShortType)
  }

  test("cast DecimalType(38,18) to IntegerType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.IntegerType)
    castTest(
      generateDecimalsPrecision38Scale18(Seq(BigDecimal("-99999999999999999999.07"))),
      DataTypes.IntegerType)
  }

  test("cast DecimalType(38,18) to LongType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.LongType)
    castTest(
      generateDecimalsPrecision38Scale18(Seq(BigDecimal("-99999999999999999999.07"))),
      DataTypes.LongType)
  }

  test("cast DecimalType(38,18) to FloatType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.FloatType)
    // small fractions exercise the i128 / 10^scale precision path
    castTest(
      generateDecimalsPrecision38Scale18(
        Seq(
          BigDecimal("0.000000000000000001"),
          BigDecimal("-0.000000000000000001"),
          BigDecimal("1.500000000000000000"),
          BigDecimal("123456789.123456789"))),
      DataTypes.FloatType)
  }

  test("cast DecimalType(38,18) to DoubleType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.DoubleType)
    // small fractions exercise the i128 / 10^scale precision path
    castTest(
      generateDecimalsPrecision38Scale18(
        Seq(
          BigDecimal("0.000000000000000001"),
          BigDecimal("-0.000000000000000001"),
          BigDecimal("1.500000000000000000"),
          BigDecimal("123456789.123456789"))),
      DataTypes.DoubleType)
  }

  test("cast DecimalType(38,18) to BooleanType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.BooleanType)
    // tiny non-zero values must be true; only exact zero is false
    castTest(
      generateDecimalsPrecision38Scale18(
        Seq(BigDecimal("0.000000000000000001"), BigDecimal("-0.000000000000000001"))),
      DataTypes.BooleanType)
  }

  test("cast DecimalType(10,2) to StringType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.StringType)
  }

  test("cast DecimalType(38,18) to StringType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.StringType)
  }

  test("cast DecimalType with negative scale to StringType") {
    // Negative-scale decimals are a legacy Spark feature gated on
    // spark.sql.legacy.allowNegativeScaleOfDecimal=true. Spark LEGACY cast uses Java's
    // BigDecimal.toString() which produces scientific notation for negative-scale values
    // (e.g. 12300 stored as Decimal(7,-2) with unscaled=123 → "1.23E+4").
    // CometCast.canCastToString checks the
    // config and returns Incompatible when it is false.
    //
    // Parquet does not support negative-scale decimals so we use checkSparkAnswer directly
    // (no parquet round-trip) to avoid schema coercion.

    // With config enabled, enable localTableScan so Comet can take over the full plan
    // and execute the cast natively. Parquet does not support negative-scale decimals so
    // the data is kept in-memory; localTableScan.enabled bridges that gap.
    withSQLConf(
      "spark.sql.legacy.allowNegativeScaleOfDecimal" -> "true",
      "spark.comet.exec.localTableScan.enabled" -> "true") {
      val dfNeg2 = Seq(
        Some(BigDecimal("0")),
        Some(BigDecimal("100")),
        Some(BigDecimal("12300")),
        Some(BigDecimal("-99900")),
        Some(BigDecimal("9999900")),
        None)
        .toDF("b")
        .withColumn("a", col("b").cast(DecimalType(7, -2)))
        .drop("b")
        .select(col("a").cast(DataTypes.StringType).as("result"))
      checkSparkAnswerAndOperator(dfNeg2)

      val dfNeg4 = Seq(
        Some(BigDecimal("0")),
        Some(BigDecimal("10000")),
        Some(BigDecimal("120000")),
        Some(BigDecimal("-9990000")),
        None)
        .toDF("b")
        .withColumn("a", col("b").cast(DecimalType(7, -4)))
        .drop("b")
        .select(col("a").cast(DataTypes.StringType).as("result"))
      checkSparkAnswerAndOperator(dfNeg4)
    }

    // With config disabled (default): the SQL parser rejects negative scale, so
    // negative-scale decimals cannot be created through normal SQL paths.
    // CometCast.isSupported returns Incompatible for this case, ensuring Comet does
    // not attempt native execution if such a value ever reaches the planner.
    // Note: DecimalType(7, -2) must be constructed while config=true, because the
    // constructor itself checks the config and throws if negative scale is disallowed.
    var negScaleType: DecimalType = null
    withSQLConf("spark.sql.legacy.allowNegativeScaleOfDecimal" -> "true") {
      negScaleType = DecimalType(7, -2)
    }
    withSQLConf("spark.sql.legacy.allowNegativeScaleOfDecimal" -> "false") {
      assert(
        CometCast.isSupported(
          negScaleType,
          DataTypes.StringType,
          None,
          CometEvalMode.LEGACY) == Incompatible())
    }
  }

  test("cast DecimalType(10,2) to TimestampType") {
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.TimestampType)
  }

  test("cast DecimalType(38,10) to TimestampType") {
    castTest(generateDecimalsPrecision38Scale18(), DataTypes.TimestampType)
  }

  // CAST from StringType

  test("cast StringType to BooleanType") {
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
    // test with hand-picked values
    castTest(castStringToIntegralInputs.toDF("a"), DataTypes.ByteType)
    // fuzz test
    castTest(gen.generateStrings(dataSize, numericPattern, 4).toDF("a"), DataTypes.ByteType)
  }

  test("cast StringType to ShortType") {
    // test with hand-picked values
    castTest(castStringToIntegralInputs.toDF("a"), DataTypes.ShortType)
    // fuzz test
    castTest(gen.generateStrings(dataSize, numericPattern, 5).toDF("a"), DataTypes.ShortType)
  }

  test("cast StringType to IntegerType") {
    // test with hand-picked values
    castTest(castStringToIntegralInputs.toDF("a"), DataTypes.IntegerType)
    // fuzz test
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.IntegerType)
  }

  test("cast StringType to LongType") {
    // test with hand-picked values
    castTest(castStringToIntegralInputs.toDF("a"), DataTypes.LongType)
    // fuzz test
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.LongType)
  }

  test("cast StringType to DoubleType") {
    // https://github.com/apache/datafusion-comet/issues/326
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.DoubleType)
  }

  test("cast StringType to FloatType") {
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.FloatType)
  }

  val specialValues: Seq[String] = Seq(
    "1.5f",
    "1.5F",
    "2.0d",
    "2.0D",
    "3.14159265358979d",
    "inf",
    "Inf",
    "INF",
    "+inf",
    "+Infinity",
    "-inf",
    "-Infinity",
    "NaN",
    "nan",
    "NAN",
    "1.23e4",
    "1.23E4",
    "-1.23e-4",
    "  123.456789  ",
    "0.0",
    "-0.0",
    "",
    "xyz",
    null)

  test("cast StringType to FloatType special values") {
    Seq(true, false).foreach { ansiMode =>
      castTest(specialValues.toDF("a"), DataTypes.FloatType, testAnsi = ansiMode)
    }
  }

  test("cast StringType to DoubleType special values") {
    Seq(true, false).foreach { ansiMode =>
      castTest(specialValues.toDF("a"), DataTypes.DoubleType, testAnsi = ansiMode)
    }
  }

//  This is to pass the first `all cast combinations are covered`
  test("cast StringType to DecimalType(10,2)") {
    val values = gen.generateStrings(dataSize, numericPattern, 12).toDF("a")
    castTest(values, DataTypes.createDecimalType(10, 2), testAnsi = false)
  }

  test("cast StringType to DecimalType(10,2) fuzz") {
    val values = gen.generateStrings(dataSize, numericPattern, 12).toDF("a")
    Seq(true, false).foreach(ansiEnabled =>
      castTest(values, DataTypes.createDecimalType(10, 2), testAnsi = ansiEnabled))
  }

  test("cast StringType to DecimalType(2,2)") {
    val values = gen.generateStrings(dataSize, numericPattern, 12).toDF("a")
    Seq(true, false).foreach(ansiEnabled =>
      castTest(values, DataTypes.createDecimalType(2, 2), testAnsi = ansiEnabled))
  }

  test("cast StringType to DecimalType check if right exception message is thrown") {
    val values = Seq("d11307\n").toDF("a")
    Seq(true, false).foreach(ansiEnabled =>
      castTest(values, DataTypes.createDecimalType(2, 2), testAnsi = ansiEnabled))
  }

  test("cast StringType to DecimalType(2,2) check if right exception is being thrown") {
    val values = gen.generateInts(10000).map("    " + _).toDF("a")
    Seq(true, false).foreach(ansiEnabled =>
      castTest(values, DataTypes.createDecimalType(2, 2), testAnsi = ansiEnabled))
  }

  test("cast StringType to DecimalType(38,10) high precision - check 0 mantissa") {
    val values = Seq("0e31", "000e3375", "0e40", "0E+695", "0e5887677").toDF("a")
    Seq(true, false).foreach(ansiEnabled =>
      castTest(values, DataTypes.createDecimalType(38, 10), testAnsi = ansiEnabled))
  }

  test("cast StringType to DecimalType(38,10) high precision") {
    val values = gen.generateStrings(dataSize, numericPattern, 38).toDF("a")
    Seq(true, false).foreach(ansiEnabled =>
      castTest(values, DataTypes.createDecimalType(38, 10), testAnsi = ansiEnabled))
  }

  test("cast StringType to DecimalType - null bytes and fullwidth digits") {
    // Spark trims null bytes (\u0000) from both ends of a string before parsing,
    // matching its whitespace-trim behavior. Null bytes in the middle produce NULL.
    // Fullwidth digits (U+FF10-U+FF19) are treated as numeric equivalents to ASCII digits.
    val values = Seq(
      // null byte positions
      "123\u0000",
      "\u0000123",
      "12\u00003",
      "1\u00002\u00003",
      "\u0000",
      // null byte with decimal point
      "12\u0000.45",
      "12.\u000045",
      // fullwidth digits (U+FF10-U+FF19)
      "１２３.４５", // "123.45" in fullwidth
      "１２３",
      "-１２３.４５",
      "+１２３.４５",
      "１２３.４５E２",
      // mixed fullwidth and ASCII
      "1２3.4５",
      null).toDF("a")
    castTest(values, DataTypes.createDecimalType(10, 2))
  }

  test("cast StringType to DecimalType(10,2) basic values") {
    val values = Seq(
      "123.45",
      "-67.89",
      "-67.89",
      "-67.895",
      "67.895",
      "0.001",
      "999.99",
      "123.456",
      "123.45D",
      ".5",
      "5.",
      "+123.45",
      "  123.45  ",
      "inf",
      "",
      "abc",
      // values from https://github.com/apache/datafusion-comet/issues/325
      "0",
      "1",
      "+1.0",
      ".34",
      "-10.0",
      "4e7",
      null).toDF("a")
    Seq(true, false).foreach(ansiEnabled =>
      castTest(values, DataTypes.createDecimalType(10, 2), testAnsi = ansiEnabled))
  }

  test("cast StringType to Decimal type scientific notation") {
    val values = Seq(
      "1.23E-5",
      "1.23e10",
      "1.23E+10",
      "-1.23e-5",
      "1e5",
      "1E-2",
      "-1.5e3",
      "1.23E0",
      "0e0",
      "1.23e",
      "e5",
      null).toDF("a")
    Seq(true, false).foreach(ansiEnabled =>
      castTest(values, DataTypes.createDecimalType(23, 8), testAnsi = ansiEnabled))
  }

  test("cast StringType to BinaryType") {
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.BinaryType)
  }

  test("cast StringType to DateType") {
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

    // due to limitations of NaiveDate we only support years between 262143 BC and 262142 AD
    // Filter out strings where the leading digit sequence represents a year > 262142.
    // All 5-digit years (10000-99999) are within bounds; only 6-digit years may exceed the limit.
    val fuzzDates = gen
      .generateStrings(dataSize, datePattern, 8)
      .filterNot { str =>
        val yearStr = str.trim.takeWhile(_.isDigit)
        yearStr.length > 6 || (yearStr.length == 6 && yearStr.toInt > 262142)
      }
    castTest((validDates ++ invalidDates ++ fuzzDates).toDF("a"), DataTypes.DateType)
  }

  // https://github.com/apache/datafusion-comet/issues/2215
  test("(ansi) cast error message should include SQL query") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      withTable("cast_error_msg") {
        // Create a table with string data using DataFrame API
        Seq("a").toDF("s").write.format("parquet").saveAsTable("cast_error_msg")
        // Try to cast invalid string to date - should throw exception with SQL context
        val exception = intercept[Exception] {
          sql("select cast(s as date) from cast_error_msg").collect()
        }
        val errorMessage = exception.getMessage
        // Verify error message contains the cast invalid input error
        assert(
          errorMessage.contains("CAST_INVALID_INPUT") ||
            errorMessage.contains("cannot be cast to"),
          s"Error message should contain cast error: $errorMessage")

        assert(
          errorMessage.contains("select cast(s as date) from cast_error_msg"),
          s"Error message should contain SQL query text but got: $errorMessage")
      }
    }
  }

  test("cast StringType to TimestampType - UTC") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
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
        "10000-01-01T12:34:56.123456",
        "213170",
        "213170-06",
        "213170-06-15",
        "213170-06-15T12",
        "213170-06-15T12:34",
        "213170-06-15T12:34:56",
        "213170-06-15T12:34:56.123456")
      castTimestampTest(values.toDF("a"), DataTypes.TimestampType, assertNative = true)
    }
  }

  test("cast StringType to TimestampType") {
    withSQLConf((SQLConf.SESSION_LOCAL_TIMEZONE.key, "UTC")) {
      val values = Seq("2020-01-01T12:34:56.123456", "T2") ++ gen.generateStrings(
        dataSize,
        timestampPattern,
        8)
      castTest(values.toDF("a"), DataTypes.TimestampType)
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
        "100",
        "100-01",
        "100-01-01",
        "100-01-01T12",
        "100-01-01T12:34",
        "100-01-01T12:34:56",
        "100-01-01T12:34:56.123456",
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
        "10000-01-01T12:34:56.123456",
        // Space separator
        "2020-01-01 12",
        "2020-01-01 12:34",
        "2020-01-01 12:34:56",
        "2020-01-01 12:34:56.123456",
        // Z and offset suffixes
        "2020-01-01T12:34:56Z",
        "2020-01-01T12:34:56+05:30",
        "2020-01-01T12:34:56-08:00",
        // Single-digit hour offset (extract_offset_suffix supports ±H:MM)
        "2020-01-01T12:34:56+5:30",
        // T-prefixed time-only with colon
        "T12:34",
        "T12:34:56",
        "T12:34:56.123456",
        // Bare time-only (hour:minute)
        "12:34",
        "12:34:56",
        // Negative year
        "-0001-01-01T12:34:56")
      castTimestampTest(values.toDF("a"), DataTypes.TimestampType, assertNative = true)
    }
  }

  test("cast StringType to TimestampType - T-hour-only whitespace handling") {
    // Spark 4.0+ changed whitespace handling for T-prefixed time-only strings:
    //   - Spark 3.x: trims all whitespace first, so " T2" → valid timestamp
    //   - Spark 4.0+: raw bytes are used; leading whitespace causes the T-check to fail → null
    // Comet matches the behaviour of whichever Spark version is running (controlled via
    // is_spark4_plus in the Cast proto, set from CometSparkSessionExtensions.isSpark40Plus).
    // This test compares Comet output against Spark output for all cases — no hard-coded
    // null/valid assertions needed.
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val values = Seq(
        // Bare T-hour-only: no leading whitespace (valid on all versions)
        "T2", // single-digit hour
        "T23", // two-digit hour
        "T0", // midnight
        // Bare T-hour-only: trailing whitespace only (valid on all versions)
        "T2 ", // trailing space
        "T2\t", // trailing tab
        "T2\n", // trailing newline
        // Bare T-hour-only: leading whitespace (null on 4.0+, valid on 3.x)
        " T2", // leading space
        "\tT2", // leading tab
        "\nT2", // leading newline
        "\r\nT2", // leading CRLF
        "\t T2", // tab then space
        "  T2", // double space
        // T-hour:minute with leading whitespace (null on 4.0+, valid on 3.x)
        " T2:30",
        "\tT2:30",
        "\nT2:30",
        // Full datetime: leading whitespace (valid on all versions — full trim applies)
        " 2020-01-01T12:34:56",
        "\t2020-01-01T12:34:56",
        "\n2020-01-01T12:34:56",
        "\r\n2020-01-01T12:34:56")
      castTimestampTest(values.toDF("a"), DataTypes.TimestampType, assertNative = true)
    }
  }

  test("cast StringType to TimestampNTZType") {
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        val values = Seq(
          "2020-01-01T12:34:56.123456",
          "2020-01-01T12:34:56",
          "2020-01-01 12:34:56",
          "2020-01-01",
          "2020-01",
          "2020",
          "2020-06-15T12:30:00Z",
          "2020-06-15T12:30:00+05:30",
          "2020-06-15T12:30:00-08:00",
          "2020-06-15T12:30:00 UTC",
          "2024-03-10 02:30:00",
          "2024-11-03 01:30:00",
          "not a timestamp",
          "",
          "T12:34:56",
          "12:34:56",
          "2020-02-29 00:00:00",
          "2023-02-29 00:00:00",
          null)
        castTimestampTest(values.toDF("a"), DataTypes.TimestampNTZType, assertNative = true)
      }
    }
  }

  // CAST from BinaryType

  test("cast BinaryType to StringType") {
    castTest(generateBinary(), DataTypes.StringType)
  }

  test("cast BinaryType to StringType - valid UTF-8 inputs") {
    castTest(gen.generateStrings(dataSize, numericPattern, 8).toDF("a"), DataTypes.StringType)
  }

  // CAST from DateType

  // Date to Boolean/Byte/Short/Int/Long/Float/Double/Decimal casts always return NULL
  // in LEGACY mode. In ANSI and TRY mode, Spark throws AnalysisException at
  // query parsing time. Hence, ANSI and Try mode are disabled in tests

  test("cast DateType to BooleanType") {
    castTest(generateDates(), DataTypes.BooleanType, testAnsi = false, testTry = false)
  }

  test("cast DateType to ByteType") {
    castTest(generateDates(), DataTypes.ByteType, testAnsi = false, testTry = false)
  }

  test("cast DateType to ShortType") {
    castTest(generateDates(), DataTypes.ShortType, testAnsi = false, testTry = false)
  }

  test("cast DateType to IntegerType") {
    castTest(generateDates(), DataTypes.IntegerType, testAnsi = false, testTry = false)
  }

  test("cast DateType to LongType") {
    castTest(generateDates(), DataTypes.LongType, testAnsi = false, testTry = false)
  }

  test("cast DateType to FloatType") {
    castTest(generateDates(), DataTypes.FloatType, testAnsi = false, testTry = false)
  }

  test("cast DateType to DoubleType") {
    castTest(generateDates(), DataTypes.DoubleType, testAnsi = false, testTry = false)
  }

  test("cast DateType to DecimalType(10,2)") {
    castTest(
      generateDates(),
      DataTypes.createDecimalType(10, 2),
      testAnsi = false,
      testTry = false)
  }

  test("cast DateType to StringType") {
    // generateDates() covers: 1970-2027 sampled monthly, DST transition dates, and edge
    // cases including "999-01-01" (year < 1000, zero-padded to "0999-01-01") and
    // "12345-01-01" (year > 9999, no truncation). Date→String is timezone-independent.
    castTest(generateDates(), DataTypes.StringType)
  }

  test("cast DateType to TimestampType") {
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        castTest(generateDates(), DataTypes.TimestampType)
      }
    }
  }

  test("cast DateType to TimestampNTZType") {
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        castTimestampTest(generateDates(), DataTypes.TimestampNTZType, assertNative = true)
      }
    }
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
    // input: 2023-12-31 10:00:00.0, expected: -21472, actual: null
    castTest(generateTimestamps(), DataTypes.ShortType)
  }

  ignore("cast TimestampType to IntegerType") {
    // https://github.com/apache/datafusion-comet/issues/352
    // input: 2023-12-31 10:00:00.0, expected: 1704045600, actual: null
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

  test("cast TimestampType to TimestampNTZType") {
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        castTimestampTest(generateTimestamps(), DataTypes.TimestampNTZType, assertNative = true)
      }
    }
  }

  // Complex Types

  test("cast StructType to StringType") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          // primitives
          checkSparkAnswerAndOperator(
            "SELECT CAST(struct(_1, _2, _3, _4, _5, _6, _7, _8) as string) FROM tbl")
          // the same field, add _11 and _12 again when
          // https://github.com/apache/datafusion-comet/issues/2256 resolved
          checkSparkAnswerAndOperator("SELECT CAST(struct(_11, _12) as string) FROM tbl")
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
      checkSparkAnswerAndOperator(
        "SELECT CAST(s AS struct<field1:string, field2:string>) AS new_struct FROM tab1")
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

  test("cast StringType to DecimalType with negative scale (allowNegativeScaleOfDecimal)") {
    // With allowNegativeScaleOfDecimal=true, Spark allows DECIMAL(p, s) where s < 0.
    // The value is rounded to the nearest 10^|s| — e.g. DECIMAL(10,-4) rounds to
    // the nearest 10000. This requires the legacy SQL parser config to be enabled.
    withSQLConf("spark.sql.legacy.allowNegativeScaleOfDecimal" -> "true") {
      val values =
        Seq("12500", "15000", "99990000", "-12500", "0", "0.001", "abc", null).toDF("a")
      // testTry=false: try_cast uses SQL string interpolation (toType.sql → "DECIMAL(10,-4)")
      // which the SQL parser rejects regardless of allowNegativeScaleOfDecimal.
      castTest(values, DataTypes.createDecimalType(10, -4), testTry = false)
    }
  }

  test("cast between decimals with negative precision") {
    // cast to negative scale
    checkSparkAnswerMaybeThrows(
      spark.sql("select a, cast(a as DECIMAL(10,-4)) from t order by a")) match {
      case (Some(expected: ParseException), Some(actual: ParseException)) =>
        assert(
          expected.getMessage.contains("PARSE_SYNTAX_ERROR") && actual.getMessage.contains(
            "PARSE_SYNTAX_ERROR"))
      case (expected, actual) =>
        fail(
          s"Expected Spark and Comet throw ParseException, but got Spark=$expected and Comet=$actual")
    }
  }

  test("cast between decimals with zero precision") {
    // cast between Decimal(10, 2) to Decimal(10,0)
    castTest(generateDecimalsPrecision10Scale2(), DataTypes.createDecimalType(10, 0))
  }

  test("cast ArrayType to StringType") {
    val hasIncompatibleType = (dt: DataType) =>
      if (CometConf.COMET_NATIVE_SCAN_IMPL.get() == "auto") {
        true
      } else {
        !CometScanTypeChecker(CometConf.COMET_NATIVE_SCAN_IMPL.get())
          .isTypeSupported(dt, "a", ListBuffer.empty)
      }
    Seq(
      BooleanType,
      StringType,
      ByteType,
      IntegerType,
      LongType,
      ShortType,
      // FloatType,
      // DoubleType,
      // BinaryType
      DecimalType(10, 2),
      DecimalType(38, 18)).foreach { dt =>
      val input = generateArrays(100, dt)
      castTest(input, StringType, hasIncompatibleType = hasIncompatibleType(input.schema))
    }
  }

  test("cast ArrayType to ArrayType") {
    assume(!isSpark41Plus, "https://github.com/apache/datafusion-comet/issues/4098")
    val types = Seq(
      BooleanType,
      StringType,
      ByteType,
      IntegerType,
      LongType,
      ShortType,
      FloatType,
      DoubleType,
      DecimalType(10, 2),
      // DecimalType(38, 18) is excluded here: random data exposes a ~1 ULP difference between
      // DataFusion's (i128 as f64) / 10^scale path and Spark's BigDecimal.doubleValue() for
      // float/double casts; and extreme boundary values that would avoid the ULP issue overflow
      // byte/short/int in ANSI mode, causing non-deterministic exception-message differences
      // between Spark's row-at-a-time and Comet's vectorized execution. The individual scalar
      // tests (cast DecimalType(38,18) to FloatType / DoubleType / BooleanType / etc.) already
      // cover this type fully.
      DateType,
      TimestampType,
      BinaryType)
    testArrayCastMatrix(types, ArrayType(_), generateArrays(100, _))
  }

  // https://github.com/apache/datafusion-comet/issues/3906
  ignore("cast nested ArrayType to nested ArrayType") {
    val types = Seq(
      BooleanType,
      StringType,
      ByteType,
      IntegerType,
      LongType,
      ShortType,
      FloatType,
      DoubleType,
      DecimalType(10, 2),
      DecimalType(38, 18),
      DateType,
      TimestampType,
      BinaryType)
    testArrayCastMatrix(
      types,
      dt => ArrayType(ArrayType(dt)),
      dt => generateArrays(100, ArrayType(dt)))
  }

  // CAST from TimestampNTZType

  test("cast TimestampNTZType to StringType") {
    // TimestampNTZ is timezone-independent, so casting to string should produce
    // the same result regardless of the session timezone.
    for (tz <- representativeTimezones) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        castTest(generateTimestampNTZ(), DataTypes.StringType)
      }
    }
  }

  test("cast TimestampNTZType to DateType") {
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        castTimestampTest(generateTimestampNTZ(), DataTypes.DateType, assertNative = true)
      }
    }
  }

  test("cast TimestampNTZType to TimestampType") {
    representativeTimezones.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        castTimestampTest(generateTimestampNTZ(), DataTypes.TimestampType, assertNative = true)
      }
    }
  }

  private def testArrayCastMatrix(
      elementTypes: Seq[DataType],
      wrapType: DataType => DataType,
      generateInput: DataType => DataFrame): Unit = {
    for (fromType <- elementTypes) {
      val input = generateInput(fromType)
      for (toType <- elementTypes) {
        val name = s"cast $fromType to $toType"
        val fromWrappedType = wrapType(fromType)
        val toWrappedType = wrapType(toType)
        if (fromType != toType &&
          testNames.contains(name) &&
          !tags
            .get(name)
            .exists(s => s.contains("org.scalatest.Ignore")) &&
          Cast.canCast(fromWrappedType, toWrappedType) &&
          isCompatible(fromWrappedType, toWrappedType, CometEvalMode.LEGACY)) {
          val ansiSupported =
            isCompatible(fromWrappedType, toWrappedType, CometEvalMode.ANSI)
          val trySupported =
            isCompatible(fromWrappedType, toWrappedType, CometEvalMode.TRY)
          castTest(input, toWrappedType, testAnsi = ansiSupported, testTry = trySupported)
        }
      }
    }
  }

  private def isCompatible(
      fromType: DataType,
      toType: DataType,
      evalMode: CometEvalMode.Value): Boolean =
    CometCast.isSupported(fromType, toType, None, evalMode) match {
      case Compatible(_) => true
      case _ => false
    }

  private def generateFloats(): DataFrame = {
    withNulls(gen.generateFloats(dataSize)).toDF("a")
  }

  private def generateSafeFloatValues(): Seq[Float] = {
    Seq(-123456.75f, -1.0f, 0.0f, 1.0f, 123456.75f)
  }

  private def generateDoubles(): DataFrame = {
    withNulls(gen.generateDoubles(dataSize)).toDF("a")
  }

  private def generateSafeDoubleValues(): Seq[Double] = {
    Seq(-123456.75d, -1.0d, 0.0d, 1.0d, 123456.75d)
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

  private def generateSafeLongValues(): Seq[Long] = {
    Seq(-123456789L, -1L, 0L, 1L, 123456789L)
  }

  private def generateArrays(rowNum: Int, elementType: DataType): DataFrame = {
    import scala.collection.JavaConverters._
    val schema = StructType(Seq(StructField("a", ArrayType(elementType), true)))
    def buildRows(values: Seq[Any]): Seq[Row] = {
      Range(0, rowNum).map { i =>
        Row(
          Seq[Any](
            values(i % values.length),
            if (i % 3 == 0) null else values((i + 1) % values.length),
            values((i + 2) % values.length)))
      }
    }

    def withEdgeCaseRows(generatedRows: Seq[Row]): Seq[Row] = {
      val sampleValue =
        generatedRows.find(_.get(0) != null).flatMap(_.getSeq[Any](0).headOption).orNull
      Seq(Row(Seq(sampleValue, null, sampleValue)), Row(Seq.empty[Any]), Row(null)) ++
        generatedRows
    }

    elementType match {
      case DateType =>
        val stringSchema = StructType(Seq(StructField("a", ArrayType(StringType), true)))
        spark
          .createDataFrame(
            withEdgeCaseRows(buildRows(generateDateLiterals())).asJava,
            stringSchema)
          .select(col("a").cast(ArrayType(DateType)).as("a"))
      case TimestampType =>
        val stringSchema = StructType(Seq(StructField("a", ArrayType(StringType), true)))
        spark
          .createDataFrame(
            withEdgeCaseRows(buildRows(generateTimestampLiterals())).asJava,
            stringSchema)
          .select(col("a").cast(ArrayType(TimestampType)).as("a"))
      case FloatType =>
        spark.createDataFrame(
          withEdgeCaseRows(buildRows(generateSafeFloatValues())).asJava,
          schema)
      case DoubleType =>
        spark.createDataFrame(
          withEdgeCaseRows(buildRows(generateSafeDoubleValues())).asJava,
          schema)
      case LongType =>
        spark.createDataFrame(
          withEdgeCaseRows(buildRows(generateSafeLongValues())).asJava,
          schema)
      case BinaryType =>
        val values = generateBinary().collect().map(_.getAs[Array[Byte]]("a")).toSeq
        spark.createDataFrame(withEdgeCaseRows(buildRows(values)).asJava, schema)
      case _ =>
        spark.createDataFrame(withEdgeCaseRows(gen.generateRows(rowNum, schema)).asJava, schema)
    }
  }

  // https://github.com/apache/datafusion-comet/issues/2038
  test("test implicit cast to dictionary with case when and dictionary type") {
    withSQLConf("parquet.enable.dictionary" -> "true") {
      withParquetTable((0 until 10000).map(i => (i < 5000, "one")), "tbl") {
        val df = spark.sql("select case when (_1 = true) then _2 else '' end as aaa from tbl")
        checkSparkAnswerAndOperator(df)
      }
    }
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
    generateDecimalsPrecision10Scale2(values)
  }

  private def generateDecimalsPrecision10Scale2(values: Seq[BigDecimal]): DataFrame = {
    withNulls(values).toDF("b").withColumn("a", col("b").cast(DecimalType(10, 2))).drop("b")
  }

  private def generateDecimalsPrecision15Scale5(): DataFrame = {
    val values = Seq(
      // just above Int.MAX_VALUE (2147483647) — overflows IntegerType
      BigDecimal("2147483648.12345"),
      BigDecimal("-2147483649.12345"),
      // fits in both int and long; exercises fractional truncation
      BigDecimal("123.45678"),
      BigDecimal("-123.45678"),
      // tiny non-zero — boolean must be true
      BigDecimal("0.00001"),
      BigDecimal("-0.00001"),
      BigDecimal("0.00000"))
    withNulls(values).toDF("b").withColumn("a", col("b").cast(DecimalType(15, 5))).drop("b")
  }

  private def generateDecimalsPrecision20Scale0(): DataFrame = {
    val values = Seq(
      // just above Long.MAX_VALUE (9223372036854775807) — overflows LongType
      BigDecimal("9223372036854775808"),
      BigDecimal("-9223372036854775809"),
      // overflows IntegerType, fits in LongType
      BigDecimal("2147483648"),
      BigDecimal("-2147483649"),
      BigDecimal("1"),
      BigDecimal("-1"),
      BigDecimal("0"))
    withNulls(values).toDF("b").withColumn("a", col("b").cast(DecimalType(20, 0))).drop("b")
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
      // Small-magnitude non-zero: adj_exp = -9 + 0 = -9 < -6, so LEGACY produces
      // scientific notation "1E-9" / "1.000000000E-9" rather than plain "0.000000001".
      BigDecimal("0.000000001"),
      BigDecimal("-0.000000001"),
      BigDecimal("123456.789"),
      // Int Max
      BigDecimal("2147483647.123123123"),
      BigDecimal("2147483648.123123123"),
      BigDecimal("9223372036854775807.123123"),
      // Long Max
      BigDecimal("9223372036854775808.234567"),
      BigDecimal("99999999999999999999.999999999999"))
    generateDecimalsPrecision38Scale18(values)
  }

  private def generateDecimalsPrecision38Scale18(values: Seq[BigDecimal]): DataFrame = {
    withNulls(values).toDF("a")
  }

  private def generateDateLiterals(): Seq[String] = {
    // add 1st, 10th, 20th of each month from epoch to 2027
    val sampledDates = (1970 to 2027).flatMap { year =>
      (1 to 12).flatMap { month =>
        Seq(1, 10, 20).map(day => f"$year-$month%02d-$day%02d")
      }
    }

    // DST transition dates (1970-2099) for US, EU, Australia
    val dstDates = (1970 to 2099).flatMap { year =>
      Seq(
        // spring forward
        s"$year-03-08",
        s"$year-03-09",
        s"$year-03-10",
        s"$year-03-11",
        s"$year-03-14",
        s"$year-03-15",
        s"$year-03-25",
        s"$year-03-26",
        s"$year-03-27",
        s"$year-03-28",
        s"$year-03-29",
        s"$year-03-30",
        s"$year-03-31",
        // April (Australia fall back)
        s"$year-04-01",
        s"$year-04-02",
        s"$year-04-03",
        s"$year-04-04",
        s"$year-04-05",
        // October (EU fall back and Australia spring forward)
        s"$year-10-01",
        s"$year-10-02",
        s"$year-10-03",
        s"$year-10-04",
        s"$year-10-05",
        s"$year-10-25",
        s"$year-10-26",
        s"$year-10-27",
        s"$year-10-28",
        s"$year-10-29",
        s"$year-10-30",
        s"$year-10-31",
        // US fall back
        s"$year-11-01",
        s"$year-11-02",
        s"$year-11-03",
        s"$year-11-04",
        s"$year-11-05",
        s"$year-11-06",
        s"$year-11-07",
        s"$year-11-08")
    }

    // Edge cases
    val edgeCases = Seq("1969-12-31", "2000-02-29", "999-01-01", "12345-01-01")
    (sampledDates ++ dstDates ++ edgeCases).distinct
  }

  private def generateDates(): DataFrame = {
    val values = generateDateLiterals()
    withNulls(values).toDF("b").withColumn("a", col("b").cast(DataTypes.DateType)).drop("b")
  }

  // Extended values are Timestamps that are outside dates supported chrono::DateTime and
  // therefore not supported by operations using it.
  private def generateTimestampsExtended(): DataFrame = {
    val values = Seq("290000-12-31T01:00:00+02:00")
    generateTimestamps().unionByName(
      values.toDF("str").select(col("str").cast(DataTypes.TimestampType).as("a")))
  }

  private def generateTimestampLiterals(): Seq[String] =
    Seq(
      "2024-01-01T12:34:56.123456",
      "2024-01-01T01:00:00Z",
      "9999-12-31T01:00:00-02:00",
      "2024-12-31T01:00:00+02:00")

  private def generateTimestamps(): DataFrame = {
    val values = generateTimestampLiterals()
    withNulls(values)
      .toDF("str")
      .withColumn("a", col("str").cast(DataTypes.TimestampType))
      .drop("str")
  }

  private def generateTimestampNTZ(): DataFrame = {
    val values = generateTimestampLiterals()
    withNulls(values)
      .toDF("str")
      .withColumn("a", col("str").cast(DataTypes.TimestampNTZType))
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

  private def castTimestampTest(
      input: DataFrame,
      toType: DataType,
      assertNative: Boolean = false) = {
    withTempPath { dir =>
      val data = roundtripParquet(input, dir).coalesce(1)
      data.createOrReplaceTempView("t")

      withSQLConf((SQLConf.ANSI_ENABLED.key, "false")) {
        // cast() should return null for invalid inputs when ansi mode is disabled
        val df = data.withColumn("converted", col("a").cast(toType))
        if (assertNative) {
          checkSparkAnswerAndOperator(df)
        } else {
          checkSparkAnswer(df)
        }

        // try_cast() should always return null for invalid inputs
        val df2 = spark.sql(s"select try_cast(a as ${toType.sql}) from t")
        checkSparkAnswer(df2)
      }

      // with ANSI enabled, we should produce the same exception as Spark
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        val df = data.withColumn("converted", col("a").cast(toType))
        checkSparkAnswerMaybeThrows(df) match {
          case (None, None) =>
          // both succeeded, results already compared
          case (None, Some(e)) =>
            throw e
          case (Some(e), None) =>
            val msg = if (e.getCause != null) e.getCause.getMessage else e.getMessage
            fail(s"Comet should have failed with $msg")
          case (Some(sparkException), Some(cometException)) =>
            val sparkMessage =
              if (sparkException.getCause != null) sparkException.getCause.getMessage
              else sparkException.getMessage
            val cometMessage =
              if (cometException.getCause != null) cometException.getCause.getMessage
              else cometException.getMessage
            if (CometSparkSessionExtensions.isSpark40Plus) {
              assert(sparkMessage.contains("SQLSTATE"))
              assert(
                sparkMessage.startsWith(
                  cometMessage.substring(0, math.min(40, cometMessage.length))))
            } else {
              assert(cometMessage == sparkMessage)
            }
        }
        // try_cast()
        val dfTryCast = spark.sql(s"select try_cast(a as ${toType.sql}) from t")
        checkSparkAnswer(dfTryCast)
      }
    }
  }

  private def castTest(
      input: DataFrame,
      toType: DataType,
      hasIncompatibleType: Boolean = false,
      testAnsi: Boolean = true,
      testTry: Boolean = true,
      useDataFrameDiff: Boolean = false): Unit = {

    withTempPath { dir =>
      val data = roundtripParquet(input, dir).coalesce(1)
      val dataWithRowId = data.withColumn("__row_id", monotonically_increasing_id())

      withSQLConf((SQLConf.ANSI_ENABLED.key, "false")) {
        // cast() should return null for invalid inputs when ansi mode is disabled
        val df = dataWithRowId
          .select(col("__row_id"), col("a"), col("a").cast(toType).as("casted"))
          .orderBy(col("__row_id"))
          .drop("__row_id")
        if (useDataFrameDiff) {
          assertDataFrameEqualsWithExceptions(df, assertCometNative = !hasIncompatibleType)
        } else {
          if (hasIncompatibleType) {
            checkSparkAnswer(df)
          } else {
            checkSparkAnswerAndOperator(df)
          }
        }

        if (testTry) {
          val df2 = tryCastWithRowId(dataWithRowId, toType)
          if (hasIncompatibleType) {
            checkSparkAnswer(df2)
          } else {
            if (useDataFrameDiff) {
              assertDataFrameEqualsWithExceptions(df2, assertCometNative = !hasIncompatibleType)
            } else {
              checkSparkAnswerAndOperator(df2)
            }
          }
        }
      }

      if (testAnsi) {
        // with ANSI enabled, we should produce the same exception as Spark
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          CometConf.getExprAllowIncompatConfigKey(classOf[Cast]) -> "true") {

          // cast() should throw exception on invalid inputs when ansi mode is enabled
          val df = dataWithRowId
            .select(col("__row_id"), col("a"), col("a").cast(toType).as("converted"))
            .orderBy(col("__row_id"))
            .drop("__row_id")
          val res = if (useDataFrameDiff) {
            assertDataFrameEqualsWithExceptions(df, assertCometNative = !hasIncompatibleType)
          } else {
            checkSparkAnswerMaybeThrows(df)
          }
          res match {
            case (None, None) =>
            // neither system threw an exception
            case (None, Some(e)) =>
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
                  assert(sparkMessage.contains("SQLSTATE"))
                  // we compare a subset of the error message. Comet grabs the query
                  // context eagerly so it displays the call site at the
                  // line of code where the cast method was called, whereas spark grabs the context
                  // lazily and displays the call site at the line of code where the error is checked.
                  assert(
                    sparkMessage.startsWith(
                      cometMessage.substring(0, math.min(40, cometMessage.length))))
                } else {
                  // for Spark 3.4 we expect to reproduce the error message exactly
                  assert(cometMessage == sparkMessage)
                }
              }
          }
        }

        if (testTry) {
          val df2 = tryCastWithRowId(dataWithRowId, toType)
          if (useDataFrameDiff) {
            assertDataFrameEqualsWithExceptions(df2, assertCometNative = !hasIncompatibleType)
          } else {
            if (hasIncompatibleType) {
              checkSparkAnswer(df2)
            } else {
              checkSparkAnswerAndOperator(df2)
            }
          }
        }
      }
    }
  }

  private def tryCastWithRowId(dataWithRowId: DataFrame, toType: DataType): DataFrame = {
    dataWithRowId.createOrReplaceTempView("t")
    // try_cast() should always return null for invalid inputs
    // not using spark DSL since `try_cast` is only available from Spark 4.x
    spark
      .sql(s"select __row_id, a, try_cast(a as ${toType.sql}) as casted from t order by __row_id")
      .drop("__row_id")
  }

  private def roundtripParquet(df: DataFrame, tempDir: File): DataFrame = {
    val filename = new File(tempDir, s"castTest_${System.currentTimeMillis()}.parquet").toString
    df.write.mode(SaveMode.Overwrite).parquet(filename)
    spark.read.parquet(filename)
  }
}
