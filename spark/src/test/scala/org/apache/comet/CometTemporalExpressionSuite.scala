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

import scala.util.Random

import org.apache.spark.sql.{CometTestBase, DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Days, Hours, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.serde.{CometDateFormat, CometTruncDate, CometTruncTimestamp}
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

class CometTemporalExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  /** Timezones used to verify that TimestampNTZ operations are timezone-independent. */
  private val crossTimezones =
    Seq("UTC", "America/Los_Angeles", "Europe/London", "Asia/Tokyo")

  test("trunc (TruncDate)") {
    val supportedFormats = CometTruncDate.supportedFormats
    val unsupportedFormats = Seq("invalid")

    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("c0", DataTypes.DateType, true),
        StructField("c1", DataTypes.StringType, true)))
    val df = FuzzDataGenerator.generateDataFrame(r, spark, schema, 1000, DataGenOptions())

    df.createOrReplaceTempView("tbl")

    for (format <- supportedFormats) {
      checkSparkAnswerAndOperator(s"SELECT c0, trunc(c0, '$format') from tbl order by c0, c1")
    }
    for (format <- unsupportedFormats) {
      // Comet should fall back to Spark for unsupported or invalid formats
      checkSparkAnswerAndFallbackReason(
        s"SELECT c0, trunc(c0, '$format') from tbl order by c0, c1",
        s"Format $format is not supported")
    }

    // Comet should fall back to Spark if format is not a literal
    checkSparkAnswerAndFallbackReason(
      "SELECT c0, trunc(c0, c1) from tbl order by c0, c1",
      "Invalid format strings will throw an exception instead of returning NULL")
  }

  test("date_trunc (TruncTimestamp) - reading from DataFrame") {
    val supportedFormats = CometTruncTimestamp.supportedFormats
    val unsupportedFormats = Seq("invalid")

    createTimestampTestData.createOrReplaceTempView("tbl")

    // TODO test fails with non-UTC timezone
    // https://github.com/apache/datafusion-comet/issues/2649
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      for (format <- supportedFormats) {
        checkSparkAnswerAndOperator(s"SELECT c0, date_trunc('$format', c0) from tbl order by c0")
      }
      for (format <- unsupportedFormats) {
        // Comet should fall back to Spark for unsupported or invalid formats
        checkSparkAnswerAndFallbackReason(
          s"SELECT c0, date_trunc('$format', c0) from tbl order by c0",
          s"Format $format is not supported")
      }
      // Comet should fall back to Spark if format is not a literal
      checkSparkAnswerAndFallbackReason(
        "SELECT c0, date_trunc(fmt, c0) from tbl order by c0, fmt",
        "Invalid format strings will throw an exception instead of returning NULL")
    }
  }

  test("date_trunc (TruncTimestamp) - reading from Parquet") {
    val supportedFormats = CometTruncTimestamp.supportedFormats
    val unsupportedFormats = Seq("invalid")

    withTempDir { path =>
      createTimestampTestData.write.mode(SaveMode.Overwrite).parquet(path.toString)
      spark.read.parquet(path.toString).createOrReplaceTempView("tbl")

      // TODO test fails with non-UTC timezone
      // https://github.com/apache/datafusion-comet/issues/2649
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        for (format <- supportedFormats) {
          checkSparkAnswerAndOperator(
            s"SELECT c0, date_trunc('$format', c0) from tbl order by c0")
        }
        for (format <- unsupportedFormats) {
          // Comet should fall back to Spark for unsupported or invalid formats
          checkSparkAnswerAndFallbackReason(
            s"SELECT c0, date_trunc('$format', c0) from tbl order by c0",
            s"Format $format is not supported")
        }
        // Comet should fall back to Spark if format is not a literal
        checkSparkAnswerAndFallbackReason(
          "SELECT c0, date_trunc(fmt, c0) from tbl order by c0, fmt",
          "Invalid format strings will throw an exception instead of returning NULL")
      }
    }
  }

  test("unix_timestamp - timestamp input") {
    createTimestampTestData.createOrReplaceTempView("tbl")
    for (timezone <- Seq("UTC", "America/Los_Angeles")) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timezone) {
        checkSparkAnswerAndOperator("SELECT c0, unix_timestamp(c0) from tbl order by c0")
      }
    }
  }

  test("unix_timestamp - date input") {
    val r = new Random(42)
    val dateSchema = StructType(Seq(StructField("d", DataTypes.DateType, true)))
    val dateDF = FuzzDataGenerator.generateDataFrame(r, spark, dateSchema, 100, DataGenOptions())
    dateDF.createOrReplaceTempView("date_tbl")
    for (timezone <- Seq("UTC", "America/Los_Angeles")) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timezone) {
        checkSparkAnswerAndOperator("SELECT d, unix_timestamp(d) from date_tbl order by d")
      }
    }
  }

  test("unix_timestamp - timestamp_ntz input") {
    // TimestampNTZ stores local time without timezone, so the unix
    // timestamp is the value divided by microseconds per second (no timezone conversion).
    // Verify this produces the same result regardless of session timezone.
    val r = new Random(42)
    val ntzSchema = StructType(Seq(StructField("ts_ntz", DataTypes.TimestampNTZType, true)))
    val ntzDF = FuzzDataGenerator.generateDataFrame(r, spark, ntzSchema, 100, DataGenOptions())
    ntzDF.createOrReplaceTempView("ntz_tbl")
    for (tz <- crossTimezones) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        checkSparkAnswerAndOperator(
          "SELECT ts_ntz, unix_timestamp(ts_ntz) from ntz_tbl order by ts_ntz")
      }
    }
  }

  test("hour/minute/second - timestamp_ntz input") {
    // TimestampNTZ extracts time components directly from the stored local time,
    // so the result should be the same regardless of session timezone.
    // Comet currently falls back to Spark for hour/minute/second on TimestampNTZ
    // inputs (https://github.com/apache/datafusion-comet/issues/3180); we verify
    // correctness (matching Spark's output) in all session timezones.
    val r = new Random(42)
    val ntzSchema = StructType(Seq(StructField("ts_ntz", DataTypes.TimestampNTZType, true)))
    val ntzDF = FuzzDataGenerator.generateDataFrame(r, spark, ntzSchema, 100, DataGenOptions())
    ntzDF.createOrReplaceTempView("ntz_tbl")
    for (tz <- crossTimezones) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        checkSparkAnswer(
          "SELECT ts_ntz, hour(ts_ntz), minute(ts_ntz), second(ts_ntz) from ntz_tbl order by ts_ntz")
      }
    }
  }

  test("date_trunc - timestamp_ntz input") {
    // TimestampNTZ truncation should be timezone-independent.
    // Verify the result is the same regardless of session timezone.
    // Catalyst wraps the NTZ child in cast(ts_ntz as timestamp), so Comet runs
    // date_trunc natively only when the session timezone is UTC (see
    // https://github.com/apache/datafusion-comet/issues/2649); for non-UTC
    // sessions it falls back to Spark but must still produce correct results.
    val r = new Random(42)
    val ntzSchema = StructType(Seq(StructField("ts_ntz", DataTypes.TimestampNTZType, true)))
    // Use a reasonable date range (around year 2024) to avoid chrono-tz DST calculation
    // issues with far-future dates. The default baseDate is year 3333 which is beyond
    // the range where chrono-tz can reliably calculate DST transitions.
    val reasonableBaseDate =
      new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2024-06-15 12:00:00").getTime
    val ntzDF = FuzzDataGenerator.generateDataFrame(
      r,
      spark,
      ntzSchema,
      100,
      DataGenOptions(baseDate = reasonableBaseDate))
    ntzDF.createOrReplaceTempView("ntz_tbl")
    for (tz <- crossTimezones) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        for (format <- CometTruncTimestamp.supportedFormats) {
          val sql =
            s"SELECT ts_ntz, date_trunc('$format', ts_ntz) from ntz_tbl order by ts_ntz"
          if (tz == "UTC") {
            checkSparkAnswerAndOperator(sql)
          } else {
            checkSparkAnswer(sql)
          }
        }
      }
    }
  }

  test("date_format - timestamp_ntz input") {
    // TimestampNTZ is timezone-independent, so date_format should produce the same
    // formatted string regardless of session timezone. Comet currently only runs this
    // natively for UTC; for non-UTC it falls back to Spark. We verify correctness
    // (matching Spark's output) in all cases.
    val r = new Random(42)
    val ntzSchema = StructType(Seq(StructField("ts_ntz", DataTypes.TimestampNTZType, true)))
    val ntzDF = FuzzDataGenerator.generateDataFrame(r, spark, ntzSchema, 100, DataGenOptions())
    ntzDF.createOrReplaceTempView("ntz_tbl")
    val supportedFormats =
      CometDateFormat.supportedFormats.keys.toSeq.filterNot(_.contains("'"))
    for (tz <- crossTimezones) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        for (format <- supportedFormats) {
          if (tz == "UTC") {
            checkSparkAnswerAndOperator(
              s"SELECT ts_ntz, date_format(ts_ntz, '$format') from ntz_tbl order by ts_ntz")
          } else {
            // Non-UTC falls back to Spark but should still produce correct results
            checkSparkAnswer(
              s"SELECT ts_ntz, date_format(ts_ntz, '$format') from ntz_tbl order by ts_ntz")
          }
        }
      }
    }
  }

  test("timestamp_ntz - cross-timezone Parquet round-trip") {
    // This test verifies the key TimestampNTZ invariant: data written to a
    // timestamp_ntz Parquet column under one session timezone can be read by
    // another session with a different timezone and produce identical results.
    // This is the defining characteristic of TimestampNTZ vs TimestampType.
    val writeTimezones = Seq("America/Los_Angeles", "Asia/Tokyo", "UTC")
    val readTimezones = Seq("Europe/London", "America/New_York", "UTC", "Pacific/Auckland")

    for (writeTz <- writeTimezones) {
      withTempDir { dir =>
        // Write data with one session timezone
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> writeTz) {
          val data = Seq(
            Row("2024-01-15T08:30:00"),
            Row("2024-07-04T23:59:59.999999"),
            Row("1970-01-01T00:00:00"),
            Row("2024-03-10T02:30:00"), // DST spring-forward time in US
            Row("2024-11-03T01:30:00"), // DST fall-back time in US
            Row(null))
          val schema = StructType(Seq(StructField("ts_str", DataTypes.StringType, true)))
          spark
            .createDataFrame(spark.sparkContext.parallelize(data), schema)
            .selectExpr("CAST(ts_str AS TIMESTAMP_NTZ) AS ts_ntz")
            .write
            .mode(SaveMode.Overwrite)
            .parquet(dir.toString)
        }

        // Read with different session timezones and verify results are identical
        for (readTz <- readTimezones) {
          withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> readTz) {
            spark.read.parquet(dir.toString).createOrReplaceTempView("ntz_cross_tz")
            // Casts and unix_timestamp are supported natively for NTZ in any session TZ
            checkSparkAnswerAndOperator(
              "SELECT ts_ntz, CAST(ts_ntz AS STRING) FROM ntz_cross_tz ORDER BY ts_ntz")
            checkSparkAnswerAndOperator(
              "SELECT ts_ntz, CAST(ts_ntz AS DATE) FROM ntz_cross_tz ORDER BY ts_ntz")
            checkSparkAnswerAndOperator(
              "SELECT ts_ntz, unix_timestamp(ts_ntz) FROM ntz_cross_tz ORDER BY ts_ntz")
            // hour/minute/second fall back for NTZ (issue #3180); date_trunc falls
            // back when the session timezone is non-UTC (issue #2649). Verify
            // correctness only.
            checkSparkAnswer(
              "SELECT ts_ntz, hour(ts_ntz), minute(ts_ntz), second(ts_ntz) FROM ntz_cross_tz ORDER BY ts_ntz")
            checkSparkAnswer(
              "SELECT ts_ntz, date_trunc('HOUR', ts_ntz) FROM ntz_cross_tz ORDER BY ts_ntz")
          }
        }
      }
    }
  }

  test("unix_timestamp - string input falls back to Spark") {
    withTempView("string_tbl") {
      // Create test data with timestamp strings
      val schema = StructType(Seq(StructField("ts_str", DataTypes.StringType, true)))
      val data = Seq(
        Row("2020-01-01 00:00:00"),
        Row("2021-06-15 12:30:45"),
        Row("2022-12-31 23:59:59"),
        Row(null))
      spark
        .createDataFrame(spark.sparkContext.parallelize(data), schema)
        .createOrReplaceTempView("string_tbl")

      // String input should fall back to Spark
      checkSparkAnswerAndFallbackReason(
        "SELECT ts_str, unix_timestamp(ts_str) from string_tbl order by ts_str",
        "unix_timestamp does not support input type: StringType")

      // String input with custom format should also fall back
      checkSparkAnswerAndFallbackReason(
        "SELECT ts_str, unix_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss') from string_tbl",
        "unix_timestamp does not support input type: StringType")
    }
  }

  private def createTimestampTestData = {
    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("c0", DataTypes.TimestampType, true),
        StructField("fmt", DataTypes.StringType, true)))
    FuzzDataGenerator.generateDataFrame(r, spark, schema, 1000, DataGenOptions())
  }

  test("last_day") {
    val r = new Random(42)
    val schema = StructType(Seq(StructField("c0", DataTypes.DateType, true)))
    val df = FuzzDataGenerator.generateDataFrame(r, spark, schema, 1000, DataGenOptions())
    df.createOrReplaceTempView("tbl")

    // Basic test with random dates
    checkSparkAnswerAndOperator("SELECT c0, last_day(c0) FROM tbl ORDER BY c0")

    // Disable constant folding to ensure literal expressions are executed by Comet
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      // Test with literal dates - various months
      checkSparkAnswerAndOperator(
        "SELECT last_day(DATE('2024-01-15')), last_day(DATE('2024-02-15')), last_day(DATE('2024-12-01'))")

      // Test leap year handling (February)
      checkSparkAnswerAndOperator(
        "SELECT last_day(DATE('2024-02-01')), last_day(DATE('2023-02-01'))")

      // Test null handling
      checkSparkAnswerAndOperator("SELECT last_day(NULL)")
    }
  }

  test("datediff") {
    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("c0", DataTypes.DateType, true),
        StructField("c1", DataTypes.DateType, true)))
    val df = FuzzDataGenerator.generateDataFrame(r, spark, schema, 1000, DataGenOptions())
    df.createOrReplaceTempView("tbl")

    // Basic test with random dates
    checkSparkAnswerAndOperator("SELECT c0, c1, datediff(c0, c1) FROM tbl ORDER BY c0, c1")

    // Disable constant folding to ensure literal expressions are executed by Comet
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      // Test positive difference (end date > start date)
      checkSparkAnswerAndOperator("SELECT datediff(DATE('2009-07-31'), DATE('2009-07-30'))")

      // Test negative difference (end date < start date)
      checkSparkAnswerAndOperator("SELECT datediff(DATE('2009-07-30'), DATE('2009-07-31'))")

      // Test same dates (should be 0)
      checkSparkAnswerAndOperator("SELECT datediff(DATE('2009-07-30'), DATE('2009-07-30'))")

      // Test larger date differences
      checkSparkAnswerAndOperator("SELECT datediff(DATE('2024-01-01'), DATE('2020-01-01'))")

      // Test null handling
      checkSparkAnswerAndOperator("SELECT datediff(NULL, DATE('2009-07-30'))")
      checkSparkAnswerAndOperator("SELECT datediff(DATE('2009-07-30'), NULL)")

      // Test leap year edge cases
      // 1900 was NOT a leap year (divisible by 100 but not 400)
      // 2000 WAS a leap year (divisible by 400)
      // So Feb 27 to Mar 1 spans different number of days:
      // 1900: 2 days (Feb 28, Mar 1)
      // 2000: 3 days (Feb 28, Feb 29, Mar 1)
      checkSparkAnswerAndOperator("SELECT datediff(DATE('1900-03-01'), DATE('1900-02-27'))")
      checkSparkAnswerAndOperator("SELECT datediff(DATE('2000-03-01'), DATE('2000-02-27'))")

      // Additional leap year tests
      // 2004 was a leap year (divisible by 4, not by 100)
      checkSparkAnswerAndOperator("SELECT datediff(DATE('2004-03-01'), DATE('2004-02-28'))")
      // 2100 will NOT be a leap year (divisible by 100 but not 400)
      checkSparkAnswerAndOperator("SELECT datediff(DATE('2100-03-01'), DATE('2100-02-28'))")
    }
  }

  test("date_format with timestamp column") {
    // Filter out formats with embedded quotes that need special handling
    val supportedFormats = CometDateFormat.supportedFormats.keys.toSeq
      .filterNot(_.contains("'"))

    createTimestampTestData.createOrReplaceTempView("tbl")

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      for (format <- supportedFormats) {
        checkSparkAnswerAndOperator(s"SELECT c0, date_format(c0, '$format') from tbl order by c0")
      }
      // Test ISO format with embedded quotes separately using double-quoted string
      checkSparkAnswerAndOperator(
        "SELECT c0, date_format(c0, \"yyyy-MM-dd'T'HH:mm:ss\") from tbl order by c0")
    }
  }

  test("date_format with specific format strings") {
    // Test specific format strings with explicit timestamp data
    createTimestampTestData.createOrReplaceTempView("tbl")

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      // Date formats
      checkSparkAnswerAndOperator("SELECT c0, date_format(c0, 'yyyy-MM-dd') from tbl order by c0")
      checkSparkAnswerAndOperator("SELECT c0, date_format(c0, 'yyyy/MM/dd') from tbl order by c0")

      // Time formats
      checkSparkAnswerAndOperator("SELECT c0, date_format(c0, 'HH:mm:ss') from tbl order by c0")
      checkSparkAnswerAndOperator("SELECT c0, date_format(c0, 'HH:mm') from tbl order by c0")

      // Combined formats
      checkSparkAnswerAndOperator(
        "SELECT c0, date_format(c0, 'yyyy-MM-dd HH:mm:ss') from tbl order by c0")

      // Day/month names
      checkSparkAnswerAndOperator("SELECT c0, date_format(c0, 'EEEE') from tbl order by c0")
      checkSparkAnswerAndOperator("SELECT c0, date_format(c0, 'MMMM') from tbl order by c0")

      // 12-hour time
      checkSparkAnswerAndOperator("SELECT c0, date_format(c0, 'hh:mm:ss a') from tbl order by c0")

      // ISO format (use double single-quotes to escape the literal T)
      checkSparkAnswerAndOperator(
        "SELECT c0, date_format(c0, \"yyyy-MM-dd'T'HH:mm:ss\") from tbl order by c0")
    }
  }

  test("date_format with literal timestamp") {
    // Test specific literal timestamp formats
    // Disable constant folding to ensure Comet actually executes the expression
    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      checkSparkAnswerAndOperator(
        "SELECT date_format(TIMESTAMP '2024-03-15 14:30:45', 'yyyy-MM-dd')")
      checkSparkAnswerAndOperator(
        "SELECT date_format(TIMESTAMP '2024-03-15 14:30:45', 'yyyy-MM-dd HH:mm:ss')")
      checkSparkAnswerAndOperator(
        "SELECT date_format(TIMESTAMP '2024-03-15 14:30:45', 'HH:mm:ss')")
      checkSparkAnswerAndOperator("SELECT date_format(TIMESTAMP '2024-03-15 14:30:45', 'EEEE')")
      checkSparkAnswerAndOperator(
        "SELECT date_format(TIMESTAMP '2024-03-15 14:30:45', 'hh:mm:ss a')")
    }
  }

  test("date_format with null") {
    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      checkSparkAnswerAndOperator("SELECT date_format(CAST(NULL AS TIMESTAMP), 'yyyy-MM-dd')")
    }
  }

  test("date_format unsupported format falls back to Spark") {
    createTimestampTestData.createOrReplaceTempView("tbl")

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      // Unsupported format string
      checkSparkAnswerAndFallbackReason(
        "SELECT c0, date_format(c0, 'yyyy-MM-dd EEEE') from tbl order by c0",
        "Format 'yyyy-MM-dd EEEE' is not supported")
    }
  }

  test("date_format with non-UTC timezone falls back to Spark") {
    createTimestampTestData.createOrReplaceTempView("tbl")

    val nonUtcTimezones =
      Seq("America/New_York", "America/Los_Angeles", "Europe/London", "Asia/Tokyo")

    for (tz <- nonUtcTimezones) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        // Non-UTC timezones should fall back to Spark as Incompatible
        checkSparkAnswerAndFallbackReason(
          "SELECT c0, date_format(c0, 'yyyy-MM-dd HH:mm:ss') from tbl order by c0",
          s"Non-UTC timezone '$tz' may produce different results")
      }
    }
  }

  test("date_format with non-UTC timezone works when allowIncompatible is enabled") {
    createTimestampTestData.createOrReplaceTempView("tbl")

    val nonUtcTimezones = Seq("America/New_York", "Europe/London", "Asia/Tokyo")

    for (tz <- nonUtcTimezones) {
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz,
        "spark.comet.expr.DateFormatClass.allowIncompatible" -> "true") {
        // With allowIncompatible enabled, Comet will execute the expression
        // Results may differ from Spark but should not throw errors
        checkSparkAnswer("SELECT c0, date_format(c0, 'yyyy-MM-dd') from tbl order by c0")
      }
    }
  }

  test("unix_date") {
    val r = new Random(42)
    val schema = StructType(Seq(StructField("c0", DataTypes.DateType, true)))
    val df = FuzzDataGenerator.generateDataFrame(r, spark, schema, 1000, DataGenOptions())
    df.createOrReplaceTempView("tbl")

    // Basic test
    checkSparkAnswerAndOperator("SELECT c0, unix_date(c0) FROM tbl ORDER BY c0")

    // Test with literal dates
    checkSparkAnswerAndOperator(
      "SELECT unix_date(DATE('1970-01-01')), unix_date(DATE('1970-01-02')), unix_date(DATE('2024-01-01'))")

    // Test dates before Unix epoch (should return negative values)
    checkSparkAnswerAndOperator(
      "SELECT unix_date(DATE('1969-12-31')), unix_date(DATE('1960-01-01'))")

    // Test null handling
    checkSparkAnswerAndOperator("SELECT unix_date(NULL)")
  }

  /**
   * Checks that the Comet-evaluated DataFrame produces the same results as the baseline DataFrame
   * evaluated by native Spark JVM, and that Comet native operators are used. This is needed
   * because Days is a PartitionTransformExpression that extends Unevaluable, so
   * checkSparkAnswerAndOperator cannot be used directly.
   */
  private def checkDays(cometDF: DataFrame, baselineDF: DataFrame): Unit = {
    // Ensure the expected answer is evaluated solely by native Spark JVM (Comet off)
    var expected: Array[Row] = Array.empty
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      expected = baselineDF.collect()
    }
    checkAnswer(cometDF, expected.toSeq)
    checkCometOperators(stripAQEPlan(cometDF.queryExecution.executedPlan))
  }

  test("days - date input") {
    val r = new Random(42)
    val dateSchema = StructType(Seq(StructField("d", DataTypes.DateType, true)))
    val dateDF = FuzzDataGenerator.generateDataFrame(r, spark, dateSchema, 1000, DataGenOptions())

    checkDays(
      dateDF.select(col("d"), getColumnFromExpression(Days(UnresolvedAttribute("d")))),
      dateDF.selectExpr("d", "unix_date(d)"))
  }

  test("days - timestamp input") {
    val r = new Random(42)
    val tsSchema = StructType(Seq(StructField("ts", DataTypes.TimestampType, true)))
    val tsDF = FuzzDataGenerator.generateDataFrame(r, spark, tsSchema, 1000, DataGenOptions())

    for (timezone <- Seq("UTC", "America/Los_Angeles", "Asia/Tokyo")) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timezone) {
        checkDays(
          tsDF.select(col("ts"), getColumnFromExpression(Days(UnresolvedAttribute("ts")))),
          tsDF.selectExpr("ts", "unix_date(cast(ts as date))"))
      }
    }
  }

  test("days - literal edge cases") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {

      val dummyDF = spark.range(1)

      // Pre-epoch (should return negative day numbers)
      checkDays(
        dummyDF.select(
          getColumnFromExpression(
            Days(Literal.create(java.sql.Date.valueOf("1969-12-31"), DataTypes.DateType))),
          getColumnFromExpression(
            Days(Literal.create(java.sql.Date.valueOf("1960-01-01"), DataTypes.DateType)))),
        dummyDF.selectExpr("unix_date(DATE('1969-12-31'))", "unix_date(DATE('1960-01-01'))"))

      // Epoch and post-epoch
      checkDays(
        dummyDF.select(
          getColumnFromExpression(
            Days(Literal.create(java.sql.Date.valueOf("1970-01-01"), DataTypes.DateType))),
          getColumnFromExpression(
            Days(Literal.create(java.sql.Date.valueOf("1970-01-02"), DataTypes.DateType))),
          getColumnFromExpression(
            Days(Literal.create(java.sql.Date.valueOf("2024-01-01"), DataTypes.DateType)))),
        dummyDF.selectExpr(
          "unix_date(DATE('1970-01-01'))",
          "unix_date(DATE('1970-01-02'))",
          "unix_date(DATE('2024-01-01'))"))

      // Timestamp literals
      checkDays(
        dummyDF.select(
          getColumnFromExpression(Days(Literal
            .create(java.sql.Timestamp.valueOf("1970-01-01 00:00:00"), DataTypes.TimestampType))),
          getColumnFromExpression(
            Days(
              Literal.create(
                java.sql.Timestamp.valueOf("2024-06-15 10:30:00"),
                DataTypes.TimestampType)))),
        dummyDF.selectExpr(
          "unix_date(cast(TIMESTAMP('1970-01-01 00:00:00') as date))",
          "unix_date(cast(TIMESTAMP('2024-06-15 10:30:00') as date))"))

      // Null handling
      checkDays(
        dummyDF.select(getColumnFromExpression(Days(Literal.create(null, DataTypes.DateType)))),
        dummyDF.selectExpr("unix_date(cast(NULL as date))"))
    }
  }

  /**
   * Checks that the Comet-evaluated DataFrame produces the same results as the baseline DataFrame
   * evaluated by native Spark JVM, and that Comet native operators are used. This is needed
   * because Hours is a PartitionTransformExpression that extends Unevaluable.
   */
  private def checkHours(cometDF: DataFrame, baselineDF: DataFrame): Unit = {
    // Ensure the expected answer is evaluated solely by native Spark JVM (Comet off)
    var expected: Array[Row] = Array.empty
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      expected = baselineDF.collect()
    }
    checkAnswer(cometDF, expected.toSeq)
    checkCometOperators(stripAQEPlan(cometDF.queryExecution.executedPlan))
  }

  test("hours - timestamp input") {
    val r = new Random(42)
    val tsSchema = StructType(Seq(StructField("ts", DataTypes.TimestampType, true)))
    val tsDF = FuzzDataGenerator.generateDataFrame(r, spark, tsSchema, 1000, DataGenOptions())

    for (timezone <- Seq("UTC", "America/Los_Angeles", "Asia/Tokyo")) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timezone) {
        checkHours(
          tsDF.select(col("ts"), getColumnFromExpression(Hours(UnresolvedAttribute("ts")))),
          tsDF.selectExpr("ts", "cast(floor(unix_micros(ts) / 3600000000D) as int)"))
      }
    }
  }

  test("hours - timestamp_ntz input") {
    val r = new Random(42)
    val ntzSchema = StructType(Seq(StructField("ts", DataTypes.TimestampNTZType, true)))
    val ntzDF = FuzzDataGenerator.generateDataFrame(r, spark, ntzSchema, 1000, DataGenOptions())

    // Spark 3.4: unix_micros() does not accept TIMESTAMP_NTZ; baseline matches micros / hour.
    val _spark = spark
    import _spark.implicits._
    val expectedDF = ntzDF
      .map { row =>
        val ts = row.getAs[java.time.LocalDateTime]("ts")
        val hoursCol: java.lang.Integer =
          if (ts == null) null
          else
            Integer.valueOf(
              Math.floorDiv(DateTimeUtils.localDateTimeToMicros(ts), 3600000000L).toInt)
        (ts, hoursCol)
      }
      .toDF("ts", "hours")

    checkHours(
      ntzDF.select(col("ts"), getColumnFromExpression(Hours(UnresolvedAttribute("ts")))),
      expectedDF)
  }

  test("cast TimestampNTZ to Timestamp - DST edge cases") {
    val data = Seq(
      Row(java.time.LocalDateTime.parse("2024-03-31T01:30:00")), // Spring forward (Europe/London)
      Row(java.time.LocalDateTime.parse("2024-10-27T01:30:00")) // Fall back (Europe/London)
    )
    val schema = StructType(Seq(StructField("ts_ntz", DataTypes.TimestampNTZType, true)))
    spark
      .createDataFrame(spark.sparkContext.parallelize(data), schema)
      .createOrReplaceTempView("dst_tbl")

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Europe/London") {
      checkSparkAnswerAndOperator(
        "SELECT ts_ntz, CAST(ts_ntz AS TIMESTAMP) FROM dst_tbl ORDER BY ts_ntz")
    }
  }
}
