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

import org.apache.spark.sql.{CometTestBase, Row, SaveMode}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.serde.{CometDateFormat, CometTruncDate, CometTruncTimestamp}
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

class CometTemporalExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

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

  test("unix_timestamp - timestamp_ntz input falls back to Spark") {
    // TimestampNTZ is not supported because Comet incorrectly applies timezone
    // conversion. TimestampNTZ stores local time without timezone, so the unix
    // timestamp should just be the value divided by microseconds per second.
    val r = new Random(42)
    val ntzSchema = StructType(Seq(StructField("ts_ntz", DataTypes.TimestampNTZType, true)))
    val ntzDF = FuzzDataGenerator.generateDataFrame(r, spark, ntzSchema, 100, DataGenOptions())
    ntzDF.createOrReplaceTempView("ntz_tbl")
    checkSparkAnswerAndFallbackReason(
      "SELECT ts_ntz, unix_timestamp(ts_ntz) from ntz_tbl order by ts_ntz",
      "unix_timestamp does not support input type: TimestampNTZType")
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
}
