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

import org.apache.spark.sql.{CometTestBase, SaveMode}
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

  private def createTimestampTestData = {
    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("c0", DataTypes.TimestampType, true),
        StructField("fmt", DataTypes.StringType, true)))
    FuzzDataGenerator.generateDataFrame(r, spark, schema, 1000, DataGenOptions())
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

}
