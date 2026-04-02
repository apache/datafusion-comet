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

import org.apache.spark.sql.{CometTestBase, DataFrame, SaveMode}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataTypes

/**
 * Tests for string-to-timestamp (and string-to-date) cast correctness, ported from Spark's
 * DateTimeUtilsSuite. That suite lives in sql/catalyst and does not extend CometTestBase, so it
 * never runs in Comet CI.
 */
class CometDateTimeUtilsSuite extends CometTestBase {

  import testImplicits._

  private def roundtripParquet(df: DataFrame, tempDir: File): DataFrame = {
    val filename = new File(tempDir, s"dtutils_${System.currentTimeMillis()}.parquet").toString
    df.write.mode(SaveMode.Overwrite).parquet(filename)
    spark.read.parquet(filename)
  }

  /**
   * Writes values to Parquet (to prevent constant folding), casts the "a" column to
   * TimestampType, and asserts that Comet produces the same result as Spark.
   */
  private def checkCastToTimestamp(values: Seq[String]): Unit = {
    withTempPath { dir =>
      val df = roundtripParquet(values.toDF("a"), dir).coalesce(1)
      checkSparkAnswer(df.withColumn("ts", col("a").cast(DataTypes.TimestampType)))
    }
  }

  /**
   * Same as checkCastToTimestamp but casts the result to STRING before collecting. Use for
   * extreme-year values (e.g. year 294247 or -290308) where collect() overflows in
   * toJavaTimestamp due to Gregorian/Julian calendar rebasing in the test harness.
   */
  private def checkCastToTimestampAsString(values: Seq[String]): Unit = {
    withTempPath { dir =>
      val df = roundtripParquet(values.toDF("a"), dir).coalesce(1)
      checkSparkAnswer(
        df.withColumn("ts", col("a").cast(DataTypes.TimestampType).cast(DataTypes.StringType)))
    }
  }

  private def checkCastToTimestampNTZ(values: Seq[String]): Unit = {
    withTempPath { dir =>
      val df = roundtripParquet(values.toDF("a"), dir).coalesce(1)
      checkSparkAnswer(df.withColumn("ts", col("a").cast(DataTypes.TimestampNTZType)))
    }
  }

  private def checkCastToDate(values: Seq[String]): Unit = {
    withTempPath { dir =>
      val df = roundtripParquet(values.toDF("a"), dir).coalesce(1)
      checkSparkAnswer(df.withColumn("dt", col("a").cast(DataTypes.DateType)))
    }
  }

  test("string to timestamp - basic date and datetime formats") {
    // Run for a few representative session timezones instead of the ALL_TIMEZONES loop in
    // Spark's DateTimeUtilsSuite. The exact UTC epoch depends on the session timezone for
    // values without an embedded offset.
    for (tz <- Seq("UTC", "America/Los_Angeles", "Asia/Kathmandu")) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        checkCastToTimestamp(
          Seq(
            "1969-12-31 16:00:00",
            "0001",
            "2015-03",
            // Leading/trailing whitespace is accepted
            "2015-03-18",
            "2015-03-18 ",
            " 2015-03-18",
            " 2015-03-18 ",
            "2015-03-18 12:03:17",
            "2015-03-18T12:03:17",
            // Milliseconds
            "2015-03-18 12:03:17.123",
            "2015-03-18T12:03:17.123",
            // Microseconds
            "2015-03-18T12:03:17.123121",
            "2015-03-18T12:03:17.12312",
            // More than 6 fractional digits — truncated to microseconds
            "2015-03-18T12:03:17.123456789",
            // Time-only: bare HH:MM:SS
            "18:12:15",
            // Milliseconds with more than 6 fractional digits (truncated)
            "2011-05-06 07:08:09.1000"))
      }
    }
  }

  test("string to timestamp - embedded timezone offsets") {
    // When the string includes a timezone offset it determines the UTC epoch regardless of the
    // session timezone. These cases exercise the offset-parsing logic.
    checkCastToTimestamp(
      Seq(
        // ±HH:MM
        "2015-03-18T12:03:17-13:53",
        "2015-03-18T12:03:17-01:00",
        "2015-03-18T12:03:17+07:30",
        "2015-03-18T12:03:17+07:03",
        // Z and UTC
        "2015-03-18T12:03:17Z",
        "2015-03-18 12:03:17Z",
        "2015-03-18 12:03:17UTC",
        // ±HHMM (no colon) — issue #3775
        "2015-03-18T12:03:17-1353",
        "2015-03-18T12:03:17-0100",
        "2015-03-18T12:03:17+0730",
        "2015-03-18T12:03:17+0703",
        // ±H:MM or ±H:M (single-digit hour) — issue #3775
        "2015-03-18T12:03:17-1:0",
        // GMT±HH:MM — issue #3775
        "2015-03-18T12:03:17GMT-13:53",
        "2015-03-18T12:03:17GMT-01:00",
        "2015-03-18T12:03:17 GMT+07:30",
        "2015-03-18T12:03:17GMT+07:03",
        // With milliseconds
        "2015-03-18T12:03:17.456Z",
        "2015-03-18 12:03:17.456Z",
        "2015-03-18 12:03:17.456 UTC",
        "2015-03-18T12:03:17.123-1:0",
        "2015-03-18T12:03:17.123-01:00",
        "2015-03-18T12:03:17.123 GMT-01:00",
        "2015-03-18T12:03:17.123-0100",
        "2015-03-18T12:03:17.123+07:30",
        "2015-03-18T12:03:17.123 GMT+07:30",
        "2015-03-18T12:03:17.123+0730",
        "2015-03-18T12:03:17.123GMT+07:30",
        // With microseconds
        "2015-03-18T12:03:17.123121+7:30",
        "2015-03-18T12:03:17.123121 GMT+0730",
        "2015-03-18T12:03:17.12312+7:30",
        "2015-03-18T12:03:17.12312 UT+07:30",
        "2015-03-18T12:03:17.12312+0730",
        // Nanoseconds truncated to microseconds
        "2015-03-18T12:03:17.123456789+0:00",
        "2015-03-18T12:03:17.123456789 UTC+0",
        "2015-03-18T12:03:17.123456789GMT+00:00",
        // Named timezone — issue #3775
        "2015-03-18T12:03:17.123456 Europe/Moscow",
        // T-prefixed time-only with offset
        "T18:12:15.12312+7:30",
        "T18:12:15.12312 UTC+07:30",
        "T18:12:15.12312+0730",
        // Bare time-only with offset
        "18:12:15.12312+7:30",
        "18:12:15.12312 GMT+07:30",
        "18:12:15.12312+0730"))
  }

  test("string to timestamp - invalid formats return null") {
    // All of these should produce null (not throw) in non-ANSI mode.
    for (tz <- Seq("UTC", "America/Los_Angeles")) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        checkCastToTimestamp(
          Seq(
            "238",
            "2015-03-18 123142",
            "2015-03-18T123123",
            "2015-03-18X",
            "2015/03/18",
            "2015.03.18",
            "20150318",
            "2015-031-8",
            "015-01-18",
            "2015-03-18T12:03.17-20:0",
            "2015-03-18T12:03.17-0:70",
            "2015-03-18T12:03.17-1:0:0",
            "1999 08 01",
            "1999-08 01",
            "1999 08",
            "",
            "    ",
            "+",
            "T",
            "2015-03-18T",
            "12::",
            "2015-03-18T12:03:17-8:",
            "2015-03-18T12:03:17-8:30:"))
      }
    }
  }

  // "SPARK-35780: support full range of timestamp string"
  test("SPARK-35780: full range of timestamp string") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      // Normal-range cases: collect() as TimestampType directly.
      checkCastToTimestamp(
        Seq(
          // Negative year
          "-1969-12-31 16:00:00",
          // Zero-padded year
          "02015-03-18 16:00:00",
          "000001",
          "-000001",
          "00238",
          // 5-digit year (within NaiveDate range)
          "99999-03-01T12:03:17",
          // These look like time-only but with a leading sign — invalid
          "+12:12:12",
          "-12:12:12",
          // Empty / whitespace
          "",
          "    ",
          "+",
          // One microsecond past Long.MaxValue — overflows to null
          "294247-01-10T04:00:54.775808Z",
          // One microsecond before Long.MinValue — overflows to null
          "-290308-12-21T19:59:05.224191Z",
          // Integer overflow in individual fields
          "4294967297",
          "2021-4294967297-11",
          "4294967297:30:00",
          "2021-11-4294967297T12:30:00",
          "2021-01-01T12:4294967297:00",
          "2021-01-01T12:30:4294967297",
          "2021-01-01T12:30:4294967297.123456",
          "2021-01-01T12:30:4294967297+07:30",
          "2021-01-01T12:30:4294967297UTC",
          "2021-01-01T12:30:4294967297+4294967297:30"))

      // Extreme-year boundary cases: collecting a TimestampType value for year 294247 or -290308
      // overflows in toJavaTimestamp due to Gregorian/Julian rebasing in the test harness.
      // Cast to STRING first to avoid that while still verifying correct parsing.
      checkCastToTimestampAsString(
        Seq(
          // Long.MaxValue boundary — valid, equals Long.MaxValue microseconds
          "294247-01-10T04:00:54.775807Z",
          // Long.MinValue boundary — valid, equals Long.MinValue microseconds
          "-290308-12-21T19:59:05.224192Z"))
    }
  }

  test("SPARK-15379: invalid calendar dates in string to date cast") {
    // Feb 29 on a non-leap year and Apr 31 must produce null for both DATE and TIMESTAMP.
    checkCastToDate(Seq("2015-02-29 00:00:00", "2015-04-31 00:00:00", "2015-02-29", "2015-04-31"))

    checkCastToTimestamp(
      Seq("2015-02-29 00:00:00", "2015-04-31 00:00:00", "2015-02-29", "2015-04-31"))
  }

  test("trailing characters while converting string to timestamp") {
    // Garbage after a valid ISO timestamp must make the whole value null.
    checkCastToTimestamp(Seq("2019-10-31T10:59:23Z:::"))
  }

  test("SPARK-37326: cast string to TIMESTAMP_NTZ rejects timezone offsets") {
    // A value with a timezone offset should be null for TIMESTAMP_NTZ.
    checkCastToTimestampNTZ(
      Seq(
        // Has offset — null
        "2021-11-22 10:54:27 +08:00",
        // No offset — parsed as local datetime
        "2021-11-22 10:54:27",
        // More NTZ-compatible values
        "2021-11-22",
        "2021-11-22T10:54:27.123456"))
  }
}
