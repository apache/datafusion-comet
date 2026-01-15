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

import org.apache.comet.serde.{CometTruncDate, CometTruncTimestamp}
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
}
