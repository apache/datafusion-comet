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

  test("date_from_unix_date") {
    // Create test data with unix dates in a reasonable range (1900-2100)
    // -25567 = 1900-01-01, 47482 = 2100-01-01
    val r = new Random(42)
    val testData = (1 to 1000).map { _ =>
      val unixDate = r.nextInt(73049) - 25567 // range from 1900 to 2100
      Row(if (r.nextDouble() < 0.1) null else unixDate)
    }
    val schema = StructType(Seq(StructField("c0", DataTypes.IntegerType, true)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)
    df.createOrReplaceTempView("tbl")

    // Basic test with random unix dates in a reasonable range
    checkSparkAnswerAndOperator("SELECT c0, date_from_unix_date(c0) FROM tbl ORDER BY c0")

    // Disable constant folding to ensure literal expressions are executed by Comet
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      // Test epoch (0 = 1970-01-01)
      checkSparkAnswerAndOperator("SELECT date_from_unix_date(0)")

      // Test day after epoch (1 = 1970-01-02)
      checkSparkAnswerAndOperator("SELECT date_from_unix_date(1)")

      // Test day before epoch (-1 = 1969-12-31)
      checkSparkAnswerAndOperator("SELECT date_from_unix_date(-1)")

      // Test a known date (18993 = 2022-01-01, calculated as days from 1970-01-01)
      checkSparkAnswerAndOperator("SELECT date_from_unix_date(18993)")

      // Test null handling
      checkSparkAnswerAndOperator("SELECT date_from_unix_date(NULL)")
    }
  }
}
