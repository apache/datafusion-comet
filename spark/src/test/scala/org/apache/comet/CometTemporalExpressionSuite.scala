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

  test("next_day") {
    import org.apache.spark.sql.Row

    // Create test data with dates
    val r = new Random(42)
    val testData = (1 to 1000).map { _ =>
      // Generate dates in a reasonable range
      val daysFromEpoch = r.nextInt(20000) - 5000 // ~1956 to ~2024
      // Convert days from epoch to java.sql.Date
      val millis = daysFromEpoch.toLong * 24 * 60 * 60 * 1000
      Row(new java.sql.Date(millis))
    }
    val schema = StructType(Seq(StructField("c0", DataTypes.DateType, false)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)
    df.createOrReplaceTempView("tbl")

    // Test with various day names
    val dayNames =
      Seq("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")
    for (day <- dayNames) {
      checkSparkAnswerAndOperator(s"SELECT c0, next_day(c0, '$day') FROM tbl ORDER BY c0")
    }

    // Test with abbreviated day names
    val abbreviations = Seq("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
    for (day <- abbreviations) {
      checkSparkAnswerAndOperator(s"SELECT c0, next_day(c0, '$day') FROM tbl ORDER BY c0")
    }

    // Disable constant folding to ensure literal expressions are executed by Comet
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      // Test with literal date
      checkSparkAnswerAndOperator("SELECT next_day(DATE('2023-01-01'), 'Monday')")
      checkSparkAnswerAndOperator("SELECT next_day(DATE('2023-01-01'), 'Sunday')")

      // Test null handling
      checkSparkAnswerAndOperator("SELECT next_day(NULL, 'Monday')")
      checkSparkAnswerAndOperator("SELECT next_day(DATE('2023-01-01'), NULL)")
    }
  }
}
