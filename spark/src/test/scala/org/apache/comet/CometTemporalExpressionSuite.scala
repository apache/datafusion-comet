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

import org.apache.comet.serde.CometTruncDate
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.util.Random

class CometTemporalExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("trunc_date") {
    val formats = Seq("year", "yyyy", "yy", "month", "mon", "mm", "week", "quarter")

    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("c0", DataTypes.TimestampType, true),
        StructField("c1", DataTypes.StringType, true)))
    val df = FuzzDataGenerator.generateDataFrame(
      r,
      spark,
      schema,
      1000,
      DataGenOptions(customStringValues = formats))

    df.createOrReplaceTempView("tbl")

    for (format <- formats) {
      checkSparkAnswerAndOperator(s"SELECT c0, trunc(c0, '$format') from tbl order by c0, c1")
    }
    // checkSparkAnswerAndOperator(s"SELECT c0, trunc(c0, c1) from tbl order by c0, c1")
  }

  test("trunc") {
    val supportedFormats = CometTruncDate.supportedFormats
    val unsupportedFormats = Seq("day", "dd", "microsecond", "millisecond", "second",
      "minute", "hour", "week", "quarter")
    val allFormats = supportedFormats ++ unsupportedFormats

    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("c0", DataTypes.DateType, true),
        StructField("c1", DataTypes.StringType, true)))
    val df = FuzzDataGenerator.generateDataFrame(
      r,
      spark,
      schema,
      1000,
      DataGenOptions(customStringValues = allFormats))
    df.createOrReplaceTempView("tbl")
    for (format <- supportedFormats) {
      checkSparkAnswerAndOperator(s"SELECT c0, trunc(c0, '$format') from tbl order by c0, c1")
    }
    for (format <- unsupportedFormats) {
      checkSparkAnswer(s"SELECT c0, trunc(c0, '$format') from tbl order by c0, c1")
    }
    // checkSparkAnswerAndOperator(s"SELECT c0, trunc(c0, c1) from tbl order by c0, c1")
  }
}
