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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{ProjectExec, RDDScanExec, WholeStageCodegenExec}
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
    val df = FuzzDataGenerator.generateDataFrame(
      r,
      spark,
      schema,
      1000,
      DataGenOptions())

    df.createOrReplaceTempView("tbl")

    for (format <- supportedFormats) {
      checkSparkAnswerAndOperator(s"SELECT c0, trunc(c0, '$format') from tbl order by c0, c1")
    }
    for (format <- unsupportedFormats) {
      // Comet should fall back to Spark for unsupported or invalid formats
      checkFallback(
        s"unsupported/invalid format $format",
        s"SELECT c0, trunc(c0, '$format') from tbl order by c0, c1")
    }

    // Comet should fall back to Spark if format is not a literal
    checkFallback("non-literal format", "SELECT c0, trunc(c0, c1) from tbl order by c0, c1")
  }

  test("date_trunc (TruncTimestamp)") {
    val supportedFormats = CometTruncTimestamp.supportedFormats
    val unsupportedFormats = Seq("invalid")

    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("c0", DataTypes.TimestampType, true),
        StructField("fmt", DataTypes.StringType, true)))
    val df = FuzzDataGenerator.generateDataFrame(
      r,
      spark,
      schema,
      1000,
      DataGenOptions())
    df.createOrReplaceTempView("tbl")

    // TODO test fails if timezone not set to UTC
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      for (format <- supportedFormats) {
        checkSparkAnswerAndOperator(s"SELECT c0, date_trunc('$format', c0) from tbl order by c0")
      }
      for (format <- unsupportedFormats) {
        // Comet should fall back to Spark for unsupported or invalid formats
        checkFallback(
          s"unsupported/invalid format $format",
          s"SELECT c0, date_trunc('$format', c0) from tbl order by c0")
      }
      // Comet should fall back to Spark if format is not a literal
      checkFallback(
        "non-literal format",
        "SELECT c0, date_trunc(fmt, c0) from tbl order by c0, fmt")
    }
  }

  /** Check that the first projection fell back to Spark */
  private def checkFallback(message: String, sql: String): Unit = {
    val (_, cometPlan) = checkSparkAnswer(sql)
    val shuffle = collect(cometPlan) { case p: CometShuffleExchangeExec => p }
    assert(shuffle.length == 1)
    shuffle.head.child match {
      case WholeStageCodegenExec(p: ProjectExec) =>
        assert(p.child.isInstanceOf[RDDScanExec])
      case _ =>
        fail(s"Comet should have fallen back to Spark for $message")
    }
  }
}
