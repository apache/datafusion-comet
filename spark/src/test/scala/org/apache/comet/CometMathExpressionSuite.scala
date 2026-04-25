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
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

class CometMathExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("abs") {
    val df = createTestData(generateNegativeZero = false)
    df.createOrReplaceTempView("tbl")
    for (field <- df.schema.fields) {
      val col = field.name
      checkSparkAnswerAndOperator(s"SELECT $col, abs($col) FROM tbl ORDER BY $col")
    }
  }

  test("abs - negative zero") {
    val df = createTestData(generateNegativeZero = true)
    df.createOrReplaceTempView("tbl")
    for (field <- df.schema.fields.filter(f =>
        f.dataType == DataTypes.FloatType || f.dataType == DataTypes.DoubleType)) {
      val col = field.name
      checkSparkAnswerAndOperator(
        s"SELECT $col, abs($col) FROM tbl WHERE CAST($col as string) = '-0.0' ORDER BY $col")
    }
  }

  test("abs (ANSI mode)") {
    val df = createTestData(generateNegativeZero = false)
    df.createOrReplaceTempView("tbl")
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      for (field <- df.schema.fields) {
        val col = field.name
        checkSparkAnswerMaybeThrows(sql(s"SELECT $col, abs($col) FROM tbl ORDER BY $col")) match {
          case (Some(sparkExc), Some(cometExc)) =>
            val cometErrorPattern =
              """.+[ARITHMETIC_OVERFLOW].+overflow. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error.""".r
            assert(cometErrorPattern.findFirstIn(cometExc.getMessage).isDefined)
            assert(sparkExc.getMessage.contains("overflow"))
          case (Some(_), None) =>
            fail("Exception should be thrown")
          case (None, Some(cometExc)) =>
            throw cometExc
          case _ =>
        }
      }
    }
  }

  private def createTestData(generateNegativeZero: Boolean) = {
    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("c0", DataTypes.ByteType, nullable = true),
        StructField("c1", DataTypes.ShortType, nullable = true),
        StructField("c2", DataTypes.IntegerType, nullable = true),
        StructField("c3", DataTypes.LongType, nullable = true),
        StructField("c4", DataTypes.FloatType, nullable = true),
        StructField("c5", DataTypes.DoubleType, nullable = true),
        StructField("c6", DataTypes.createDecimalType(10, 2), nullable = true)))
    FuzzDataGenerator.generateDataFrame(
      r,
      spark,
      schema,
      1000,
      DataGenOptions(generateNegativeZero = generateNegativeZero))
  }

  test("width_bucket") {
    assume(isSpark35Plus, "width_bucket was added in Spark 3.5")
    withSQLConf("spark.comet.exec.localTableScan.enabled" -> "true") {
      spark
        .createDataFrame(
          Seq((5.3, 0.2, 10.6, 5), (8.1, 0.0, 5.7, 4), (-0.9, 5.2, 0.5, 2), (-2.1, 1.3, 3.4, 3)))
        .toDF("c1", "c2", "c3", "c4")
        .createOrReplaceTempView("width_bucket_test")
      checkSparkAnswerAndOperator(
        "SELECT c1, width_bucket(c1, c2, c3, c4) FROM width_bucket_test")
    }
  }

  test("width_bucket - edge cases") {
    assume(isSpark35Plus, "width_bucket was added in Spark 3.5")
    withSQLConf("spark.comet.exec.localTableScan.enabled" -> "true") {
      spark
        .createDataFrame(Seq(
          (0.0, 10.0, 0.0, 5), // Value equals max (reversed bounds)
          (10.0, 0.0, 10.0, 5), // Value equals max (normal bounds)
          (10.0, 0.0, 0.0, 5), // Min equals max - returns NULL
          (5.0, 0.0, 10.0, 0) // Zero buckets - returns NULL
        ))
        .toDF("c1", "c2", "c3", "c4")
        .createOrReplaceTempView("width_bucket_edge")
      checkSparkAnswerAndOperator(
        "SELECT c1, width_bucket(c1, c2, c3, c4) FROM width_bucket_edge")
    }
  }

  test("width_bucket - NaN values") {
    assume(isSpark35Plus, "width_bucket was added in Spark 3.5")
    withSQLConf("spark.comet.exec.localTableScan.enabled" -> "true") {
      spark
        .createDataFrame(
          Seq((Double.NaN, 5.0, 0.0), (5.0, Double.NaN, 0.0), (5.0, 0.0, Double.NaN)))
        .toDF("c1", "c2", "c3")
        .createOrReplaceTempView("width_bucket_nan")
      checkSparkAnswerAndOperator("SELECT c1, width_bucket(c1, c2, c3, 5) FROM width_bucket_nan")
    }
  }

  test("width_bucket - with range data") {
    assume(isSpark35Plus, "width_bucket was added in Spark 3.5")
    withSQLConf("spark.comet.exec.localTableScan.enabled" -> "true") {
      spark
        .range(10)
        .selectExpr("id", "CAST(id AS DOUBLE) as value")
        .createOrReplaceTempView("width_bucket_range")
      checkSparkAnswerAndOperator(
        "SELECT id, width_bucket(value, 0.0, 10.0, 5) FROM width_bucket_range ORDER BY id")
    }
  }
}
