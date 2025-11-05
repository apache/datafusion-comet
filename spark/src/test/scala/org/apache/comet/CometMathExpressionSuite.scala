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
        checkSparkMaybeThrows(sql(s"SELECT $col, abs($col) FROM tbl ORDER BY $col")) match {
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
}
