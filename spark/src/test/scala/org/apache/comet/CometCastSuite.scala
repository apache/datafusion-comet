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

import scala.util.Random

import org.apache.spark.sql.{CometTestBase, DataFrame, SaveMode}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, DataTypes}

class CometCastSuite extends CometTestBase with AdaptiveSparkPlanHelper {
  import testImplicits._

  ignore("cast long to short") {
    castTest(generateLongs, DataTypes.ShortType)
  }

  test("cast float to bool") {
    castTest(generateFloats, DataTypes.BooleanType)
  }

  test("cast float to int") {
    castTest(generateFloats, DataTypes.IntegerType)
  }

  ignore("cast float to string") {
    castTest(generateFloats, DataTypes.StringType)
  }

  ignore("cast string to bool") {
    castTest(
      Seq("TRUE", "True", "true", "FALSE", "False", "false", "1", "0", "").toDF("a"),
      DataTypes.BooleanType)
    fuzzCastFromString("truefalseTRUEFALSEyesno10 \t\r\n", 8, DataTypes.BooleanType)
  }

  ignore("cast string to short") {
    fuzzCastFromString("0123456789e+- \t\r\n", 8, DataTypes.ShortType)
  }

  ignore("cast string to float") {
    fuzzCastFromString("0123456789e+- \t\r\n", 8, DataTypes.FloatType)
  }

  ignore("cast string to double") {
    fuzzCastFromString("0123456789e+- \t\r\n", 8, DataTypes.DoubleType)
  }

  ignore("cast string to date") {
    fuzzCastFromString("0123456789/ \t\r\n", 16, DataTypes.DateType)
  }

  ignore("cast string to timestamp") {
    castTest(Seq("2020-01-01T12:34:56.123456", "T2").toDF("a"), DataTypes.TimestampType)
    fuzzCastFromString("0123456789/:T \t\r\n", 32, DataTypes.TimestampType)
  }

  private def generateFloats = {
    val r = new Random(0)
    Range(0, 10000).map(_ => r.nextFloat()).toDF("a")
  }

  private def generateLongs = {
    val r = new Random(0)
    Range(0, 10000).map(_ => r.nextLong()).toDF("a")
  }

  private def genString(r: Random, chars: String, maxLen: Int): String = {
    val len = r.nextInt(maxLen)
    Range(0, len).map(_ => chars.charAt(r.nextInt(chars.length))).mkString
  }

  private def fuzzCastFromString(chars: String, maxLen: Int, toType: DataType) {
    val r = new Random(0)
    val inputs = Range(0, 10000).map(_ => genString(r, chars, maxLen))
    castTest(inputs.toDF("a"), toType)
  }

  private def castTest(input: DataFrame, toType: DataType) {
    withTempPath { dir =>
      val df = roundtripParquet(input, dir)
        .withColumn("converted", col("a").cast(toType))
      checkSparkAnswer(df)
    }
  }

  private def roundtripParquet(df: DataFrame, tempDir: File): DataFrame = {
    val filename = new File(tempDir, s"castTest_${System.currentTimeMillis()}.parquet").toString
    df.write.mode(SaveMode.Overwrite).parquet(filename)
    spark.read.parquet(filename)
  }

}
