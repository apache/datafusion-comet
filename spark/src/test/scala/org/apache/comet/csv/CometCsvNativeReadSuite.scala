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

package org.apache.comet.csv

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import org.apache.comet.CometConf

class CometCsvNativeReadSuite extends CometTestBase {
  private val TEST_CSV_PATH_NO_HEADER = "src/test/resources/test-data/csv-test-1.csv"
  private val TEST_CSV_PATH_HAS_HEADER = "src/test/resources/test-data/csv-test-2.csv"

  test("Native csv read - with schema") {
    withSQLConf(
      CometConf.COMET_CSV_V2_NATIVE_ENABLED.key -> "true",
      SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      val schema = new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)
        .add("c", IntegerType)
      val df = spark.read
        .options(Map("header" -> "false", "delimiter" -> ","))
        .schema(schema)
        .csv(TEST_CSV_PATH_NO_HEADER)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("Native csv read - without schema") {
    withSQLConf(
      CometConf.COMET_CSV_V2_NATIVE_ENABLED.key -> "true",
      SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      val df = spark.read
        .options(Map("header" -> "true", "delimiter" -> ","))
        .csv(TEST_CSV_PATH_HAS_HEADER)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("Native csv read - test fallback reasons") {
    withSQLConf(
      CometConf.COMET_CSV_V2_NATIVE_ENABLED.key -> "true",
      SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      val columnNameOfCorruptedRecords =
        SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)
      val schema = new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)
        .add("c", IntegerType)
        .add(columnNameOfCorruptedRecords, StringType)
      var df = spark.read
        .options(Map("header" -> "false", "delimiter" -> ","))
        .schema(schema)
        .csv(TEST_CSV_PATH_NO_HEADER)
      checkSparkAnswerAndFallbackReason(
        df,
        "Comet doesn't support the processing of corrupted records")
      df = spark.read
        .options(Map("header" -> "false", "delimiter" -> ",", "inferSchema" -> "true"))
        .csv(TEST_CSV_PATH_NO_HEADER)
      checkSparkAnswerAndFallbackReason(df, "Comet doesn't support inferSchema=true option")
      df = spark.read
        .options(Map("header" -> "false", "delimiter" -> ",,"))
        .csv(TEST_CSV_PATH_NO_HEADER)
      checkSparkAnswerAndFallbackReason(
        df,
        "Comet supports only single-character delimiters, but got: ',,'")
    }
  }
}
