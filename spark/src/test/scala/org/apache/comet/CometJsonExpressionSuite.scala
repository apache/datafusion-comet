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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.JsonToStructs
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

class CometJsonExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.getExprAllowIncompatConfigKey(classOf[JsonToStructs]) -> "true",
        CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {
        testFun
      }
    }
  }

  test("from_json - basic primitives") {
    assume(!isSpark40Plus)

    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 100).map(i => {
          val json = s"""{"a":$i,"b":"str_$i"}"""
          (i, json)
        }),
        "tbl",
        withDictionary = dictionaryEnabled) {

        val schema = "a INT, b STRING"
        checkSparkAnswerAndOperator(s"SELECT from_json(_2, '$schema') FROM tbl")
        checkSparkAnswerAndOperator(s"SELECT from_json(_2, '$schema').a FROM tbl")
        checkSparkAnswerAndOperator(s"SELECT from_json(_2, '$schema').b FROM tbl")
      }
    }
  }

  test("from_json - null and error handling") {
    assume(!isSpark40Plus)

    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        Seq(
          (1, """{"a":123,"b":"test"}"""), // Valid JSON
          (2, """{"a":456}"""), // Missing field b
          (3, """{"a":null}"""), // Explicit null
          (4, """invalid json"""), // Parse error
          (5, """{}"""), // Empty object
          (6, """null"""), // JSON null
          (7, null) // Null input
        ),
        "tbl",
        withDictionary = dictionaryEnabled) {

        val schema = "a INT, b STRING"
        checkSparkAnswerAndOperator(
          s"SELECT _1, from_json(_2, '$schema') as parsed FROM tbl ORDER BY _1")
      }
    }
  }

  test("from_json - all primitive types") {
    assume(!isSpark40Plus)

    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 50).map(i => {
          val sign = if (i % 2 == 0) 1 else -1
          val json =
            s"""{"i32":${sign * i},"i64":${sign * i * 1000000000L},"f32":${sign * i * 1.5},"f64":${sign * i * 2.5},"bool":${i % 2 == 0},"str":"value_$i"}"""
          (i, json)
        }),
        "tbl",
        withDictionary = dictionaryEnabled) {

        val schema = "i32 INT, i64 BIGINT, f32 FLOAT, f64 DOUBLE, bool BOOLEAN, str STRING"
        checkSparkAnswerAndOperator(s"SELECT from_json(_2, '$schema') FROM tbl")
        checkSparkAnswerAndOperator(s"SELECT from_json(_2, '$schema').i32 FROM tbl")
        checkSparkAnswerAndOperator(s"SELECT from_json(_2, '$schema').str FROM tbl")
      }
    }
  }
}
