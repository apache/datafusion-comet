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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{JsonToStructs, StructsToJson}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.serde.CometStructsToJson
import org.apache.comet.testing.{DataGenOptions, ParquetGenerator, SchemaGenOptions}

class CometJsonExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.getExprAllowIncompatConfigKey(classOf[JsonToStructs]) -> "true",
        CometConf.getExprAllowIncompatConfigKey(classOf[StructsToJson]) -> "true") {
        testFun
      }
    }
  }

  test("to_json - all supported types") {
    assume(!isSpark40Plus)
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        ParquetGenerator.makeParquetFile(
          random,
          spark,
          filename,
          100,
          SchemaGenOptions(generateArray = false, generateStruct = false, generateMap = false),
          DataGenOptions(generateNaN = false, generateInfinity = false))
      }
      val table = spark.read.parquet(filename)
      val fieldsNames = table.schema.fields
        .filter(sf => CometStructsToJson.isSupportedType(sf.dataType))
        .map(sf => col(sf.name))
        .toSeq
      val df = table.select(to_json(struct(fieldsNames: _*)))
      checkSparkAnswerAndOperator(df)
    }
  }

  test("from_json - basic primitives") {
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

  test("from_json - null input produces null struct") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        Seq(
          (1, """{"a":1,"b":"x"}"""), // Valid JSON to establish column type
          (2, null) // Null input
        ),
        "tbl",
        withDictionary = dictionaryEnabled) {

        val schema = "a INT, b STRING"
        // Verify that null input produces a NULL struct (not a struct with null fields)
        checkSparkAnswerAndOperator(
          s"SELECT _1, from_json(_2, '$schema') IS NULL as struct_is_null FROM tbl WHERE _1 = 2")
        // Field access on null struct should return null
        checkSparkAnswerAndOperator(
          s"SELECT _1, from_json(_2, '$schema').a FROM tbl WHERE _1 = 2")
      }
    }
  }

  test("from_json - nested struct") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 50).map(i => {
          val json = s"""{"outer":{"inner_a":$i,"inner_b":"nested_$i"},"top_level":${i * 10}}"""
          (i, json)
        }),
        "tbl",
        withDictionary = dictionaryEnabled) {

        val schema = "outer STRUCT<inner_a: INT, inner_b: STRING>, top_level INT"
        checkSparkAnswerAndOperator(s"SELECT from_json(_2, '$schema') FROM tbl")
        checkSparkAnswerAndOperator(s"SELECT from_json(_2, '$schema').outer FROM tbl")
        checkSparkAnswerAndOperator(s"SELECT from_json(_2, '$schema').outer.inner_a FROM tbl")
        checkSparkAnswerAndOperator(s"SELECT from_json(_2, '$schema').top_level FROM tbl")
      }
    }
  }

  test("from_json - valid json with incompatible schema") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        Seq(
          (1, """{"a":"not_a_number","b":"test"}"""), // String where INT expected
          (2, """{"a":123,"b":456}"""), // Number where STRING expected
          (3, """{"a":{"nested":"value"},"b":"test"}"""), // Object where INT expected
          (4, """{"a":[1,2,3],"b":"test"}"""), // Array where INT expected
          (5, """{"a":123.456,"b":"test"}"""), // Float where INT expected
          (6, """{"a":true,"b":"test"}"""), // Boolean where INT expected
          (7, """{"a":123,"b":null}""") // Null value for STRING field
        ),
        "tbl",
        withDictionary = dictionaryEnabled) {

        val schema = "a INT, b STRING"
        // When types don't match, Spark typically returns null for that field
        checkSparkAnswerAndOperator(
          s"SELECT _1, from_json(_2, '$schema') as parsed FROM tbl ORDER BY _1")
        checkSparkAnswerAndOperator(s"SELECT _1, from_json(_2, '$schema').a FROM tbl ORDER BY _1")
        checkSparkAnswerAndOperator(s"SELECT _1, from_json(_2, '$schema').b FROM tbl ORDER BY _1")
      }
    }
  }
}
