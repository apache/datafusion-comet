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

import scala.collection.immutable.HashSet
import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.StructType

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometArrayExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("array_remove - integer") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(path, dictionaryEnabled, 10000)
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        withSQLConf(
          CometConf.COMET_EXPLAIN_NATIVE_ENABLED.key -> "true",
          CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {
//          checkSparkAnswerAndOperator(
//            sql("SELECT array_remove(array(_2, _3,_4), _2) from t1 where _2 is null"))
//          checkSparkAnswerAndOperator(
//            sql("SELECT array_remove(array(_2, _3,_4), _3) from t1 where _3 is not null"))
          checkSparkAnswerAndOperator(sql(
            "SELECT array_remove(case when _2 = _3 THEN array(_2, _3,_4) ELSE null END, _3) from t1"))
        }
      }
    }
  }

  test("array_remove - test all types (native Parquet reader)") {
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
          DataGenOptions(
            allowNull = true,
            generateNegativeZero = true,
            generateArray = false,
            generateStruct = false,
            generateMap = false))
      }
      val table = spark.read.parquet(filename)
      table.createOrReplaceTempView("t1")
      // test with array of each column
      for (fieldName <- table.schema.fieldNames) {
        sql(s"SELECT array($fieldName, $fieldName) as a, $fieldName as b FROM t1")
          .createOrReplaceTempView("t2")
        val df = sql("SELECT array_remove(a, b) FROM t2")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("array_remove - test all types (convert from Parquet)") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val options = DataGenOptions(
          allowNull = true,
          generateNegativeZero = true,
          generateArray = true,
          generateStruct = true,
          generateMap = false)
        ParquetGenerator.makeParquetFile(random, spark, filename, 100, options)
      }
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
        val table = spark.read.parquet(filename)
        table.createOrReplaceTempView("t1")
        // test with array of each column
        for (field <- table.schema.fields) {
          val fieldName = field.name
          sql(s"SELECT array($fieldName, $fieldName) as a, $fieldName as b FROM t1")
            .createOrReplaceTempView("t2")
          val df = sql("SELECT array_remove(a, b) FROM t2")
          field.dataType match {
            case _: StructType =>
            // skip due to https://github.com/apache/datafusion-comet/issues/1314
            case _ =>
              checkSparkAnswer(df)
          }
        }
      }
    }
  }

  test("array_remove - fallback for unsupported type struct") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      makeParquetFileAllTypes(path, dictionaryEnabled = true, 100)
      spark.read.parquet(path.toString).createOrReplaceTempView("t1")
      sql("SELECT array(struct(_1, _2)) as a, struct(_1, _2) as b FROM t1")
        .createOrReplaceTempView("t2")
      val expectedFallbackReasons = HashSet(
        "data type not supported: ArrayType(StructType(StructField(_1,BooleanType,true),StructField(_2,ByteType,true)),false)")
      // note that checkExtended is disabled here due to an unrelated issue
      // https://github.com/apache/datafusion-comet/issues/1313
      checkSparkAnswerAndCompareExplainPlan(
        sql("SELECT array_remove(a, b) FROM t2"),
        expectedFallbackReasons,
        checkExplainString = false)
    }
  }
}
