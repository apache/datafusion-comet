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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import scala.util.Random

class CometArrayExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("array_remove - integer") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(path, dictionaryEnabled, 10000)
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(
          sql("SELECT array_remove(array(_2, _3,_4), _2) from t1 where _2 is null"))
        checkSparkAnswerAndOperator(
          sql("SELECT array_remove(array(_2, _3,_4), _3) from t1 where _3 is not null"))
        checkSparkAnswerAndOperator(sql(
          "SELECT array_remove(case when _2 = _3 THEN array(_2, _3,_4) ELSE null END, _3) from t1"))
      }
    }
  }

  test("array_remove - test all types") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      DataGenerator.DEFAULT.generateRandomParquetFile(random, spark, filename, 100, true)
      val table = spark.read.parquet(filename)
      table.createOrReplaceTempView("t1")

      // test with array of each column
      for (fieldName <- table.schema.fieldNames) {
        sql(s"SELECT array($fieldName, $fieldName) as a, $fieldName as b FROM t1")
          .createOrReplaceTempView("t2")
        val df = sql("SELECT array_remove(a, b) FROM t2")
        checkSparkAnswerAndOperator(df)
        // scalastyle:off println
        println(df.queryExecution.executedPlan)
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
      checkSparkAnswerAndCompareExplainPlan(
        sql("SELECT array_remove(a, b) FROM t2"),
        expectedFallbackReasons)
    }
  }
}
