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

package org.apache.comet.exec

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.functions.col

import org.apache.comet.CometConf

class CometGenerateExecSuite extends CometTestBase {

  import testImplicits._

  test("explode with simple array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(1, 2, 3)), (2, Array(4, 5)), (3, Array(6)))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with empty array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(1, 2)), (2, Array.empty[Int]), (3, Array(3)))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with null array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Some(Array(1, 2))), (2, None), (3, Some(Array(3))))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode_outer with simple array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(1, 2, 3)), (2, Array(4, 5)), (3, Array(6)))
        .toDF("id", "arr")
        .selectExpr("id", "explode_outer(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode_outer with empty array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(1, 2)), (2, Array.empty[Int]), (3, Array(3)))
        .toDF("id", "arr")
        .selectExpr("id", "explode_outer(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode_outer with null array") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Some(Array(1, 2))), (2, None), (3, Some(Array(3))))
        .toDF("id", "arr")
        .selectExpr("id", "explode_outer(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with multiple columns") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, "A", Array(1, 2, 3)), (2, "B", Array(4, 5)), (3, "C", Array(6)))
        .toDF("id", "name", "arr")
        .selectExpr("id", "name", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with array of strings") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array("a", "b", "c")), (2, Array("d", "e")), (3, Array("f")))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode with filter") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Array(1, 2, 3)), (2, Array(4, 5, 6)), (3, Array(7, 8, 9)))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
        .filter(col("value") > 5)
      checkSparkAnswerAndOperator(df)
    }
  }

  test("explode fallback when disabled") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "false") {
      val df = Seq((1, Array(1, 2, 3)), (2, Array(4, 5)))
        .toDF("id", "arr")
        .selectExpr("id", "explode(arr) as value")
      checkSparkAnswerAndFallbackReason(
        df,
        "Native support for operator GenerateExec is disabled")
    }
  }

  test("explode with map input falls back") {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true") {
      val df = Seq((1, Map("a" -> 1, "b" -> 2)), (2, Map("c" -> 3)))
        .toDF("id", "map")
        .selectExpr("id", "explode(map) as (key, value)")
      checkSparkAnswerAndFallbackReason(
        df,
        "Comet only supports explode/explode_outer for arrays, not maps")
    }
  }

}
