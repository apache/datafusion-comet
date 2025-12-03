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

package org.apache.comet.expressions.conditional

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometCoalesceSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_AUTO) {
        testFun
      }
    }
  }

  test("coalesce should return correct datatype") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          checkSparkAnswerAndOperator(
            "SELECT coalesce(cast(_18 as date), cast(_19 as date), _20) FROM tbl")
        }
      }
    }
  }

  test("test coalesce lazy eval") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val data = Seq((9999999999999L, 0))
      withParquetTable(data, "t1") {
        val res = spark.sql("""
            |SELECT coalesce(_1, CAST(_1 AS TINYINT)) from t1;
            |  """.stripMargin)
        checkSparkAnswerAndOperator(res)
      }
    }
  }

  test("string with coalesce") {
    withParquetTable(
      (0 until 10).map(i => (i.toString, if (i > 5) None else Some((i + 100).toString))),
      "tbl") {
      checkSparkAnswerAndOperator(
        "SELECT coalesce(_1), coalesce(_1, 1), coalesce(null, _1), coalesce(null, 1), coalesce(_2, _1), coalesce(null) FROM tbl")
    }
  }

}
