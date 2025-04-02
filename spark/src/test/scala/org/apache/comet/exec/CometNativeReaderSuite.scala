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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometNativeReaderSuite extends CometTestBase with AdaptiveSparkPlanHelper {
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    Seq(CometConf.SCAN_NATIVE_DATAFUSION, CometConf.SCAN_NATIVE_ICEBERG_COMPAT).foreach(scan =>
      super.test(s"$testName - $scan", testTags: _*) {
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "false",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> scan) {
          testFun
        }
      })
  }

  test("native reader - read simple STRUCT fields") {
    testSingleLineQuery(
      """
        |select named_struct('firstName', 'John', 'lastName', 'Doe', 'age', 35) as personal_info union all
        |select named_struct('firstName', 'Jane', 'lastName', 'Doe', 'age', 40) as personal_info
        |""".stripMargin,
      "select personal_info.* from tbl")
  }

  test("native reader - read simple ARRAY fields") {
    testSingleLineQuery(
      """
        |select array(1, 2, 3) as arr union all
        |select array(2, 3, 4) as arr
        |""".stripMargin,
      "select arr from tbl")
  }

  test("native reader - read STRUCT of ARRAY fields") {
    testSingleLineQuery(
      """
        |select named_struct('col', arr) c0 from
        |(
        |  select array(1, 2, 3) as arr union all
        |  select array(2, 3, 4) as arr
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read ARRAY of ARRAY fields") {
    testSingleLineQuery(
      """
        |select array(arr0, arr1) c0 from
        |(
        |  select array(1, 2, 3) as arr0, array(2, 3, 4) as arr1
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read ARRAY of STRUCT fields") {
    testSingleLineQuery(
      """
        |select array(str0, str1) c0 from
        |(
        |  select named_struct('a', 1, 'b', 'n') str0, named_struct('a', 2, 'b', 'w') str1
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read STRUCT of STRUCT fields") {
    testSingleLineQuery(
      """
        |select named_struct('a', str0, 'b', str1) c0 from
        |(
        |  select named_struct('a', 1, 'b', 'n') str0, named_struct('c', 2, 'd', 'w') str1
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read STRUCT of ARRAY of STRUCT fields") {
    testSingleLineQuery(
      """
        |select named_struct('a', array(str0, str1), 'b', array(str2, str3)) c0 from
        |(
        |  select named_struct('a', 1, 'b', 'n') str0,
        |         named_struct('a', 2, 'b', 'w') str1,
        |         named_struct('x', 3, 'y', 'a') str2,
        |         named_struct('x', 4, 'y', 'c') str3
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read ARRAY of STRUCT of ARRAY fields") {
    testSingleLineQuery(
      """
        |select array(named_struct('a', a0, 'b', a1), named_struct('a', a2, 'b', a3)) c0 from
        |(
        |  select array(1, 2, 3) a0,
        |         array(2, 3, 4) a1,
        |         array(3, 4, 5) a2,
        |         array(4, 5, 6) a3
        |)
        |""".stripMargin,
      "select c0 from tbl")
  }
}
