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

  test("native reader case sensitivity") {
    withTempPath { path =>
      spark.range(10).toDF("a").write.parquet(path.toString)
      Seq(true, false).foreach { caseSensitive =>
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
          val tbl = s"case_sensitivity_${caseSensitive}_${System.currentTimeMillis()}"
          sql(s"create table $tbl (A long) using parquet options (path '" + path + "')")
          val df = sql(s"select A from $tbl")
          checkSparkAnswer(df)
        }
      }
    }
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

  test("native reader - read simple MAP fields") {
    testSingleLineQuery(
      """
        |select map('a', 1) as c0 union all
        |select map('b', 2)
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read MAP of value ARRAY fields") {
    testSingleLineQuery(
      """
        |select map('a', array(1), 'c', array(3)) as c0 union all
        |select map('b', array(2))
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read MAP of value STRUCT fields") {
    testSingleLineQuery(
      """
        |select map('a', named_struct('f0', 0, 'f1', 'foo'), 'b', named_struct('f0', 1, 'f1', 'bar')) as c0 union all
        |select map('c', named_struct('f2', 0, 'f1', 'baz')) as c0
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read MAP of value MAP fields") {
    testSingleLineQuery(
      """
        |select map('a', map('a1', 1, 'b1', 2), 'b', map('a2', 2, 'b2', 3)) as c0 union all
        |select map('c', map('a3', 3, 'b3', 4))
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read STRUCT of MAP fields") {
    testSingleLineQuery(
      """
        |select named_struct('m0', map('a', 1)) as c0 union all
        |select named_struct('m1', map('b', 2))
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read ARRAY of MAP fields") {
    testSingleLineQuery(
      """
        |select array(map('a', 1), map('b', 2)) as c0 union all
        |select array(map('c', 3))
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read ARRAY of MAP of ARRAY value fields") {
    testSingleLineQuery(
      """
        |select array(map('a', array(1, 2, 3), 'b', array(2, 3, 4)), map('c', array(4, 5, 6), 'd', array(7, 8, 9))) as c0 union all
        |select array(map('x', array(1, 2, 3), 'y', array(2, 3, 4)), map('c', array(4, 5, 6), 'z', array(7, 8, 9)))
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read STRUCT of MAP of STRUCT value fields") {
    testSingleLineQuery(
      """
        |select named_struct('m0', map('a', named_struct('f0', 1)), 'm1', map('b', named_struct('f1', 1))) as c0 union all
        |select named_struct('m0', map('c', named_struct('f2', 1)), 'm1', map('d', named_struct('f3', 1))) as c0
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read MAP of ARRAY of MAP fields") {
    testSingleLineQuery(
      """
        |select map('a', array(map(1, 'a', 2, 'b'), map(1, 'a', 2, 'b'))) as c0 union all
        |select map('b', array(map(1, 'a', 2, 'b'), map(1, 'a', 2, 'b'))) as c0
        |""".stripMargin,
      "select c0 from tbl")
  }

  test("native reader - read MAP of STRUCT of MAP fields") {
    testSingleLineQuery(
      """
        |select map('a', named_struct('f0', map(1, 'b')), 'b', named_struct('f0', map(1, 'b'))) as c0 union all
        |select map('c', named_struct('f0', map(1, 'b'))) as c0
        |""".stripMargin,
      "select c0 from tbl")
  }
  test("native reader - read a STRUCT subfield from ARRAY of STRUCTS - second field") {
    testSingleLineQuery(
      """
        | select array(str0, str1) c0 from
        | (
        |   select
        |     named_struct('a', 1, 'b', 'n', 'c', 'x') str0,
        |     named_struct('a', 2, 'b', 'w', 'c', 'y') str1
        | )
        |""".stripMargin,
      "select c0[0].b col0 from tbl")
  }

  test("native reader - read a STRUCT subfield - field from second") {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "false",
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion") {
      testSingleLineQuery(
        """
          |select 1 a, named_struct('a', 1, 'b', 'n') c0
          |""".stripMargin,
        "select c0.b from tbl")
    }
  }

  test("native reader - read a STRUCT subfield from ARRAY of STRUCTS - field from first") {
    testSingleLineQuery(
      """
        | select array(str0, str1) c0 from
        | (
        |   select
        |     named_struct('a', 1, 'b', 'n', 'c', 'x') str0,
        |     named_struct('a', 2, 'b', 'w', 'c', 'y') str1
        | )
        |""".stripMargin,
      "select c0[0].a, c0[0].b, c0[0].c from tbl")
  }

  test("native reader - read a STRUCT subfield from ARRAY of STRUCTS - reverse fields") {
    testSingleLineQuery(
      """
        | select array(str0, str1) c0 from
        | (
        |   select
        |     named_struct('a', 1, 'b', 'n', 'c', 'x') str0,
        |     named_struct('a', 2, 'b', 'w', 'c', 'y') str1
        | )
        |""".stripMargin,
      "select c0[0].c, c0[0].b, c0[0].a from tbl")
  }

  test("native reader - read a STRUCT subfield from ARRAY of STRUCTS - skip field") {
    testSingleLineQuery(
      """
        | select array(str0, str1) c0 from
        | (
        |   select
        |     named_struct('a', 1, 'b', 'n', 'c', 'x') str0,
        |     named_struct('a', 2, 'b', 'w', 'c', 'y') str1
        | )
        |""".stripMargin,
      "select c0[0].a, c0[0].c from tbl")
  }

  test("native reader - read a STRUCT subfield from ARRAY of STRUCTS - duplicate first field") {
    testSingleLineQuery(
      """
        | select array(str0, str1) c0 from
        | (
        |   select
        |     named_struct('a', 1, 'b', 'n', 'c', 'x') str0,
        |     named_struct('a', 2, 'b', 'w', 'c', 'y') str1
        | )
        |""".stripMargin,
      "select c0[0].a, c0[0].a from tbl")
  }
}
