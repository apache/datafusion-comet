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
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometJoinSuite extends CometTestBase {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  // TODO: Add a test for SortMergeJoin with join filter after new DataFusion release
  test("SortMergeJoin without join filter") {
    withSQLConf(
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withParquetTable((0 until 10).map(i => (i, i % 5)), "tbl_a") {
        withParquetTable((0 until 10).map(i => (i % 10, i + 2)), "tbl_b") {
          val df1 = sql("SELECT * FROM tbl_a JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df1)

          val df2 = sql("SELECT * FROM tbl_a LEFT JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df2)

          val df3 = sql("SELECT * FROM tbl_b LEFT JOIN tbl_a ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df3)

          val df4 = sql("SELECT * FROM tbl_a RIGHT JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df4)

          val df5 = sql("SELECT * FROM tbl_b RIGHT JOIN tbl_a ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df5)

          val df6 = sql("SELECT * FROM tbl_a FULL JOIN tbl_b ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df6)

          val df7 = sql("SELECT * FROM tbl_b FULL JOIN tbl_a ON tbl_a._2 = tbl_b._1")
          checkSparkAnswerAndOperator(df7)

          val left = sql("SELECT * FROM tbl_a")
          val right = sql("SELECT * FROM tbl_b")

          val df8 = left.join(right, left("_2") === right("_1"), "leftsemi")
          checkSparkAnswerAndOperator(df8)

          val df9 = right.join(left, left("_2") === right("_1"), "leftsemi")
          checkSparkAnswerAndOperator(df9)

          val df10 = left.join(right, left("_2") === right("_1"), "leftanti")
          checkSparkAnswerAndOperator(df10)

          val df11 = right.join(left, left("_2") === right("_1"), "leftanti")
          checkSparkAnswerAndOperator(df11)
        }
      }
    }
  }
}
