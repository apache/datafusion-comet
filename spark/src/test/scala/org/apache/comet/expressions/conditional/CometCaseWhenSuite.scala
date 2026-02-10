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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.CometConf

class CometCaseWhenSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_AUTO) {
        testFun
      }
    }
  }

  test("case_when") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(id int) using parquet")
          sql(s"insert into $table values(1), (NULL), (2), (2), (3), (3), (4), (5), (NULL)")
          checkSparkAnswerAndOperator(
            s"SELECT CASE WHEN id > 2 THEN 3333 WHEN id > 1 THEN 2222 ELSE 1111 END FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT CASE WHEN id > 2 THEN NULL WHEN id > 1 THEN 2222 ELSE 1111 END FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT CASE id WHEN 1 THEN 1111 WHEN 2 THEN 2222 ELSE 3333 END FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT CASE id WHEN 1 THEN 1111 WHEN 2 THEN 2222 ELSE NULL END FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT CASE id WHEN 1 THEN 1111 WHEN 2 THEN 2222 WHEN 3 THEN 3333 WHEN 4 THEN 4444 END FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT CASE id WHEN NULL THEN 0 WHEN 1 THEN 1111 WHEN 2 THEN 2222 ELSE 3333 END FROM $table")
        }
      }
    }
  }

}
