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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class CometBitwiseExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("bitwise expressions") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col1 int, col2 int) using parquet")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(3333, 4)")
          sql(s"insert into $table values(5555, 6)")

          checkSparkAnswerAndOperator(
            s"SELECT col1 & col2,  col1 | col2, col1 ^ col2 FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT col1 & 1234,  col1 | 1234, col1 ^ 1234 FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT shiftright(col1, 2), shiftright(col1, col2) FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT shiftleft(col1, 2), shiftleft(col1, col2) FROM $table")
          checkSparkAnswerAndOperator(s"SELECT ~(11), ~col1, ~col2 FROM $table")
        }
      }
    }
  }

  test("bitwise shift with different left/right types") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col1 long, col2 int) using parquet")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(3333, 4)")
          sql(s"insert into $table values(5555, 6)")

          checkSparkAnswerAndOperator(
            s"SELECT shiftright(col1, 2), shiftright(col1, col2) FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT shiftleft(col1, 2), shiftleft(col1, col2) FROM $table")
        }
      }
    }
  }
}
