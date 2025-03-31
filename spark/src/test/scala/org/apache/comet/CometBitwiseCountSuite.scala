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

class CometBitwiseCountSuite extends CometTestBase {

  test("bitwise_count") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "bitwise_count_test"
        withTable(table) {
          sql(s"create table $table(col1 long, col2 int, col3 short, col4 byte) using parquet")
          sql(s"insert into $table values(1111, 2222, 17, 7)")

          checkSparkAnswerAndOperator(sql(s"SELECT bit_count(col1) FROM $table"))
          checkSparkAnswerAndOperator(sql(s"SELECT bit_count(col2) FROM $table"))
          checkSparkAnswerAndOperator(sql(s"SELECT bit_count(col3) FROM $table"))
          checkSparkAnswerAndOperator(sql(s"SELECT bit_count(col4) FROM $table"))
          // checkSparkAnswerAndOperator(sql(s"SELECT bit_count(col5) FROM $table"))
        }
      }
    }
  }
}
