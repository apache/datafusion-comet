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
import org.apache.spark.sql.internal.SQLConf

class Test extends CometTestBase {

  test("divide by zero (ANSI disable)") {
    // Enabling ANSI will cause native engine failure, but as we cannot catch
    // native error now, we cannot test it here.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withParquetTable(Seq((1, 0, 1.0, 0.0, -0.0)), "tbl") {
        checkSparkAnswerAndOperator("SELECT _1 / _2, _3 / _4, _3 / _5 FROM tbl")
        checkSparkAnswerAndOperator("SELECT _1 % _2, _3 % _4, _3 % _5 FROM tbl")
        checkSparkAnswerAndOperator("SELECT _1 / 0, _3 / 0.0, _3 / -0.0 FROM tbl")
        checkSparkAnswerAndOperator("SELECT _1 % 0, _3 % 0.0, _3 % -0.0 FROM tbl")
      }
    }
  }
}
