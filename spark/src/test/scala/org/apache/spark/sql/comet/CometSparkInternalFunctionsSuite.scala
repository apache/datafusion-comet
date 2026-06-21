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

package org.apache.spark.sql.comet

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.Empty2Null
import org.apache.spark.sql.classic.ExpressionUtils
import org.apache.spark.sql.functions._

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

class CometSparkInternalFunctionsSuite extends CometTestBase {

  test("empty2null is offloaded to Comet") {
    assume(isSpark40Plus)
    withParquetTable(Seq("", "a", null, "b").map(Tuple1(_)), "tbl") {
      val df = sql("select _1 from tbl")
        .select(ExpressionUtils.column(Empty2Null(ExpressionUtils.expression(col("_1")))).as("p"))
      checkSparkAnswerAndOperator(df)
    }
  }
}
