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
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

class CometStringDecodeFrameworkSuite extends CometTestBase {

  private def countSparkProjectExec(plan: SparkPlan): Int =
    plan.collect { case _: ProjectExec => true }.length

  test("StringDecode honors spark.comet.expression.StringDecode.enabled") {
    assume(
      !isSpark40Plus,
      "Spark 4.0+ rewrites decode() to StaticInvoke; that path is intentionally not " +
        "registered through the framework (see issue #4077).")
    withParquetTable(Seq(("hello".getBytes, 0)), "tbl") {
      val query = "select decode(_1, 'utf-8') from tbl"
      val (_, cometPlan) = checkSparkAnswerAndOperator(query)
      assert(0 == countSparkProjectExec(cometPlan))

      withSQLConf(CometConf.getExprEnabledConfigKey("StringDecode") -> "false") {
        val (_, cometPlan2) = checkSparkAnswerAndFallbackReason(
          query,
          "Expression support is disabled. Set " +
            "spark.comet.expression.StringDecode.enabled=true to enable it.")
        assert(1 == countSparkProjectExec(cometPlan2))
      }
    }
  }
}
