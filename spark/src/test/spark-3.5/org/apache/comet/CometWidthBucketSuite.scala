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

class CometWidthBucketSuite extends CometTestBase {

  private def countSparkProjectExec(plan: SparkPlan): Int =
    plan.collect { case _: ProjectExec => true }.length

  test("WidthBucket honors spark.comet.expression.WidthBucket.enabled") {
    withParquetTable(Seq((1.5, 0)), "tbl") {
      val sql = "select width_bucket(_1, 0.0, 10.0, 5) from tbl"
      val (_, cometPlan) = checkSparkAnswerAndOperator(sql)
      assert(0 == countSparkProjectExec(cometPlan))

      withSQLConf(CometConf.getExprEnabledConfigKey("WidthBucket") -> "false") {
        val (_, cometPlan2) = checkSparkAnswerAndFallbackReason(
          sql,
          "Expression support is disabled. Set " +
            "spark.comet.expression.WidthBucket.enabled=true to enable it.")
        assert(1 == countSparkProjectExec(cometPlan2))
      }
    }
  }
}
