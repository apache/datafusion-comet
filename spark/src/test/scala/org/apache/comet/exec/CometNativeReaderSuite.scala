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
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "false",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION) {
        testFun
      }
    }
  }

  test("native reader - read simple struct fields") {
    testSingleLineQuery(
      """
        |select named_struct('firstName', 'John', 'lastName', 'Doe', 'age', 35) as personal_info union all
        |select named_struct('firstName', 'Jane', 'lastName', 'Doe', 'age', 40) as personal_info
        |""".stripMargin,
      "select personal_info.* from tbl")
  }

  test("native reader - read simple array fields") {
    testSingleLineQuery(
      """
        |select array(1, 2, 3) as arr union all
        |select array(2, 3, 4) as arr
        |""".stripMargin,
      "select arr from tbl")
  }
}
