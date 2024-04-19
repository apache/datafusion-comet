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

import org.apache.comet.CometConf

/**
 * This test suite contains tests for only Spark 3.4.
 */
class CometExec3_4Suite extends CometTestBase {
  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  // Dataset.offset API is not available before Spark 3.4
  test("offset") {
    withSQLConf(CometConf.COMET_COLUMNAR_SHUFFLE_ENABLED.key -> "true") {
      checkSparkAnswer(testData.offset(90))
      checkSparkAnswer(arrayData.toDF().offset(99))
      checkSparkAnswer(mapData.toDF().offset(99))
    }
  }
}
