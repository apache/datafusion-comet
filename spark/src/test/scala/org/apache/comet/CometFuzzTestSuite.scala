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

import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometFuzzTestSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    Seq("native_comet", "native_datafusion", "native_iceberg_compat").foreach { scanImpl =>
      super.test(testName + s" ($scanImpl)", testTags: _*) {
        withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> scanImpl) {
          testFun
        }
      }
    }
  }

  // TODO get system temp dir
  val path = new Path(s"/tmp/CometFuzzTestSuite_${System.currentTimeMillis()}.parquet")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val filename = path.toString
    val random = new Random(42)
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val options = DataGenOptions(generateArray = true, generateStruct = true)
      ParquetGenerator.makeParquetFile(random, spark, filename, 10000, options)
    }
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    // TODO delete test file(s)
  }

  test("aggregate group by single column") {
    val df = spark.read.parquet(path.toString)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      checkSparkAnswer(s"SELECT $col, count(*) FROM t1 GROUP BY $col ORDER BY $col")
    }
  }

}
