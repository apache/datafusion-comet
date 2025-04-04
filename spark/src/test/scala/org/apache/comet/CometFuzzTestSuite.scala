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

import java.io.File

import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometFuzzTestSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private var filename: String = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tempDir = System.getProperty("java.io.tmpdir")
    filename = s"$tempDir/CometFuzzTestSuite_${System.currentTimeMillis()}.parquet"
    val random = new Random(42)
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val options =
        DataGenOptions(generateArray = true, generateStruct = true, generateNegativeZero = false)
      ParquetGenerator.makeParquetFile(random, spark, filename, 1000, options)
    }
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteDirectory(new File(filename))
  }

  test("select *") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    checkSparkAnswer("SELECT * FROM t1")
  }

  test("select * with limit") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    checkSparkAnswer("SELECT * FROM t1 LIMIT 500")
  }

  test("order by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      checkSparkAnswer(s"SELECT $col FROM t1 ORDER BY $col")
    }
  }

  test("order by multiple columns") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    val allCols = df.columns.mkString(",")
    checkSparkAnswer(s"SELECT $allCols FROM t1 ORDER BY $allCols")
  }

  test("aggregate group by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      checkSparkAnswer(s"SELECT $col, count(*) FROM t1 GROUP BY $col ORDER BY $col")
    }
  }

  test("min/max aggregate") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      checkSparkAnswer(s"SELECT min($col), max($col) FROM t1")
    }
  }

  test("join") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    df.createOrReplaceTempView("t2")
    for (col <- df.columns) {
      checkSparkAnswer(s"SELECT count(*) FROM t1 JOIN t2 ON t1.$col = t2.$col")
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    Seq("native", "jvm").foreach { shuffleMode =>
      Seq("native_comet", "native_datafusion", "native_iceberg_compat").foreach { scanImpl =>
        super.test(testName + s" ($scanImpl, $shuffleMode shuffle)", testTags: _*) {
          withSQLConf(
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> scanImpl,
            CometConf.COMET_SHUFFLE_MODE.key -> shuffleMode) {
            testFun
          }
        }
      }
    }
  }

}
