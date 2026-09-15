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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CometTestBase, Observation, SaveMode}
import org.apache.spark.sql.functions.{count, lit}

import org.apache.comet.CometConf

class CometCollectMetricsExecSuite extends CometTestBase {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.COMET_EXEC_COLLECT_METRICS_ENABLED.key -> "false",
        "spark.comet.explain.fallback.enabled" -> "true",
        "spark.comet.explain.fallback.log.enabled" -> "true") {
        testFun
      }
    }
  }

  test("collect row count metrics") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val rowCount = 1000
      makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = false, rowCount)
      readParquetFile(path.toString) { df =>
        val observation = new Observation()
        df.observe(observation, count(lit(1)).as("row_count"))
          .write
          .mode(SaveMode.Overwrite)
          .format("noop")
          .save()
        println(observation.get)
      }
    }
  }
}
