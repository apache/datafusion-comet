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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometBitmapExpressionSuite extends CometTestBase {

  test("bitmap_count") {
    assume(isSpark35Plus)
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        ParquetGenerator.makeParquetFile(
          random,
          spark,
          filename,
          100,
          DataGenOptions(
            allowNull = true,
            generateNegativeZero = true,
            generateArray = false,
            generateStruct = false,
            generateMap = false))
      }
      val table = spark.read.parquet(filename)
      table.createOrReplaceTempView("t1")
      val df = sql("SELECT bitmap_count(c13) FROM t1")
      checkSparkAnswerAndOperator(df)
    }
  }
}
