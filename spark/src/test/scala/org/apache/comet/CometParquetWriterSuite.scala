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

import org.apache.spark.sql.CometTestBase

class CometParquetWriterSuite extends CometTestBase {
  import testImplicits._

  test("basic parquet write") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = new File(inputDir, "input.parquet").getAbsolutePath
        val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
        df.write.parquet(inputPath)

        withSQLConf(
          CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
          CometConf.COMET_EXPLAIN_NATIVE_ENABLED.key -> "true") {
          val df = spark.read.parquet(inputPath)

          // perform native write
          df.write.parquet(outputPath)

          assert(spark.read.parquet(outputPath).count() == 3)
        }
      }
    }
  }

}
