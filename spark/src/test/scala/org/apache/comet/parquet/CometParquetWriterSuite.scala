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

package org.apache.comet.parquet

import java.io.File

import org.apache.spark.sql.CometTestBase

import org.apache.comet.CometConf

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

        withSQLConf(CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true") {
          val df = spark.read.parquet(inputPath)

          // perform native write
          df.write.parquet(outputPath)

          // Verify the data was written correctly
          val resultDf = spark.read.parquet(outputPath)
          assert(resultDf.count() == 3, "Expected 3 rows to be written")

          // Verify correct data
          // TODO native parquet writer loses column names
          val rows = resultDf.orderBy("col_0").collect()
          assert(rows.length == 3)
          assert(rows(0).getInt(0) == 1 && rows(0).getString(1) == "a")
          assert(rows(1).getInt(0) == 2 && rows(1).getString(1) == "b")
          assert(rows(2).getInt(0) == 3 && rows(2).getString(1) == "c")

          // Verify multiple part files were created
          val outputDir = new File(outputPath)
          val partFiles = outputDir.listFiles().filter(_.getName.startsWith("part-"))
          // With 3 rows and default parallelism, we should get multiple partitions
          assert(partFiles.length > 0, "Expected at least one part file to be created")
        }
      }
    }
  }

}
