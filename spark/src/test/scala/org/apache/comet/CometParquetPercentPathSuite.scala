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
import org.apache.spark.sql.functions.col

// Core repro for the "literal % in a data-file name" read failure surfaced by the Delta own-suite
// (Delta's tests prefix every data file with `test%file%prefix-`). The failure is NOT in the Delta
// kernel-read (which handles % fine) -- it's in Comet's regular native parquet scan
// (init_datasource_exec / planner.rs), reached when the Delta scan declines and falls back. This
// reproduces it directly with a plain parquet table whose part file is renamed to contain a '%'.
class CometParquetPercentPathSuite extends CometTestBase {

  test("native scan reads a parquet file whose name contains a literal %") {
    withTempDir { dir =>
      val dataDir = new java.io.File(dir, "data")
      spark
        .range(10)
        .select(col("id"), (col("id") * 2).as("v"))
        .repartition(1)
        .write
        .parquet(dataDir.getAbsolutePath)

      val part = dataDir
        .listFiles()
        .find(f => f.getName.startsWith("part-") && f.getName.endsWith(".parquet"))
        .getOrElse(fail("no part file written"))
      val renamed = new java.io.File(dataDir, "test%file%prefix-" + part.getName)
      assert(part.renameTo(renamed), s"rename to '%' name failed: $renamed")

      checkSparkAnswerAndOperator(spark.read.parquet(dataDir.getAbsolutePath))
    }
  }
}
