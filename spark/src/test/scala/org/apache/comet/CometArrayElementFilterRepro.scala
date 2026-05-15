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

import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase

/**
 * Minimal reproduction for an apparent Comet-core expression-evaluator issue where native
 * execution of `array_element > integer_literal` (and `concat(string, string) > 'str'`) on a
 * renamed / column-projected input produces: Invalid comparison operation: Utf8 <= Int32
 *
 * No Delta, no Iceberg, no column mapping. Plain Parquet file written by Spark and read through
 * Comet's native scan path. Exercises filter + shuffle (group-by) so the expression is evaluated
 * post-scan in Comet's native pipeline.
 *
 * If any of these cases fail on an unpatched main, the bug is general Comet-core, not a
 * Delta-integration artifact.
 */
class CometArrayElementFilterRepro extends CometTestBase {

  private def rmTree(f: java.io.File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(rmTree)
    f.delete()
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    conf.set("spark.comet.exec.shuffle.enabled", "true")
    conf
  }

  test("REPRO: filter + shuffle over array element on plain parquet") {
    val dir = Files.createTempDirectory("comet-arr-repro").toFile
    val path = new java.io.File(dir, "t.parquet").getAbsolutePath
    try {
      val ss = spark
      import ss.implicits._

      // Plain Spark write -- no Delta, no Iceberg, no column mapping.
      (1 to 10)
        .map(i => (i, Array(i, i + 10, i + 100)))
        .toDF("id", "arr")
        .repartition(2)
        .write
        .mode("overwrite")
        .parquet(path)

      // SELECT id, count(*) FROM parquet.`$path` WHERE arr[0] > 0 GROUP BY id
      // The group-by forces a shuffle so any post-scan native operators (filter, project,
      // partial aggregate) have to evaluate `arr[0] > 0` against Comet's batch schema.
      val df = spark.read
        .parquet(path)
        .where("arr[0] > 0")
        .groupBy("id")
        .count()

      // Trigger execution.
      val rows = df.collect()
      assert(rows.length == 10, s"expected 10 groups, got ${rows.length}")
    } finally {
      rmTree(dir)
    }
  }

  test("REPRO: filter + shuffle over string concat on plain parquet") {
    val dir = Files.createTempDirectory("comet-concat-repro").toFile
    val path = new java.io.File(dir, "t.parquet").getAbsolutePath
    try {
      val ss = spark
      import ss.implicits._

      (1 to 10)
        .map(i => (i, s"str$i"))
        .toDF("id", "a")
        .repartition(2)
        .write
        .mode("overwrite")
        .parquet(path)

      // concat(a, a) > 'str'  triggers the same code path as the DeltaColumnRenameSuite
      // "rename with constraints" failure.
      val df = spark.read
        .parquet(path)
        .where("concat(a, a) > 'str'")
        .groupBy("id")
        .count()

      val rows = df.collect()
      assert(rows.length == 10, s"expected 10 groups, got ${rows.length}")
    } finally {
      rmTree(dir)
    }
  }

  test("REPRO: filter + shuffle over array element with intermediate projection") {
    // Variant that more closely mirrors Delta's column-mapping path: we insert a projection
    // before the filter, which is the pattern that Delta's `PreprocessTableWithDVs` and
    // Comet's own rename projection produce after a logical->physical alias.
    val dir = Files.createTempDirectory("comet-arr-proj-repro").toFile
    val path = new java.io.File(dir, "t.parquet").getAbsolutePath
    try {
      val ss = spark
      import ss.implicits._

      (1 to 10)
        .map(i => (i, Array(i, i + 10, i + 100)))
        .toDF("id", "arr_physical") // parquet has this name
        .repartition(2)
        .write
        .mode("overwrite")
        .parquet(path)

      // Read with an alias to simulate a rename projection above the scan.
      val df = spark.read
        .parquet(path)
        .selectExpr("id", "arr_physical AS arr")
        .where("arr[0] > 0")
        .groupBy("id")
        .count()

      val rows = df.collect()
      assert(rows.length == 10, s"expected 10 groups, got ${rows.length}")
    } finally {
      rmTree(dir)
    }
  }
}
