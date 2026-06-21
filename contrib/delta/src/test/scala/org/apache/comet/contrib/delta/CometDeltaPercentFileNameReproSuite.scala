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

package org.apache.comet.contrib.delta

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}

// Coverage that the kernel-read correctly handles a literal '%' in data-file names (and spaces in
// the table path). Delta's own-suite prefixes every data file with `test%file%prefix-`
// (DeltaSQLConf.TEST_FILE_NAME_PREFIX, default in testing), so its add.path is stored URI-encoded as
// `test%25file%25prefix-part-...`. This was originally suspected to be the root cause of a large
// `[FAILED_READ_FILE.NO_HINT]` own-suite family, but the '%' turned out to be INCIDENTAL: the
// kernel-read resolves '%' paths fine (`to_file_path` decodes `%25`->`%`). The real bug behind that
// family was nested-struct schema evolution (see task #74 / B9 and the ALTER-ADD test in
// CometDeltaNestedArrayStructReproSuite). These tests therefore PASS and stand as regression
// coverage that '%'/space paths + MERGE schema evolution read correctly through the kernel-read.
class CometDeltaPercentFileNameReproSuite extends CometDeltaTestBase {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    // Static Delta conf: prefix all data files with a name containing a literal '%'.
    conf.set("spark.databricks.delta.testOnly.dataFileNamePrefix", "test%file%prefix-")
    conf
  }

  test("read Delta table under a path with a space and % in data-file names") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    // Faithful to Delta's own-suite layout: Utils.createTempDir() yields `/T/spark test-<uuid>/`
    // (a literal SPACE in the directory), and the data files carry the `test%file%prefix-` prefix
    // (a literal '%'). The failing own-suite reads were all under such a path; a plain table whose
    // dir has no space reads fine, so the space (encoded %20) in the table root is the trigger.
    val parent = java.nio.file.Files.createTempDirectory("comet delta pct").toFile
    try {
      val tablePath = new java.io.File(parent, "spark test-tbl").getAbsolutePath
      spark.range(10).toDF("id").write.format("delta").save(tablePath)
      val dataFiles = new java.io.File(tablePath)
        .listFiles()
        .filter(f => f.getName.endsWith(".parquet"))
        .map(_.getName)
      assert(
        dataFiles.exists(_.contains("%")),
        s"expected a data file name containing '%', got: ${dataFiles.toSeq}")
      assert(tablePath.contains(" "), s"table path should contain a space: $tablePath")
      checkSparkAnswerAndOperator(spark.read.format("delta").load(tablePath))
    } finally {
      deleteRecursively(parent)
    }
  }

  // Faithful combination of the failing own-suite tests: a schema-evolution MERGE over an
  // array<struct> column, with data files under a space-containing dir AND the '%' file prefix.
  // (Plain reads of % / space paths pass; the MERGE/evolution read path is what trips it.)
  test("MERGE array<struct> schema evolution under space + % path") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    import testImplicits._

    val targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(new StructType().add("a", IntegerType).add("b", IntegerType)))
    val sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(new StructType().add("b", IntegerType).add("a", IntegerType)))

    val parent = java.nio.file.Files.createTempDirectory("comet delta pct").toFile
    try {
      val tablePath = new java.io.File(parent, "spark test-tbl").getAbsolutePath
      spark.read
        .schema(targetSchema)
        .json(Seq("""{ "key": "A", "value": [ { "a": 1, "b": 2 } ] }""").toDS)
        .write
        .format("delta")
        .save(tablePath)

      spark.read
        .schema(sourceSchema)
        .json(
          Seq(
            """{ "key": "A", "value": [ { "b": 4, "a": 3 } ] }""",
            """{ "key": "B", "value": [ { "b": 2, "a": 5 } ] }""").toDS)
        .createOrReplaceTempView("merge_src_pct")

      withSQLConf("spark.databricks.delta.schema.autoMerge.enabled" -> "true") {
        spark.sql(s"""
          MERGE INTO delta.`$tablePath` t
          USING merge_src_pct s
          ON t.key = s.key
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *
        """)
      }
      checkSparkAnswerAndOperator(
        spark.read.format("delta").load(tablePath).orderBy("key"))
    } finally {
      deleteRecursively(parent)
    }
  }
}
