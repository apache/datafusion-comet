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

import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.functions._

/**
 * Reproducers for Delta tests in the full v4.1.0 regression that exercise file
 * names containing literal `%`, whitespace, and other URI-reserved characters.
 *
 * Delta's test mode unconditionally prepends `test%file%prefix-` to every parquet
 * data file (`spark.databricks.delta.testOnly.dataFileNamePrefix`) and many tests
 * use temp directories that contain spaces ("spark test-..."). The combination
 * surfaces URI-encoding bugs anywhere we transform between filesystem paths and
 * Hadoop `Path` / `java.net.URI` representations -- in particular,
 * `input_file_name()` and `_metadata.file_path` must round-trip the literal bytes
 * a SECOND time when downstream code (Delta's MergeIntoCommand) looks the path
 * back up in a map keyed by the AddFile path.
 *
 * The failures in the v4.1.0 regression looked like:
 *
 *   org.apache.spark.sql.delta.DeltaIllegalStateException:
 *     [DELTA_FILE_TO_OVERWRITE_NOT_FOUND] File (file:/.../spark test-...) to
 *     be rewritten not found among candidate files:
 *     file:/.../spark test-.../test%file%prefix-part-00000-...
 *
 * -- i.e. the lookup key (what `input_file_name()` returned) was the table
 * root with no file under it, while the candidates were the actual file paths.
 */
class CometDeltaSpecialCharFilenameSuite extends CometDeltaTestBase {

  test("input_file_name on a file whose name contains literal % matches AddFile.path round-trip") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    // Delta's TEST_FILE_NAME_PREFIX defaults to `test%file%prefix-` when
    // `DeltaUtils.isTesting=true` (Delta autodetects "testing" via classpath
    // sniffing for ScalaTest classes), so the % prefix is on by default in
    // this test JVM. Verify by reading back and checking the file name.
    withDeltaTable("special_chars_pct") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 6).map(i => (i.toLong, s"v_$i")).toDF("id", "v")
        .repartition(1)
        .write.format("delta").save(tablePath)
      // Read files via list -- confirm Delta is in test-prefix mode so the
      // reproducer is actually exercising the bug shape.
      val parquetFiles = new java.io.File(tablePath).listFiles()
        .filter(_.getName.endsWith(".parquet")).map(_.getName)
      assume(
        parquetFiles.exists(_.contains("test%file%prefix-")),
        s"Delta isn't using the `test%file%prefix-` file-name prefix in this JVM " +
          s"(found: ${parquetFiles.mkString(", ")}); cannot exercise the literal-% " +
          "path bug here.")
      val df = spark.read.format("delta").load(tablePath)
        .withColumn("ifn", input_file_name())
        .orderBy("id")
      val rows = df.collect()
      assert(rows.length === 6, s"expected 6 rows, got ${rows.length}")
      val ifn = rows.head.getString(2)
      // What matters for Delta's getTouchedFile lookup is whether the value
      // we hand to input_file_name() ROUND-TRIPS through Delta's
      // DeltaFileOperations.absolutePath to a key that lives in
      // nameToAddFileMap. Replicate that round-trip locally.
      val deltaLog = org.apache.spark.sql.delta.DeltaLog.forTable(spark, tablePath)
      val addFiles = deltaLog.update().allFiles.collect()
      val addFilePaths = addFiles.map(_.path).toSet
      val dataPath = deltaLog.dataPath.toString
      val resolved =
        org.apache.spark.sql.delta.util.DeltaFileOperations
          .absolutePath(dataPath, ifn).toString
      // The lookup map is keyed by the result of `absolutePath(dataPath, addFile.path)`.
      // Build the same expected keys and check our resolved input_file_name is one
      // of them.
      val expectedKeys = addFilePaths
        .map(p => org.apache.spark.sql.delta.util.DeltaFileOperations
          .absolutePath(dataPath, p).toString)
      assert(
        expectedKeys.contains(resolved),
        s"input_file_name() round-trip must produce a key in the AddFile map. " +
          s"input_file_name=$ifn -> absolutePath=$resolved\n" +
          s"AddFile keys=$expectedKeys")
      // input_file_name() makes the native Delta scan decline (it bypasses FileScanRDD,
      // which is what maintains InputFileBlockHolder), so Spark handles it -- and the
      // literal-% path still round-trips correctly through Spark's own reader (above).
      assert(
        collect(df.queryExecution.executedPlan) {
          case s: CometDeltaNativeScanExec => s
        }.isEmpty,
        s"expected fallback (no Comet native scan) for input_file_name():\n" +
          s"${df.queryExecution.executedPlan}")
    }
  }

  test("input_file_name on a path containing a space returns the file path") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    // Build a table path that contains a literal space (matches Delta's
    // withTempDir("spark test-...") test convention).
    val baseDir = java.nio.file.Files.createTempDirectory("comet space test-").toFile
    try {
      val tablePath = new java.io.File(baseDir, "t").getAbsolutePath
      val ss = spark
      import ss.implicits._
      (0 until 4).map(i => (i.toLong, s"v_$i")).toDF("id", "v")
        .repartition(1)
        .write.format("delta").save(tablePath)
      val df = spark.read.format("delta").load(tablePath)
        .withColumn("ifn", input_file_name())
        .orderBy("id")
      val rows = df.collect()
      assert(rows.length === 4)
      val ifn = rows.head.getString(2)
      assert(ifn.endsWith(".parquet"),
        s"input_file_name on a path with a space should be the full file path, got: $ifn")
      // The reported path should round-trip back to the SAME file (whether URI- or
      // plain-encoded). Compare by Hadoop Path normalization rather than string eq.
      val reported = new org.apache.hadoop.fs.Path(ifn)
      assert(reported.getName.endsWith(".parquet"),
        s"reported path's filename should be the parquet file, got: ${reported.getName}")
    } finally {
      def del(f: java.io.File): Unit = {
        if (f.isDirectory) f.listFiles().foreach(del)
        f.delete()
      }
      del(baseDir)
    }
  }

  test("MERGE INTO on a table whose files have literal % in their name") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("merge_pct") { tablePath =>
      val ss = spark
      import ss.implicits._
      // Target: 5 rows with id 0..4
      (0 until 5).map(i => (i.toLong, s"old_$i")).toDF("id", "v")
        .repartition(1)
        .write.format("delta").save(tablePath)
      // Source: rows id 1..3 with new values + new row id=5
      val src = Seq((1L, "new_1"), (2L, "new_2"), (3L, "new_3"), (5L, "new_5"))
        .toDF("id", "v")
      src.createOrReplaceTempView("merge_pct_src")
      // MERGE updates 1..3 and inserts 5. This is the failing pattern from the
      // Delta v4.1.0 regression run -- Delta's getTouchedFile() looks up the
      // result of input_file_name() in nameToAddFileMap; if the round-trip
      // mangles the literal `%` in `test%file%prefix-`, the lookup fails with
      // DELTA_FILE_TO_OVERWRITE_NOT_FOUND.
      spark.sql(
        s"""MERGE INTO delta.`$tablePath` AS t
            USING merge_pct_src AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.v = s.v
            WHEN NOT MATCHED THEN INSERT *""")
      val after = spark.read.format("delta").load(tablePath)
        .orderBy("id").collect().toSeq.map(r => (r.getLong(0), r.getString(1)))
      assert(after === Seq(
        (0L, "old_0"), (1L, "new_1"), (2L, "new_2"), (3L, "new_3"),
        (4L, "old_4"), (5L, "new_5")),
        s"MERGE produced unexpected rows: $after")
    }
  }

  test("Delta scan falls back to Spark when input_file_name is referenced") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("ifn_diag") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 3).map(i => (i.toLong, s"a_$i")).toDF("id", "v")
        .write.format("delta").save(tablePath)
      (3 until 6).map(i => (i.toLong, s"b_$i")).toDF("id", "v")
        .write.format("delta").mode("append").save(tablePath)

      val df = spark.read.format("delta").load(tablePath)
        .withColumn("ifn", input_file_name())
        .orderBy("id")
      df.collect() // materialise plan
      val plan = df.queryExecution.executedPlan
      // input_file_name() reads InputFileBlockHolder, which only FileScanRDD maintains.
      // The native Delta scan bypasses FileScanRDD, so it declines and Spark handles the
      // scan (mirrors CometScanRule's native-DataFusion gate). Verify no Comet scan engaged.
      val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
      assert(scans.isEmpty, s"expected fallback (no CometDeltaNativeScanExec):\n$plan")
    }
  }

  test("input_file_name returns real file path when target has MULTIPLE files (Delta MERGE shape)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("ifn_multi") { tablePath =>
      val ss = spark
      import ss.implicits._
      // Build a table with multiple data files (mimics Delta MERGE's target shape).
      (0 until 3).map(i => (i.toLong, s"a_$i")).toDF("id", "v")
        .write.format("delta").save(tablePath)
      (3 until 6).map(i => (i.toLong, s"b_$i")).toDF("id", "v")
        .write.format("delta").mode("append").save(tablePath)
      (6 until 9).map(i => (i.toLong, s"c_$i")).toDF("id", "v")
        .write.format("delta").mode("append").save(tablePath)

      // Mimic the projection Delta MERGE injects: scan + withColumn(input_file_name()).
      val df = spark.read.format("delta").load(tablePath)
        .withColumn("ifn", input_file_name())
        .orderBy("id")
      val rows = df.collect()
      assert(rows.length === 9)
      // Every row must report a NON-EMPTY input_file_name that ends in `.parquet`.
      // Because the native scan declines on input_file_name(), Spark's own FileScanRDD
      // reader handles it and attributes each row to its source file correctly.
      rows.foreach { r =>
        val ifn = r.getString(2)
        assert(ifn != null && ifn.nonEmpty,
          s"row id=${r.getLong(0)}: input_file_name was empty (would trigger " +
            "DELTA_FILE_TO_OVERWRITE_NOT_FOUND in MERGE)")
        assert(ifn.endsWith(".parquet"),
          s"row id=${r.getLong(0)}: input_file_name should end in .parquet, got: $ifn")
      }
      // All rows from the same file should report the same path -- spot-check by
      // grouping and asserting >=3 distinct paths (one per write).
      val distinct = rows.map(_.getString(2)).toSet
      assert(distinct.size >= 3,
        s"expected at least 3 distinct file paths, got ${distinct.size}: $distinct")
    }
  }

  test("MERGE INTO when target has MULTIPLE files") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("merge_multi") { tablePath =>
      val ss = spark
      import ss.implicits._
      // Three separate writes -> three files. MERGE's findTouchedFiles references
      // input_file_name(), so the native Delta scan declines and Spark's reader handles
      // it -- which attributes each row to its source file, so the MERGE resolves the
      // touched files correctly across all three.
      (0 until 3).map(i => (i.toLong, s"a_$i")).toDF("id", "v")
        .write.format("delta").save(tablePath)
      (3 until 6).map(i => (i.toLong, s"b_$i")).toDF("id", "v")
        .write.format("delta").mode("append").save(tablePath)
      (6 until 9).map(i => (i.toLong, s"c_$i")).toDF("id", "v")
        .write.format("delta").mode("append").save(tablePath)
      // Source updates one row in each underlying file.
      Seq((0L, "u_0"), (4L, "u_4"), (7L, "u_7"))
        .toDF("id", "v").createOrReplaceTempView("merge_multi_src")
      spark.sql(
        s"""MERGE INTO delta.`$tablePath` AS t
            USING merge_multi_src AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.v = s.v""")
      val after = spark.read.format("delta").load(tablePath)
        .orderBy("id").collect().toSeq.map(r => (r.getLong(0), r.getString(1)))
      assert(after === Seq(
        (0L, "u_0"), (1L, "a_1"), (2L, "a_2"),
        (3L, "b_3"), (4L, "u_4"), (5L, "b_5"),
        (6L, "c_6"), (7L, "u_7"), (8L, "c_8")),
        s"MERGE across multiple files produced unexpected rows: $after")
    }
  }
}
