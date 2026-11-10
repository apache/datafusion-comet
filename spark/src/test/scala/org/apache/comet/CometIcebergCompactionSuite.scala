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
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.CometListenerBusUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageSubmitted}
import org.apache.spark.sql.{CometTestBase, Row}

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.iceberg.compaction.CometRewriteDataFiles

/**
 * End to end tests for `CALL system.rewrite_data_files` with Comet's bin-pack rewrite
 * (`spark.comet.iceberg.compaction.enabled`). Spark runs the procedure while it analyzes the
 * statement, so the tests call it through SQL and look at the jobs it ran. Comet's jobs carry the
 * job group prefix `COMET-REWRITE-DATA-FILES` and Iceberg's `REWRITE-DATA-FILES`.
 */
class CometIcebergCompactionSuite extends CometTestBase with CometIcebergTestBase {

  private val catalog = "test_cat"

  private val SmallFileOptions = "options => map('min-input-files', '2')"

  private def withCompaction(enabled: Boolean = true)(body: => Unit): Unit = {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(isSpark40Plus, "Comet intercepts the CALL statement on Spark 4.0 and later only")
    withHadoopCatalog(catalog) {
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_COMPACTION_ENABLED.key -> enabled.toString)(body)
    }
  }

  // -- Fixtures --------------------------------------------------------------------------

  /** Creates `table` and appends `files` one-file commits of 100 rows each. */
  private def createTable(table: String, files: Int, partitioned: Boolean = false): Unit = {
    val partitioning = if (partitioned) "PARTITIONED BY (part)" else ""
    sql(s"CREATE TABLE $table (id INT, part STRING, value DOUBLE) USING iceberg $partitioning")
    (0 until files).foreach(i => appendFile(table, i, "a"))
  }

  private def appendFile(table: String, index: Int, part: String): Unit =
    spark
      .range(index * 100L, (index + 1) * 100L)
      .selectExpr(
        "CAST(id AS INT) AS id",
        s"'$part' AS part",
        "CAST(id * 1.5 AS DOUBLE) AS value")
      .coalesce(1)
      .write
      .format("iceberg")
      .mode("append")
      .saveAsTable(table)

  /** Creates a table with the partitions `a`, `b` and `c`, each with three small files. */
  private def createThreePartitions(table: String): Unit = {
    createTable(table, files = 0, partitioned = true)
    Seq("a", "b", "c").zipWithIndex.foreach { case (part, p) =>
      (0 until 3).foreach(i => appendFile(table, p * 10 + i, part))
    }
  }

  private def rows(table: String): Seq[Row] =
    sql(s"SELECT * FROM $table ORDER BY id").collect().toSeq

  private def dataFileCount(table: String): Long =
    sql(s"SELECT COUNT(*) FROM $table.data_files").collect().head.getLong(0)

  private def replaceSnapshotCount(table: String): Long =
    sql(s"SELECT COUNT(*) FROM $table.snapshots WHERE operation = 'replace'")
      .collect()
      .head
      .getLong(0)

  private def call(table: String, arguments: String): Row =
    sql(s"CALL $catalog.system.rewrite_data_files(table => '$table', $arguments)")
      .collect()
      .head

  /** The job groups of the jobs that ran while `body` ran. */
  private def jobGroups(body: => Unit): Seq[String] = {
    val groups = new ConcurrentLinkedQueue[String]()
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        val properties = jobStart.properties
        if (properties != null) {
          Option(properties.getProperty("spark.jobGroup.id")).foreach(groups.add)
        }
      }
    }
    CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    spark.sparkContext.addSparkListener(listener)
    try {
      body
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    groups.asScala.toSeq
  }

  /** The number of tasks of each stage that ran while `body` ran. */
  private def stageTaskCounts(body: => Unit): Seq[Int] = {
    val counts = new ConcurrentLinkedQueue[Int]()
    val listener = new SparkListener {
      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        counts.add(stageSubmitted.stageInfo.numTasks)
      }
    }
    CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    spark.sparkContext.addSparkListener(listener)
    try {
      body
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
    counts.asScala.toSeq
  }

  private def cometJobs(groups: Seq[String]): Seq[String] =
    groups.filter(_.startsWith(CometRewriteDataFiles.JobGroupPrefix))

  private def icebergJobs(groups: Seq[String]): Seq[String] =
    groups.filter(_.startsWith("REWRITE-DATA-FILES"))

  /**
   * Comet's native Iceberg writer stamps `(Comet)` into the footer's `created_by`. Reading the
   * footer shows who wrote the file, which the plan alone does not.
   */
  private def assertWrittenByCometNative(table: String): Unit = {
    val paths = sql(s"SELECT file_path FROM $table.data_files").collect().map(_.getString(0))
    assert(paths.nonEmpty)
    paths.foreach { path =>
      val reader = ParquetFileReader.open(
        HadoopInputFile
          .fromPath(new Path(path.stripPrefix("file:")), spark.sparkContext.hadoopConfiguration))
      val createdBy =
        try reader.getFooter.getFileMetaData.getCreatedBy
        finally reader.close()
      assert(
        createdBy != null && createdBy.contains("(Comet)"),
        s"$path was written by $createdBy")
    }
  }

  // -- Tests -----------------------------------------------------------------------------

  test("bin-pack rewrite compacts small files and returns Iceberg's result row") {
    withCompaction() {
      val table = s"$catalog.db.compact_basic"
      try {
        createTable(table, files = 4)
        val before = rows(table)
        assert(dataFileCount(table) == 4)

        var result: Row = null
        val groups = jobGroups {
          result = call("db.compact_basic", SmallFileOptions)
        }

        assert(cometJobs(groups).nonEmpty, s"Comet did not run the rewrite, jobs: $groups")
        assert(icebergJobs(groups).isEmpty, s"Iceberg ran a rewrite job too, jobs: $groups")
        assert(result.length == 5)
        assert(result.getInt(0) == 4, "rewritten_data_files_count")
        assert(result.getInt(1) == 1, "added_data_files_count")
        assert(result.getLong(2) > 0L, "rewritten_bytes_count")
        assert(result.getInt(3) == 0, "failed_data_files_count")
        assert(result.getInt(4) == 0, "removed_delete_files_count")

        assert(dataFileCount(table) == 1)
        assert(rows(table) == before)
        assert(replaceSnapshotCount(table) == 1)
        assertWrittenByCometNative(table)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("the rewrite is left to Iceberg unless Comet's compaction is enabled") {
    withCompaction(enabled = false) {
      val table = s"$catalog.db.compact_disabled"
      try {
        createTable(table, files = 4)
        val groups = jobGroups {
          assert(call("db.compact_disabled", SmallFileOptions).getInt(0) == 4)
        }
        assert(cometJobs(groups).isEmpty)
        assert(icebergJobs(groups).nonEmpty)
        assert(dataFileCount(table) == 1)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("a sort strategy runs Iceberg's procedure") {
    withCompaction() {
      val table = s"$catalog.db.compact_sort"
      try {
        createTable(table, files = 4)
        setIcebergSortOrderAsc(table, "id")
        val before = rows(table)
        val groups = jobGroups {
          call("db.compact_sort", s"strategy => 'sort', $SmallFileOptions")
        }
        assert(cometJobs(groups).isEmpty)
        assert(icebergJobs(groups).nonEmpty)
        assert(rows(table) == before)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("an option Comet does not reproduce runs Iceberg's procedure and its error") {
    withCompaction() {
      val table = s"$catalog.db.compact_bad_option"
      try {
        createTable(table, files = 2)
        val error = intercept[Exception] {
          call("db.compact_bad_option", "options => map('no-such-option', '1')")
        }
        val messages =
          Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).map(_.getMessage)
        assert(messages.exists(m => m != null && m.contains("no-such-option")))
        assert(dataFileCount(table) == 2)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("rewriting an empty table does nothing") {
    withCompaction() {
      val table = s"$catalog.db.compact_empty"
      try {
        sql(s"CREATE TABLE $table (id INT) USING iceberg")
        val groups = jobGroups {
          val result = call("db.compact_empty", SmallFileOptions)
          assert(result == Row(0, 0, 0L, 0, 0))
        }
        assert(groups.isEmpty)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("files already near the target size are left alone") {
    withCompaction() {
      val table = s"$catalog.db.compact_nothing"
      try {
        createTable(table, files = 3)
        // Every file is within [size / 2, size * 4], so none is worth rewriting.
        val size = sql(s"SELECT MIN(file_size_in_bytes) FROM $table.data_files")
          .collect()
          .head
          .getLong(0)
        val result = call(
          "db.compact_nothing",
          s"options => map('min-input-files', '10', 'min-file-size-bytes', '${size / 2}', " +
            s"'target-file-size-bytes', '${size * 2}', 'max-file-size-bytes', '${size * 4}')")
        assert(result == Row(0, 0, 0L, 0, 0))
        assert(dataFileCount(table) == 3)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("where limits the rewrite to the matching partitions") {
    withCompaction() {
      val table = s"$catalog.db.compact_where"
      try {
        createTable(table, files = 0, partitioned = true)
        (0 until 3).foreach(i => appendFile(table, i, "a"))
        (3 until 6).foreach(i => appendFile(table, i, "b"))
        val before = rows(table)

        val result = call("db.compact_where", s"""where => "part = 'a'", $SmallFileOptions""")

        assert(result.getInt(0) == 3 && result.getInt(1) == 1)
        def files(part: String): Long =
          sql(s"SELECT COUNT(*) FROM $table.data_files WHERE partition.part = '$part'")
            .collect()
            .head
            .getLong(0)
        assert(files("a") == 1)
        assert(files("b") == 3)
        assert(rows(table) == before)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("one Spark job rewrites all the file groups of a commit") {
    withCompaction() {
      val table = s"$catalog.db.compact_one_job"
      try {
        createThreePartitions(table)
        val before = rows(table)

        var result: Row = null
        val groups = jobGroups {
          result = call("db.compact_one_job", SmallFileOptions)
        }

        // Three partitions are three file groups, which one job rewrites.
        assert(cometJobs(groups).distinct.size == 1, s"Expected one rewrite job, got $groups")
        assert(icebergJobs(groups).isEmpty)
        assert(result.getInt(0) == 9 && result.getInt(1) == 3 && result.getInt(3) == 0)
        assert(dataFileCount(table) == 3)
        assert(replaceSnapshotCount(table) == 1)
        assert(rows(table) == before)
        assertWrittenByCometNative(table)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("each file group has a job of its own when the single job rewrite is off") {
    withCompaction() {
      withSQLConf(CometConf.COMET_ICEBERG_COMPACTION_SINGLE_JOB_ENABLED.key -> "false") {
        val table = s"$catalog.db.compact_job_per_group"
        try {
          createThreePartitions(table)
          val before = rows(table)

          var result: Row = null
          val groups = jobGroups {
            result = call("db.compact_job_per_group", SmallFileOptions)
          }

          assert(
            cometJobs(groups).distinct.size == 3,
            s"Expected three rewrite jobs, got $groups")
          assert(result.getInt(0) == 9 && result.getInt(1) == 3)
          assert(dataFileCount(table) == 3)
          assert(replaceSnapshotCount(table) == 1)
          assert(rows(table) == before)
        } finally {
          sql(s"DROP TABLE IF EXISTS $table")
        }
      }
    }
  }

  test("file groups are rewritten one job each when Comet's native scan is off") {
    withCompaction() {
      withSQLConf(CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "false") {
        val table = s"$catalog.db.compact_no_native_scan"
        try {
          createThreePartitions(table)
          val before = rows(table)

          val groups = jobGroups {
            assert(call("db.compact_no_native_scan", SmallFileOptions).getInt(0) == 9)
          }

          // Spark cannot read a bin without the native scan, so each group is read as Iceberg does.
          assert(
            cometJobs(groups).distinct.size == 3,
            s"Expected three rewrite jobs, got $groups")
          assert(dataFileCount(table) == 3)
          assert(rows(table) == before)
        } finally {
          sql(s"DROP TABLE IF EXISTS $table")
        }
      }
    }
  }

  test("large files are split across several read tasks") {
    withCompaction() {
      val table = s"$catalog.db.compact_split"
      try {
        // Row groups of 16 KiB, so that a file of about 100 KiB has several that a split can
        // end at.
        sql(s"""
          CREATE TABLE $table (id INT, part STRING, value DOUBLE, payload STRING) USING iceberg
          TBLPROPERTIES ('write.parquet.row-group-size-bytes' = '16384')""")
        (0 until 4).foreach { i =>
          spark
            .range(i * 3000L, (i + 1) * 3000L)
            .selectExpr(
              "CAST(id AS INT) AS id",
              "'a' AS part",
              "CAST(id * 1.5 AS DOUBLE) AS value",
              "uuid() AS payload")
            .coalesce(1)
            .write
            .format("iceberg")
            .mode("append")
            .saveAsTable(table)
        }
        val before = rows(table)
        val inputBytes = sql(s"SELECT SUM(file_size_in_bytes) FROM $table.data_files")
          .collect()
          .head
          .getLong(0)

        // A target of a third of a file puts every file past the maximum size, so it is rewritten
        // into pieces of about the target.
        val target = inputBytes / 4 / 3
        var result: Row = null
        val taskCounts = stageTaskCounts {
          result = call(
            "db.compact_split",
            s"options => map('target-file-size-bytes', '$target', 'min-input-files', '2')")
        }

        assert(result.getInt(0) == 4, s"rewritten_data_files_count: $result")
        assert(result.getInt(1) >= 6, s"Expected the files to be split into pieces: $result")
        // Every read task is a Spark task of the write, so there is a stage with that many.
        assert(taskCounts.max >= 6, s"Stages ran $taskCounts tasks")
        assert(rows(table) == before)
        assertWrittenByCometNative(table)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("partial progress commits file groups in several snapshots") {
    withCompaction() {
      val table = s"$catalog.db.compact_partial"
      try {
        createThreePartitions(table)
        val before = rows(table)

        var result: Row = null
        val groups = jobGroups {
          result = call(
            "db.compact_partial",
            "options => map('min-input-files', '2', 'partial-progress.enabled', 'true', " +
              "'partial-progress.max-commits', '3')")
        }

        assert(result.getInt(0) == 9 && result.getInt(1) == 3 && result.getInt(3) == 0)
        assert(dataFileCount(table) == 3)
        assert(replaceSnapshotCount(table) == 3)
        // A job covers the groups of one commit, so three commits are three jobs.
        assert(cometJobs(groups).distinct.size == 3, s"Expected three rewrite jobs, got $groups")
        assert(rows(table) == before)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("partial progress covers several file groups per job") {
    withCompaction() {
      val table = s"$catalog.db.compact_partial_batches"
      try {
        createThreePartitions(table)
        val before = rows(table)

        // Three groups in at most two commits is two groups in the first and one in the second.
        val groups = jobGroups {
          val result = call(
            "db.compact_partial_batches",
            "options => map('min-input-files', '2', 'partial-progress.enabled', 'true', " +
              "'partial-progress.max-commits', '2')")
          assert(result.getInt(0) == 9 && result.getInt(1) == 3)
        }

        // The jobs finish in either order, and a commit takes every job that has finished by
        // then, so there are one or two commits but never more than the option allows.
        assert(Set(1L, 2L).contains(replaceSnapshotCount(table)))
        assert(cometJobs(groups).distinct.size == 2, s"Expected two rewrite jobs, got $groups")
        assert(dataFileCount(table) == 3)
        assert(rows(table) == before)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("rewrite preserves the rows of every column type, nulls included") {
    withCompaction() {
      val table = s"$catalog.db.compact_types"
      try {
        sql(s"""
          CREATE TABLE $table (
            id INT, l BIGINT, d DOUBLE, f FLOAT, dec DECIMAL(10, 2), s STRING, b BOOLEAN,
            dt DATE, ts TIMESTAMP, bin BINARY, arr ARRAY<INT>, st STRUCT<a: INT, b: STRING>,
            m MAP<STRING, INT>) USING iceberg""")
        (0 until 4).foreach { i =>
          sql(s"""
            INSERT INTO $table
            SELECT /*+ COALESCE(1) */
              CAST(id AS INT),
              CASE WHEN id % 5 = 0 THEN NULL ELSE id * 1000 END,
              CASE WHEN id % 5 = 1 THEN NULL ELSE id * 1.5 END,
              CAST(id / 3 AS FLOAT),
              CASE WHEN id % 5 = 2 THEN NULL ELSE CAST(id AS DECIMAL(10, 2)) / 7 END,
              CASE WHEN id % 5 = 3 THEN NULL ELSE CAST(id AS STRING) END,
              id % 2 = 0,
              date_add(DATE '2020-01-01', CAST(id AS INT)),
              timestamp_micros(id * 1000003),
              CAST(CAST(id AS STRING) AS BINARY),
              CASE WHEN id % 4 = 0 THEN NULL ELSE array(CAST(id AS INT), CAST(id + 1 AS INT)) END,
              named_struct('a', CAST(id AS INT), 'b', CAST(id AS STRING)),
              map(CAST(id AS STRING), CAST(id AS INT))
            FROM range(${i * 50}, ${(i + 1) * 50})""")
        }
        val before = rows(table)
        assert(before.size == 200)
        assert(dataFileCount(table) == 4)

        assert(call("db.compact_types", SmallFileOptions).getInt(0) == 4)

        assert(dataFileCount(table) == 1)
        assert(rows(table) == before)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("a delete that lands before the commit fails the rewrite and leaves no new files") {
    withCompaction() {
      val table = s"$catalog.db.compact_conflict"
      try {
        sql(s"""
          CREATE TABLE $table (id INT, part STRING, value DOUBLE) USING iceberg
          TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read')""")
        (0 until 4).foreach(appendFile(table, _, "a"))
        val icebergTable = loadIcebergTable(spark, catalog, "db", "compact_conflict")
        val location = icebergTable.getClass.getMethod("location").invoke(icebergTable).toString
        def parquetFiles(): Int =
          new File(location.stripPrefix("file:"), "data")
            .listFiles()
            .count(_.getName.endsWith(".parquet"))

        // The rewrite plans against the four files, and then a delete changes one of them.
        val rewrite = CometRewriteDataFiles.prepare(
          CometRewriteDataFiles
            .Request(spark, icebergTable, Map("min-input-files" -> "2"), None, () => ())) match {
          case Right(prepared) => prepared
          case Left(reason) => fail(s"Comet declined the rewrite: $reason")
        }
        sql(s"DELETE FROM $table WHERE id = 5")
        val before = rows(table)
        val filesBefore = parquetFiles()
        assert(before.size == 399)

        val error = intercept[RuntimeException](rewrite.execute())

        assert(error.getMessage.contains("Cannot commit rewrite"), error.getMessage)
        assert(dataFileCount(table) == 4)
        assert(rows(table) == before)
        // The file the rewrite wrote was deleted when the commit failed.
        assert(parquetFiles() == filesBefore)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("rewrite applies position deletes of a merge-on-read table") {
    withCompaction() {
      val table = s"$catalog.db.compact_deletes"
      try {
        sql(s"""
          CREATE TABLE $table (id INT, part STRING, value DOUBLE) USING iceberg
          TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read')""")
        appendFile(table, 0, "a")
        appendFile(table, 1, "a")
        sql(s"DELETE FROM $table WHERE id = 5")
        val before = rows(table)
        assert(before.size == 199)

        val groups = jobGroups {
          call("db.compact_deletes", SmallFileOptions)
        }

        assert(cometJobs(groups).nonEmpty)
        assert(rows(table) == before)
        // The deleted row is gone from the data files, not just masked by a delete file.
        assert(
          sql(s"SELECT SUM(record_count) FROM $table.data_files").collect().head.getLong(0) ==
            199L)
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }
}
