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

import java.io.File

import org.apache.spark.sql.functions._

// One minimum-viable reproducer per root-cause cluster identified in the
// Delta 4.1 own-test-suite regression (killed mid-run at 40 unique failing
// tests). Each test mirrors the Spark shape of the upstream failing tests
// closely enough to trigger the same code path, but is small enough that
// fixing it gives us a tight inner loop.
//
// Cluster -> upstream representative test -> reproducer here:
//
//   1. CDC off-by-one column count   -> DeltaCDCSuite.scala "CDC for point update"
//      -> testCDCPointUpdateColumnCount
//   2. MERGE schema-evolution wrong  -> MergeIntoSchemaEvolutionSuite "schema
//      evolution - upcast int source type into long target"
//      -> testMergeIntoUpcastIntToLong
//   3. Optimize on special-char path -> OptimizeCompactionSuite "optimize
//      command: only first partition is compactable"
//      -> testOptimizeOnPathWithSpaces
//   4. StatsCollection wrong         -> StatsCollectionSuite "gather stats"
//      -> testGatherStatsAfterAppend
//   5. Constraint w/ analyzer expr   -> CheckConstraintsSuite "constraint with
//      analyzer-evaluated expressions. Expression: year(current_date())"
//      -> testConstraintWithCurrentDate
//
// All five run today and SHOULD pass; once the rebuild + reinstall lands
// they're expected to flip to failing (matching the regression), at which
// point each gets its own dedicated suite as it gets fixed.
class CometDeltaRegressionReproSuite extends CometDeltaTestBase {

  // 1. CDC: scan output is DeltaCDFRelation (custom BaseRelation -- contrib's
  // DeltaScanRule never sees it). The failure appears in a downstream Comet
  // operator (Sort + WholeStageCodegen) which is given a column count from
  // the Delta-side schema (N + 3 CDC metadata cols) but the native scan only
  // produces N. Mirrors DeltaCDCSuite "CDC for point update".
  test("CDC for point update reproduces Output column count mismatch") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    val tblName = s"cdc_point_update_${System.nanoTime()}"
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tblName")
      spark.sql(s"CREATE TABLE $tblName(id INT, name STRING, age INT) " +
        s"USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)")
      spark.sql(s"INSERT INTO $tblName VALUES (1,'a',10), (2,'b',20), (3,'c',30)")
      spark.sql(s"UPDATE $tblName SET age = 11 WHERE id = 1")
      val df = spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .table(tblName)
        .orderBy("_commit_version", "_change_type")
        .select(col("_commit_version"), col("_change_type"),
          col("_commit_timestamp"), col("id"), col("age"))
      val rows = df.collect()
      assert(rows.length > 0, "CDC read returned no rows")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tblName")
    }
  }

  // 2. MERGE schema evolution: source has an INT column; target has a BIGINT
  // column of the same name. Delta inserts an implicit cast in the merge plan.
  // Regression shows results don't match through the CometScan
  // [native_delta_compat] code path. Mirrors MergeIntoSchemaEvolutionSuite
  // "upcast int source type into long target".
  test("MERGE upcast int->long source produces wrong rows") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("merge_upcast") { tablePath =>
      val ss = spark
      import ss.implicits._
      Seq((1, 100L), (2, 200L), (3, 300L))
        .toDF("key", "value")
        .write.format("delta").save(tablePath)
      // Source has int value column; target has bigint.
      Seq((1, 999), (4, 444))
        .toDF("key", "value")
        .createOrReplaceTempView("merge_upcast_src")
      spark.sql(
        s"""MERGE INTO delta.`$tablePath` AS t
            USING merge_upcast_src AS s
            ON t.key = s.key
            WHEN MATCHED THEN UPDATE SET t.value = s.value
            WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""")
      val rows = spark.read.format("delta").load(tablePath)
        .orderBy("key").collect()
        .map(r => (r.getInt(0), r.getLong(1)))
        .toSeq
      assert(rows === Seq((1, 999L), (2, 200L), (3, 300L), (4, 444L)),
        s"MERGE upcast produced wrong rows: $rows")
    }
  }

  // 3. OPTIMIZE / write-path: tables in directories with spaces and URL-encoded
  // chars in the path fail at write time. Upstream failure was
  // TASK_WRITE_FAILED on file:/.../s p a r k %2a-... Mirrors
  // OptimizeCompactionSuite "optimize command: only first partition is
  // compactable".
  test("OPTIMIZE on table located in a path with spaces and %2a") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    val parent = File.createTempFile("comet-delta-optimize", "").getAbsoluteFile
    parent.delete()
    val weirdDir = new File(parent.getParentFile, s"s p a r k %2a-${parent.getName}")
    assert(weirdDir.mkdirs(), s"could not create $weirdDir")
    try {
      val tablePath = weirdDir.toURI.toString.stripSuffix("/")
      val ss = spark
      import ss.implicits._
      // Two files, both compactable -- forces OPTIMIZE to rewrite both into one.
      (0 until 5).map(i => (i, s"v_$i")).toDF("id", "v")
        .write.format("delta").save(tablePath)
      (5 until 10).map(i => (i, s"v_$i")).toDF("id", "v")
        .write.format("delta").mode("append").save(tablePath)
      spark.sql(s"OPTIMIZE delta.`$tablePath`")
      val n = spark.read.format("delta").load(tablePath).count()
      assert(n === 10, s"expected 10 rows after OPTIMIZE, got $n")
    } finally {
      def rm(f: File): Unit = {
        if (f.isDirectory) f.listFiles().foreach(rm)
        f.delete()
      }
      rm(weirdDir)
    }
  }

  // 4. StatsCollection: after an append the table's stat collection produces
  // the wrong count. Upstream failure: "9 did not equal 1". Mirrors
  // StatsCollectionSuite "gather stats".
  test("ANALYZE TABLE after append produces correct numFiles stat") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    val tblName = s"stats_basic_${System.nanoTime()}"
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tblName")
      spark.sql(s"CREATE TABLE $tblName(id INT, v STRING) USING DELTA")
      // Write 9 separate files (9 single-row inserts) so numFiles=9.
      (0 until 9).foreach { i =>
        spark.sql(s"INSERT INTO $tblName VALUES ($i, 'v_$i')")
      }
      val numFiles = spark.sql(s"DESCRIBE DETAIL $tblName")
        .selectExpr("numFiles").head().getLong(0)
      assert(numFiles === 9L, s"expected numFiles=9 after 9 inserts, got $numFiles")
      // Total row count via aggregate -- this is the path StatsCollectionSuite
      // checks and what surfaced "9 did not equal 1" upstream.
      val count = spark.read.format("delta").table(tblName).count()
      assert(count === 9L, s"expected count=9, got $count")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tblName")
    }
  }

  // 6. MERGE schema-evolution under DV + CDC + PredPushdownDisabled returns
  // empty target. Upstream:
  // MergeIntoSchemaEvolutionBaseExistingColumnSQLPathBasedCDCOnDVsPredPushOffSuite
  // "schema evolution - upcast int source type into long target". Plan dump
  // shows `CometScan [native_delta_compat]` reading
  // `__delta_internal_is_row_deleted:tinyint` and the pushed-down filter
  // `__delta_internal_is_row_deleted = 0`. The column does not exist in the
  // fresh parquet file (no DV materialised yet); Spark normally synthesises
  // it as 0, our scan returns 0 rows.
  // Note: this repro does NOT yet fail locally even though the upstream
  // MergeIntoSchemaEvolutionBaseExistingColumnSQLPathBasedCDCOnDVsPredPushOffSuite
  // variant fails on the same shape. Documented as a known-incomplete
  // reproducer -- fixing the upstream failure will require nailing down
  // which additional session state the Delta test mixins set up that we
  // aren't mirroring here.
  test("MERGE upcast int->long under DV+CDC+PredPushdownDisabled doesn't empty target") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("merge_upcast_cdc_dv") { tablePath =>
      val ss = spark
      import ss.implicits._
      // Mirror MergeIntoSchemaEvolutionBaseExistingColumnSQLPathBasedCDCOnDVsPredPushOffSuite
      // mixin chain: CDCEnabled + MergeIntoDVsMixin + PredicatePushdownDisabled
      // + MergeCDCMixin + MergeCDCWithDVsMixin
      val keys = Seq(
        "spark.databricks.delta.properties.defaults.enableChangeDataFeed" -> "true",
        "spark.databricks.delta.properties.defaults.enableDeletionVectors" -> "true",
        "spark.databricks.delta.merge.persistentDeletionVectors.enabled" -> "true",
        "spark.databricks.delta.update.persistentDeletionVectors.enabled" -> "false",
        "spark.databricks.delta.delete.persistentDeletionVectors.enabled" -> "false",
        "spark.databricks.delta.deletionVectors.useMetadataRowIndex" -> "false")
      val originals = keys.map { case (k, _) => k -> spark.conf.getOption(k) }
      try {
        keys.foreach { case (k, v) => spark.conf.set(k, v) }
        Seq((0, 0L), (1, 10L), (3, 30L)).toDF("key", "value")
          .write
          .format("delta")
          .save(tablePath)
        Seq((1, 1), (2, 2)).toDF("key", "value")
          .createOrReplaceTempView("merge_upcast_src")
        spark.sql(
          s"""MERGE INTO delta.`$tablePath` AS t
              USING merge_upcast_src AS s
              ON t.key = s.key
              WHEN MATCHED THEN UPDATE SET *
              WHEN NOT MATCHED THEN INSERT *""")
        val rows = spark.read.format("delta").load(tablePath)
          .orderBy("key").collect()
          .map(r => (r.getInt(0), r.getLong(1)))
          .toSeq
        assert(rows === Seq((0, 0L), (1, 1L), (2, 2L), (3, 30L)),
          s"MERGE upcast under DV+CDC produced unexpected rows: $rows")
      } finally {
        originals.foreach {
          case (k, Some(orig)) => spark.conf.set(k, orig)
          case (k, None) => spark.conf.unset(k)
        }
      }
    }
  }

  // 7. Stats recompute returns null stats for one of multiple files.
  // Upstream: StatsCollectionSuite "recompute stats multiple columns and files".
  test("Stats recompute over 3-file table produces non-null per-file stats") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("stats_recompute") { tablePath =>
      val ss = spark
      import ss.implicits._
      val df = spark.range(10, 20).withColumn("x", $"id" + 10).repartition(3)
      spark.conf.set("spark.databricks.delta.stats.collect", "false")
      try {
        df.write.format("delta").save(tablePath)
        // Directly call StatisticsCollection.recompute -- this uses
        // `deltaLog.createDataFrame(snapshot, addFiles)` which builds
        // `TahoeBatchFileIndex` (different from the
        // `PreparedDeltaFileIndex` used by `spark.read.format("delta")`).
        // The failing test in regression follows this exact path.
        val deltaLog = org.apache.spark.sql.delta.DeltaLog
          .forTable(spark, tablePath)
        org.apache.spark.sql.delta.stats.StatisticsCollection
          .recompute(
            spark,
            deltaLog,
            catalogTable = None,
            predicates = Seq(org.apache.spark.sql.catalyst.expressions.Literal(true)),
            fileFilter = (_: org.apache.spark.sql.delta.actions.AddFile) => true)
        // After recompute, read the per-file stats. Use the same
        // accessor pattern the upstream test does: iterate AddFiles and
        // pull `stats` JSON.
        val snapshot = deltaLog.unsafeVolatileSnapshot
        val allStats = snapshot.allFiles.collect().map(_.stats).toSeq
        assert(allStats.length == 3,
          s"expected 3 stats jsons, got ${allStats.length}: $allStats")
        allStats.foreach { s =>
          assert(s != null && s.nonEmpty, s"empty stats: $s")
          val mn = """"minValues":\{"id":(\d+)""".r.findFirstMatchIn(s)
          val mx = """"maxValues":\{"id":(\d+)""".r.findFirstMatchIn(s)
          assert(mn.isDefined && mx.isDefined,
            s"missing min/max values in stats: $s")
        }
      } finally {
        spark.conf.unset("spark.databricks.delta.stats.collect")
      }
    }
  }

  // 5. CHECK constraint with analyzer-evaluated expression
  // (year(current_date())). Delta resolves it at definition time; the
  // constraint then has to evaluate against inserted rows. Mirrors
  // CheckConstraintsSuite "constraint with analyzer-evaluated expressions.
  // Expression: year(current_date())".
  test("CHECK constraint with year(current_date()) accepts in-range rows") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    val tblName = s"check_yearct_${System.nanoTime()}"
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tblName")
      spark.sql(s"CREATE TABLE $tblName(id INT, yr INT) USING DELTA")
      spark.sql(s"ALTER TABLE $tblName ADD CONSTRAINT yr_now " +
        s"CHECK (yr <= year(current_date()))")
      // Inserting a row whose `yr` is well below the current year must succeed.
      spark.sql(s"INSERT INTO $tblName VALUES (1, 2000)")
      val rows = spark.read.format("delta").table(tblName).collect()
      assert(rows.length === 1)
      assert(rows.head.getInt(1) === 2000)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tblName")
    }
  }
}
