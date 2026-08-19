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

import org.apache.spark.sql.functions._

// Regression guard for F3: MATERIALISED row-tracking columns.
//
// When a Delta file is rewritten (OPTIMIZE / z-order / compaction / MERGE /
// UPDATE) on a row-tracking-enabled table, Delta persists stable row IDs and
// commit versions into real parquet columns `_row-id-col-<uuid>` /
// `_row-commit-version-col-<uuid>`. The Spark plan reads
// `coalesce(_metadata.row_id, base_row_id + row_index)`. The native scan must
// READ those materialised columns from the file (returning null for files that
// don't carry them) -- it previously classified them as synthetic and
// synthesised `base_row_id + row_index` instead, so row IDs and commit versions
// were not stable across rewrites. Covers the same root cause as the failing
// Delta own-suite tests in rowid/RowTracking{Merge,Delete,Compaction,
// ReadWrite}Suite.
class CometDeltaRowTrackingMaterializedSuite extends CometDeltaTestBase {

  private def writeRowTracked(tablePath: String, n: Int, files: Int): Unit = {
    val ss = spark
    import ss.implicits._
    (0 until n)
      .map(i => (i.toLong, s"v_$i"))
      .toDF("id", "v")
      .repartition(files)
      .write
      .format("delta")
      .option("delta.enableRowTracking", "true")
      .option("delta.minReaderVersion", "3")
      .option("delta.minWriterVersion", "7")
      .save(tablePath)
  }

  private def rowIdsByValue(tablePath: String): Map[String, Long] =
    spark.read.format("delta").load(tablePath)
      .select(col("v"), col("_metadata.row_id").as("rid"))
      .collect()
      .map(r => r.getString(0) -> r.getLong(1))
      .toMap

  test("row IDs are stable across OPTIMIZE") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("rt_optimize") { tablePath =>
      writeRowTracked(tablePath, n = 100, files = 5)
      val before = rowIdsByValue(tablePath)
      spark.sql(s"OPTIMIZE delta.`$tablePath`")
      val after = rowIdsByValue(tablePath)
      assert(before.size == 100)
      val changed = before.keys.filter(k => before(k) != after.getOrElse(k, -1L)).toSeq.sorted
      assert(changed.isEmpty, s"row IDs changed across OPTIMIZE for ${changed.size} rows")
      // The before/after check above is native-vs-native; also gate on engagement +
      // equality vs vanilla so a silent fallback or a consistent-but-wrong native
      // row_id can't pass on stability alone.
      assertDeltaNativeMatches(
        tablePath,
        _.select(col("v"), col("_metadata.row_id").as("rid")).orderBy("v"))
    }
  }

  test("row IDs are stable across UPDATE (file rewrite)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("rt_update") { tablePath =>
      writeRowTracked(tablePath, n = 40, files = 2)
      val before = rowIdsByValue(tablePath)
      // UPDATE rewrites the touched file(s), materialising stable row IDs.
      spark.sql(s"UPDATE delta.`$tablePath` SET v = concat(v, '_x') WHERE id % 2 = 0")
      val after = spark.read.format("delta").load(tablePath)
        .select(col("id"), col("_metadata.row_id").as("rid"))
        .collect()
        .map(r => r.getLong(0) -> r.getLong(1))
        .toMap
      // Each id keeps its original row_id regardless of whether v changed.
      val beforeById = before.map { case (v, rid) => v.stripPrefix("v_").toLong -> rid }
      val changed = beforeById.keys
        .filter(id => beforeById(id) != after.getOrElse(id, -1L)).toSeq.sorted
      assert(changed.isEmpty, s"row IDs changed across UPDATE for ${changed.size} ids: " +
        changed.take(5).map(id => s"$id: ${beforeById(id)} -> ${after.get(id)}").mkString(", "))
      // Engagement + equality vs vanilla (the before/after check above is native-only).
      assertDeltaNativeMatches(
        tablePath,
        _.select(col("id"), col("_metadata.row_id").as("rid")).orderBy("id"))
    }
  }

  test("row_id correct with a pushed data filter on an UNMATERIALIZED table") {
    // H2 (review finding): row_id is synthesised as base_row_id + row_index on an
    // unmaterialised row-tracking table. row_index uses a running per-batch offset
    // that assumes the parquet reader returns every physical row. If a data filter is
    // pushed to parquet, it skips rows, decoupling the offset from the true physical
    // position -> wrong row_index -> wrong row_id. Filter pushdown must be suppressed
    // when row_index/row_id is synthesised (as it already is for is_row_deleted).
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("rt_filter_unmat") { tablePath =>
      writeRowTracked(tablePath, n = 100, files = 1) // single file, unmaterialised
      assertDeltaNativeMatches(
        tablePath,
        _.select(col("id"), col("_metadata.row_id").as("rid"))
          .filter(col("id") >= 50)
          .orderBy("id"))
    }
  }

  test("materialised row IDs read correctly under column-mapping id mode (M3)") {
    // M3 (review finding): materialised `_row-id-col-*` columns are matched by NAME and
    // carry no parquet field id. Under column-mapping id mode the reader matches by
    // field id, so this exercises the reader's fall-back to name matching for fields
    // without an id. Row IDs must stay stable across OPTIMIZE and match vanilla.
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("rt_cm_id") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 60).map(i => (i.toLong, s"v_$i")).toDF("id", "v")
        .repartition(3)
        .write
        .format("delta")
        .option("delta.columnMapping.mode", "id")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      val before = rowIdsByValue(tablePath)
      spark.sql(s"OPTIMIZE delta.`$tablePath`")
      val after = rowIdsByValue(tablePath)
      assert(before.size == 60)
      val changed = before.keys.filter(k => before(k) != after.getOrElse(k, -1L)).toSeq.sorted
      assert(changed.isEmpty, s"CM-id row IDs changed across OPTIMIZE for ${changed.size} rows")
      // Also confirm native == vanilla for the materialised projection.
      assertDeltaNativeMatches(
        tablePath,
        _.select(col("v"), col("_metadata.row_id").as("rid")).orderBy("v"))
    }
  }

  test("materialised row_commit_version read after OPTIMIZE matches vanilla") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("rt_rcv") { tablePath =>
      writeRowTracked(tablePath, n = 60, files = 3)
      spark.sql(s"OPTIMIZE delta.`$tablePath`")
      // Compare native vs vanilla for both materialised metadata columns.
      assertDeltaNativeMatches(
        tablePath,
        _.select(
          col("v"),
          col("_metadata.row_id").as("rid"),
          col("_metadata.row_commit_version").as("rcv"))
          .orderBy("v"))
    }
  }
}
