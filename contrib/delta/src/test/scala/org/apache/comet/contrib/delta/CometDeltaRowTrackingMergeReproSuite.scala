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
import org.apache.spark.sql.functions.col

// Regression guard for the row-tracking MERGE row-drop bug surfaced by the full
// Delta 4.1 own-suite run:
//   RowTrackingMergeCommonNameBasedCDCOnSuite "INSERT NOT MATCHED only MERGE" et al.
//
// Root cause: on a row-tracking table whose schema defines a materialized
// `_row-id-col-<uuid>` column, that column is physically present only in files
// rewritten by a row-id-preserving op -- and ABSENT from freshly appended/inserted
// files. When several such files pack into one Spark partition, the native scan
// emits one parquet file-group per file (needed for per-file row_index) and reads
// the materialized column across the concurrently-executed file-groups. Reading a
// column physically absent from some files under that cross-file-group concurrency
// non-deterministically dropped whole file-groups' rows.
//
// Fix: CometDeltaNativeScan.createExec pins one file per Spark partition when the
// scan reads materialized row-tracking columns, so each native plan is
// single-file-group and the absent-column null-fill runs without cross-file-group
// concurrency.
class CometDeltaRowTrackingMergeReproSuite extends CometDeltaTestBase {

  test("INSERT-only MERGE on row-tracking table: native read drops no rows") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("rt_merge_insert") { tablePath =>
      val ss = spark
      import ss.implicits._
      val numRows = 4000
      val numNew = 2000

      // Target: keys 0..numRows-1 in 2 files, row tracking enabled.
      (0 until numRows)
        .map(i => (i.toLong, i.toLong, 0L))
        .toDF("key", "stored_id", "last_modified_version")
        .repartition(2)
        .write
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)

      // Source: unmatched keys numRows..numRows+numNew-1.
      (numRows until numRows + numNew)
        .map(i => (i.toLong, i.toLong, 1L))
        .toDF("key", "stored_id", "last_modified_version")
        .createOrReplaceTempView("rt_merge_src")

      spark.sql(
        s"""MERGE INTO delta.`$tablePath` t
           |USING rt_merge_src s
           |ON s.key = t.key
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin)

      // Read back WITH `_metadata.row_id` -- drives the materialized row-tracking
      // column read that previously dropped rows.
      def readBack() =
        spark.read
          .format("delta")
          .load(tablePath)
          .select(
            col("key"),
            col("stored_id"),
            col("last_modified_version"),
            col("_metadata.row_id").as("rid"))

      val nativeDf = readBack()
      val nativeRows = nativeDf.collect()
      val plan = nativeDf.queryExecution.executedPlan
      val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
      assert(scans.nonEmpty, s"expected CometDeltaNativeScanExec in plan:\n$plan")

      // Differential vs vanilla Delta reader (native scan disabled). Assign via a var
      // because `withSQLConf` returns Unit on Spark 3.5 (it returns the block value only
      // on Spark 4.x); this pattern compiles on both.
      var vanillaKeys: Set[Long] = Set.empty
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        vanillaKeys = readBack().collect().map(_.getLong(0)).toSet
      }

      val nativeKeys = nativeRows.map(_.getLong(0)).toSet
      val expectedKeys = (0L until (numRows + numNew)).toSet

      assert(
        vanillaKeys.diff(nativeKeys).isEmpty,
        s"native scan dropped ${vanillaKeys.diff(nativeKeys).size} rows vs vanilla; " +
          s"missing keys (sample): ${vanillaKeys.diff(nativeKeys).toSeq.sorted.take(10)}")
      assert(
        nativeKeys == expectedKeys,
        s"native key set wrong: ${expectedKeys.diff(nativeKeys).toSeq.sorted.take(10)} missing, " +
          s"${nativeKeys.diff(expectedKeys).toSeq.sorted.take(10)} unexpected")
      assert(
        nativeRows.length == numRows + numNew,
        s"native row count ${nativeRows.length} != ${numRows + numNew}")
    }
  }
}
