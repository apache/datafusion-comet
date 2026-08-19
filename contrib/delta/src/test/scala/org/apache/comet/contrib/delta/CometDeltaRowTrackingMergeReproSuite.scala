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

  // Regression guard: an IntegerType (Int32) PARTITION column on a row-tracking table.
  // Kernel's per-file transform injects partition values as Int32 literals from the Add action;
  // this verifies the injected Int32 partition values survive the read intact (each row's
  // partition equals the written value and matches the vanilla Delta reader). With no row_id
  // projected there is no RowId metadata column, so the partition is the last emitted column and
  // its Int32 literal lands in the Int32 partition slot directly -- no swap, no schema widening.
  test("IntegerType partition column on row-tracking table: native kernel read matches vanilla") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("int_part_rt") { tablePath =>
      val ss = spark
      import ss.implicits._
      val numRows = 4000

      // `p` is a Scala Int -> Spark IntegerType -> the logical-only partition column that
      // triggers kernel's Int64 literal injection. Several partitions, multiple files.
      (0 until numRows)
        .map(i => (i.toLong, i.toLong, (i % 4)))
        .toDF("key", "stored_id", "p")
        .write
        .partitionBy("p")
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)

      // Project the Int32 partition column: kernel injects it as an Int64 literal in the
      // per-file transform, so this read exercises the widen + Int32 cast-back.
      def readBack() =
        spark.read
          .format("delta")
          .load(tablePath)
          .select(col("key"), col("p"))

      val nativeDf = readBack()
      val nativeRows = nativeDf.collect()
      val plan = nativeDf.queryExecution.executedPlan
      val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
      assert(scans.nonEmpty, s"expected CometDeltaNativeScanExec in plan:\n$plan")

      var vanillaRows: Array[org.apache.spark.sql.Row] = Array.empty
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        vanillaRows = readBack().collect()
      }

      // Access by NAME (Spark moves the partition column to the end of the schema, so
      // positional access mislabels columns). Compare the (key -> partition) mapping against
      // the vanilla reader -- exact, every row -- to pin that kernel's injected Int32 partition
      // literal survives the read intact.
      def partByKey(rows: Array[org.apache.spark.sql.Row]): Map[Long, Int] =
        rows.map(r => r.getAs[Long]("key") -> r.getAs[Int]("p")).toMap
      val nativePart = partByKey(nativeRows)
      val vanillaPart = partByKey(vanillaRows)
      val partMismatch =
        vanillaPart.keys.toSeq.sorted.filter(k => nativePart.get(k) != vanillaPart.get(k))
      assert(
        partMismatch.isEmpty,
        s"native partition != vanilla for ${partMismatch.size} keys; sample: " +
          partMismatch
            .take(10)
            .map(k => s"key=$k native=${nativePart.get(k)} vanilla=${vanillaPart.get(k)}")
            .mkString(", "))
      assert(
        nativeRows.length == numRows,
        s"native row count ${nativeRows.length} != $numRows")
      // Partition values must be exactly the written Int32 set, undamaged by widen/cast-back.
      assert(
        nativeRows.map(_.getAs[Int]("p")).toSet == Set(0, 1, 2, 3),
        s"native partition values wrong: ${nativeRows.map(_.getAs[Int]("p")).toSet}")
      // Each key's partition must equal key % 4 (the value written) -- proves the cast-back
      // produced the real partition value, not a corrupted/zeroed one.
      val wrongPart = nativePart.filter { case (k, p) => p != (k % 4).toInt }
      assert(
        wrongPart.isEmpty,
        s"native partition value != key%4 for ${wrongPart.size} keys; sample: ${wrongPart.take(10)}")
    }
  }

  // Regression guard for the partitioned row-tracking row_id SWAP (#30). On a PARTITIONED row-tracking
  // table the read projects `_metadata.row_id`, shipped to kernel as a RowId metadata column. Kernel's
  // per-file transform injects the partition literal BEFORE that RowId column (it doesn't advance
  // kernel's `last_physical_field`), and the executor labels output columns POSITIONALLY -- so if the
  // driver appended the partition column LAST, the Int32 partition value landed in the row_id slot and
  // the Long row_id in the partition slot: native row_id == key % 4 (the partition), not the stable
  // row_id. This is the same failure as the Delta own suite
  // RowTrackingMergeCommonNameBasedCDCOnSuite "Optimized writes [disabled] on partitioned table"
  // (merge source `0 AS partition` -> every inserted row_id == 0 -> "Row IDs are not unique"). The fix
  // (`spliceKernelPartitions`) ships the partition column at kernel's injection slot so the labeling
  // lines up. (The UPDATE creates a materialised/absent file mix; the swap affected BOTH, so it was
  // never a null-fill issue.)
  test("partitioned row-tracking table: row_id must not be the partition value (swap)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("rt_part_rowid") { tablePath =>
      val ss = spark
      import ss.implicits._
      val numRows = 4000

      (0 until numRows)
        .map(i => (i.toLong, i.toLong, (i % 4)))
        .toDF("key", "stored_id", "p")
        .write
        .partitionBy("p")
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)

      // Materialise row-ids in a SUBSET of files (the touched ones); untouched files stay absent.
      spark.sql(
        s"UPDATE delta.`$tablePath` SET stored_id = stored_id + 1000000 WHERE key < ${numRows / 2}")

      def readBack() =
        spark.read
          .format("delta")
          .load(tablePath)
          .select(col("key"), col("_metadata.row_id").as("rid"))

      val nativeDf = readBack()
      val nativeRows = nativeDf.collect()
      val plan = nativeDf.queryExecution.executedPlan
      assert(
        collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty,
        s"expected CometDeltaNativeScanExec in plan:\n$plan")

      var vanillaRows: Array[org.apache.spark.sql.Row] = Array.empty
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        vanillaRows = readBack().collect()
      }

      val nativeRid = nativeRows.map(r => r.getAs[Long]("key") -> r.getAs[Long]("rid")).toMap
      val vanillaRid = vanillaRows.map(r => r.getAs[Long]("key") -> r.getAs[Long]("rid")).toMap
      val mismatch =
        vanillaRid.keys.toSeq.sorted.filter(k => nativeRid.get(k) != vanillaRid.get(k))
      assert(
        mismatch.isEmpty,
        s"native row_id != vanilla for ${mismatch.size} keys; sample: " +
          mismatch
            .take(10)
            .map(k => s"key=$k native=${nativeRid.get(k)} vanilla=${vanillaRid.get(k)}")
            .mkString(", "))
      val rids = nativeRows.map(_.getAs[Long]("rid"))
      assert(
        rids.distinct.length == rids.length,
        s"native row_id not unique: ${rids.length - rids.distinct.length} duplicates " +
          s"(row_id=0 count=${rids.count(_ == 0L)})")
    }
  }
}
