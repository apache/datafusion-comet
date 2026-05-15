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

import org.apache.spark.sql.comet.CometDeltaNativeScanExec

/**
 * Targeted tests for Delta row tracking acceleration. Covers:
 *   - unmaterialised case: a freshly-written rowTracking-enabled table has all _row_id_ /
 *     _row_commit_version_ values still null in the physical column, so every row must be
 *     synthesised from baseRowId + parquet_row_index.
 *   - materialised case: after a MERGE, the physical columns are populated and the coalesce falls
 *     through to the materialised value for rewritten rows.
 */
class CometDeltaRowTrackingSuite extends CometDeltaTestBase {

  test("row tracking: unmaterialised _metadata.row_id synthesised from baseRowId") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("row_tracking_unmat") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 12)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)

      val df = spark.read
        .format("delta")
        .load(tablePath)
        .selectExpr("id", "_metadata.row_id AS rid")
        .orderBy("id")
      val plan = df.queryExecution.executedPlan
      assert(
        collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty,
        s"expected Comet to accelerate rowTracking scan:\n$plan")

      val rows = df.collect().toSeq
      assert(rows.size == 12, s"expected 12 rows, got ${rows.size}")
      // Row IDs must be contiguous starting from 0 since we wrote a single
      // partition into a fresh table.
      rows.zipWithIndex.foreach { case (row, idx) =>
        assert(
          row.getLong(1) == idx.toLong,
          s"row $idx: expected rid=$idx, got rid=${row.getLong(1)}")
      }
    }
  }

  test("row tracking: native and vanilla agree on row_id after MERGE") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("row_tracking_merge") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 8)
        .map(i => (i.toLong, s"v_$i"))
        .toDF("id", "v")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)

      spark.sql(s"""MERGE INTO delta.`$tablePath` AS t
           |USING (SELECT 3L AS id, 'merged' AS v UNION ALL
           |       SELECT 5L AS id, 'merged' AS v) AS s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET v = s.v""".stripMargin)

      def collectRows(): Seq[(Long, Long)] =
        spark.read
          .format("delta")
          .load(tablePath)
          .selectExpr("id", "_metadata.row_id AS rid")
          .collect()
          .toSeq
          .map(r => (r.getLong(0), r.getLong(1)))
          .sortBy(_._1)

      val nativeRows = collectRows()
      val vanillaRows = withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        collectRows()
      }
      assert(
        nativeRows == vanillaRows,
        s"row_id mismatch after MERGE:\nnative=$nativeRows\nvanilla=$vanillaRows")
    }
  }
}
