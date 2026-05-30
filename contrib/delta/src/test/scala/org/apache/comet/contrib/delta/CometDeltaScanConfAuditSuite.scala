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

// Audit 1: scan-affecting SQLConfs and table properties.
//
// For every conf that can change WHAT the scan returns (vs purely a write
// path), assert one of two contracts:
//
//   1. CONTRACT_NATIVE: native scan engages AND results match vanilla Spark
//   2. CONTRACT_FALLBACK: DeltaScanRule declines AND results still match
//      vanilla Spark
//
// The goal is to make every silent-disengagement (or worse: silent-wrong-
// answer) regression a test failure. Confs that don't affect scan output
// (e.g. coordinator capacity-units, write-path validation) are out of
// scope.
//
// Each test names the exact conf key in a comment so future grep-driven
// readers can find the regression coverage for it.
class CometDeltaScanConfAuditSuite extends CometDeltaTestBase {

  // ---- DV-related confs -----------------------------------------------------

  // `spark.databricks.delta.deletionVectors.useMetadataRowIndex`
  // (DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX, default true)
  // CONTRACT_FALLBACK when set to false on a DV-bearing table.
  test("DV: useMetadataRowIndex=false declines on DV-bearing read") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("conf_useMetaRowIdx") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, s"n_$i"))
        .toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id % 5 = 0")
      withSQLConf(
        "spark.databricks.delta.deletionVectors.useMetadataRowIndex" -> "false") {
        assertDeltaFallback(tablePath, _.select("id", "name"))
      }
    }
  }

  // Companion positive case: default (true) must engage native.
  test("DV: useMetadataRowIndex=true (default) engages native") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("conf_useMetaRowIdx_default") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, s"n_$i"))
        .toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id % 5 = 0")
      assertDeltaNativeMatches(tablePath, _.select("id", "name"))
    }
  }

  // `spark.databricks.delta.merge.usePersistentDeletionVectors` (boolean,
  // default true on supported tables). Forces MERGE to write DVs instead of
  // rewriting files. Native scan must still read the resulting table
  // correctly (CONTRACT_NATIVE) -- this is what bb0686c1 / 9ab8b842 fixed.
  test("DV: MERGE_USE_PERSISTENT_DELETION_VECTORS=true read-side engages native") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("conf_mergeDv") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, s"n_$i"))
        .toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      withSQLConf(
        "spark.databricks.delta.merge.usePersistentDeletionVectors" -> "true") {
        val src = (0 until 10)
          .map(i => (i.toLong, s"updated_$i"))
          .toDF("id", "name")
        src.createOrReplaceTempView("src")
        spark.sql(
          s"""MERGE INTO delta.`$tablePath` t USING src s ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET name = s.name""".stripMargin)
      }
      assertDeltaNativeMatches(tablePath, _.select("id", "name"))
    }
  }

  // ---- Column mapping -------------------------------------------------------

  // `delta.columnMapping.mode` table property: "none" / "name" / "id".
  // All three must engage native (id mode wired in commit 7ace165e; name
  // mode rides the same field-ID path).
  Seq("none", "name", "id").foreach { mode =>
    test(s"columnMapping.mode=$mode engages native") {
      assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
      withDeltaTable(s"conf_cm_$mode") { tablePath =>
        val ss = spark
        import ss.implicits._
        val w = (0 until 6).map(i => (i.toLong, s"v_$i"))
          .toDF("id", "v")
          .write
          .format("delta")
          .option("delta.minReaderVersion", "2")
          .option("delta.minWriterVersion", "5")
        val w2 = if (mode == "none") w else w.option("delta.columnMapping.mode", mode)
        w2.save(tablePath)
        assertDeltaNativeMatches(tablePath, _.select("id", "v"))
      }
    }
  }

  // ---- CDF / CDC ------------------------------------------------------------

  // `delta.enableChangeDataFeed` table prop + `readChangeFeed` read option.
  // GAP: path-based CDF reads route through `DeltaCDFRelation` which our
  // `DeltaScanRule` (matching on `CometScanExec` over `HadoopFsRelation`)
  // does not currently intercept. CometDeltaCdcSuite covers table-API CDC
  // reads, which DO engage native; the path-API form documented here is a
  // known limitation. Remove the GAP assertion (flip to assert non-empty
  // CometDeltaNativeScanExec) when the rule learns to handle DeltaCDFRelation.
  test("GAP CDF: path-based readChangeFeed does not engage native (DeltaCDFRelation)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("conf_cdf_append") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 5).map(i => (i.toLong, s"a_$i")).toDF("id", "v")
        .write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      (5 until 10).map(i => (i.toLong, s"b_$i")).toDF("id", "v")
        .write.mode("append").format("delta").save(tablePath)
      val df = spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .load(tablePath)
      df.collect()
      val plan = df.queryExecution.executedPlan
      val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
      assert(
        scans.isEmpty,
        "GAP CLOSED: path-based CDF read now engages native -- flip this " +
          "assertion to assert engagement and move the test to the positive matrix")
    }
  }

  // ---- Row tracking ---------------------------------------------------------

  // `delta.enableRowTracking` table prop -- both materialised and synthesised
  // row-id paths covered by CometDeltaFeaturesSuite. Here we just lock in
  // that the conf doesn't disengage native.
  test("rowTracking: enableRowTracking=true does not disengage native") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("conf_rt") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 8).map(i => (i.toLong, s"r_$i")).toDF("id", "v")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      assertDeltaNativeMatches(tablePath, _.select("id", "v"))
    }
  }

  // ---- Comet-side scan conf -------------------------------------------------

  // `spark.comet.scan.deltaNative.enabled=false`: kill switch must work.
  test("kill switch: deltaNative.enabled=false forces fallback") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("conf_kill") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 4).map(i => (i.toLong, s"k_$i")).toDF("id", "v")
        .write.format("delta").save(tablePath)
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        assertDeltaFallback(tablePath, _.select("id", "v"))
      }
    }
  }

  // ---- GAP markers (no decline gate today) ----------------------------------

  // GAP: `delta.deletedFileRetentionDuration` / `logRetentionDuration` --
  // these affect what versions are reachable for time-travel, not scan
  // output of the current version. Not actually a gap; documented here so
  // future audit readers know we considered it.
  // (No assertion -- this is a doc-only entry; keep as a comment so the
  //  audit trail in the suite stays grep-discoverable.)
}
