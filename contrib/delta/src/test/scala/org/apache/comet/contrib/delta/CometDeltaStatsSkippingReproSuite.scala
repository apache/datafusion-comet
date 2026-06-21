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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.comet.{CometDeltaNativeScanExec, CometDeltaScanMarker, CometScanExec}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions.col

// Investigates StatsCollectionSuite "gather stats", which fails under Comet with
// `java.util.NoSuchElementException: None.get` from `recordsScanned`. That helper locates
// the scan in `executedPlan` and reads its `numOutputRows`. Two things broke it under Comet:
//
//   1. (native bug) `CometDeltaNativeScanExec` never recorded output rows -- it streamed
//      batches with no BaselineMetrics, so `numOutputRows` was always 0. Fixed in
//      contrib/delta/native/src/kernel_scan.rs (BaselineMetrics.record_output per batch).
//
//   2. (plan-shape) the Delta scan appears in DIFFERENT shapes depending on whether AQE
//      wrapped the plan, which gates Comet's marker->native conversion (it runs in AQE
//      query-stage prep):
//        - AQE on  (this base forces it): CometDeltaNativeScanExec (converted);
//        - AQE off (Delta's own suites disable it for deterministic plans): the plan keeps
//          a CometDeltaScanMarker -- a LeafExecNode that delegates execution to the wrapped
//          FileSourceScanExec (vanilla Delta read), so its rows live on `originalScan`.
//      The suite's pre-execution `.find` also missed the scan entirely under AQE.
//
// This suite proves the data-skipping behaviour is correct in BOTH shapes by reading the
// scan's numOutputRows the same robust way the diff patches `recordsScanned` to: materialize
// first, strip AQE, then read the metric off whichever scan node the plan carries.
class CometDeltaStatsSkippingReproSuite extends CometDeltaTestBase {

  private def numOutputRowsOfScan(df: DataFrame): Long = {
    // Materialize ONCE: finalizes the plan AND populates the executed scan's metric. Do not
    // execute the scan node again -- the native metric accumulates and would double-count.
    df.collect()
    val plan = df.queryExecution.executedPlan match {
      case a: AdaptiveSparkPlanExec => a.executedPlan
      case p => p
    }
    val metrics = plan.collectFirst {
      case s: CometDeltaNativeScanExec => s.metrics // AQE on: converted native scan
      case m: CometDeltaScanMarker => m.originalScan.metrics // AQE off: marker -> wrapped scan
      case s: CometScanExec => s.metrics
      case f: FileSourceScanExec => f.metrics
    }.getOrElse(fail(s"no Delta scan node found in finalized plan:\n$plan"))
    metrics("numOutputRows").value
  }

  private def writeGatherStatsTable(path: String): Unit = {
    // Exact mirror of StatsCollectionSuite "gather stats": 9 rows (1..9) across 10 partitions
    // (~1 row per file), partitioned by `odd = id % 2 == 1`.
    spark
      .range(1, 10, 1, 10)
      .withColumn("odd", col("id") % 2 === 1)
      .write
      .partitionBy("odd")
      .format("delta")
      .save(path)
  }

  private def checkSkipping(): Unit = {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      writeGatherStatsTable(path)
      val df = spark.read.format("delta").load(path)
      // Disable parquet row-level pushdown exactly as the suite does, so the only thing that
      // can reduce the scan's output below the full row set is Delta data skipping.
      withSQLConf("spark.sql.parquet.filterPushdown" -> "false") {
        assert(numOutputRowsOfScan(df) == 9, "unfiltered read should scan all 9 rows")
        assert(
          numOutputRowsOfScan(df.where("id = 1")) == 1,
          "id = 1 should data-skip to the single file whose min/max bracket 1")
      }
    }
  }

  test("data skipping parity with AQE on (marker converted to native scan)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withSQLConf("spark.sql.adaptive.enabled" -> "true") {
      checkSkipping()
    }
  }

  test("data skipping parity with AQE off (CometDeltaScanMarker fallback)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    // Reproduce Delta's own-suite environment, where AQE is off so the marker is left in the
    // plan and delegates to the vanilla FileSourceScanExec.
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      checkSkipping()
    }
  }
}
