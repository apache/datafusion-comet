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

// Guards the per-file read-error provenance plumbing: a Delta native scan exposes its
// per-partition data-file paths through the `CometScanWithPlanData.perPartitionFilePaths`
// trait member, so that `CometNativeExec` can thread them into `CometExecRDD` and
// `CometExecIterator` can attribute a per-file read failure to the offending file
// (`FAILED_READ_FILE.NO_HINT`), mirroring `CometNativeScanExec`.
//
// Note: for read errors raised inside the kernel read itself, the native side already
// carries the path (the contrib's `map_file_read_error` is always called with `&file.path`,
// so `SparkError::CannotReadFile` is path-bearing -- this is why the corrupted-file case in
// `CometDeltaEdgeCaseRegressionSuite` F6 already surfaces a path-bearing `cannotReadFilesError`
// without this member). `perPartitionFilePaths` is the parity/fallback provenance used by the
// shared `CometExecRDD` path: `SparkErrorConverter` fills in the partition's file paths when a
// failure reaches the JVM without a native path. The guard below pins the member that feeds it.
class CometDeltaFailedReadFileSuite extends CometDeltaTestBase {

  test("CometDeltaNativeScanExec exposes per-partition data-file paths") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("a8_file_provenance") { tablePath =>
      val ss = spark
      import ss.implicits._
      // Two appends => at least two data files, so the per-partition mapping is non-trivial.
      Seq(1, 2, 3).toDF("id").write.format("delta").mode("append").save(tablePath)
      Seq(4, 5, 6).toDF("id").write.format("delta").mode("append").save(tablePath)

      val df = spark.read.format("delta").load(tablePath)
      df.collect() // materialise AQE so Comet's rules fire and the native scan is planted

      val scans =
        collect(df.queryExecution.executedPlan) { case s: CometDeltaNativeScanExec => s }
      assert(
        scans.nonEmpty,
        s"expected a CometDeltaNativeScanExec in the plan, got:\n${df.queryExecution.executedPlan}")

      val perPartition = scans.head.perPartitionFilePaths
      val allPaths = perPartition.flatten

      // RED before A.8 (the trait default returns Array.empty -> no paths); GREEN after, where
      // the override parses the per-partition DeltaScan task lists into their file paths.
      assert(
        allPaths.nonEmpty,
        "expected CometDeltaNativeScanExec.perPartitionFilePaths to expose the scan's data files, " +
          "but it was empty (FAILED_READ_FILE provenance not wired)")
      assert(
        allPaths.forall(_.endsWith(".parquet")),
        s"expected only .parquet data-file paths, got: ${allPaths.mkString(", ")}")

      // Every physical data file under the table must be represented across the partitions.
      val onDiskDataFiles = new java.io.File(tablePath)
        .listFiles()
        .filter(f => !f.getName.startsWith("_") && f.getName.endsWith(".parquet"))
        .map(_.getName)
        .toSet
      val namedFiles = allPaths.map(_.split("/").last).toSet
      assert(
        onDiskDataFiles.subsetOf(namedFiles),
        s"perPartitionFilePaths is missing data files; on-disk=$onDiskDataFiles named=$namedFiles")
    }
  }
}
