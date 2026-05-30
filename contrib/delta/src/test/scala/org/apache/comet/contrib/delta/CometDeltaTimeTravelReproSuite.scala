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

// Repro for Delta own-suite regression family F2 (time travel):
//   DeltaTimeTravelSuite / DeltaHistoryManager* tests fail because the native
//   scan returns data from the LATEST version instead of the requested
//   time-travel version. Observed: "Time travel with schema changes" expected
//   10 rows (v0) but Comet returned 20 (v1/head).
//
// Root cause (DeltaReflection.refreshedSnapshotFiles): for a
// PreparedDeltaFileIndex, extractBatchAddFiles calls deltaLog.update() which
// refreshes to HEAD, then filesForScan -- discarding the version the relation
// was pinned to. Correct for the consecutive-DELETE DV-staleness case it was
// added for, wrong for time travel.
class CometDeltaTimeTravelReproSuite extends CometDeltaTestBase {

  test("versionAsOf reads the pinned version, not head") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("tt_version") { tablePath =>
      val ss = spark
      import ss.implicits._
      // v0: 10 rows
      (1 to 10).toDF("id").write.format("delta").save(tablePath)
      // v1: append 10 more -> 20 rows at head
      (11 to 20).toDF("id").write.mode("append").format("delta").save(tablePath)

      val v0 = spark.read.format("delta").option("versionAsOf", "0").load(tablePath)
      val rows = v0.collect().map(_.getInt(0)).toSet
      assert(
        rows == (1 to 10).toSet,
        s"versionAsOf=0 must return v0's 10 rows, got ${rows.toList.sorted}")

      // Sanity: head still returns 20.
      val head = spark.read.format("delta").load(tablePath)
      assert(head.count() == 20, s"head must return 20 rows, got ${head.count()}")
    }
  }
}
