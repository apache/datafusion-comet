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

// Repro for DefaultRowCommitVersionSuite "can read default row commit versions".
// `_metadata.default_row_commit_version` is a per-file constant metadata column
// (= AddFile.defaultRowCommitVersion, the commit version that added the file) exposed when
// row tracking is enabled. Requesting it makes core_glue emit one file-group per file
// (need_per_file_groups). When several files pack into one Spark partition those per-file
// groups run concurrently and whole groups were dropped non-deterministically (here: the
// middle file's rows vanished -> 200 rows instead of 300). The fix forces one file per
// partition (CometDeltaNativeScan.needsPerFileGroups -> oneTaskPerPartition).
//
// NOTE: the drop is non-deterministic, so this is a best-effort local guard; the
// authoritative reproduction is the regression's DefaultRowCommitVersionSuite. Three
// single-row-group appends pack readily into one partition to maximise the chance of
// exercising the concurrent-group path.
class CometDeltaDefaultRowCommitVersionReproSuite extends CometDeltaTestBase {

  test("can read _metadata.default_row_commit_version") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    val rtKey = "spark.databricks.delta.properties.defaults.enableRowTracking"
    val rtPrev = spark.conf.getOption(rtKey)
    spark.conf.set(rtKey, "true")
    try withDeltaTable("default_rcv") { tablePath =>
      // Three appends -> three files at commit versions 0, 1, 2.
      spark.range(0, 100, 1, 1).write.format("delta").mode("append").save(tablePath)
      spark.range(100, 200, 1, 1).write.format("delta").mode("append").save(tablePath)
      spark.range(200, 300, 1, 1).write.format("delta").mode("append").save(tablePath)

      val got = spark.read.format("delta").load(tablePath)
        .select("id", "_metadata.default_row_commit_version")
        .collect()
        .map(r => (r.getLong(0), r.getLong(1)))
        .sortBy(_._1)
      val expected =
        ((0L until 100L).map((_, 0L)) ++
          (100L until 200L).map((_, 1L)) ++
          (200L until 300L).map((_, 2L))).toArray
      assert(
        got.sameElements(expected),
        s"default_row_commit_version wrong: sample=${got.take(3).toSeq} " +
          s"..${got.slice(98, 103).toSeq}.. (${got.length} rows)")
    } finally {
      rtPrev match {
        case Some(v) => spark.conf.set(rtKey, v)
        case None => spark.conf.unset(rtKey)
      }
    }
  }
}
