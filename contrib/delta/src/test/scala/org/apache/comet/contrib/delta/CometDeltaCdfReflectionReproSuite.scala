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

import org.apache.spark.sql.execution.datasources.LogicalRelation

// Groundwork for native CDC (#84): a readChangeFeed read is a RowDataSourceScanExec over a
// DeltaCDFRelation (BaseRelation with CatalystScan), NOT a FileSourceScanExec/HadoopFsRelation,
// which is why Comet doesn't currently intercept it. This suite proves (a) the plan shape and
// (b) that DeltaReflection can extract the table root + [start, end] version range from that
// relation -- the inputs the native kernel TableChanges read needs.
class CometDeltaCdfReflectionReproSuite extends CometDeltaTestBase {

  test("DeltaCDFRelation is reachable and its table-root + version range are extractable") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cdf_reflect") { tablePath =>
      spark.sql(
        s"""CREATE OR REPLACE TABLE delta.`$tablePath`(id INT, v STRING)
            USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (1, 'a'), (2, 'b')")
      spark.sql(s"UPDATE delta.`$tablePath` SET v = 'A' WHERE id = 1")

      val df = spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .option("endingVersion", "2")
        .load(tablePath)
      df.collect() // finalize plan

      // A CDF read starts as a RowDataSourceScanExec over the DeltaCDFRelation; Comet now intercepts
      // it (CometExecRule) and replaces it with a native CometDeltaCdfScanExec, so the relation is no
      // longer in the PHYSICAL plan. It remains in the analyzed (logical) plan as a LogicalRelation,
      // where the accessors run. Confirm the relation is reachable there + the accessors extract what
      // the native kernel TableChanges read needs.
      val logicalRelations = df.queryExecution.analyzed.collect {
        case lr: LogicalRelation => lr.relation
      }
      val cdfRel = logicalRelations
        .find(DeltaReflection.isCdfRelation)
        .getOrElse(fail(s"no DeltaCDFRelation found in:\n${df.queryExecution.analyzed}"))

      assert(
        DeltaReflection.extractCdfTableRoot(cdfRel).exists(_.nonEmpty),
        s"could not extract CDF table root from $cdfRel")
      assert(
        DeltaReflection.extractCdfVersions(cdfRel).contains((0L, Some(2L))),
        s"expected (0, Some(2)) got ${DeltaReflection.extractCdfVersions(cdfRel)}")
    }
  }

  test("readChangeFeed engages native CometDeltaCdfScanExec and matches vanilla") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cdf_native") { tablePath =>
      spark.sql(
        s"""CREATE OR REPLACE TABLE delta.`$tablePath`(id INT, v STRING)
            USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES (1, 'a'), (2, 'b')")
      spark.sql(s"UPDATE delta.`$tablePath` SET v = 'A' WHERE id = 1")
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id = 2")

      // _commit_timestamp is wall-clock / nondeterministic; drop it for the equality check.
      val read = () =>
        spark.read
          .format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", "0")
          .load(tablePath)
          .drop("_commit_timestamp")

      val df = read()
      df.collect() // finalize plan
      val plan = df.queryExecution.executedPlan
      val cdfExecs = collect(plan) {
        case s: org.apache.spark.sql.comet.CometDeltaCdfScanExec => s
      }
      assert(cdfExecs.nonEmpty, s"expected a native CometDeltaCdfScanExec in plan, got:\n$plan")

      val nativeRows = read().collect().toSeq.map(normalizeRow)
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        val vanillaRows = read().collect().toSeq.map(normalizeRow)
        assert(
          nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
          s"CDC native=$nativeRows vanilla=$vanillaRows")
      }
    }
  }

  test("readChangeFeed splits a multi-version range across Spark partitions (#2)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("cdf_split") { tablePath =>
      spark.sql(
        s"""CREATE OR REPLACE TABLE delta.`$tablePath`(id INT, v STRING)
            USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
      // Each INSERT is its own commit, so the [0, 6] range spans 7 commits -- enough to chunk.
      (1 to 6).foreach(i => spark.sql(s"INSERT INTO delta.`$tablePath` VALUES ($i, 'v$i')"))

      val read = () =>
        spark.read
          .format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", "0")
          .load(tablePath)
          .drop("_commit_timestamp")

      // Cap the split at 4 partitions. The single CometDeltaCdfScanExec should carry >= 2 sub-ranges
      // (so it emits multiple Spark partitions) WITHOUT a CometUnionExec wrapper -- it stays a single
      // CometNativeExec so a downstream native shuffle / aggregation remains eligible.
      withSQLConf("spark.comet.delta.cdf.maxPartitions" -> "4") {
        val df = read()
        df.collect() // finalize plan
        val plan = df.queryExecution.executedPlan
        val cdfExecs = collect(plan) {
          case s: org.apache.spark.sql.comet.CometDeltaCdfScanExec => s
        }
        assert(cdfExecs.nonEmpty, s"expected a native CometDeltaCdfScanExec in plan, got:\n$plan")
        assert(
          collect(plan) { case u: org.apache.spark.sql.comet.CometUnionExec => u }.isEmpty,
          s"the split must not introduce a CometUnionExec (breaks native shuffle), got:\n$plan")
        val ranges = cdfExecs.head.subRanges
        assert(
          ranges.size >= 2,
          s"expected the [0,6] range to split into >= 2 sub-ranges, got ${ranges.size}: $ranges")

        // The split must be correct: native (split) == vanilla Spark.
        val nativeRows = read().collect().toSeq.map(normalizeRow)
        withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
          val vanillaRows = read().collect().toSeq.map(normalizeRow)
          assert(
            nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
            s"split CDC native=$nativeRows vanilla=$vanillaRows")
        }
      }
    }
  }
}
