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

/**
 * Core read tests for the native Delta Lake scan path. Covers basic reads,
 * projections, filters, partitioning, schema evolution, time travel, complex
 * types, and primitive type coverage.
 *
 * Column mapping and deletion vector tests live in
 * [[CometDeltaColumnMappingSuite]]. Joins, aggregations, DPP, metrics, and
 * other advanced queries belong in a follow-up `CometDeltaAdvancedSuite`.
 *
 * Ported from the pre-SPI `delta-kernel-phase-1` branch with no semantic
 * changes -- this is the same vertical-slice coverage Phase-1 had, exercising
 * the current `CometDeltaNativeScanExec` plan-rewrite path via
 * [[CometDeltaTestBase#assertDeltaNativeMatches]].
 */
class CometDeltaNativeSuite extends CometDeltaTestBase {

  test("read a tiny unpartitioned delta table via the native scan") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("smoke") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
      // Explicit accelerator-coverage assertion: the contrib's scan exec must be
      // in the plan. Guards against silent disengagement bugs.
      assertNativePlanContains(
        spark.read.format("delta").load(tablePath),
        "CometDeltaNativeScanExec")
    }
  }

  test("multi-file delta table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("multifile") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 30)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(3)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("projection pushdown reads only selected columns") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("projection") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10)
        .map(i => (i.toLong, s"name_$i", i * 1.5, i % 2 == 0))
        .toDF("id", "name", "score", "active")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.select("id", "score"))
    }
  }

  test("partitioned delta table surfaces partition column values") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("partitioned") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 12)
        .map(i => (i.toLong, s"name_$i", if (i < 6) "a" else "b"))
        .toDF("id", "name", "category")
        .write
        .partitionBy("category")
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("filter pushdown returns correct rows") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("filter") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .repartition(2)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.where(col("id") >= 5 && col("id") < 15))
    }
  }

  test("predicate variety: eq, lt, gt, is null, in, and/or") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("predicates") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, if (i % 3 == 0) null else s"n_$i", i.toDouble))
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      // eq
      assertDeltaNativeMatches(tablePath, _.where(col("id") === 5))
      // lt + gt
      assertDeltaNativeMatches(tablePath, _.where(col("id") < 7 || col("id") > 15))
      // is null
      assertDeltaNativeMatches(tablePath, _.where(col("name").isNull))
      // in
      assertDeltaNativeMatches(tablePath, _.where(col("id").isin(1L, 4L, 9L, 16L)))
      // mixed
      assertDeltaNativeMatches(
        tablePath,
        _.where((col("id") > 5 && col("id") < 12) || col("name").isNull))
    }
  }

  test("empty delta table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("empty") { tablePath =>
      val ss = spark
      import ss.implicits._
      Seq.empty[(Long, String)]
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("multiple appends produce many files, native scan reads them all") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("appends") { tablePath =>
      val ss = spark
      import ss.implicits._
      for (batch <- 0 until 3) {
        (0 until 10)
          .map(i => ((batch * 10 + i).toLong, s"b${batch}_$i"))
          .toDF("id", "name")
          .repartition(1)
          .write
          .format("delta")
          .mode("append")
          .save(tablePath)
      }

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("multi-column partitioning") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("multicol-part") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 16)
        .map { i =>
          (i.toLong, s"n_$i", if (i < 8) "a" else "b", i % 4)
        }
        .toDF("id", "name", "p1", "p2")
        .write
        .partitionBy("p1", "p2")
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
      // Filter that prunes one partition column
      assertDeltaNativeMatches(tablePath, _.where(col("p1") === "a"))
      // Filter that prunes both partition columns
      assertDeltaNativeMatches(tablePath, _.where(col("p1") === "b" && col("p2") === 2))
    }
  }

  test("typed partition columns: int, long, date") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("typed-partitions") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 6)
        .map { i =>
          (
            i.toLong,
            s"n_$i",
            i,                                 // int partition
            (1000L + i),                       // long partition
            java.sql.Date.valueOf(s"2024-01-${i + 1}") // date partition
          )
        }
        .toDF("id", "name", "p_int", "p_long", "p_date")
        .write
        .partitionBy("p_int", "p_long", "p_date")
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
      // Partition prune by date
      assertDeltaNativeMatches(
        tablePath,
        _.where(col("p_date") === java.sql.Date.valueOf("2024-01-03")))
    }
  }

  test("schema evolution: new column added in later commit") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("schema-evo") { tablePath =>
      val ss = spark
      import ss.implicits._

      // V0: two columns
      (0 until 5)
        .map(i => (i.toLong, s"n_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      // V1: add a column with schema-evolution enabled
      ss.sql(s"ALTER TABLE delta.`$tablePath` ADD COLUMNS (extra INT)")
      (5 until 10)
        .map(i => (i.toLong, s"n_$i", Some(i * 100)))
        .toDF("id", "name", "extra")
        .write
        .format("delta")
        .mode("append")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("time travel by version reads the older snapshot") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("tt-version") { tablePath =>
      val ss = spark
      import ss.implicits._

      // V0: 3 rows
      (0 until 3).map(i => (i.toLong, s"v0_$i")).toDF("id", "name")
        .write.format("delta").save(tablePath)
      // V1: append 3 more
      (3 until 6).map(i => (i.toLong, s"v1_$i")).toDF("id", "name")
        .write.format("delta").mode("append").save(tablePath)

      // Read at version 0 -- should only see the original 3 rows.
      val v0Native =
        ss.read.format("delta").option("versionAsOf", "0").load(tablePath)
      val plan = v0Native.queryExecution.executedPlan
      assert(
        collect(plan) {
          case s: org.apache.spark.sql.comet.CometDeltaNativeScanExec => s
        }.nonEmpty,
        s"expected CometDeltaNativeScanExec in time-travel v0 plan:\n$plan")
      assert(v0Native.count() === 3)
    }
  }
}
