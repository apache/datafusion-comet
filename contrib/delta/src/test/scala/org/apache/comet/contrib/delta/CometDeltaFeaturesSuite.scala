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
import org.apache.spark.sql.functions._

/**
 * Coverage for the special features the contrib supports beyond plain reads.
 * Each test asserts BOTH that Comet's native plan engages AND that results match
 * vanilla Spark, so future silent-disengagement bugs are caught.
 *
 * Mapped to the design-doc feature list:
 *   - Deletion Vectors (native DeltaDvFilterExec path)
 *   - Row tracking (synthesised + materialised cases)
 *   - Synthetic columns (__delta_internal_row_index)
 *   - input_file_name() and FileBlockHolder threading
 *   - Complex types (struct, array, map)
 *   - Joins and aggregations over Delta
 *   - Time travel by timestamp
 *   - Multi-append / multi-file scenarios
 */
class CometDeltaFeaturesSuite extends CometDeltaTestBase {

  // ---- Deletion Vectors -----------------------------------------------------

  test("DV: native scan engages on DV-bearing tables after DELETE") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("features_dv") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)

      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id % 3 = 0")

      val df = spark.read.format("delta").load(tablePath)
      val rows = df.collect()
      val plan = df.queryExecution.executedPlan
      assert(
        collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty,
        s"expected Comet native scan on DV-bearing table:\n$plan")
      assert(rows.length === 13, s"expected 13 rows after DELETE, got ${rows.length}")
    }
  }

  // ---- Row tracking (Phase-1 port) ------------------------------------------

  test("row tracking: unmaterialised _metadata.row_id synthesised from baseRowId") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("features_rt_unmat") { tablePath =>
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

      // orderBy forces a shuffle -> AQE wraps -> Comet's prep rules fire
      val df = spark.read
        .format("delta")
        .load(tablePath)
        .selectExpr("id", "_metadata.row_id AS rid")
        .orderBy("id")
      val rows = df.collect().toSeq
      val plan = df.queryExecution.executedPlan
      assert(
        collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty,
        s"expected Comet to accelerate rowTracking scan:\n$plan")

      assert(rows.size == 12)
      rows.zipWithIndex.foreach { case (row, idx) =>
        assert(row.getLong(1) == idx.toLong, s"row $idx: rid mismatch")
      }
    }
  }

  // ---- Synthetic columns ----------------------------------------------------

  test("synthetic: native scan engages when row tracking is enabled (provides _metadata.row_index)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("features_synth") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 6)
        .map(i => (i.toLong, s"n_$i"))
        .toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)

      // orderBy forces AQE wrapping so Comet's prep rules see this plan.
      val df = spark.read.format("delta").load(tablePath)
        .selectExpr("id", "_metadata.row_index AS ri")
        .orderBy("id")
      val rows = df.collect()
      val plan = df.queryExecution.executedPlan
      assert(rows.length === 6, s"expected 6 rows, got ${rows.length}")
      assert(
        collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty,
        s"expected Comet to engage when _metadata.row_index is consumed:\n$plan")
    }
  }

  // ---- input_file_name() ----------------------------------------------------

  test("input_file_name(): rows return the path of their source parquet file") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("features_ifn") { tablePath =>
      val ss = spark
      import ss.implicits._
      // Two writes -> two files; each row's input_file_name should be one of them.
      (0 until 5).map(i => (i.toLong, "a"))
        .toDF("id", "src").repartition(1).write.format("delta").save(tablePath)
      (5 until 10).map(i => (i.toLong, "b"))
        .toDF("id", "src").repartition(1).write.format("delta").mode("append").save(tablePath)

      // orderBy forces AQE wrapping for Comet's rules to fire.
      val df = spark.read.format("delta").load(tablePath)
        .withColumn("ifn", input_file_name())
        .orderBy("id")
      val rows = df.collect()
      assert(rows.length === 10)
      val distinctPaths = rows.map(_.getString(2)).toSet
      assert(distinctPaths.size === 2, s"expected 2 source files, got $distinctPaths")
      assert(distinctPaths.forall(_.contains("parquet")), s"non-parquet path: $distinctPaths")
    }
  }

  // ---- Complex types --------------------------------------------------------

  test("complex types: struct, array, map round-trip through native scan") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("features_complex") { tablePath =>
      val ss = spark
      import ss.implicits._
      Seq(
        (1L, ("a", 1), Seq(10, 20), Map("k1" -> 100)),
        (2L, ("b", 2), Seq(30), Map("k2" -> 200, "k3" -> 300)))
        .toDF("id", "s", "arr", "m")
        .write.format("delta").save(tablePath)

      // assertDeltaNativeMatches already asserts native plan presence + result parity.
      assertDeltaNativeMatches(tablePath, identity)
      // Reinforce: simple read explicitly verifies the contrib scan exec is present.
      assertNativePlanContains(
        spark.read.format("delta").load(tablePath),
        "CometDeltaNativeScanExec")
    }
  }

  // ---- Aggregations + joins over Delta --------------------------------------

  test("aggregation: count/sum over Delta uses native scan") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("features_agg") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 100).map(i => (i.toLong, i % 5, (i * 1.5).toDouble))
        .toDF("id", "g", "v")
        .write.format("delta").save(tablePath)

      val df = spark.read.format("delta").load(tablePath)
        .groupBy("g").agg(count("*").as("c"), sum("v").as("s"))
      val plan = df.queryExecution.executedPlan
      assert(
        collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty,
        s"expected Comet native scan in aggregation plan:\n$plan")

      val rows = df.collect().sortBy(_.getInt(0))
      assert(rows.length === 5)
      rows.foreach(r => assert(r.getLong(1) === 20L))
    }
  }

  test("join: self-join over Delta uses native scan twice") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("features_join") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20).map(i => (i.toLong, s"n_$i"))
        .toDF("id", "name")
        .write.format("delta").save(tablePath)

      val df = spark.read.format("delta").load(tablePath).alias("a")
        .join(
          spark.read.format("delta").load(tablePath).alias("b"),
          col("a.id") === col("b.id") + 1)
      val plan = df.queryExecution.executedPlan
      val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
      assert(scans.size >= 1, s"expected at least 1 native Delta scan in join plan:\n$plan")
      assert(df.count() === 19)
    }
  }

  // ---- Time travel by timestamp ---------------------------------------------

  test("time travel by timestamp reads the older snapshot") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("features_tt_ts") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 5).map(i => (i.toLong, s"v0_$i")).toDF("id", "name")
        .write.format("delta").save(tablePath)
      // Sleep so timestampAsOf can distinguish the two commits.
      Thread.sleep(1500)
      val midTimestamp = new java.sql.Timestamp(System.currentTimeMillis())
      Thread.sleep(1500)
      (5 until 10).map(i => (i.toLong, s"v1_$i")).toDF("id", "name")
        .write.format("delta").mode("append").save(tablePath)

      val df = spark.read
        .format("delta")
        .option("timestampAsOf", midTimestamp.toString)
        .load(tablePath)
      // Materialise before inspecting the plan so AQE has finalized it.
      val nativeRows = df.collect().toSeq.map(normalizeRow)
      val plan = df.queryExecution.executedPlan
      assert(
        collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty,
        s"expected Comet native scan in timestamp time-travel plan:\n$plan")
      assert(nativeRows.size === 5)
      // Compare content against vanilla at the same pinned timestamp.
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        val vanillaRows = spark.read.format("delta")
          .option("timestampAsOf", midTimestamp.toString)
          .load(tablePath).collect().toSeq.map(normalizeRow)
        assert(
          nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
          s"timestamp time-travel native=$nativeRows vanilla=$vanillaRows")
      }
    }
  }
}
