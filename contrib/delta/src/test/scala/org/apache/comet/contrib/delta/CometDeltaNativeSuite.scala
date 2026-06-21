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

  test("kernel-read path (Phase 1b): plain table reads correctly and engages DeltaKernelScanExec") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("kernel_read_smoke") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      // Correctness: the kernel-read result matches vanilla Spark (and stays on the native
      // CometDeltaNativeScanExec, i.e. no Spark-side fallback).
      assertDeltaNativeMatches(tablePath, identity)
      // Routing: kernel-read via DeltaKernelScanExec is the only path, so it engaged.
      assertKernelReadEngaged(tablePath)
    }
  }

  test("kernel-read path (Phase 1c #44): name-mode column-mapped table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("kernel_read_cm") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 8)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.columnMapping.mode", "name")
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .save(tablePath)
      // Rename so a logical name diverges from its physical name (the real column-mapping case).
      spark.sql(s"ALTER TABLE delta.`$tablePath` RENAME COLUMN name TO full_name")

      // Force name-mode resolution: with parquet field-id read off, the kernel-read path reads by
      // physical name and relabels to logical via the identity transform.
      withSQLConf("spark.sql.parquet.fieldId.read.enabled" -> "false") {
        assertDeltaNativeMatches(tablePath, identity)
        assertKernelReadEngaged(tablePath)
        // Projection of the renamed column also reads correctly.
        assertDeltaNativeMatches(tablePath, _.select("id", "full_name"))
      }
    }
  }

  test("kernel-read path (#47): nested column-mapped table with a nested rename") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("kernel_read_cm_nested") { tablePath =>
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath` (
           |  id INT,
           |  s STRUCT<a:INT, b:STRING>,
           |  arr ARRAY<STRUCT<x:INT>>)
           |USING delta
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.minReaderVersion' = '2',
           |  'delta.minWriterVersion' = '5')""".stripMargin)
      spark.sql(
        s"""INSERT INTO delta.`$tablePath` VALUES
           |(1, NAMED_STRUCT('a', 10, 'b', 'x'), ARRAY(NAMED_STRUCT('x', 100))),
           |(2, NULL, ARRAY()),
           |(3, NAMED_STRUCT('a', 30, 'b', 'z'), ARRAY(NAMED_STRUCT('x', 300), NAMED_STRUCT('x', 301)))
           |""".stripMargin)
      // Rename a NESTED field so its logical name diverges from its physical name -- the case that
      // requires the kernel-read path to physicalise + relabel at every nesting level (#47).
      spark.sql(s"ALTER TABLE delta.`$tablePath` RENAME COLUMN s.a TO renamed_a")

      withSQLConf("spark.sql.parquet.fieldId.read.enabled" -> "false") {
        assertDeltaNativeMatches(tablePath, _.orderBy("id"))
        assertKernelReadEngaged(tablePath)
        // Project into the renamed nested field (nested pruning + relabel on the kernel path).
        assertDeltaNativeMatches(tablePath, _.select("id", "s.renamed_a").orderBy("id"))
        assertDeltaNativeMatches(tablePath, _.select("id", "arr").orderBy("id"))
      }
    }
  }

  test("kernel-read path (#48): zero-data-column read (partition-only count)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("kernel_read_partonly") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 12)
        .map(i => (i.toLong, i % 3, s"v$i"))
        .toDF("id", "grp", "v")
        .write
        .format("delta")
        .partitionBy("grp")
        .save(tablePath)
      // Partition-only aggregate: no data column is read, so the row count is driven from
      // record_count (the parquet footer as fallback) -- exercises the #48 zero-data-column path.
      assertDeltaNativeMatches(
        tablePath,
        _.groupBy("grp").agg(count("*").as("c")).orderBy("grp"))
      assertKernelReadEngaged(tablePath)
      // A bare row count over the partition column also reads no data columns.
      assertDeltaNativeMatches(tablePath, _.select("grp").orderBy("grp"))
    }
  }

  test("kernel-read path (Phase 1c #44): id-mode column-mapped table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("kernel_read_cm_id") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 8)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.columnMapping.mode", "id")
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .save(tablePath)
      // Rename so a logical name diverges from its physical name.
      spark.sql(s"ALTER TABLE delta.`$tablePath` RENAME COLUMN name TO full_name")

      // id-mode reads through the same rename-then-relabel kernel path; field ids ride along on
      // the physical schema as a fallback matcher.
      assertDeltaNativeMatches(tablePath, identity)
      assertKernelReadEngaged(tablePath)
      assertDeltaNativeMatches(tablePath, _.select("id", "full_name"))
    }
  }

  test("kernel-read path (Phase 1c #45): partitioned table with projections + filters") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("kernel_read_part") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 12)
        .map(i => (i.toLong, s"name_$i", i % 3))
        .toDF("id", "name", "part")
        .write
        .format("delta")
        .partitionBy("part")
        .save(tablePath)

      // The scan outputs data ++ partition; the kernel exec reproduces that, so projections
      // and partition filters all work without special handling.
      assertDeltaNativeMatches(tablePath, identity) // SELECT *
      assertKernelReadEngaged(tablePath)
      assertDeltaNativeMatches(tablePath, _.select("id")) // data-only projection
      assertDeltaNativeMatches(tablePath, _.select("id", "part")) // data + partition
      assertDeltaNativeMatches(tablePath, _.select("part", "name")) // reordered
      assertDeltaNativeMatches(tablePath, _.where("part = 1")) // partition filter
    }
  }

  test("kernel-read path (Phase 1c #46): _metadata columns + DELETE via deletion vectors") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("kernel_read_synth") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, s"v_$i"))
        .toDF("id", "v")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .save(tablePath)

      // _metadata.* is synthesized in-worker by DeltaKernelScanExec.
      assertDeltaNativeMatches(tablePath, _.select($"id", $"_metadata.file_path"))
      // DELETE writes a deletion vector; the read applies it in-worker, and the surviving rows
      // must match vanilla.
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id % 4 = 0")
      assertDeltaNativeMatches(tablePath, identity)
      assertKernelReadEngaged(tablePath)
    }
  }

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
      // Materialise BEFORE inspecting the plan so AQE's query-stage prep rules
      // (incl. Comet's) have fired (see CometDeltaTestBase plan-ordering note).
      val nativeRows = v0Native.collect().toSeq.map(normalizeRow)
      val plan = v0Native.queryExecution.executedPlan
      assert(
        collect(plan) {
          case s: org.apache.spark.sql.comet.CometDeltaNativeScanExec => s
        }.nonEmpty,
        s"expected CometDeltaNativeScanExec in time-travel v0 plan:\n$plan")
      assert(nativeRows.size === 3)
      // Compare CONTENT, not just count, against vanilla at the same pinned version,
      // so a scan returning the right count from the wrong version is caught.
      withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
        val vanillaRows = ss.read.format("delta").option("versionAsOf", "0")
          .load(tablePath).collect().toSeq.map(normalizeRow)
        assert(
          nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
          s"time-travel v0 native=$nativeRows vanilla=$vanillaRows")
      }
    }
  }
}
