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

import java.io.File

import scala.collection.mutable

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.comet.{IcebergCommitExec, IcebergWriteExec}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * State sampled around each test action to check commit-once invariants. `snapshotDelta` is the
 * change in the Iceberg table's snapshot history during `action`; for a single Comet-Iceberg
 * commit it should be exactly `1` (one logical write = one Iceberg snapshot).
 */
private case class WriteSnapshot(snapshotDelta: Long, plans: Seq[SparkPlan])

/**
 * End-to-end coverage for Comet's two-op Iceberg V2 write path. Every test runs against a fresh
 * temporary Hadoop catalog (warehouse directory deleted at the end of `withTempIcebergDir`),
 * drives the write through Spark SQL, then asserts:
 *   - the captured executed plan contains the expected `IcebergCommitExec` + `IcebergWriteExec`
 *     pair (or, for fallback scenarios, does NOT contain them);
 *   - the committed data is queryable with the expected row set.
 *
 * Variant coverage mirrors Iceberg's copy-on-write V2 logical-write surface:
 *   - `AppendData` (INSERT INTO) -- unpartitioned, partitioned, empty result, INSERT FROM SELECT.
 *   - `OverwriteByExpression` (INSERT OVERWRITE static partition mode).
 *   - `OverwritePartitionsDynamic` (INSERT OVERWRITE dynamic partition mode).
 *   - `ReplaceData` (copy-on-write DELETE/UPDATE/MERGE).
 *
 * `WriteDelta` (merge-on-read DML) is intentionally not intercepted; it stays on Spark's default
 * path. Plus negative cases: config disabled => Spark's default path.
 */
class CometIcebergWriteActionSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometIcebergTestBase {

  override protected def sparkConf: SparkConf = {
    // Mirror the documented Comet + Iceberg setup in `docs/source/user-guide/latest/iceberg.md`:
    // `IcebergSparkSessionExtensions` provides the analyzer rules that lower UPDATE / MERGE on
    // V2 tables into `ReplaceData` -- without it, Spark 3.4's stock
    // `SparkStrategies$BasicOperators` throws `"UPDATE TABLE is not supported temporarily"` for
    // V2 paths and the SQL tests cancel.
    super.sparkConf
      .set(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key, "true")
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  }

  // --- AppendData (INSERT INTO) ------------------------------------------------------------------

  test("AppendData unpartitioned INSERT INTO routes through two-op") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "append_unpart", partitionSpec = "")
      val snapshot = captureWrite("append_unpart") {
        spark.sql(
          "INSERT INTO cat.db.append_unpart VALUES " +
            "(1, 'us-east', 10.5), (2, 'us-west', 20.3), (3, 'eu', 30.7)")
      }
      assertExactlyOneCommit(snapshot)
      assertRows("append_unpart", expectedIds = Seq(1, 2, 3))
    }
  }

  test("AppendData partitioned INSERT INTO routes through two-op") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "append_part", partitionSpec = "PARTITIONED BY (region)")
      val snapshot = captureWrite("append_part") {
        spark.sql(
          "INSERT INTO cat.db.append_part VALUES " +
            "(1, 'us-east', 10.5), (2, 'us-east', 20.3), (3, 'eu', 30.7)")
      }
      assertExactlyOneCommit(snapshot)
      assertRows("append_part", expectedIds = Seq(1, 2, 3))
    }
  }

  test("AppendData INSERT FROM SELECT survives the intervening exchange/sort") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "src", partitionSpec = "")
      createTable(warehouseDir, "append_from_select", partitionSpec = "PARTITIONED BY (region)")
      spark.sql(
        "INSERT INTO cat.db.src VALUES " +
          "(1, 'us-east', 10.5), (2, 'us-west', 20.3), (3, 'eu', 30.7)")

      val snapshot = captureWrite("append_from_select") {
        spark.sql(
          "INSERT INTO cat.db.append_from_select " +
            "SELECT id, region, amount FROM cat.db.src ORDER BY id")
      }
      // The capture sees the SELECT (driven by spark.read) AND the INSERT. We only care that
      // *some* captured plan has the two-op pair, since the SELECT plan won't.
      assertExactlyOneCommit(snapshot)
      assertRows("append_from_select", expectedIds = Seq(1, 2, 3))
    }
  }

  test("AppendData on an empty source still emits a single commit") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "empty_target", partitionSpec = "")
      val snapshot = captureWrite("empty_target") {
        spark.sql(
          "INSERT INTO cat.db.empty_target SELECT id, region, amount " +
            "FROM (SELECT 1 AS id, 'r' AS region, 1.0 AS amount) WHERE id < 0")
      }
      assertExactlyOneCommit(snapshot)
      assertRows("empty_target", expectedIds = Seq.empty)
    }
  }

  // --- OverwriteByExpression (INSERT OVERWRITE static) -------------------------------------------

  test("OverwriteByExpression replaces existing rows via two-op") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "overwrite_static", partitionSpec = "")
      spark.sql(
        "INSERT INTO cat.db.overwrite_static VALUES " +
          "(1, 'old', 1.0), (2, 'old', 2.0), (3, 'old', 3.0)")

      val snapshot = captureWrite("overwrite_static") {
        withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "STATIC") {
          spark.sql(
            "INSERT OVERWRITE cat.db.overwrite_static VALUES " +
              "(10, 'new', 100.0), (11, 'new', 110.0)")
        }
      }
      assertExactlyOneCommit(snapshot)
      assertRows("overwrite_static", expectedIds = Seq(10, 11))
    }
  }

  // --- OverwritePartitionsDynamic (INSERT OVERWRITE dynamic) -------------------------------------

  test("OverwritePartitionsDynamic replaces only touched partitions") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "overwrite_dynamic", partitionSpec = "PARTITIONED BY (region)")
      spark.sql(
        "INSERT INTO cat.db.overwrite_dynamic VALUES " +
          "(1, 'us-east', 1.0), (2, 'us-west', 2.0), (3, 'eu', 3.0)")

      val snapshot = captureWrite("overwrite_dynamic") {
        withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "DYNAMIC") {
          // Overwrite only the 'us-east' partition; 'us-west' and 'eu' should survive.
          spark.sql("INSERT OVERWRITE cat.db.overwrite_dynamic VALUES (10, 'us-east', 100.0)")
        }
      }
      assertExactlyOneCommit(snapshot)
      // Order changes after dynamic overwrite; assert membership rather than positional rows.
      val ids = spark
        .sql("SELECT id FROM cat.db.overwrite_dynamic ORDER BY id")
        .collect()
        .map(_.getInt(0))
        .toSeq
      assert(ids == Seq(2, 3, 10), s"expected (2,3,10), got $ids")
    }
  }

  // --- ReplaceData (copy-on-write DML) -----------------------------------------------------------

  test("ReplaceData (CoW DELETE) on a row predicate goes through two-op") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "cow_delete",
        partitionSpec = "",
        // Default V2 write mode is CoW already, but be explicit for resilience to upstream defaults.
        properties = Some("'write.delete.mode'='copy-on-write'"))
      // Use Spark's default path for the setup INSERT to isolate the DELETE under test from any
      // interaction with our two-op AppendData path. Test correctness depends on the DELETE alone.
      withSQLConf(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "false") {
        coalesceInsert(
          "cow_delete",
          Seq((1, "us-east", 10.0), (2, "us-west", 20.0), (3, "eu", 30.0), (4, "us-east", 40.0)))
      }

      val snapshot = captureWrite("cow_delete") {
        spark.sql("DELETE FROM cat.db.cow_delete WHERE id = 2")
      }
      assertExactlyOneCommit(snapshot)
      assertRows("cow_delete", expectedIds = Seq(1, 3, 4))
    }
  }

  test("ReplaceData (CoW UPDATE) routes through two-op") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "cow_update",
        partitionSpec = "",
        properties = Some("'write.update.mode'='copy-on-write'"))
      coalesceInsert(
        "cow_update",
        Seq((1, "us-east", 10.0), (2, "us-west", 20.0), (3, "eu", 30.0)))

      val snapshot = captureWrite("cow_update") {
        spark.sql("UPDATE cat.db.cow_update SET amount = amount * 2 WHERE id = 2")
      }
      assertExactlyOneCommit(snapshot)
      val r = spark
        .sql("SELECT id, amount FROM cat.db.cow_update WHERE id = 2")
        .collect()
      assert(r.length == 1 && r(0).getDouble(1) == 40.0, s"got ${r.toSeq}")
    }
  }

  test("ReplaceData (CoW MERGE) routes through two-op") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "cow_merge",
        partitionSpec = "",
        properties = Some("'write.merge.mode'='copy-on-write'"))
      coalesceInsert("cow_merge", Seq((1, "us-east", 10.0), (2, "us-west", 20.0)))

      // Use a one-row source via inline VALUES so the merge has both matched + unmatched legs.
      val snapshot = captureWrite("cow_merge") {
        spark.sql("""
          |MERGE INTO cat.db.cow_merge t
          |USING (SELECT 2 AS id, 'us-west' AS region, 200.0 AS amount UNION ALL
          |       SELECT 3 AS id, 'eu' AS region, 30.0 AS amount) s
          |ON t.id = s.id
          |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
          |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
          |""".stripMargin)
      }
      assertExactlyOneCommit(snapshot)
      assertRows("cow_merge", expectedIds = Seq(1, 2, 3))
    }
  }

  // --- Fall-back when config is disabled ---------------------------------------------------------

  test("sanity check: Spark's default DELETE path works against a Hadoop catalog") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      withSQLConf(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "false") {
        createTable(
          warehouseDir,
          "spark_cow_delete",
          partitionSpec = "",
          properties = Some("'write.delete.mode'='copy-on-write'"))
        coalesceInsert(
          "spark_cow_delete",
          Seq((1, "us-east", 10.0), (2, "us-west", 20.0), (3, "eu", 30.0), (4, "us-east", 40.0)))
        spark.sql("DELETE FROM cat.db.spark_cow_delete WHERE id = 2")
        assertRows("spark_cow_delete", expectedIds = Seq(1, 3, 4))
      }
    }
  }

  test("disabled config falls through to Spark's V2ExistingTableWriteExec") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "disabled_conf", partitionSpec = "")

      val snapshot = captureWrite("disabled_conf") {
        withSQLConf(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "false") {
          spark.sql("INSERT INTO cat.db.disabled_conf VALUES (1, 'us-east', 10.5)")
        }
      }
      // Spark's stock V2 write path still commits one snapshot to the table; we only assert
      // that Comet's two-op execs do NOT appear in the captured plans.
      val (commits, writes) = collectIcebergWriteOps(snapshot.plans)
      assert(commits.isEmpty, s"unexpected IcebergCommitExec: $commits")
      assert(writes.isEmpty, s"unexpected IcebergWriteExec: $writes")
      assertRows("disabled_conf", expectedIds = Seq(1))
    }
  }

  // --- Round-trip parity vs Spark default path ---------------------------------------------------

  test("Comet-written rows round-trip through Spark's reader unchanged") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "parity_comet", partitionSpec = "PARTITIONED BY (region)")
      createTable(warehouseDir, "parity_spark", partitionSpec = "PARTITIONED BY (region)")

      spark.sql(
        "INSERT INTO cat.db.parity_comet VALUES " +
          "(1, 'us', 1.5), (2, 'eu', 2.5), (3, 'ap', 3.5), (4, 'us', 4.5)")

      withSQLConf(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "false") {
        spark.sql(
          "INSERT INTO cat.db.parity_spark VALUES " +
            "(1, 'us', 1.5), (2, 'eu', 2.5), (3, 'ap', 3.5), (4, 'us', 4.5)")
      }

      val cometRows: Array[Row] = spark
        .sql("SELECT id, region, amount FROM cat.db.parity_comet ORDER BY id")
        .collect()
      val sparkRows: Array[Row] = spark
        .sql("SELECT id, region, amount FROM cat.db.parity_spark ORDER BY id")
        .collect()
      assert(cometRows.toSeq == sparkRows.toSeq, s"$cometRows vs $sparkRows")
    }
  }

  // --- Helpers -----------------------------------------------------------------------------------

  private val catalog = "cat"
  private val ns = "db"

  /**
   * Spin up an isolated Hadoop catalog under a temp dir and run `f`. Tests share the catalog name
   * within a single `withIcebergCatalog` block but get a fresh warehouse directory per outer
   * call.
   */
  private def withIcebergCatalog(f: File => Unit): Unit = withTempIcebergDir { warehouseDir =>
    withSQLConf(
      s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog.warehouse" -> warehouseDir.getAbsolutePath,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      f(warehouseDir)
    }
  }

  private def createTable(
      warehouseDir: File,
      tableName: String,
      partitionSpec: String,
      properties: Option[String] = None): Unit = {
    val props = properties.map(s => s" TBLPROPERTIES ($s)").getOrElse("")
    spark.sql(s"""
      CREATE TABLE $catalog.$ns.$tableName (
        id INT,
        region STRING,
        amount DOUBLE
      ) USING iceberg
      $partitionSpec
      $props
    """)
  }

  /**
   * INSERT a small in-memory dataset into `tableName` as a single data file. `INSERT INTO ...
   * VALUES (...), (...)` can be split per VALUES row into multiple files; `coalesce(1)` keeps the
   * setup consistent so DML predicates target a single file, exercising the rewrite path
   * deterministically. Uses the COMET write path itself (no fallback) so the setup also serves as
   * additional AppendData coverage.
   */
  private def coalesceInsert(tableName: String, rows: Seq[(Int, String, Double)]): Unit = {
    // `spark` is a `def` on the base class, so `spark.implicits._` is not a stable path. Capture
    // a local handle first so the implicit `Encoder`s come from a stable identifier.
    val session = spark
    import session.implicits._
    rows
      .toDF("id", "region", "amount")
      .coalesce(1)
      .writeTo(s"$catalog.$ns.$tableName")
      .append()
  }

  /**
   * Count the snapshot history of `tableName` before and after `action`, capturing executed plans
   * in between. The delta = number of snapshots committed during `action`; a successful
   * single-write should produce exactly one new snapshot. Driven from Iceberg's
   * `<table>.snapshots` metadata table (no production-side counter needed) -- the snapshot
   * history is the canonical record of commits, and using it here keeps tests honest against what
   * Iceberg actually persisted.
   *
   * AQE finalisation can fire multiple `QueryExecutionListener.onSuccess` events for a single
   * command, so we additionally accumulate every captured `executedPlan` -- callers check
   * `IcebergCommitExec` / `IcebergWriteExec` presence in the union of all captures.
   */
  private def captureWrite(tableName: String)(action: => Unit): WriteSnapshot = {
    val before = countSnapshots(tableName)
    val captured = mutable.Buffer.empty[SparkPlan]
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        captured += qe.executedPlan
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        ()
    }
    spark.listenerManager.register(listener)
    try {
      action
      // Drain the SparkContext listener bus so any post-action SparkListener events have a
      // chance to land before we read `captured`. We don't *require* the bus to be empty (the
      // QueryExecutionListener fires synchronously enough for our purposes), so a timeout here
      // -- which Iceberg 1.5.2's `<table>.snapshots` query path can trigger on Spark 3.4 by
      // queueing more SparkListener traffic than `waitUntilEmpty`'s default 10s budget -- is
      // not fatal to the test.
      try CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      catch { case _: java.util.concurrent.TimeoutException => () }
    } finally {
      spark.listenerManager.unregister(listener)
    }
    val after = countSnapshots(tableName)
    WriteSnapshot(after - before, captured.toSeq)
  }

  /**
   * Read the current snapshot count from Iceberg's `<table>.snapshots` metadata table. Returns
   * `0` if the table has no snapshots yet (e.g. an empty newly-created table) and short-circuits
   * to `0` if the snapshot read throws (the catalog might not yet have indexed a brand-new table,
   * in which case "no snapshots" is the right interpretation).
   */
  private def countSnapshots(tableName: String): Long =
    try {
      spark
        .sql(s"SELECT count(*) FROM $catalog.$ns.$tableName.snapshots")
        .collect()
        .head
        .getLong(0)
    } catch {
      case _: Throwable => 0L
    }

  private def collectIcebergWriteOps(
      plans: Seq[SparkPlan]): (Seq[IcebergCommitExec], Seq[IcebergWriteExec]) = {
    val commits = plans.flatMap { plan =>
      collectWithSubqueries(plan) { case c: IcebergCommitExec => c }
    }
    val writes = plans.flatMap { plan =>
      collectWithSubqueries(plan) { case w: IcebergWriteExec => w }
    }
    (commits, writes)
  }

  /**
   * Strongest invariant we can pin on a single Comet-Iceberg write: the Iceberg table's snapshot
   * history advanced by exactly one entry, and the captured plans contain at least one
   * `IcebergCommitExec` plus at least one `IcebergWriteExec`.
   *
   * The snapshot-delta check guards against AQE re-planning, logical-link drift, and any future
   * planner-level bug that could re-emit `IcebergCommitExec` mid-execution -- each extra commit
   * produces a new snapshot, so duplicates surface as a delta > 1. The plan-presence check guards
   * against regressions that silently fall back to Spark's own V2 path.
   *
   * Plan counts >=1 because AQE finalisation can fire multiple `onSuccess` events for a single
   * write, each carrying the same operator instances; exact event count isn't load-bearing.
   */
  private def assertExactlyOneCommit(snapshot: WriteSnapshot): Unit = {
    assert(
      snapshot.snapshotDelta == 1L,
      s"expected exactly 1 new Iceberg snapshot, got ${snapshot.snapshotDelta}. Plans:\n" +
        snapshot.plans.mkString("\n--\n"))
    val (commits, writes) = collectIcebergWriteOps(snapshot.plans)
    assert(
      commits.nonEmpty,
      s"expected >= 1 IcebergCommitExec in captured plans, got ${commits.size}. Plans:\n" +
        snapshot.plans.mkString("\n--\n"))
    assert(
      writes.nonEmpty,
      s"expected >= 1 IcebergWriteExec in captured plans, got ${writes.size}. Plans:\n" +
        snapshot.plans.mkString("\n--\n"))
  }

  private def assertRows(tableName: String, expectedIds: Seq[Int]): Unit = {
    val ids = spark
      .sql(s"SELECT id FROM $catalog.$ns.$tableName ORDER BY id")
      .collect()
      .map(_.getInt(0))
      .toSeq
    assert(ids == expectedIds, s"expected $expectedIds, got $ids")
  }

}
