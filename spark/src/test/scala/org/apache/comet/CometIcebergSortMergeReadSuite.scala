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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{Add, Ascending, AttributeReference, Literal, SortOrder}
import org.apache.spark.sql.comet.{CometIcebergNativeScanExec, CometSortExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.types.IntegerType

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.serde.operator.CometIcebergNativeScan

/**
 * Tests for the sort-aware native Iceberg scan (branch `stream-merge`): the scan reports the
 * Iceberg table sort order to Spark and does a per-partition streaming k-way merge of the
 * already-sorted files, so Catalyst can drop the Sort and (with storage-partitioned join) the
 * Exchange that a sort-merge join / grouped aggregate / window would otherwise need.
 *
 * The suite has two parts:
 *   - Unit tests of the [[CometIcebergNativeScan.reportableOrdering]] gate (no SparkSession
 *     required) -- the single decision shared by the proto serialization (which turns on the
 *     native SortPreservingMergeExec) and CometIcebergNativeScanExec.outputOrdering (which tells
 *     Spark the scan is sorted).
 *   - End-to-end tests over real Iceberg tables that exercise every Spark 4.0 mechanism which
 *     exploits already-sorted input to avoid a Sort or a shuffle (all keyed off
 *     `SortOrder.orderingSatisfies`, prefix semantics): `SupportsReportOrdering` ->
 *     `BatchScanExec.outputOrdering` and `EnsureRequirements` eliding the required-child Sort;
 *     `EliminateSorts` / `RemoveRedundantSorts`; `SortMergeJoinExec.requiredChildOrdering` +
 *     storage-partitioned join (`KeyGroupedPartitioning`,
 *     `spark.sql.sources.v2.bucketing.enabled`); `ReplaceHashWithSortAgg` / `SortAggregateExec`;
 *     `WindowExec`; `TakeOrderedAndProjectExec`.
 *
 * Two invariants determine the end-to-end assertions:
 *   1. Correctness is checked unconditionally via `checkSparkAnswer` (Comet vs vanilla Spark).
 *      This is the primary guarantee: any k-way-merge defect (dropped, duplicated or mis-ordered
 *      rows, or an outputOrdering/outputPartitioning that does not match the rows the native
 *      operator actually produces) shows up as a result mismatch. It holds on any Iceberg build.
 *      2. The strict "no Sort / no Exchange" plan assertions are the *target* of this feature.
 *      They only hold where the Iceberg build actually reports the ordering
 *      (`SupportsReportOrdering`, today an Iceberg fork feature -- the published/upstream Iceberg
 *      used in CI does not report it) and where the native scan reports `KeyGroupedPartitioning`.
 *      Each such test therefore runs the correctness check first, then `assume`s the reporting is
 *      active before asserting the plan shape, so it enforces the contract on a reporting build
 *      and is skipped (not failed) elsewhere. `sort = 0` is asserted only for *operator-required*
 *      orderings (SMJ / aggregate / window), never for a global `ORDER BY`, which Spark keeps
 *      regardless (the per-partition merge is not a global order).
 *
 * These cannot be SQL-file fixtures: setting an Iceberg sort order needs the Iceberg Java API
 * (the Comet test session registers no Iceberg SQL extensions, so `WRITE ORDERED BY` will not
 * parse), and the plan-shape assertions need access to the executed SparkPlan.
 *
 * Each test gets its own catalog name and temp warehouse (the Hadoop `SparkCatalog` instance is
 * cached per catalog name, so a shared name would bind every test to the first warehouse), and
 * its tables are dropped in a `finally` so a failing test cannot leak a table into a later one.
 */
class CometIcebergSortMergeReadSuite
    extends CometTestBase
    with CometIcebergTestBase
    with AdaptiveSparkPlanHelper {

  // ---------------------------------------------------------------------------------------------
  // Unit tests of the reportableOrdering gate (merged from the former CometIcebergSortMergeSuite).
  // v1 identity-scope gate as a pure function, independent of a live SparkSession or an Iceberg
  // build that reports ordering. The flag defaults to enabled, so no SQLConf override is needed.
  // ---------------------------------------------------------------------------------------------

  private val gateA = AttributeReference("a", IntegerType)()
  private val gateB = AttributeReference("b", IntegerType)()

  test("gate: identity ordering on projected columns is reportable") {
    val ordering = Seq(SortOrder(gateA, Ascending))
    assert(
      CometIcebergNativeScan.reportableOrdering(Some(ordering), Seq(gateA, gateB)) === ordering)
  }

  test("gate: ordering on a column outside the projection falls back") {
    val ordering = Seq(SortOrder(gateA, Ascending))
    assert(CometIcebergNativeScan.reportableOrdering(Some(ordering), Seq(gateB)).isEmpty)
  }

  test("gate: a transform (non-AttributeReference) sort child falls back") {
    val ordering = Seq(SortOrder(Add(gateA, Literal(1)), Ascending))
    assert(CometIcebergNativeScan.reportableOrdering(Some(ordering), Seq(gateA)).isEmpty)
  }

  test("gate: if any sort field is unreportable, the whole ordering falls back") {
    val ordering = Seq(SortOrder(gateA, Ascending), SortOrder(Add(gateB, Literal(1)), Ascending))
    assert(CometIcebergNativeScan.reportableOrdering(Some(ordering), Seq(gateA, gateB)).isEmpty)
  }

  test("gate: absent or empty ordering falls back") {
    assert(CometIcebergNativeScan.reportableOrdering(None, Seq(gateA)).isEmpty)
    assert(CometIcebergNativeScan.reportableOrdering(Some(Seq.empty), Seq(gateA)).isEmpty)
  }

  test("gate: a sort key on an ordering-unsafe column (e.g. UUID) falls back") {
    // "a" stands in for a UUID column: Iceberg maps it to StringType but sorts by its own order,
    // so it is unsafe to honour even though it looks like a plain string at the Spark level.
    val ordering = Seq(SortOrder(gateA, Ascending))
    assert(
      CometIcebergNativeScan
        .reportableOrdering(Some(ordering), Seq(gateA, gateB), Set("a"))
        .isEmpty)
  }

  // ---------------------------------------------------------------------------------------------
  // End-to-end fixtures.
  // ---------------------------------------------------------------------------------------------

  private val catalogCounter = new AtomicInteger(0)

  // preserve-data-ordering makes Iceberg report the table sort order; adaptive off keeps the
  // executed plan stable for the sort/shuffle counts below. preserve-data-grouping is also set
  // because newer Iceberg builds reject preserve-data-ordering without it (SparkPartitioningAware
  // Scan throws "Cannot preserve data ordering without data grouping"); it is a harmless no-op on
  // builds that do not require it.
  private val orderedReadConf: Seq[(String, String)] = Seq(
    "spark.sql.iceberg.planning.preserve-data-ordering" -> "true",
    "spark.sql.iceberg.planning.preserve-data-grouping" -> "true",
    "spark.sql.adaptive.enabled" -> "false")

  // Storage-partitioned join config. preserve-data-grouping + v2 bucketing let Iceberg report
  // KeyGroupedPartitioning on the BatchScanExec, and the join knobs force a sort-merge join over
  // co-partitioned inputs so the Exchange can be eliminated. Comet does not report partitioning
  // itself; Spark's EnsureRequirements eliminates the shuffle on the BatchScanExec before Comet
  // converts the scan. Adaptive off keeps the executed plan stable for the counts below.
  private val spjConf: Seq[(String, String)] = Seq(
    "spark.sql.iceberg.planning.preserve-data-ordering" -> "true",
    "spark.sql.iceberg.planning.preserve-data-grouping" -> "true",
    "spark.sql.sources.v2.bucketing.enabled" -> "true",
    "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true",
    "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.join.preferSortMergeJoin" -> "true",
    "spark.sql.adaptive.enabled" -> "false")

  /**
   * Runs `f` against a fresh, uniquely-named Hadoop catalog backed by a fresh temp warehouse,
   * then drops the named tables (IF EXISTS) in a `finally` -- so tables are cleaned up even when
   * the test body fails, and no two tests can collide on a table name. `f` receives the catalog
   * name; tables live under the `db` namespace, e.g. `$cat.db.$table`.
   */
  private def withSortedTables(extraConf: Seq[(String, String)])(tables: String*)(
      f: String => Unit): Unit = {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withTempIcebergDir { warehouseDir =>
      val cat = s"sort_cat_${catalogCounter.incrementAndGet()}"
      val cometConf = Seq(
        s"spark.sql.catalog.$cat" -> "org.apache.iceberg.spark.SparkCatalog",
        s"spark.sql.catalog.$cat.type" -> "hadoop",
        s"spark.sql.catalog.$cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true")
      withSQLConf((cometConf ++ extraConf): _*) {
        try f(cat)
        finally tables.foreach(t => spark.sql(s"DROP TABLE IF EXISTS $cat.db.$t"))
      }
    }
  }

  /**
   * Sets the table sort order via the Iceberg Java API. `cols` is (column, ascending); ascending
   * uses Iceberg's default NULLS FIRST and descending its default NULLS LAST, matching the ORDER
   * BY null-ordering the tests use.
   */
  private def replaceSortOrder(
      cat: String,
      namespace: String,
      table: String,
      cols: (String, Boolean)*): Unit = {
    val catalog = spark.sessionState.catalogManager
      .catalog(cat)
      .asInstanceOf[org.apache.iceberg.spark.SparkCatalog]
    val ident =
      org.apache.spark.sql.connector.catalog.Identifier.of(Array(namespace), table)
    val icebergTable = catalog
      .loadTable(ident)
      .asInstanceOf[org.apache.iceberg.spark.source.SparkTable]
      .table()
    var sortOrder = icebergTable.replaceSortOrder()
    cols.foreach { case (c, asc) =>
      sortOrder = if (asc) sortOrder.asc(c) else sortOrder.desc(c)
    }
    sortOrder.commit()
  }

  /**
   * Sets a transform (bucket) sort order via the Iceberg Java API. Iceberg orders files by the
   * bucket hash, so the reported sort field is a transform expression, not a plain column. Spark
   * 4.0+ can convert a bucket transform ordering (V2ScanPartitioningAndOrdering threads the
   * function catalog and V2ExpressionUtils special-cases BucketTransform); Spark 3.4 cannot, so
   * the caller gates the test on isSpark40Plus. Either way Comet's reportableOrdering rejects the
   * non-AttributeReference sort child (v1 is identity-scope only, #5339), so Comet must fall
   * back.
   */
  private def replaceSortOrderBucket(
      cat: String,
      namespace: String,
      table: String,
      col: String,
      numBuckets: Int): Unit = {
    val catalog = spark.sessionState.catalogManager
      .catalog(cat)
      .asInstanceOf[org.apache.iceberg.spark.SparkCatalog]
    val ident =
      org.apache.spark.sql.connector.catalog.Identifier.of(Array(namespace), table)
    val icebergTable = catalog
      .loadTable(ident)
      .asInstanceOf[org.apache.iceberg.spark.source.SparkTable]
      .table()
    icebergTable
      .replaceSortOrder()
      .asc(org.apache.iceberg.expressions.Expressions.bucket(col, numBuckets))
      .commit()
  }

  /**
   * Each string becomes a separate INSERT, hence a separate data file, so merging is required.
   */
  private def insertBatches(cat: String, table: String, batches: String*): Unit =
    batches.foreach(values => spark.sql(s"INSERT INTO $cat.db.$table VALUES $values"))

  private def nativeScans(plan: SparkPlan): Seq[CometIcebergNativeScanExec] =
    collect(stripAQEPlan(plan)) { case s: CometIcebergNativeScanExec => s }

  private def countSorts(plan: SparkPlan): Int =
    collect(stripAQEPlan(plan)) {
      case s: SortExec => s
      case s: CometSortExec => s
    }.size

  private def countShuffles(plan: SparkPlan): Int =
    collect(stripAQEPlan(plan)) {
      case e: ShuffleExchangeExec => e
      case e: CometShuffleExchangeExec => e
    }.size

  /** True once every native scan in the plan advertises the reported ordering. */
  private def orderingReported(plan: SparkPlan): Boolean = {
    val scans = nativeScans(plan)
    scans.nonEmpty && scans.forall(_.outputOrdering.nonEmpty)
  }

  // NOTE ON CANCELED TESTS: the helper below CANCELS the test (ScalaTest `assume`, reported as
  // "!!! CANCELED !!!", not a failure) when the Iceberg build on the classpath does not implement
  // the DSv2 `SupportsReportOrdering` API. Published/upstream Iceberg (the runtime used in CI and
  // the default mvn profiles) does not report a sort order, so `outputOrdering` comes back empty
  // and there is no eliminated Sort to assert on. These tests therefore show as canceled there --
  // that is expected, NOT a regression. The preceding `checkSparkAnswer` has already validated
  // correctness; only the sort/shuffle-elimination plan assertion is skipped. Run against an
  // ordering-reporting (fork) Iceberg build to exercise those assertions.

  /** Cancels the test (see note above) unless the scan reported an ordering. */
  private def assumeOrderingReported(plan: SparkPlan): Unit =
    assume(
      orderingReported(plan),
      "current Iceberg build does not implement SupportsReportOrdering (no ordering reported); " +
        "sort-elimination assertion skipped")

  /**
   * True if the Iceberg build on the classpath reports a sort order for `query`. Determined
   * independently of Comet: the query is planned with the native Iceberg scan disabled, and we
   * check whether Spark's own BatchScanExec advertises an outputOrdering. This is the signal that
   * makes the fallback checks below meaningful -- when Iceberg does not report (the published
   * Iceberg in CI), there is no reported ordering for Comet to decline, so those checks are
   * skipped rather than firing on a scan that was never eligible to fall back.
   */
  private def icebergReportsOrdering(query: String): Boolean = {
    // withSQLConf returns the block value on Spark 4.x but Unit on 3.4/3.5, so capture in a var
    // rather than relying on its return value.
    var reported = false
    withSQLConf(CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "false") {
      val plan = spark.sql(query).queryExecution.executedPlan
      reported = collect(stripAQEPlan(plan)) { case b: BatchScanExec => b }
        .exists(_.outputOrdering.nonEmpty)
    }
    reported
  }

  /**
   * The Spark-fallback contract for a reported-but-unhonorable ordering: when the native scan
   * cannot guarantee an ordering Iceberg reported, Comet must not convert the scan at all.
   * Reading unordered natively would return silently-wrong results, because EnsureRequirements
   * has already dropped the Sort above the scan on the strength of Iceberg's report. So the plan
   * must contain no CometIcebergNativeScanExec -- the scan stays on Spark's Iceberg reader. Gated
   * on a reporting Iceberg build (see icebergReportsOrdering); correctness is asserted separately
   * by the caller.
   */
  private def assertFellBackToSpark(query: String, plan: SparkPlan): Unit = {
    assume(
      icebergReportsOrdering(query),
      "current Iceberg build does not report an ordering; the Spark-fallback path is not exercised")
    assert(
      nativeScans(plan).isEmpty,
      s"Comet reported an ordering it cannot honour instead of falling back to Spark:\n$plan")
  }

  // The reporting mechanism: SupportsReportOrdering -> CometIcebergNativeScanExec.outputOrdering

  test("native scan reports the table sort order for a multi-file sorted table") {
    withSortedTables(orderedReadConf)("t") { cat =>
      spark.sql(s"CREATE TABLE $cat.db.t (id INT, data STRING) USING iceberg")
      replaceSortOrder(cat, "db", "t", "id" -> true)
      insertBatches(cat, "t", "(1,'a'),(3,'c')", "(2,'b'),(4,'d')")

      val (_, plan) = checkSparkAnswer(s"SELECT id, data FROM $cat.db.t ORDER BY id")
      assume(nativeScans(plan).nonEmpty, "query did not use the native Iceberg scan")
      assumeOrderingReported(plan)
      assert(
        nativeScans(plan).head.outputOrdering.head.child.references.exists(_.name == "id"),
        s"expected the scan to report an ordering on id:\n$plan")
    }
  }

  test("native scan reports no ordering when the sort-merge flag is disabled") {
    withSortedTables(
      orderedReadConf ++ Seq(CometConf.COMET_ICEBERG_SORT_MERGE_ENABLED.key -> "false"))("t") {
      cat =>
        spark.sql(s"CREATE TABLE $cat.db.t (id INT, data STRING) USING iceberg")
        replaceSortOrder(cat, "db", "t", "id" -> true)
        insertBatches(cat, "t", "(1,'a'),(3,'c')", "(2,'b'),(4,'d')")

        // Correctness must hold with the feature off, and no ordering may be advertised.
        val (_, plan) = checkSparkAnswer(s"SELECT id, data FROM $cat.db.t ORDER BY id")
        nativeScans(plan).foreach { scan =>
          assert(
            scan.outputOrdering.isEmpty,
            s"no ordering must be reported when the flag is off:\n$plan")
        }
    }
  }

  // K-way merge correctness (EnsureRequirements / EliminateSorts consume the reported ordering;
  // ORDER BY keeps the comparison order-sensitive so a merge defect is caught directly).

  test("merges multiple sorted files into one globally-ordered stream") {
    withSortedTables(orderedReadConf)("t") { cat =>
      spark.sql(s"CREATE TABLE $cat.db.t (id INT, data STRING) USING iceberg")
      replaceSortOrder(cat, "db", "t", "id" -> true)
      insertBatches(
        cat,
        "t",
        "(1,'a'),(2,'b')",
        "(3,'c'),(4,'d')",
        "(5,'e'),(6,'f')",
        "(7,'g'),(8,'h')")

      val (_, plan) = checkSparkAnswer(s"SELECT id, data FROM $cat.db.t ORDER BY id")
      assert(nativeScans(plan).length == 1, s"expected exactly one native scan:\n$plan")
    }
  }

  test("merge interleaves duplicate sort-key values across files") {
    withSortedTables(orderedReadConf)("t") { cat =>
      spark.sql(s"CREATE TABLE $cat.db.t (id INT, data STRING) USING iceberg")
      replaceSortOrder(cat, "db", "t", "id" -> true)
      // The same id appears in several files; the merge must keep every row, not drop or mis-order.
      insertBatches(cat, "t", "(1,'a'),(2,'b')", "(1,'c'),(2,'d')", "(1,'e'),(3,'f')")

      checkSparkAnswer(s"SELECT id, data FROM $cat.db.t ORDER BY id, data")
    }
  }

  test("merge applies merge-on-read deletes across a multi-file sorted partition") {
    withSortedTables(orderedReadConf)("t") { cat =>
      spark.sql(
        s"CREATE TABLE $cat.db.t (id INT, data STRING) USING iceberg " +
          "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')")
      replaceSortOrder(cat, "db", "t", "id" -> true)
      // Several files so a merge is required; then delete rows from some of them. On a v2
      // merge-on-read table DELETE writes delete files rather than rewriting the data files, so
      // the scan must apply the deletes while merging the still-sorted files.
      insertBatches(cat, "t", "(1,'a'),(4,'d')", "(2,'b'),(5,'e')", "(3,'c'),(6,'f')")
      spark.sql(s"DELETE FROM $cat.db.t WHERE id IN (2, 5)")

      checkSparkAnswer(s"SELECT id, data FROM $cat.db.t ORDER BY id")
    }
  }

  test("merge honours NULLS FIRST on an ascending sort key") {
    withSortedTables(spjConf)("t") { cat =>
      spark.sql(
        s"CREATE TABLE $cat.db.t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
          "PARTITIONED BY (c3)")
      replaceSortOrder(cat, "db", "t", "c1" -> true) // ASC -> Iceberg default NULLS FIRST
      insertBatches(
        cat,
        "t",
        "(null,'x','P1'),(3,'c','P1')",
        "(null,'y','P1'),(1,'a','P1'),(2,'b','P1')")

      checkSparkAnswer(
        s"SELECT c1, c2 FROM $cat.db.t WHERE c3 = 'P1' ORDER BY c1 ASC NULLS FIRST, c2")
    }
  }

  test("merge honours NULLS LAST on a descending sort key") {
    withSortedTables(spjConf)("t") { cat =>
      spark.sql(
        s"CREATE TABLE $cat.db.t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
          "PARTITIONED BY (c3)")
      replaceSortOrder(cat, "db", "t", "c1" -> false) // DESC -> Iceberg default NULLS LAST
      insertBatches(
        cat,
        "t",
        "(null,'x','P1'),(1,'a','P1')",
        "(null,'y','P1'),(3,'c','P1'),(2,'b','P1')")

      checkSparkAnswer(
        s"SELECT c1, c2 FROM $cat.db.t WHERE c3 = 'P1' ORDER BY c1 DESC NULLS LAST, c2")
    }
  }

  test("merge on a descending sort order") {
    withSortedTables(orderedReadConf)("t") { cat =>
      spark.sql(s"CREATE TABLE $cat.db.t (id INT, data STRING) USING iceberg")
      replaceSortOrder(cat, "db", "t", "id" -> false)
      insertBatches(cat, "t", "(10,'j'),(9,'i')", "(8,'h'),(7,'g')", "(6,'f'),(4,'d')")

      checkSparkAnswer(s"SELECT id FROM $cat.db.t ORDER BY id DESC")
    }
  }

  test("merge on a multi-column sort order") {
    withSortedTables(orderedReadConf)("t") { cat =>
      spark.sql(s"CREATE TABLE $cat.db.t (c1 INT, c2 STRING, c3 STRING) USING iceberg")
      replaceSortOrder(cat, "db", "t", "c3" -> true, "c1" -> true)
      insertBatches(
        cat,
        "t",
        "(1,'a','A'),(3,'c','A')",
        "(2,'b','A'),(1,'a','B')",
        "(2,'b','B'),(3,'c','B')")

      checkSparkAnswer(s"SELECT c3, c1, c2 FROM $cat.db.t ORDER BY c3, c1")
    }
  }

  test("single file needs no merge") {
    withSortedTables(orderedReadConf)("t") { cat =>
      spark.sql(s"CREATE TABLE $cat.db.t (id INT, data STRING) USING iceberg")
      replaceSortOrder(cat, "db", "t", "id" -> true)
      insertBatches(cat, "t", "(1,'a'),(2,'b')")

      checkSparkAnswer(s"SELECT id, data FROM $cat.db.t ORDER BY id")
    }
  }

  test("many small files in one partition merge correctly") {
    // Raise the per-partition file limit so the k-way merge (not the sort fallback) runs with many
    // files -- the shape this feature targets, where the merge opens one reader per file at once.
    // On an ordering-reporting Iceberg build this exercises a ~70-way SortPreservingMerge; on the
    // published Iceberg (no reported ordering) it is a plain read. checkSparkAnswer guards
    // correctness; asserting on memory-pool usage / peak concurrent readers is a TODO for #5343.
    val conf =
      orderedReadConf :+ (CometConf.COMET_ICEBERG_SORT_MERGE_MAX_FILES_PER_PARTITION.key -> "1000")
    withSortedTables(conf)("t") { cat =>
      spark.sql(s"CREATE TABLE $cat.db.t (id INT, data STRING) USING iceberg")
      replaceSortOrder(cat, "db", "t", "id" -> true)
      val rows = (1 to 70).map(i => s"($i,'v$i')")
      insertBatches(cat, "t", rows: _*)

      checkSparkAnswer(s"SELECT id, data FROM $cat.db.t ORDER BY id")
    }
  }

  test("many small files in one partition stay correct (exercises the sort fallback)") {
    withSortedTables(orderedReadConf)("t") { cat =>
      spark.sql(s"CREATE TABLE $cat.db.t (id INT, data STRING) USING iceberg")
      replaceSortOrder(cat, "db", "t", "id" -> true)
      // 70 single-row inserts -> 70 files in one unpartitioned partition, above the default
      // maxFilesPerPartition (64). On an ordering-reporting Iceberg build this drives the native
      // fallback -- a single unordered read plus a spillable SortExec, not a 70-way merge -- which
      // is the shape this feature actually targets (a sorted table with many small commits). On
      // the published Iceberg (no reported ordering) it is a plain read. checkSparkAnswer guards
      // correctness either way; asserting on memory-pool usage / peak concurrent readers is a
      // TODO for #5343.
      val rows = (1 to 70).map(i => s"($i,'v$i')")
      insertBatches(cat, "t", rows: _*)

      checkSparkAnswer(s"SELECT id, data FROM $cat.db.t ORDER BY id")
    }
  }

  test("partitioned table with several files per partition") {
    withSortedTables(spjConf)("t") { cat =>
      spark.sql(
        s"CREATE TABLE $cat.db.t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
          "PARTITIONED BY (c3)")
      replaceSortOrder(cat, "db", "t", "c1" -> true)
      insertBatches(
        cat,
        "t",
        "(1,'a','P1'),(3,'c','P1')",
        "(2,'b','P1'),(4,'d','P1')",
        "(5,'e','P2'),(7,'g','P2')",
        "(6,'f','P2'),(8,'h','P2')")

      checkSparkAnswer(s"SELECT c1, c2 FROM $cat.db.t WHERE c3 = 'P1' ORDER BY c1")
      checkSparkAnswer(s"SELECT c1, c2 FROM $cat.db.t WHERE c3 = 'P2' ORDER BY c1")
    }
  }

  test("sort key absent from the projection: falls back to an unordered read but stays correct") {
    withSortedTables(spjConf)("t") { cat =>
      spark.sql(
        s"CREATE TABLE $cat.db.t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
          "PARTITIONED BY (c3)")
      replaceSortOrder(cat, "db", "t", "c1" -> true) // c1 is the sort key
      insertBatches(cat, "t", "(1,'a','P1'),(3,'c','P1')", "(2,'b','P1'),(4,'d','P1')")

      // c1 is NOT selected -> reportableOrdering returns Nil (the identity-projected gate fails),
      // so no ordering is reported. Result must still be correct.
      val (_, plan) = checkSparkAnswer(s"SELECT c2 FROM $cat.db.t WHERE c3 = 'P1'")
      nativeScans(plan).foreach { scan =>
        assert(
          scan.outputOrdering.isEmpty,
          s"ordering must not be reported when the sort key is not projected:\n$plan")
      }
    }
  }

  test("results are identical with the sort-merge feature on and off") {
    withSortedTables(orderedReadConf)("t") { cat =>
      spark.sql(s"CREATE TABLE $cat.db.t (id INT, data STRING) USING iceberg")
      replaceSortOrder(cat, "db", "t", "id" -> true)
      insertBatches(cat, "t", "(1,'a'),(3,'c')", "(2,'b'),(4,'d')", "(5,'e'),(6,'f')")
      val query = s"SELECT id, data FROM $cat.db.t ORDER BY id"

      val enabled = spark.sql(query).collect().toSeq
      // withSQLConf returns the block value on Spark 4.x but Unit on 3.4/3.5, so capture the rows
      // in a var rather than relying on its return value.
      var disabled: Seq[org.apache.spark.sql.Row] = Seq.empty
      withSQLConf(CometConf.COMET_ICEBERG_SORT_MERGE_ENABLED.key -> "false") {
        disabled = spark.sql(query).collect().toSeq
      }
      assert(enabled == disabled, "reporting the ordering must not change the result")
    }
  }

  // -------------------------------------------------------------------------------------------
  // Fallback to Spark. These are the v1 non-targets from
  // https://github.com/apache/datafusion-comet/issues/5323: cases where Iceberg reports a sort
  // order but the native scan cannot honour it. When Iceberg reports an ordering, EnsureRequirements
  // may already have dropped the Sort above the scan, so a native unordered read would be silently
  // wrong -- Comet must instead leave the scan on Spark's Iceberg reader. Correctness is checked
  // unconditionally (checkSparkAnswer, holds on any Iceberg build); the "stayed on Spark" assertion
  // is gated on a reporting build (assertFellBackToSpark).
  //
  // Triggers: (a) sort-merge disabled while Iceberg still reports an ordering, and (b) a transform
  // (bucket) sort key (#5339), which Comet's reportableOrdering rejects as a non-column sort child.
  // The transform test is gated on Spark 4.0+: Spark converts a transform sort ordering only where
  // V2ScanPartitioningAndOrdering threads the function catalog into the scan's outputOrdering (4.0+
  // does, 3.4 does not -- there a transform ordering throws _LEGACY_ERROR_TEMP_3054 in Spark before
  // Comet is reached), and it also needs a reporting Iceberg build (assertFellBackToSpark gates on
  // that). UUID sort keys are covered by the "ordering-unsafe column" gate test since a UUID column
  // has no Spark DDL type. All of these feed the same reportableOrdering gate the fallback uses.
  // -------------------------------------------------------------------------------------------

  test("fallback: sort-merge disabled but Iceberg reports an ordering stays on Spark (SMJ)") {
    withSortedTables(spjConf ++ Seq(CometConf.COMET_ICEBERG_SORT_MERGE_ENABLED.key -> "false"))(
      "a",
      "b") { cat =>
      Seq("a", "b").foreach { t =>
        spark.sql(
          s"CREATE TABLE $cat.db.$t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
            "PARTITIONED BY (bucket(4, c1))")
        replaceSortOrder(cat, "db", t, "c1" -> true)
        insertBatches(cat, t, "(1,'a','X'),(2,'b','X')", "(1,'c','X'),(3,'d','X')")
      }
      val query = s"SELECT a.c1, b.c2 FROM $cat.db.a a JOIN $cat.db.b b ON a.c1 = b.c1"
      val (_, plan) = checkSparkAnswer(query)
      assertFellBackToSpark(query, plan)
    }
  }

  test(
    "fallback: sort-merge disabled but Iceberg reports an ordering stays on Spark (group-by)") {
    withSortedTables(spjConf ++ Seq(CometConf.COMET_ICEBERG_SORT_MERGE_ENABLED.key -> "false"))(
      "t") { cat =>
      spark.sql(
        s"CREATE TABLE $cat.db.t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
          "PARTITIONED BY (bucket(4, c1))")
      replaceSortOrder(cat, "db", "t", "c1" -> true)
      insertBatches(cat, "t", "(1,'a','X'),(2,'b','X')", "(1,'c','X'),(3,'d','X')")
      val query = s"SELECT c1, COUNT(*) FROM $cat.db.t GROUP BY c1"
      val (_, plan) = checkSparkAnswer(query)
      assertFellBackToSpark(query, plan)
    }
  }

  test("fallback: a transform (bucket) sort order stays on Spark (#5339)") {
    // Spark converts a transform sort ordering only on 4.0+ (see the section note); on 3.4 the
    // query would throw in Spark before Comet is reached, so skip there.
    assume(isSpark40Plus, "transform sort orderings are only convertible on Spark 4.0+")
    withSortedTables(spjConf)("t") { cat =>
      spark.sql(
        s"CREATE TABLE $cat.db.t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
          "PARTITIONED BY (bucket(4, c1))")
      // Sort by bucket(8, c1): Iceberg reports the ordering as a bucket transform. Spark 4.0+
      // converts it, but Comet's reportableOrdering rejects the non-AttributeReference sort child,
      // so Comet stays on Spark (v1 identity-only, #5339).
      replaceSortOrderBucket(cat, "db", "t", "c1", 8)
      insertBatches(cat, "t", "(1,'a','X'),(2,'b','X')", "(3,'c','X'),(4,'d','X')")
      val query = s"SELECT c1, c2 FROM $cat.db.t"
      val (_, plan) = checkSparkAnswer(query)
      assertFellBackToSpark(query, plan)
    }
  }

  // Sort-merge join: sorted + storage-partitioned inputs let EnsureRequirements drop both the
  // required-child Sort AND the Exchange. Target: zero of each (assume-gated on the reporting).

  test("sort-merge join over co-partitioned sorted tables drops the sort and the shuffle") {
    withSortedTables(spjConf)("a", "b") { cat =>
      Seq("a", "b").foreach { t =>
        spark.sql(
          s"CREATE TABLE $cat.db.$t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
            "PARTITIONED BY (bucket(4, c1))")
        replaceSortOrder(cat, "db", t, "c1" -> true)
        insertBatches(
          cat,
          t,
          s"(1,'${t}1','X'),(2,'${t}2','X')",
          s"(3,'${t}3','X'),(4,'${t}4','X')")
      }

      val query =
        s"SELECT t1.c1, t1.c2, t2.c2 FROM $cat.db.a t1 JOIN $cat.db.b t2 ON t1.c1 = t2.c1"
      val (_, plan) = checkSparkAnswer(query)

      // Shuffle elimination needs only the grouping (upstream Iceberg), so assert it always.
      assert(countShuffles(plan) == 0, s"storage-partitioned join must not shuffle:\n$plan")
      // Sort elimination needs the reported ordering (fork Iceberg today), so gate that one.
      assumeOrderingReported(plan)
      assert(countSorts(plan) == 0, s"reported ordering must eliminate the join sort:\n$plan")
    }
  }

  test("storage-partitioned join on the bucket key drops the shuffle") {
    withSortedTables(spjConf)("a", "b") { cat =>
      // No sort order set: this isolates the SPJ shuffle-elimination (KeyGroupedPartitioning),
      // independent of the ordering path. A join sort may remain; the Exchange must not.
      Seq("a", "b").foreach { t =>
        spark.sql(
          s"CREATE TABLE $cat.db.$t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
            "PARTITIONED BY (bucket(4, c1))")
        insertBatches(
          cat,
          t,
          s"(1,'${t}1','X'),(2,'${t}2','X')",
          s"(3,'${t}3','X'),(4,'${t}4','X')")
      }

      val query =
        s"SELECT t1.c1, t1.c2, t2.c2 FROM $cat.db.a t1 JOIN $cat.db.b t2 ON t1.c1 = t2.c1"
      val (_, plan) = checkSparkAnswer(query)

      // Comet no longer reports partitioning; EnsureRequirements eliminates the shuffle on the
      // BatchScanExec before Comet converts the scan, so this holds on any Iceberg with grouping.
      assert(countShuffles(plan) == 0, s"storage-partitioned join must not shuffle:\n$plan")
    }
  }

  // AQE runtime partition-value pushdown. Under AQE Spark computes the exact common set of
  // partition values once both sides materialize and pushes it into the scan
  // (EnsureRequirements.populateCommonPartitionInfo, which matches only `case scan: BatchScanExec`).
  // Because Comet converts the scan AFTER EnsureRequirements, that push-down lands on the vanilla
  // BatchScanExec, and Comet's execution partitions come from originalPlan.inputRDD -- already
  // padded to the merged set. So the native leaf stays aligned with Spark's spec without Comet
  // reporting any partitioning itself. Offset partition-value sets (a: c1 in 1..4, b: 3..6) make
  // the merged/padded set differ from each side's own list, exactly where a misalignment would
  // surface.
  //
  // The asserts are hard, not `assume`d: correctness (Comet vs vanilla Spark) must hold, both sides
  // must stay native (a fallback to Spark's BatchScanExec would let correctness pass vacuously),
  // and the join must not shuffle (a shuffle would re-partition correctly and mask the pushdown).
  // Only Iceberg is a precondition (grouping is upstream, unlike ordering).

  /** a: c1 in 1..4, b: c1 in 3..6, both bucketed the same way -- offset partition-value sets. */
  private def createAqePartitionedPair(cat: String): Unit = {
    Seq("a", "b").foreach { t =>
      spark.sql(
        s"CREATE TABLE $cat.db.$t (c1 INT, c2 STRING) USING iceberg " +
          "PARTITIONED BY (bucket(8, c1))")
    }
    insertBatches(cat, "a", "(1,'a1'),(2,'a2')", "(3,'a3'),(4,'a4')")
    insertBatches(cat, "b", "(3,'b3'),(4,'b4')", "(5,'b5'),(6,'b6')")
  }

  private val aqeSpjConf: Seq[(String, String)] =
    spjConf.filterNot(_._1 == "spark.sql.adaptive.enabled") :+
      ("spark.sql.adaptive.enabled" -> "true")

  private def assertNativeSpj(plan: SparkPlan): Unit = {
    assert(
      nativeScans(plan).length == 2,
      s"both join sides must stay on the native Iceberg scan (no fallback):\n$plan")
    assert(countShuffles(plan) == 0, s"storage-partitioned join must not shuffle:\n$plan")
  }

  // Left outer is the sensitive case: every left row must appear, so if the leaf's partition
  // indices are shifted relative to Spark's padded spec, a left row whose key exists on the right
  // gets a wrong NULL (or a match against the wrong group) -- caught by checkSparkAnswer. An inner
  // join would only drop the same unmatched rows on both sides and can pass despite misalignment.
  test("AQE storage-partitioned left outer join with runtime partition pushdown is correct") {
    withSortedTables(aqeSpjConf)("a", "b") { cat =>
      createAqePartitionedPair(cat)
      val query =
        s"SELECT t1.c1, t1.c2, t2.c2 FROM $cat.db.a t1 LEFT OUTER JOIN $cat.db.b t2 " +
          "ON t1.c1 = t2.c1"
      val (_, plan) = checkSparkAnswer(query)
      assertNativeSpj(plan)
    }
  }

  test("AQE storage-partitioned inner join with runtime partition pushdown is correct") {
    val innerConf = aqeSpjConf :+
      ("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled" -> "true")
    withSortedTables(innerConf)("a", "b") { cat =>
      createAqePartitionedPair(cat)
      val query =
        s"SELECT t1.c1, t1.c2, t2.c2 FROM $cat.db.a t1 JOIN $cat.db.b t2 ON t1.c1 = t2.c1"
      val (_, plan) = checkSparkAnswer(query)
      assertNativeSpj(plan)
    }
  }

  // Grouped aggregate / distinct: ReplaceHashWithSortAgg + a satisfied ClusteredDistribution.

  test("group-by on the sort key drops the sort and the shuffle") {
    withSortedTables(spjConf)("t") { cat =>
      spark.sql(
        s"CREATE TABLE $cat.db.t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
          "PARTITIONED BY (bucket(4, c1))")
      replaceSortOrder(cat, "db", "t", "c1" -> true)
      insertBatches(cat, "t", "(1,'a','X'),(2,'b','X')", "(1,'c','X'),(3,'d','X')")

      // No trailing ORDER BY: checkSparkAnswer compares order-insensitively, so sort = 0 is a
      // legitimate target (a grouped aggregate over sorted + clustered input needs neither).
      val query = s"SELECT c1, COUNT(*) FROM $cat.db.t GROUP BY c1"
      val (_, plan) = checkSparkAnswer(query)

      assert(countShuffles(plan) == 0, s"grouped aggregate must not shuffle:\n$plan")
      assumeOrderingReported(plan)
      assert(countSorts(plan) == 0, s"grouped aggregate over sorted input must not sort:\n$plan")
    }
  }

  test("distinct on the sort key is correct") {
    withSortedTables(spjConf)("t") { cat =>
      spark.sql(
        s"CREATE TABLE $cat.db.t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
          "PARTITIONED BY (bucket(4, c1))")
      replaceSortOrder(cat, "db", "t", "c1" -> true)
      insertBatches(cat, "t", "(1,'a','X'),(2,'b','X')", "(1,'c','X'),(3,'d','X')")

      checkSparkAnswer(s"SELECT DISTINCT c1 FROM $cat.db.t ORDER BY c1")
    }
  }

  // Window: WindowExec.requiredChildOrdering (partitionSpec ++ orderSpec) + ClusteredDistribution.

  test("window partitioned + ordered on the sort keys drops the sort and the shuffle") {
    withSortedTables(spjConf)("t") { cat =>
      spark.sql(
        s"CREATE TABLE $cat.db.t (c1 INT, c2 STRING, c3 STRING) USING iceberg " +
          "PARTITIONED BY (bucket(4, c1))")
      replaceSortOrder(cat, "db", "t", "c1" -> true, "c2" -> true)
      insertBatches(cat, "t", "(1,'a','X'),(2,'b','X')", "(1,'c','X'),(2,'d','X')")

      val query =
        s"SELECT c1, c2, ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY c2) AS rn FROM $cat.db.t"
      val (_, plan) = checkSparkAnswer(query)

      assert(countShuffles(plan) == 0, s"window over clustered input must not shuffle:\n$plan")
      assumeOrderingReported(plan)
      assert(countSorts(plan) == 0, s"window over sorted input must not sort:\n$plan")
    }
  }

  // Limit: TakeOrderedAndProjectExec degenerates to a cheap take when the child is already sorted.

  test("order-by-limit over a sorted table is correct") {
    withSortedTables(orderedReadConf)("t") { cat =>
      spark.sql(s"CREATE TABLE $cat.db.t (id INT, data STRING) USING iceberg")
      replaceSortOrder(cat, "db", "t", "id" -> true)
      insertBatches(cat, "t", "(1,'a'),(3,'c')", "(2,'b'),(4,'d')", "(5,'e'),(6,'f')")

      // TakeOrderedAndProject is planned for ORDER BY ... LIMIT and, when the child is already
      // reported sorted, degenerates to a cheap bounded take instead of a full top-N heap.
      // Correctness (including the LIMIT cut over the merged order) is the guarantee here.
      checkSparkAnswer(s"SELECT id, data FROM $cat.db.t ORDER BY id LIMIT 3")
    }
  }
}
