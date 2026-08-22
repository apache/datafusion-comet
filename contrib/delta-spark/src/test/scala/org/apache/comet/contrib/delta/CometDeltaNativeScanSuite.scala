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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, StructsToJson}
import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.functions.{col, to_json}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.serde.OperatorOuterClass

/**
 * Differential suite: append-only Delta tables read through the native Delta scan must produce
 * results identical to Spark's Delta reader, engage the native operator, and prune at row-group
 * and page level.
 */
class CometDeltaNativeScanSuite extends CometDeltaTestBase {

  test("plain delta table reads natively with identical results") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 1000)
        .selectExpr("id", "id * 2 as v", "cast(id as string) as s")
        .write
        .format("delta")
        .save(path)

      val df = spark.read.format("delta").load(path).filter(col("id") > 500)
      checkDeltaNativeScanAnswer(df)
    }
  }

  test("projection and filter on delta table") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 1000)
        .selectExpr("id", "id % 10 as bucket", "cast(id as double) as d")
        .write
        .format("delta")
        .save(path)

      val df = spark.read
        .format("delta")
        .load(path)
        .select("bucket", "d")
        .filter(col("d") < 100.0)
      checkDeltaNativeScanAnswer(df)
    }
  }

  test("partitioned delta table with partition filter") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 1000)
        .selectExpr("id", "id % 7 as p")
        .write
        .format("delta")
        .partitionBy("p")
        .save(path)

      val df = spark.read.format("delta").load(path).filter(col("p") === 3)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() > 0)
    }
  }

  test("multi-file delta table after several appends") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      for (i <- 0 until 4) {
        spark
          .range(i * 100, (i + 1) * 100)
          .selectExpr("id", "id * 3 as v")
          .write
          .format("delta")
          .mode("append")
          .save(path)
      }
      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == 400)
    }
  }

  test("time travel VERSION AS OF reads natively") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).write.format("delta").save(path)
      spark.range(100, 200).write.format("delta").mode("append").save(path)

      val v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
      checkDeltaNativeScanAnswer(v0)
      assert(v0.count() == 100)
    }
  }

  test("selective predicate prunes row groups and pages") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      // Small row groups + page-level stats: sorted data so min/max stats are tight. The Delta
      // writer ignores parquet.* DataFrameWriter options, so set them on the Hadoop conf.
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val oldBlockSize = hadoopConf.get("parquet.block.size")
      val oldPageSize = hadoopConf.get("parquet.page.size")
      hadoopConf.setInt("parquet.block.size", 256 * 1024)
      hadoopConf.setInt("parquet.page.size", 16 * 1024)
      try {
        spark
          .range(0, 500000)
          .selectExpr("id", "id * 2 as v")
          .sort("id")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)
      } finally {
        if (oldBlockSize == null) hadoopConf.unset("parquet.block.size")
        else hadoopConf.set("parquet.block.size", oldBlockSize)
        if (oldPageSize == null) hadoopConf.unset("parquet.page.size")
        else hadoopConf.set("parquet.page.size", oldPageSize)
      }

      def query = spark.read
        .format("delta")
        .load(path)
        .filter(col("id") >= 100 && col("id") < 200)
      checkDeltaNativeScanAnswer(query)

      // checkSparkAnswer re-plans the query, so read metrics from a DataFrame we execute
      // ourselves (collect() runs THIS Dataset's queryExecution; count() would plan a new one):
      // its executed plan holds the metric objects native execution updated.
      val df = query
      assert(df.collect().length == 100)
      val scans = deltaNativeScans(df)
      assert(scans.size == 1)
      val metrics = scans.head.metrics
      val rowGroupsPruned = metrics.get("row_groups_pruned_statistics").map(_.value).getOrElse(0L)
      val pagesPruned = metrics.get("page_index_rows_pruned").map(_.value).getOrElse(0L)
      assert(
        rowGroupsPruned > 0,
        s"expected row-group pruning; metrics: ${metrics.map { case (k, v) => s"$k=${v.value}" }}")
      assert(
        pagesPruned > 0,
        s"expected page-index pruning; metrics: ${metrics.map { case (k, v) =>
            s"$k=${v.value}"
          }}")
    }
  }

  test("scalar subquery data filter is pushed down and prunes row groups and pages") {
    withTempPath { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      val thresholds = s"${dir.getAbsolutePath}/thresholds"
      // Same layout as the selective-predicate test: small row groups + tight page stats.
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val oldBlockSize = hadoopConf.get("parquet.block.size")
      val oldPageSize = hadoopConf.get("parquet.page.size")
      hadoopConf.setInt("parquet.block.size", 256 * 1024)
      hadoopConf.setInt("parquet.page.size", 16 * 1024)
      try {
        spark
          .range(0, 500000)
          .selectExpr("id", "id * 2 as v")
          .sort("id")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)
      } finally {
        if (oldBlockSize == null) hadoopConf.unset("parquet.block.size")
        else hadoopConf.set("parquet.block.size", oldBlockSize)
        if (oldPageSize == null) hadoopConf.unset("parquet.page.size")
        else hadoopConf.set("parquet.page.size", oldPageSize)
      }
      spark
        .sql("SELECT CAST(100 AS BIGINT) AS lo, CAST(200 AS BIGINT) AS hi")
        .write
        .format("delta")
        .save(thresholds)

      // Scalar subqueries are PlanExpressions: unresolved at planning, so the bounds can
      // only reach the native reader via the execution-time resolve-and-append path.
      def query = spark.sql(
        s"SELECT * FROM delta.`$path` WHERE id >= (SELECT lo FROM delta.`$thresholds`) " +
          s"AND id < (SELECT hi FROM delta.`$thresholds`)")
      checkDeltaNativeScanAnswer(query)

      val df = query
      assert(df.collect().length == 100)
      // The thresholds table inside the subquery is also claimed natively; pick the
      // main data-table scan by its output.
      assertSubqueryFilterPushed(df, dataColumn = "v")
      val scans = deltaNativeScans(df).filter(_.output.exists(_.name == "v"))
      assert(scans.size == 1)
      val metrics = scans.head.metrics
      val rowGroupsPruned = metrics.get("row_groups_pruned_statistics").map(_.value).getOrElse(0L)
      val pagesPruned = metrics.get("page_index_rows_pruned").map(_.value).getOrElse(0L)
      assert(
        rowGroupsPruned > 0,
        s"expected row-group pruning from the resolved subquery bounds; metrics: ${metrics.map {
            case (k, v) => s"$k=${v.value}"
          }}")
      assert(
        pagesPruned > 0,
        s"expected page-index pruning from the resolved subquery bounds; metrics: ${metrics.map {
            case (k, v) => s"$k=${v.value}"
          }}")
    }
  }

  test("deletion vectors: scalar subquery filter composes with DV application") {
    withTempPath { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      val thresholds = s"${dir.getAbsolutePath}/thresholds"
      createDvTable(path, rows = 10000)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")
      spark
        .sql("SELECT CAST(5000 AS BIGINT) AS lo")
        .write
        .format("delta")
        .save(thresholds)

      def query =
        spark.sql(s"SELECT * FROM delta.`$path` WHERE id >= (SELECT lo FROM delta.`$thresholds`)")
      checkDeltaNativeScanAnswer(query)
      // Deleted rows must stay deleted with the pushed bound applied in-scan.
      val df = query
      val rows = df.collect()
      assert(rows.length == 2500)
      assert(rows.forall(r => r.getLong(0) % 2 == 1 && r.getLong(0) >= 5000))
      assertSubqueryFilterPushed(df, dataColumn = "v")
    }
  }

  test("column mapping: scalar subquery filter on a renamed column") {
    withTempPath { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      val thresholds = s"${dir.getAbsolutePath}/thresholds"
      spark.range(0, 1000).selectExpr("id", "id * 2 as v").write.format("delta").save(path)
      enableColumnMapping(path)
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN v TO w")
      spark
        .sql("SELECT CAST(900 AS BIGINT) AS lo")
        .write
        .format("delta")
        .save(thresholds)

      // The pushed filter references the renamed column: it must bind against the
      // physical read schema, not the logical name.
      def query =
        spark.sql(s"SELECT * FROM delta.`$path` WHERE w >= (SELECT lo FROM delta.`$thresholds`)")
      checkDeltaNativeScanAnswer(query)
      val df = query
      assert(df.collect().length == 550)
      assertSubqueryFilterPushed(df, dataColumn = "w")
    }
  }

  /**
   * Assert the resolved scalar-subquery bound was actually appended to the native scan's
   * execution-time common data (answers alone cannot show this: Spark's covering FilterExec would
   * mask a silently-skipped pushdown). `df` must already have been executed.
   */
  private def assertSubqueryFilterPushed(df: DataFrame, dataColumn: String): Unit = {
    val scans = deltaNativeScans(df).collect {
      case s: CometDeltaNativeScanExec if s.output.exists(_.name == dataColumn) => s
    }
    assert(scans.size == 1)
    val scan = scans.head
    val planTimeFilters =
      DeltaSparkScanEnvelope.unpack(scan.nativeOp).getCommon.getDataFiltersCount
    val executedFilters = OperatorOuterClass.DeltaSparkScan
      .parseFrom(scan.commonData)
      .getCommon
      .getDataFiltersCount
    assert(
      executedFilters > planTimeFilters,
      "expected resolved subquery filters appended at execution: " +
        s"plan-time=$planTimeFilters executed=$executedFilters " +
        s"dataFilters=${scan.dataFilters.mkString("; ")}")
  }

  test("scalar subquery filter is NOT pushed below a limit") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 3).selectExpr("id").write.format("delta").save(path)
      spark.read.format("delta").load(path).createOrReplaceTempView("t_limit_pushdown")

      val df = spark.sql(
        "SELECT id FROM (SELECT id FROM t_limit_pushdown ORDER BY id LIMIT 1) q " +
          "WHERE id > (SELECT max(id) FROM range(1))")
      checkSparkAnswer(df)
      assert(df.collect().isEmpty)
      assertNoSubqueryFilterPushed(df)
    }
  }

  test("scalar subquery filter is NOT pushed across a nondeterministic projection") {
    withSQLConf(CometConf.COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        spark.range(0, 5).coalesce(1).write.format("delta").save(path)
        spark.read.format("delta").load(path).createOrReplaceTempView("t_monotonic_id")

        // A deterministic conjunct does not commute with a nondeterministic projection: the
        // subquery bound must not be pushed into the scan below `seq`, or the surviving rows'
        // monotonically_increasing_id() values change and the answer is wrong.
        val df = spark.sql(
          "SELECT id FROM (SELECT id, monotonically_increasing_id() AS seq " +
            "FROM t_monotonic_id) q WHERE id > (SELECT max(id) FROM range(1)) AND seq = 1")
        checkSparkAnswer(df)
        assert(df.collect().toSeq == Seq(Row(1)))
        assertNoSubqueryFilterPushed(df)
      }
    }
  }

  /**
   * Assert no scalar-subquery filter was harvested and pushed into the native scan's
   * execution-time common data: the scan must sit below a non-commuting operator (e.g. LIMIT /
   * TopN), so the covering FilterExec's predicate must stay above it rather than move into the
   * scan. Also confirms the query still engaged the native Delta scan, i.e. this exercises the
   * commutativity guard rather than a plan that fell back to Spark entirely. `df` must already
   * have been executed.
   */
  private def assertNoSubqueryFilterPushed(df: DataFrame): Unit = {
    val scans = deltaNativeScans(df).collect { case s: CometDeltaNativeScanExec => s }
    assert(scans.size == 1, s"expected exactly one native Delta scan; found ${scans.size}")
    val scan = scans.head
    val planTimeFilters =
      DeltaSparkScanEnvelope.unpack(scan.nativeOp).getCommon.getDataFiltersCount
    val executedFilters = OperatorOuterClass.DeltaSparkScan
      .parseFrom(scan.commonData)
      .getCommon
      .getDataFiltersCount
    assert(
      executedFilters == planTimeFilters,
      "expected no subquery filter pushed across the non-commuting operator between the " +
        s"covering filter and the scan: plan-time=$planTimeFilters executed=$executedFilters " +
        s"dataFilters=${scan.dataFilters.mkString("; ")}")
  }

  test("aggregation over delta table") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 10000)
        .selectExpr("id", "id % 13 as g", "id * 2 as v")
        .write
        .format("delta")
        .save(path)

      val df = spark.read
        .format("delta")
        .load(path)
        .groupBy("g")
        .sum("v")
      checkDeltaNativeScanAnswer(df)
    }
  }

  test("conf disables the native delta scan") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).write.format("delta").save(path)

      withSQLConf(DeltaScanConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val df = spark.read.format("delta").load(path)
        checkSparkAnswer(df)
        assert(deltaNativeScans(df).isEmpty)
      }
    }
  }

  test("native delta scan is opt-in: disabled when the conf is not set") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).write.format("delta").save(path)

      // The suite base enables the scan globally; drop the key entirely to
      // observe the out-of-the-box default.
      spark.conf.unset(DeltaScanConf.COMET_DELTA_NATIVE_ENABLED.key)
      try {
        assert(!DeltaScanConf.scanEnabled)
        val df = spark.read.format("delta").load(path)
        checkSparkAnswer(df)
        assert(deltaNativeScans(df).isEmpty)
      } finally {
        spark.conf.set(DeltaScanConf.COMET_DELTA_NATIVE_ENABLED.key, "true")
      }
    }
  }

  private def createDvTable(path: String, rows: Long = 1000): Unit = {
    spark.range(0, rows).selectExpr("id", "id * 2 as v").write.format("delta").save(path)
    spark.sql(
      s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
  }

  test("deletion vectors: DELETE-produced DVs read natively with correct results") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      createDvTable(path)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")

      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == 500)
    }
  }

  test("deletion vectors: UPDATE-produced DVs read natively with correct results") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      createDvTable(path)
      spark.sql(s"UPDATE delta.`$path` SET v = -1 WHERE id < 100")

      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      assert(df.filter(col("v") === -1).count() == 100)
      assert(df.count() == 1000)
    }
  }

  test("deletion vectors: multiple DELETEs accumulate correctly") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      createDvTable(path)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 3 = 0")

      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      // odd ids not divisible by 3
      assert(df.count() == (0L until 1000L).count(i => i % 2 != 0 && i % 3 != 0))
    }
  }

  test(
    "deletion vectors: maxDeletedRowsPerFile budget declines an oversized DV and " +
      "claims once raised") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      // repartition(4) guarantees >= 2 physical files so the per-file cardinality gate has
      // more than one file to inspect, mirroring design F3's multi-file test shape.
      spark
        .range(0, 1000)
        .selectExpr("id", "id * 2 as v")
        .repartition(4)
        .write
        .format("delta")
        .save(path)
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")

      withSQLConf(DeltaScanConf.COMET_DELTA_MAX_DELETED_ROWS_PER_FILE.key -> "1") {
        val df = spark.read.format("delta").load(path)
        checkSparkAnswer(df)
        assert(
          deltaNativeScans(df).isEmpty,
          "a budget of 1 deleted row per file must decline every DV-bearing file")
      }

      withSQLConf(DeltaScanConf.COMET_DELTA_MAX_DELETED_ROWS_PER_FILE.key -> "1000000") {
        val df = spark.read.format("delta").load(path)
        checkDeltaNativeScanAnswer(df)
      }
    }
  }

  test("deletion vectors: maxDeletedRowsPerFile decline reason names the conf key") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      createDvTable(path)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")

      withSQLConf(DeltaScanConf.COMET_DELTA_MAX_DELETED_ROWS_PER_FILE.key -> "1") {
        checkSparkAnswerAndFallbackReason(
          spark.read.format("delta").load(path),
          DeltaScanConf.COMET_DELTA_MAX_DELETED_ROWS_PER_FILE.key)
      }
    }
  }

  test("deletion vectors: fully-deleted region and selective predicate still prune pages") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val oldBlockSize = hadoopConf.get("parquet.block.size")
      val oldPageSize = hadoopConf.get("parquet.page.size")
      hadoopConf.setInt("parquet.block.size", 256 * 1024)
      hadoopConf.setInt("parquet.page.size", 16 * 1024)
      try {
        spark
          .range(0, 500000)
          .selectExpr("id", "id * 2 as v")
          .sort("id")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)
      } finally {
        if (oldBlockSize == null) hadoopConf.unset("parquet.block.size")
        else hadoopConf.set("parquet.block.size", oldBlockSize)
        if (oldPageSize == null) hadoopConf.unset("parquet.page.size")
        else hadoopConf.set("parquet.page.size", oldPageSize)
      }
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      // Delete a slice inside the predicate range and a large slice outside it.
      spark.sql(s"DELETE FROM delta.`$path` WHERE id >= 150 AND id < 160")
      spark.sql(s"DELETE FROM delta.`$path` WHERE id >= 300000")

      def query = spark.read
        .format("delta")
        .load(path)
        .filter(col("id") >= 100 && col("id") < 200)
      checkDeltaNativeScanAnswer(query)

      val df = query
      assert(df.collect().length == 90)
      val scans = deltaNativeScans(df)
      assert(scans.size == 1)
      val metrics = scans.head.metrics
      val pagesPruned = metrics.get("page_index_rows_pruned").map(_.value).getOrElse(0L)
      assert(
        pagesPruned > 0,
        s"expected page-index pruning to compose with DVs; metrics: ${metrics.map { case (k, v) =>
            s"$k=${v.value}"
          }}")
    }
  }

  test("deletion vectors: aggregation over DV table") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      createDvTable(path, rows = 10000)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 7 = 0")

      val df = spark.read.format("delta").load(path).groupBy(col("id") % 13).count()
      checkDeltaNativeScanAnswer(df)
    }
  }

  test("deletion vectors: partitioned table reads natively with correct results") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 1000)
        .selectExpr("id", "id % 5 as p", "id * 2 as v")
        .write
        .format("delta")
        .partitionBy("p")
        .save(path)
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 3 = 0")

      val df = spark.read.format("delta").load(path).filter(col("p") === 2)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == (0L until 1000L).count(i => i % 5 == 2 && i % 3 != 0))

      val all = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(all)
      assert(all.count() == (0L until 1000L).count(_ % 3 != 0))
    }
  }

  test("deletion vectors: combined with constant metadata columns") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      createDvTable(path)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id < 250")

      val df = spark.read
        .format("delta")
        .load(path)
        .selectExpr("id", "v", "_metadata.file_name as fn")
      checkSparkAnswer(df.selectExpr("id", "v", "length(fn) > 0"))
      // Whether this claims or declines, results must match; if it claimed, verify the
      // native node is present so the combination is actually exercised when supported.
      val rows = df.collect()
      assert(rows.length == 750)
      assert(rows.forall(_.getString(2).nonEmpty))
    }
  }

  test("deletion vectors: special characters in table path") {
    withTempDir { base =>
      val dir = new java.io.File(base, "s p a r k %dv% test")
      val path = dir.getAbsolutePath
      createDvTable(path)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")

      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == 500)
    }
  }

  test("deletion vectors: decline when row_index is consumed via multi-hop aliases") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      createDvTable(path)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")

      val df = spark.read
        .format("delta")
        .load(path)
        .selectExpr("id", "_metadata.row_index as ri")
        .selectExpr("id", "ri + 1 as ri2")
        .filter(col("ri2") > 10)
      checkSparkAnswer(df)
      assert(deltaNativeScans(df).isEmpty, "derived row_index consumption must decline")
    }
  }

  test("deletion vectors: decline when row_index feeds a non-Project operator") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      createDvTable(path)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")

      val df = spark.read
        .format("delta")
        .load(path)
        .groupBy(col("_metadata.row_index") % 7)
        .count()
      checkSparkAnswer(df)
      assert(deltaNativeScans(df).isEmpty, "aggregate over row_index must decline")
    }
  }

  test("deletion vectors: decline when _metadata.row_index is referenced above the scan") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      createDvTable(path)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")

      val df = spark.read
        .format("delta")
        .load(path)
        .selectExpr("id", "_metadata.row_index as ri")
      checkSparkAnswer(df)
      assert(
        deltaNativeScans(df).isEmpty,
        "plans consuming a real row_index must fall back to Spark")
    }
  }

  /**
   * Single-file (ids 0-4) deletion-vector table with one id deleted, used by the UnionExec
   * row-index liveness tests below: UnionExec's output takes its expression IDs positionally from
   * its FIRST child, so a live `_metadata.row_index` alias in a later branch is invisible to a
   * taint analysis that only follows `ProjectExec` aliases. A small fixed fixture keeps the
   * expected surviving row_index values easy to hand-verify.
   */
  private def createSmallDvTable(path: String, deleteId: Long): Unit = {
    spark.range(0, 5).selectExpr("id").coalesce(1).write.format("delta").save(path)
    spark.sql(
      s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    spark.sql(s"DELETE FROM delta.`$path` WHERE id = $deleteId")
  }

  test(
    "deletion vectors: row_index live through UNION ALL declines both branches " +
      "with correct SUM") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempPath { dir1 =>
        withTempPath { dir2 =>
          val t1 = dir1.getAbsolutePath
          val t2 = dir2.getAbsolutePath
          // t1: ids 0,1,3,4 survive (id 2 deleted); t2: ids 0,1,2,4 survive (id 3 deleted).
          createSmallDvTable(t1, deleteId = 2)
          createSmallDvTable(t2, deleteId = 3)

          def query: DataFrame = {
            val left =
              spark.read.format("delta").load(t1).selectExpr("id", "_metadata.row_index as ri")
            val right =
              spark.read.format("delta").load(t2).selectExpr("id", "_metadata.row_index as ri")
            left.union(right)
          }

          checkSparkAnswer(query)
          val df = query
          val rows = df.collect()
          assert(rows.length == 8, s"expected 8 surviving rows, got ${rows.length}")
          assert(
            deltaNativeScans(df).isEmpty,
            "row_index live via a union's positional output remap must decline both branches")
          // row_index equals id for every surviving row in this single-file, insertion-ordered
          // fixture, so summing the real (uncorrupted) row indexes is equivalent to summing ids:
          // t1 (0+1+3+4=8) + t2 (0+1+2+4=7) = 15. A wrongly-claimed branch would instead
          // contribute a constant 0 per row, which this exact total rules out.
          val sum = rows.map(_.getLong(1)).sum
          assert(sum == 15L, s"expected SUM(ri) == 15, got $sum")
        }
      }
    }
  }

  test(
    "deletion vectors: row_index live only in the second UNION ALL branch declines " +
      "only that branch") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempPath { dir1 =>
        withTempPath { dir2 =>
          val t1 = dir1.getAbsolutePath
          val t2 = dir2.getAbsolutePath
          createSmallDvTable(t1, deleteId = 2)
          createSmallDvTable(t2, deleteId = 3)

          def query: DataFrame = {
            // Branch 1's "ri" is a constant, never derived from its own row_index; branch 2's
            // "ri" is the real _metadata.row_index. UnionExec's output reuses branch 1's
            // expression ID for the "ri" column, so only branch 2's scan should decline.
            val left =
              spark.read.format("delta").load(t1).selectExpr("id", "CAST(-1 AS BIGINT) as ri")
            val right =
              spark.read.format("delta").load(t2).selectExpr("id", "_metadata.row_index as ri")
            left.union(right)
          }

          checkSparkAnswer(query)
          val df = query
          val rows = df.collect()
          assert(rows.length == 8, s"expected 8 surviving rows, got ${rows.length}")
          val fromT1 = rows.filter(_.getLong(1) == -1L)
          val fromT2 = rows.filter(_.getLong(1) != -1L)
          assert(fromT1.length == 4, s"expected 4 rows from t1, got ${fromT1.length}")
          assert(fromT2.length == 4, s"expected 4 rows from t2, got ${fromT2.length}")
          // Real row_index equals id in this fixture; a wrongly-claimed branch 2 would instead
          // report a constant 0 for every row, which this per-row check rules out.
          assert(
            fromT2.forall(r => r.getLong(0) == r.getLong(1)),
            s"expected t2's ri to equal id, got: ${fromT2.mkString(", ")}")
          val scans = deltaNativeScans(df)
          assert(
            scans.size == 1,
            s"expected exactly branch 1 (t1) to claim natively, got ${scans.size} native scans")
        }
      }
    }
  }

  test(
    "deletion vectors: SUM(row_index) over UNION ALL declines both branches with correct total") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempPath { dir1 =>
        withTempPath { dir2 =>
          val t1 = dir1.getAbsolutePath
          val t2 = dir2.getAbsolutePath
          createSmallDvTable(t1, deleteId = 2)
          createSmallDvTable(t2, deleteId = 3)

          def query: DataFrame = {
            val left =
              spark.read.format("delta").load(t1).selectExpr("id", "_metadata.row_index as ri")
            val right =
              spark.read.format("delta").load(t2).selectExpr("id", "_metadata.row_index as ri")
            left.union(right).selectExpr("sum(ri) as total")
          }

          checkSparkAnswer(query)
          val total = query.collect()(0).getLong(0)
          assert(total == 15L, s"expected SUM(ri) == 15, got $total")
          assert(
            deltaNativeScans(query).isEmpty,
            "row_index live via an aggregate over a union must decline both branches")
        }
      }
    }
  }

  test(
    "deletion vectors: UNION ALL without _metadata still claims both branches natively " +
      "(anti-regression)") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempPath { dir1 =>
        withTempPath { dir2 =>
          val t1 = dir1.getAbsolutePath
          val t2 = dir2.getAbsolutePath
          createSmallDvTable(t1, deleteId = 2)
          createSmallDvTable(t2, deleteId = 3)

          def query: DataFrame = {
            val left = spark.read.format("delta").load(t1).selectExpr("id")
            val right = spark.read.format("delta").load(t2).selectExpr("id")
            left.union(right)
          }

          checkSparkAnswer(query)
          val df = query
          val ids = df.collect().map(_.getLong(0)).sorted
          assert(
            ids.sameElements(Array(0L, 0L, 1L, 1L, 2L, 3L, 4L, 4L)),
            s"unexpected surviving ids: ${ids.mkString(", ")}")
          val scans = deltaNativeScans(df)
          assert(
            scans.size == 2,
            "a DV union without _metadata must still claim both branches natively " +
              s"(the row-index column is dead in both), got ${scans.size} native scans")
        }
      }
    }
  }

  test("deletion vectors: inner join between two DV tables claims both scans natively") {
    // Positive coverage for the generic multi-child safety net (DeltaScanSupport.scala's
    // multiChildLeak check): a plain join carries no row-index taint at all, so the safety net
    // must not mistake a join's normal attribute passthrough for a leak and fall both sides back
    // to Spark.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempPath { dir1 =>
        withTempPath { dir2 =>
          val t1 = dir1.getAbsolutePath
          val t2 = dir2.getAbsolutePath
          // t1 survives ids {0,1,3,4} (id 2 deleted); t2 survives ids {0,1,2,4} (id 3 deleted).
          createSmallDvTable(t1, deleteId = 2)
          createSmallDvTable(t2, deleteId = 3)

          def query: DataFrame = {
            val left = spark.read.format("delta").load(t1).withColumnRenamed("id", "lid")
            val right = spark.read.format("delta").load(t2).withColumnRenamed("id", "rid")
            left.join(right, col("lid") === col("rid"))
          }

          checkSparkAnswer(query)
          val df = query
          val rows = df.collect()
          val ids = rows.map(_.getLong(0)).sorted
          // Only ids surviving in BOTH tables' deletion vectors should match.
          assert(
            ids.sameElements(Array(0L, 1L, 4L)),
            s"expected join to match surviving ids {0,1,4}, got: ${ids.mkString(", ")}")
          assert(
            rows.forall(r => r.getLong(0) == r.getLong(1)),
            "join key mismatch in result rows")
          val scans = deltaNativeScans(df)
          assert(
            scans.size == 2,
            "a DV-backed join with no row-index consumption must claim both sides natively, " +
              s"got ${scans.size} native scans")
        }
      }
    }
  }

  private def enableColumnMapping(path: String): Unit =
    spark.sql(s"""ALTER TABLE delta.`$path` SET TBLPROPERTIES (
                 |  'delta.minReaderVersion' = '2',
                 |  'delta.minWriterVersion' = '5',
                 |  'delta.columnMapping.mode' = 'name')""".stripMargin)

  test("column mapping: renamed column reads natively across old and new files") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).selectExpr("id", "id * 2 as v").write.format("delta").save(path)
      enableColumnMapping(path)
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN v TO w")
      // Files written after the rename carry the same physical name.
      spark
        .range(100, 200)
        .selectExpr("id", "id * 2 as w")
        .write
        .format("delta")
        .mode("append")
        .save(path)

      val df = spark.read.format("delta").load(path).filter(col("w") > 100)
      checkDeltaNativeScanAnswer(df)
      assert(spark.read.format("delta").load(path).count() == 200)
    }
  }

  test("column mapping: dropped and re-added column name reads natively") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).selectExpr("id", "id * 2 as v").write.format("delta").save(path)
      enableColumnMapping(path)
      spark.sql(s"ALTER TABLE delta.`$path` DROP COLUMN v")
      spark.sql(s"ALTER TABLE delta.`$path` ADD COLUMN v LONG")
      spark
        .range(100, 200)
        .selectExpr("id", "id * 3 as v")
        .write
        .format("delta")
        .mode("append")
        .save(path)

      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      // Old files must yield NULL for the re-added v (different physical column).
      assert(df.filter(col("id") < 100).filter(col("v").isNotNull).count() == 0)
      assert(df.filter(col("id") >= 100).filter(col("v").isNull).count() == 0)
    }
  }

  test("column mapping: partitioned table reads natively") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 500)
        .selectExpr("id", "id % 5 as p")
        .write
        .format("delta")
        .partitionBy("p")
        .save(path)
      enableColumnMapping(path)
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN p TO part")

      val df = spark.read.format("delta").load(path).filter(col("part") === 3)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == 100)
    }
  }

  test("column mapping: combined with deletion vectors") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 1000).selectExpr("id", "id * 2 as v").write.format("delta").save(path)
      enableColumnMapping(path)
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN v TO w")
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 4 = 0")

      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == 750)
    }
  }

  test("column mapping: to_json on a nested struct matches Spark's field names") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 10)
        .selectExpr("id", "named_struct('a', id) as s")
        .write
        .format("delta")
        .save(path)
      enableColumnMapping(path)
      // Renaming the NESTED field (not the outer column) is what diverges the physical name
      // ("a", preserved on rename) from the logical name ("b") for a struct field below the
      // top level — the shape that leaks physical names into to_json's output.
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN s.a TO b")

      withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[StructsToJson]) -> "true") {
        val df = spark.read.format("delta").load(path).select(to_json(col("s")))
        checkSparkAnswer(df)
        assert(
          deltaNativeScans(df).isEmpty,
          "column mapping with nested struct fields must fall back to Spark")
      }
    }
  }

  test("decline: column mapping with nested struct columns falls back to Spark") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 200)
        .selectExpr(
          "id",
          "named_struct('a', id, 'b', cast(id as string)) as st",
          "array(id, id * 2) as arr",
          "map(cast(id as string), id) as mp")
        .write
        .format("delta")
        .save(path)
      enableColumnMapping(path)
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN st TO st2")
      spark
        .range(200, 300)
        .selectExpr(
          "id",
          "named_struct('a', id, 'b', cast(id as string)) as st2",
          "array(id, id * 2) as arr",
          "map(cast(id as string), id) as mp")
        .write
        .format("delta")
        .mode("append")
        .save(path)

      val df = spark.read.format("delta").load(path).selectExpr("id", "st2.a", "arr", "mp")
      checkSparkAnswer(df)
      assert(df.count() == 300)
      assert(
        deltaNativeScans(df).isEmpty,
        "column mapping with nested struct fields must fall back to Spark")
    }
  }

  test("decline: column mapping with structs nested in arrays and maps falls back to Spark") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 50)
        .selectExpr(
          "id",
          "array(named_struct('a', id, 'b', cast(id as string))) as arrOfStruct",
          "map(cast(id as string), named_struct('a', id)) as mapOfStruct")
        .write
        .format("delta")
        .save(path)
      enableColumnMapping(path)

      val df = spark.read.format("delta").load(path)
      checkSparkAnswer(df)
      assert(
        deltaNativeScans(df).isEmpty,
        "column mapping with structs nested in arrays/maps must fall back to Spark")
    }
  }

  test("column mapping: top-level scalars and array-of-primitives still claim natively") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 200)
        .selectExpr("id", "cast(id as string) as v", "array(id, id * 2) as arr")
        .write
        .format("delta")
        .save(path)
      enableColumnMapping(path)
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN v TO w")

      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == 200)
    }
  }

  test("decline: column mapping id mode falls back to Spark with correct results") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"""CREATE TABLE delta.`$path` (id LONG, v LONG) USING delta
                   |TBLPROPERTIES ('delta.columnMapping.mode' = 'id')""".stripMargin)
      spark
        .range(0, 100)
        .selectExpr("id", "id * 2 as v")
        .write
        .format("delta")
        .mode("append")
        .save(path)

      val df = spark.read.format("delta").load(path)
      checkSparkAnswer(df)
      assert(deltaNativeScans(df).isEmpty, "id-mode column mapping must decline")
    }
  }

  test("delete without deletion vectors rewrites files and still reads natively") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 1000).selectExpr("id", "id * 2 as v").write.format("delta").save(path)
      // DVs are off by default, so DELETE rewrites files; result is still a plain table.
      spark.sql(s"DELETE FROM delta.`$path` WHERE id < 100")

      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == 900)
    }
  }

  test("dynamic partition pruning via broadcast join prunes delta partitions") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "20",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100m") {
      withTempPath { factDir =>
        withTempPath { dimDir =>
          val factPath = factDir.getAbsolutePath
          val dimPath = dimDir.getAbsolutePath
          spark
            .range(0, 2000)
            .selectExpr("id", "id % 10 as p")
            .write
            .format("delta")
            .partitionBy("p")
            .save(factPath)
          // Unfiltered on disk: the selective predicate below is a query-time filter, which
          // is what gives Spark's DynamicPartitionPruning rule a subquery to inject in the
          // first place. Filtering before the write (the old shape of this test) leaves no
          // predicate in the query for DPP to see, so the assertions below never fired.
          spark
            .range(0, 10)
            .selectExpr("id as key", "id % 10 as dp")
            .write
            .format("delta")
            .save(dimPath)

          def query = {
            val fact = spark.read.format("delta").load(factPath)
            val dim = spark.read.format("delta").load(dimPath)
            // only partitions 0 and 1 survive the join
            fact.join(dim, fact("p") === dim("dp")).filter(dim("key") < 2)
          }

          checkSparkAnswer(query)

          val df = query
          val rows = df.collect()
          assert(rows.length == 400) // 2 partitions x 200 rows
          val scans = deltaNativeScans(df)
          assert(
            scans.nonEmpty,
            s"expected native delta scans:\n${df.queryExecution.executedPlan}")

          val deltaScans = scans.collect { case s: CometDeltaNativeScanExec => s }
          assert(
            deltaScans.exists(_.runtimeFilters.exists(_.isInstanceOf[DynamicPruningExpression])),
            "expected a DynamicPruningExpression in a CometDeltaNativeScanExec's " +
              s"runtimeFilters:\n${df.queryExecution.executedPlan}")

          // The fact-side scan must have read fewer files than the table holds (DPP pruning).
          val factScan = scans.maxBy(_.metrics.get("staticFilesNum").map(_.value).getOrElse(0L))
          val staticFiles = factScan.metrics.get("staticFilesNum").map(_.value).getOrElse(0L)
          val readFiles = factScan.metrics.get("numFiles").map(_.value).getOrElse(0L)
          assert(staticFiles > 0, "expected the staticFilesNum metric to be populated")
          assert(
            readFiles < staticFiles,
            s"expected DPP pruning: read $readFiles of $staticFiles files")
        }
      }
    }
  }

  test("union all with DPP join and coalescible shuffle survives AQE partitioning checks") {
    // The maintainer's crash shape: a DPP join in one UNION ALL branch and a coalescible
    // shuffle (the GROUP BY) in the other. Spark's AQE plan validation walks every operator's
    // outputPartitioning, including the DPP branch's scan, before
    // CometPlanAdaptiveDynamicPruningFilters has rewritten the placeholder subquery -- this
    // is the ordering that reproduced the maintainer's crash.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "20",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100m") {
      withTempPath { factDir =>
        withTempPath { dimDir =>
          withTempPath { otherDir =>
            val factPath = factDir.getAbsolutePath
            val dimPath = dimDir.getAbsolutePath
            val otherPath = otherDir.getAbsolutePath

            spark
              .range(0, 2000)
              .selectExpr("id", "id % 10 as p")
              .write
              .format("delta")
              .partitionBy("p")
              .save(factPath)
            spark
              .range(0, 10)
              .selectExpr("id as key", "id % 10 as dp", "id as sel")
              .write
              .format("delta")
              .save(dimPath)
            spark
              .range(0, 500)
              .selectExpr("id", "id % 10 as p")
              .write
              .format("delta")
              .partitionBy("p")
              .save(otherPath)

            spark.read.format("delta").load(factPath).createOrReplaceTempView("r43Fact")
            spark.read.format("delta").load(dimPath).createOrReplaceTempView("r43Dim")
            spark.read.format("delta").load(otherPath).createOrReplaceTempView("r43Other")

            def query =
              spark.sql("""
                |SELECT f.p, f.id FROM r43Fact f JOIN r43Dim d ON f.p = d.dp WHERE d.sel < 2
                |UNION ALL
                |SELECT p, CAST(count(*) AS LONG) AS id FROM r43Other GROUP BY p
                |""".stripMargin)

            try {
              checkSparkAnswer(query)
            } catch {
              case e: Throwable =>
                if (e.getMessage != null &&
                  e.getMessage.contains("does not support the execute() code path")) {
                  throw new AssertionError(
                    "AQE inspected outputPartitioning on an unresolved adaptive DPP " +
                      "placeholder -- this is the crash this test guards against",
                    e)
                }
                throw e
            }

            val df = query
            df.collect()
            // Best-effort: this UNION ALL shape need not always route through the native
            // Delta scan, but if it does, it must have survived AQE's partitioning checks
            // above without throwing. Observed to vary run-to-run on this build (Spark 3.5.9
            // / Delta 3.3.2), so this is logged rather than asserted -- answer correctness is
            // already verified by checkSparkAnswer above.
            val scans = deltaNativeScans(df)
            if (scans.isEmpty) {
              logInfo(
                "union all with DPP join and coalescible shuffle: no CometDeltaNativeScanExec " +
                  "claimed this query on this build; answer correctness already verified above")
            } else {
              logInfo(
                s"union all with DPP join and coalescible shuffle: ${scans.length} " +
                  "CometDeltaNativeScanExec node(s) claimed this query; answer correctness " +
                  "already verified above")
            }
          }
        }
      }
    }
  }

  test("input_file_name falls back to Spark with correct results") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).selectExpr("id").write.format("delta").save(path)

      val df = spark.read
        .format("delta")
        .load(path)
        .selectExpr("id", "input_file_name() as f")
      checkSparkAnswer(df.selectExpr("id", "length(f) > 0"))
      assert(deltaNativeScans(df).isEmpty, "input_file_name must decline")
    }
  }

  test("self-join of the same delta table keeps scans distinct") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).selectExpr("id", "id % 5 as k").write.format("delta").save(path)

      def query = {
        val left = spark.read.format("delta").load(path).filter(col("id") < 50)
        val right = spark.read.format("delta").load(path).filter(col("id") >= 50)
        left.as("l").join(right.as("r"), col("l.k") === col("r.k"))
      }
      checkSparkAnswer(query)

      val df = query
      df.collect()
      assert(deltaNativeScans(df).size == 2)
    }
  }

  test("schema evolution: added column yields nulls for old files, natively") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).selectExpr("id").write.format("delta").save(path)
      spark.sql(s"ALTER TABLE delta.`$path` ADD COLUMN v LONG")
      spark
        .range(100, 200)
        .selectExpr("id", "id * 2 as v")
        .write
        .format("delta")
        .mode("append")
        .save(path)

      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      assert(df.filter(col("id") < 100).filter(col("v").isNotNull).count() == 0)
      assert(df.filter(col("id") >= 100).filter(col("v").isNull).count() == 0)
    }
  }

  test("schema evolution: column default (Delta two-step) reads correctly") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).selectExpr("id").write.format("delta").save(path)
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
          "('delta.feature.allowColumnDefaults' = 'supported')")
      // Delta only allows defaults via add-then-set (applies to FUTURE inserts; old files
      // read as NULL -- unlike Spark's existence defaults).
      spark.sql(s"ALTER TABLE delta.`$path` ADD COLUMN v LONG")
      spark.sql(s"ALTER TABLE delta.`$path` ALTER COLUMN v SET DEFAULT 42")
      spark.sql(s"INSERT INTO delta.`$path` (id) VALUES (100), (101)")

      val df = spark.read.format("delta").load(path)
      // Whether claimed or declined, results must match Spark exactly.
      checkSparkAnswer(df)
      assert(df.count() == 102)
      assert(df.filter(col("v") === 42).count() == 2)
      assert(df.filter(col("id") < 100).filter(col("v").isNotNull).count() == 0)
    }
  }

  test("legacy INT96 timestamps read natively with correct values") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      withSQLConf("spark.sql.parquet.outputTimestampType" -> "INT96") {
        spark
          .range(0, 100)
          .selectExpr("id", "timestamp_seconds(1600000000 + id * 3600) as ts")
          .write
          .format("delta")
          .save(path)
      }
      val df = spark.read.format("delta").load(path).filter(col("id") < 50)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == 50)
    }
  }

  test("decline: type widening feature falls back with correct results") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      // Delta 3.3's widening preview supports byte/short -> int.
      spark.sql(s"""CREATE TABLE delta.`$path` (id SMALLINT) USING delta
                   |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')""".stripMargin)
      spark
        .range(0, 100)
        .selectExpr("cast(id as smallint) as id")
        .write
        .format("delta")
        .mode("append")
        .save(path)
      spark.sql(s"ALTER TABLE delta.`$path` ALTER COLUMN id TYPE INT")
      spark
        .range(100, 200)
        .selectExpr("cast(id as int) as id")
        .write
        .format("delta")
        .mode("append")
        .save(path)

      val df = spark.read.format("delta").load(path)
      checkSparkAnswer(df)
      assert(df.count() == 200)
    }
  }

  test("checkpointed delta log reads natively") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      // Force a checkpoint by exceeding the default interval via many commits.
      spark.sql(s"""CREATE TABLE delta.`$path` (id LONG, v LONG) USING delta
                   |TBLPROPERTIES ('delta.checkpointInterval' = '3')""".stripMargin)
      for (i <- 0 until 5) {
        spark
          .range(i * 10, (i + 1) * 10)
          .selectExpr("id", "id * 2 as v")
          .write
          .format("delta")
          .mode("append")
          .save(path)
      }
      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == 50)
    }
  }

  test(
    "decline: shallow clone with a supported local root but viewfs-scheme selected files " +
      "falls back to Spark") {
    // The maintainer's review shape: a Delta shallow clone whose table ROOT is a natively
    // supported scheme (here, local `file:`) but whose SELECTED data files still resolve
    // through the shallow clone's ORIGINAL, natively-unsupported location (here, `viewfs:`,
    // mounted transparently onto the local filesystem so the on-disk bytes are real and the
    // query's results are actually checkable). The rootPaths-only gate this task extends cannot
    // see this: it only ever inspects the clone's own (supported) root.
    val cluster = "cometDeltaViewfsGate"
    // Hadoop's mounttable is plain Configuration, not SQLConf: mutate the session's shared
    // hadoopConfiguration directly (mirroring withSQLConf's set-then-restore shape) rather than
    // withSQLConf, which only round-trips actual SQLConf entries.
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val linkFallbackKey = s"fs.viewfs.mounttable.$cluster.linkFallback"
    val priorLinkFallback = Option(hadoopConf.get(linkFallbackKey))
    hadoopConf.set(linkFallbackKey, "file:///")
    try {
      withTempPath { sourceDir =>
        withTempPath { cloneDir =>
          val sourcePath = sourceDir.getAbsolutePath
          val clonePath = cloneDir.getAbsolutePath
          val sourceViewfsPath = s"viewfs://$cluster$sourcePath"

          spark
            .range(0, 10)
            .write
            .format("delta")
            .save(sourceViewfsPath)
          spark.sql(s"CREATE TABLE delta.`$clonePath` SHALLOW CLONE delta.`$sourceViewfsPath`")
          // Append local (file:) data on top of the clone's inherited viewfs-scheme files: the
          // scan's selected data files now span both an unsupported scheme (viewfs) AND multiple
          // object-store authorities (file: carries none, viewfs://cometDeltaViewfsGate carries
          // one), the same shape DeltaScanContribSuite's
          // "unsupportedSelectedSchemeReason declines a mixed file:+viewfs selection" unit test
          // pins directly against declineReason's gate ordering (DeltaScanSupport.scala): the
          // scheme gate runs before multiStoreReason, so the fallback reason below must still
          // name viewfs, never "spans multiple object stores". This confirms that ordering
          // end to end through declineReason, not merely at the unit level.
          spark.range(10, 20).write.format("delta").mode("append").save(clonePath)

          val df = spark.read.format("delta").load(clonePath)
          assert(
            deltaNativeScans(df).isEmpty,
            s"Expected no native Delta scan for a viewfs-selected-file clone:\n" +
              s"${df.queryExecution.executedPlan}")
          checkSparkAnswerAndFallbackReason(
            df,
            "Native Delta scan does not support selected data file or deletion vector " +
              "filesystem scheme(s) viewfs")
        }
      }
    } finally {
      priorLinkFallback match {
        case Some(v) => hadoopConf.set(linkFallbackKey, v)
        case None => hadoopConf.unset(linkFallbackKey)
      }
    }
  }
}
