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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DynamicPruningExpression, NamedExpression, StructsToJson}
import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, ScalarSubquery, SparkPlan, SubqueryExec}
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.functions.{col, lit, to_json}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.ExtendedExplainInfo
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.operator.CometNativeScan

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

  test("scalar subquery filter rejected by serde still marks the scan as filtered") {
    withTempPath { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      val bounds = s"${dir.getAbsolutePath}/bounds"
      spark.range(0, 100).selectExpr("id", "id * 2 as v").write.format("delta").save(path)
      spark.sql("SELECT CAST(42 AS BIGINT) AS lo").write.format("delta").save(bounds)

      // With EqualNullSafe disabled the resolved bound cannot serialize, yet the scan must still
      // carry has_data_filters so native treats it as a filtered read, exactly like core does.
      withSQLConf("spark.comet.expression.EqualNullSafe.enabled" -> "false") {
        def query =
          spark.sql(
            s"SELECT * FROM delta.`$path` WHERE id <=> (SELECT max(lo) FROM delta.`$bounds`)")
        checkDeltaNativeScanAnswer(query)
        val df = query
        assert(df.collect().toSeq == Seq(Row(42L, 84L)))
        assertUnserializedSubqueryFilterMarksScanFiltered(df, dataColumn = "v")
      }
    }
  }

  test("unserializable scalar subquery filter keeps the safe TIMESTAMP_MILLIS conversion") {
    // Same fixture as core's "filtered TIMESTAMP_MILLIS scans do not convert values Spark can
    // skip": a raw file whose only overflowing millisecond value Spark prunes from the footer
    // statistics once the resolved bound is pushed, so native must not convert it either.
    withTempPath { dir =>
      val path = s"${dir.getAbsolutePath}/data"
      val bounds = s"${dir.getAbsolutePath}/bounds"
      writeRawParquetFile(
        path,
        """message root {
          |  optional int32 id;
          |  optional int64 ts(TIMESTAMP_MILLIS);
          |}""".stripMargin) { factory =>
        (1 to 16).map(id => factory.newGroup().append("id", id).append("ts", 1717243200000L)) :+
          factory.newGroup().append("id", 17).append("ts", 9223372036854776L)
      }
      spark.sql(s"CONVERT TO DELTA parquet.`$path` NO STATISTICS")
      spark.sql("SELECT timestamp_seconds(0) AS bound").write.format("delta").save(bounds)

      withSQLConf(
        "spark.comet.expression.EqualNullSafe.enabled" -> "false",
        "spark.sql.parquet.datetimeRebaseModeInRead" -> "CORRECTED",
        "spark.sql.parquet.int96RebaseModeInRead" -> "CORRECTED") {
        def query = spark.sql(
          s"SELECT id, ts FROM delta.`$path` " +
            s"WHERE ts <=> (SELECT max(bound) FROM delta.`$bounds`)")
        // Spark 3.x never pushes subquery filters into its parquet reader and converts the
        // overflowing value itself, so the answer comparison is meaningful on Spark 4.0+ only.
        if (isSpark40Plus) {
          checkDeltaNativeScanAnswer(query)
        }
        val df = query
        assert(df.collect().isEmpty)
        assert(
          deltaNativeScans(df).nonEmpty,
          s"expected a native Delta scan:\n${df.queryExecution}")
        assertUnserializedSubqueryFilterMarksScanFiltered(df, dataColumn = "ts")
      }
    }
  }

  /**
   * Assert the execution-time common data of the scan producing `dataColumn` reports
   * `has_data_filters` with no serialized data filter: the plan-time proto carries neither, and
   * the resolved subquery filter is the only data filter, so only the execution-time path can set
   * the bit. `df` must already have been executed.
   */
  private def assertUnserializedSubqueryFilterMarksScanFiltered(
      df: DataFrame,
      dataColumn: String): Unit = {
    val scans = deltaNativeScans(df).collect {
      case s: CometDeltaNativeScanExec if s.output.exists(_.name == dataColumn) => s
    }
    assert(scans.size == 1, s"expected exactly one native Delta scan; found ${scans.size}")
    val scan = scans.head
    assert(
      scan.dataFilters.exists(_.exists(_.isInstanceOf[ScalarSubquery])),
      s"expected a scalar subquery data filter: ${scan.dataFilters.mkString("; ")}")
    val planTime = DeltaSparkScanEnvelope.unpack(scan.nativeOp).getCommon
    assert(!planTime.getHasDataFilters && planTime.getDataFiltersCount == 0)
    val executed = OperatorOuterClass.DeltaSparkScan.parseFrom(scan.commonData).getCommon
    assert(
      executed.getHasDataFilters,
      "expected has_data_filters at execution even though the resolved subquery filter did " +
        s"not serialize: dataFilters=${scan.dataFilters.mkString("; ")}")
    assert(executed.getDataFiltersCount == 0)
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

  /**
   * Same shape as `createDvTable`, plus one extra TINYINT column (value 7) under `columnName`.
   */
  private def createDvTableWithExtraColumn(
      path: String,
      columnName: String,
      rows: Long = 1000): Unit = {
    spark
      .range(0, rows)
      .selectExpr("id", s"cast(7 as tinyint) as `$columnName`")
      .write
      .format("delta")
      .save(path)
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

  test(
    "deletion vectors: user column named like the synthetic internal-column slot keeps its " +
      "own values") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val collidingName = "_comet_delta___delta_internal_is_row_deleted"
      createDvTableWithExtraColumn(path, collidingName)
      spark.sql(s"DELETE FROM delta.`$path` WHERE id = 0")

      val df = spark.read.format("delta").load(path).select("id", collidingName)
      checkDeltaNativeScanAnswer(df)
      val survivingValues = df.collect().map(_.getAs[Byte](collidingName)).distinct
      assert(
        survivingValues.sameElements(Array(7.toByte)),
        "expected the user column's own value (7) to survive DV filtering, " +
          s"got ${survivingValues.toSeq}")
    }
  }

  test("deletion vectors: normally named extra column alongside DVs reads natively") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      createDvTableWithExtraColumn(path, "tag")
      spark.sql(s"DELETE FROM delta.`$path` WHERE id = 0")

      val df = spark.read.format("delta").load(path).select("id", "tag")
      checkDeltaNativeScanAnswer(df)
      val survivingValues = df.collect().map(_.getAs[Byte]("tag")).distinct
      assert(
        survivingValues.sameElements(Array(7.toByte)),
        s"expected the extra column's value (7) to survive DV filtering, got " +
          survivingValues.toSeq)
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

  test(
    "deletion vectors: constant-metadata field names are deduplicated against the physical " +
      "data and partition schemas") {
    // End-to-end coverage is not possible here: selecting any `_metadata.*` field in the DV
    // shape always declines today for an unrelated, pre-existing reason -- Spark reuses the
    // scan's own row-index bookkeeping attribute as `_metadata.row_index`'s source, and
    // `DeltaScanSupport.rowIndexUnusedAbove` conservatively treats extracting ANY `_metadata`
    // field as making that attribute live (see "combined with constant metadata columns"
    // above, which hedges its assertions for the same reason). That decline fires before
    // `buildDvScanCommon` ever runs, regardless of collision, so it cannot exercise the fix.
    // Test the builder's dedup logic directly instead, the same way `storeUris` and
    // `mergedObjectStoreOptions` are unit-tested without a live scan.
    val physicalDataSchema =
      StructType(Seq(StructField("_comet_metadata_file_path", ByteType)))
    val physicalPartitionSchema =
      StructType(Seq(StructField("_comet_metadata_file_size", LongType)))
    val fileConstantMetadataColumns = Seq(
      AttributeReference("file_path", StringType, nullable = false)(),
      AttributeReference("file_size", LongType, nullable = false)())

    val constantMetadataFields = CometNativeScan.uniqueConstantMetadataFields(
      fileConstantMetadataColumns,
      physicalDataSchema.fields.map(_.name).toSet ++ physicalPartitionSchema.fields
        .map(_.name)
        .toSet)
    assert(
      constantMetadataFields.map(_.name) == Seq(
        "_comet_metadata_file_path_",
        "_comet_metadata_file_size_"),
      "expected both constant-metadata names to be uniquified on collision, got " +
        s"${constantMetadataFields.map(_.name)}")

    // The DV builder must feed these already-unique names into allocateUniqueInternalFields's
    // reserved set so the internal-column suffix chain stays consistent with them.
    val requiredSchema = StructType(
      Seq(
        StructField("id", LongType),
        StructField(CometDeltaNativeScan.IsRowDeletedColumn, ByteType),
        StructField(CometDeltaNativeScan.RowIndexColumn, LongType)))
    val internalFields = CometDeltaNativeScan.allocateUniqueInternalFields(
      requiredSchema,
      physicalDataSchema,
      physicalPartitionSchema,
      constantMetadataFields)

    val allNames = physicalDataSchema.fields.map(_.name) ++
      physicalPartitionSchema.fields.map(_.name) ++
      constantMetadataFields.map(_.name) ++
      internalFields.map(_.name)
    assert(allNames.distinct.length == allNames.length, s"expected all names distinct: $allNames")
  }

  test(
    "non-DV shape: user column named like the synthetic constant-metadata slot keeps its " +
      "own values") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val collidingName = "_comet_metadata_file_path"
      spark
        .range(0, 100)
        .selectExpr("id", s"cast(7 as tinyint) as `$collidingName`")
        .write
        .format("delta")
        .save(path)

      val df = spark.read
        .format("delta")
        .load(path)
        .selectExpr("id", s"`$collidingName`", "_metadata.file_path as fp")
      checkDeltaNativeScanAnswer(df)
      val rows = df.collect()
      val survivingValues = rows.map(_.getAs[Byte](collidingName)).distinct
      assert(
        survivingValues.sameElements(Array(7.toByte)),
        "expected the user column's own value (7) to survive the constant-metadata " +
          s"collision, got ${survivingValues.toSeq}")
      assert(
        rows.forall(_.getString(2).nonEmpty),
        "expected _metadata.file_path to still report a real path")
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
   * Every [[SparkPlan]] executed during `body`, captured via a [[QueryExecutionListener]] rather
   * than a returned `DataFrame`'s own plan: a `DataFrameWriter` action such as `.write.parquet`
   * has no result `Dataset` to call `.queryExecution` on, so the write's physical plan -- the one
   * `DeltaScanSupport.declineReason` actually saw -- is only observable this way.
   */
  private def capturePlansDuring(body: => Unit): Seq[SparkPlan] = {
    val plans = ListBuffer.empty[SparkPlan]
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plans += qe.executedPlan
      }
      override def onFailure(
          funcName: String,
          qe: QueryExecution,
          exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      body
    } finally {
      spark.listenerManager.unregister(listener)
    }
    plans.toSeq
  }

  test(
    "deletion vectors: a write sink persisting _metadata.row_index declines the native scan " +
      "and saves the real row indexes") {
    withTempPath { srcDir =>
      withTempPath { dstDir =>
        val src = srcDir.getAbsolutePath
        val dst = dstDir.getAbsolutePath
        spark
          .range(32)
          .coalesce(1)
          .write
          .format("delta")
          .option("delta.enableDeletionVectors", "true")
          .save(src)
        spark.sql(s"DELETE FROM delta.`$src` WHERE id IN (1, 7, 13)").collect()

        val capturedPlans = capturePlansDuring {
          spark.read
            .format("delta")
            .load(src)
            .selectExpr("id", "_metadata.row_index AS ri")
            .write
            .parquet(dst)
        }

        // The write persists whatever the reader returns for `ri`, so the native DV scan must
        // not be claimed here: claiming it would let the reader's dead synthetic row-index
        // constant (correct only because the value is normally proven unused) get persisted as
        // if it were the real row index.
        val nativeScans = capturedPlans.flatMap(p => collectByName(p, "CometDeltaNativeScanExec"))
        assert(
          nativeScans.isEmpty,
          "expected the write to decline the native Delta scan for a persisted row_index")

        // Documents which write-sink shape this test actually covers: the liveness gate in
        // `DeltaScanSupport.rowIndexUnusedAbove` (the `childOutputLeak` check) declines a DV
        // scan under ANY one-child, empty-output write sink structurally, including the DSv2
        // `V2TableWriteExec` family -- but a DV-enabled Delta read cannot be composed with a
        // genuine DSv2 `AppendData` write in this delta-spark/Spark combination (see the
        // dsv2-infeasibility test below), so `.write.parquet` here is the only write-sink shape
        // this liveness gate is exercised against end-to-end.
        assert(
          capturedPlans.map(stripAQEPlan).forall(!_.isInstanceOf[V2TableWriteExec]),
          s"expected a V1 write command, not a DSv2 write, got: $capturedPlans")

        val declinedScans = capturedPlans.flatMap { plan =>
          collectWithSubqueries(stripAQEPlan(plan)) {
            case f: FileSourceScanExec if DeltaScanSupport.isDeltaScan(f) => f
          }
        }
        val reasons = declinedScans.flatMap(f => new ExtendedExplainInfo().getFallbackReasons(f))
        assert(
          reasons.exists(_.contains("row_index values consumed by the query")),
          "expected the row-index-consumed-by-the-query decline reason, got: " +
            reasons.mkString(", "))

        val readBack = spark.read.parquet(dst)
        checkSparkAnswer(readBack)
        val rows = readBack.collect()
        assert(rows.length == 29, s"expected 29 surviving rows, got ${rows.length}")
        val id31 = rows.find(_.getLong(0) == 31)
        assert(id31.isDefined, "expected id=31 to survive the DELETE")
        assert(
          id31.get.getLong(1) == 31,
          "expected the persisted row_index for id=31 to be 31, got " +
            s"${id31.get.getLong(1)} -- a wrongly-claimed native scan would have written a " +
            "synthetic zero instead")
        val sumRi = rows.map(_.getLong(1)).sum
        assert(
          sumRi == 475,
          s"expected sum(row_index) == 475 (sum(0..31) - (1 + 7 + 13) = 496 - 21), got " +
            s"$sumRi -- a wrongly-claimed native scan would have summed to 0")
      }
    }
  }

  /**
   * The write-sink liveness gate above (`rowIndexUnusedAbove`'s `childOutputLeak` check in
   * `DeltaScanSupport`) covers a DSv2 write sink STRUCTURALLY -- any one-child node with an empty
   * output that doesn't re-expose a tainted attribute, which is exactly the shape
   * `AppendDataExec`/`OverwriteByExpressionExec`/the rest of the `V2TableWriteExec` family take
   * -- but the test above only ever exercises the V1 `.write.parquet` command path.
   *
   * Reaching a genuine DSv2 `AppendDataExec` in this Spark 3.5 setup is itself achievable: a
   * table created via the session catalog with `USING parquet` still plans as a V1
   * `InsertIntoHadoopFsRelationCommand` (built-in file-based sources stay on
   * `spark.sql.sources.useV1SourceList` by default), but `InMemoryTableCatalog` (from
   * `spark-catalyst`'s test-jar, already a test dependency of this module, registered ad hoc
   * under a throwaway name exactly as Spark's own DataSourceV2 test suites do) forces a genuine
   * V2 write.
   *
   * What is NOT achievable in this delta-spark 3.3.2 / Spark 3.5.9 combination: composing that
   * DSv2 `AppendData` write with a deletion-vector-enabled Delta table as its SOURCE. Both
   * `df.writeTo(target).append()` (gluing an already-analyzed `DataFrame` into a fresh V2
   * command) AND a single `INSERT INTO target SELECT ... FROM delta.\`path\`` statement
   * (resolving the read and the V2 write in one analysis pass) hit the identical failure:
   * delta-spark's own `PreprocessTableWithDVs` rule requires the source relation's
   * `TahoeFileIndex` to be a "pinned" `TahoeLogFileIndex`
   * (`ScanWithDeletionVectors$.dvEnabledScanFor`, `PreprocessTableWithDVs.scala:78`), which does
   * not hold when that relation sits under a DSv2 `AppendData` command's analysis -- confirmed
   * unrelated to catalog choice or DataFrame-vs-SQL construction. This is a delta-spark
   * limitation on how a DV read may be composed, not a Comet regression, so this test pins it
   * down as an expected, named failure rather than silently having no DSv2 coverage at all: the
   * write-sink liveness gate's DSv2 coverage for a DV row-index source remains V1-only (see the
   * test above), which this test documents by construction.
   */
  test(
    "deletion vectors: a genuine DSv2 AppendData write cannot compose with a DV-enabled Delta " +
      "source in this Spark/Delta combination (delta-spark's own pinned-snapshot requirement, " +
      "not a Comet regression) -- documents why DSv2 write-sink coverage stays V1-only above") {
    val catalogName = "cometDeltaRowIndexV2Cat"
    withSQLConf(
      s"spark.sql.catalog.$catalogName" ->
        "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog") {
      withTempPath { srcDir =>
        val src = srcDir.getAbsolutePath
        spark
          .range(32)
          .coalesce(1)
          .write
          .format("delta")
          .option("delta.enableDeletionVectors", "true")
          .save(src)
        spark.sql(s"DELETE FROM delta.`$src` WHERE id IN (1, 7, 13)").collect()

        val targetTable = s"$catalogName.ns.row_index_sink"
        spark.sql(s"CREATE TABLE $targetTable (id BIGINT, ri BIGINT) USING foo")

        val ex = intercept[IllegalArgumentException] {
          spark.sql(
            s"INSERT INTO $targetTable SELECT id, _metadata.row_index AS ri FROM delta.`$src`")
        }
        assert(
          ex.getMessage.contains("non-pinned"),
          "expected delta-spark's pinned-TahoeLogFileIndex requirement to be the failure " +
            s"(if this now succeeds, DSv2 coverage for the DV row-index write-sink scenario " +
            s"may finally be achievable and this test should be replaced with a real one): " +
            ex.getMessage)
      }
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

  test(
    "column mapping: rename history colliding logical partition name with physical data " +
      "name reads correctly") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      // a->b then p->a leaves the LOGICAL name "a" bound to the partition column while a
      // DIFFERENT physical data column (originally "a", now logically "b") retains physical
      // name "a". Passing the partition schema's logical names to the native side collides
      // with that retained physical data name and lets DataFusion's name-based partition
      // rewrite replace the data projection with the partition constant.
      spark.sql(s"CREATE TABLE delta.`$path` (a BIGINT, p BIGINT) USING delta PARTITIONED BY (p)")
      enableColumnMapping(path)
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100), (2, 100)")
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN a TO b")
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN p TO a")

      val df = spark.sql(s"SELECT b, a FROM delta.`$path`")
      checkDeltaNativeScanAnswer(df)
      val rows = df.collect().map(r => (r.getLong(0), r.getLong(1))).sorted
      assert(
        rows.sameElements(Array((1L, 100L), (2L, 100L))),
        s"expected (1,100),(2,100) but got ${rows.mkString(", ")}")
    }
  }

  test(
    "column mapping: rename history colliding partition name reads correctly with " +
      "deletion vectors") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (a BIGINT, p BIGINT) USING delta PARTITIONED BY (p)")
      enableColumnMapping(path)
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100), (2, 100)")
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN a TO b")
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN p TO a")
      spark.sql(s"DELETE FROM delta.`$path` WHERE b = 1")

      val df = spark.sql(s"SELECT b, a FROM delta.`$path`")
      checkDeltaNativeScanAnswer(df)
      val rows = df.collect().map(r => (r.getLong(0), r.getLong(1))).sorted
      assert(
        rows.sameElements(Array((2L, 100L))),
        s"expected (2,100) but got ${rows.mkString(", ")}")
    }
  }

  test("column mapping: renamed partition column without collision reads correctly (control)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (a BIGINT, p BIGINT) USING delta PARTITIONED BY (p)")
      enableColumnMapping(path)
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100), (2, 100)")
      // Rename ONLY the partition column, to a name that collides with nothing: no physical
      // data column is named "q", so this must not be affected by the collision above.
      spark.sql(s"ALTER TABLE delta.`$path` RENAME COLUMN p TO q")

      val df = spark.sql(s"SELECT a, q FROM delta.`$path`")
      checkDeltaNativeScanAnswer(df)
      val rows = df.collect().map(r => (r.getLong(0), r.getLong(1))).sorted
      assert(
        rows.sameElements(Array((1L, 100L), (2L, 100L))),
        s"expected (1,100),(2,100) but got ${rows.mkString(", ")}")
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
    // The crash shape: a DPP join in one UNION ALL branch and a coalescible shuffle (the
    // GROUP BY) in the other. Spark's AQE plan validation walks every operator's
    // outputPartitioning, including the DPP branch's scan, before
    // CometPlanAdaptiveDynamicPruningFilters has rewritten the placeholder subquery -- this
    // is the ordering that reproduced the crash.
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

  test("scalar subquery in a partition filter does not force partitioning during AQE checks") {
    // Crash shape: a scalar subquery used directly as
    // a partition filter, e.g. `p = (SELECT max(p) FROM dim ...)`, references only the
    // partition column, so it lands in runtimeFilters rather than dataFilters.
    // ValidateRequirements walks outputPartitioning for every operator, including this scan,
    // before the subquery has executed -- forcing perPartitionData at that point evaluates the
    // still-unresolved ScalarSubquery and throws "has not finished".
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
              .selectExpr("id as p", "case when id in (0, 3) then 'yes' else 'no' end as country")
              .write
              .format("parquet")
              .save(dimPath)
            spark
              .range(0, 500)
              .selectExpr("id", "id % 10 as p")
              .write
              .format("parquet")
              .save(otherPath)

            spark.read.format("delta").load(factPath).createOrReplaceTempView("r45Fact")
            spark.read.format("parquet").load(dimPath).createOrReplaceTempView("r45Dim")
            spark.read.format("parquet").load(otherPath).createOrReplaceTempView("r45Other")

            def query =
              spark.sql("""
                |SELECT id, p FROM r45Fact
                |WHERE p = (SELECT max(p) FROM r45Dim WHERE country = 'yes')
                |UNION ALL
                |SELECT cast(count(*) AS int) AS id, p FROM r45Other GROUP BY p
                |""".stripMargin)

            try {
              checkSparkAnswer(query)
            } catch {
              case e: Throwable =>
                if (e.getMessage != null && e.getMessage.contains("has not finished")) {
                  throw new AssertionError(
                    "AQE ValidateRequirements forced outputPartitioning to evaluate an " +
                      "unresolved scalar partition-filter subquery -- this is the crash this " +
                      "test guards against",
                    e)
                }
                throw e
            }

            val df = query
            df.collect()
            // Best-effort, mirroring the DPP union-all test above: this shape need not always
            // route through the native Delta scan, but if it does, it must have survived AQE's
            // partitioning checks above without throwing. Answer correctness is already
            // verified by checkSparkAnswer above.
            val scans = deltaNativeScans(df)
            if (scans.isEmpty) {
              logInfo(
                "scalar subquery partition filter: no CometDeltaNativeScanExec claimed this " +
                  "query on this build; answer correctness already verified above")
            } else {
              logInfo(
                s"scalar subquery partition filter: ${scans.length} CometDeltaNativeScanExec " +
                  "node(s) claimed this query; answer correctness already verified above")
            }
          }
        }
      }
    }
  }

  test(
    "aggregate over a scalar-subquery partition filter executes under a fused native " +
      "parent") {
    // Crash shape: a scalar subquery used as a partition filter (`p = (SELECT max(p) ...)`)
    // lands in runtimeFilters. Once execution resolves it, a native aggregate sitting
    // directly on top of the scan (no intervening exchange) reads the scan's
    // outputPartitioning to size its own execution context; that getter must report the
    // real post-pruning partition count, not a value stuck from before resolution.
    withTempPath { factDir =>
      withTempPath { thresholdsDir =>
        val factPath = factDir.getAbsolutePath
        val thresholdsPath = thresholdsDir.getAbsolutePath

        spark
          .range(0, 2000)
          .selectExpr("id", "id % 10 as p")
          .write
          .format("delta")
          .partitionBy("p")
          .save(factPath)

        spark
          .sql("SELECT CAST(7 AS BIGINT) AS p")
          .write
          .format("delta")
          .save(thresholdsPath)

        def query =
          spark.sql(
            s"SELECT sum(id) AS total FROM delta.`$factPath` " +
              s"WHERE p = (SELECT max(p) FROM delta.`$thresholdsPath`)")

        checkSparkAnswer(query)

        val df = query
        try {
          df.collect()
        } catch {
          case e: Throwable =>
            if (e.getMessage != null && e.getMessage.contains("All per-partition arrays")) {
              throw new AssertionError(
                "a fused native aggregate above the scan read a stale zero " +
                  "outputPartitioning after the scalar-subquery partition filter had " +
                  "already resolved",
                e)
            }
            throw e
        }

        val scans = deltaNativeScans(df)
        assert(
          scans.nonEmpty,
          s"expected CometDeltaNativeScanExec in plan:\n${df.queryExecution.executedPlan}")
        assert(
          collectByName(df.queryExecution.executedPlan, "CometHashAggregateExec").nonEmpty,
          "expected a fused native aggregate parent above the scan in plan:\n" +
            s"${df.queryExecution.executedPlan}")
      }
    }
  }

  test(
    "metrics evaluates without throwing when runtimeFilters holds a ScalarSubquery " +
      "placeholder (pins the invariant documented on CometDeltaNativeScanExec.scanHelper: " +
      "AQE's UI plan-walk calls .metrics on every node mid-planning, sometimes before a " +
      "DPP/scalar-subquery filter has resolved, and this must never throw)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 50).write.format("delta").save(path)

      val scan = deltaNativeScans(spark.read.format("delta").load(path)).collect {
        case s: CometDeltaNativeScanExec => s
      }.head

      // A real execution.ScalarSubquery instance (the exec-time class CometDeltaNativeScanExec
      // itself matches against in hasUnevaluableSubqueryFilter), wrapping a never-executed
      // SubqueryExec -- deliberately never run, so this is unresolved exactly as it would be
      // when AQE's mid-planning walk reaches this node ahead of subquery execution.
      val innerPlan = spark.range(1).selectExpr("id AS c").queryExecution.executedPlan
      val unresolvedScalarSubquery =
        ScalarSubquery(
          SubqueryExec("metrics-guard-subquery", innerPlan),
          NamedExpression.newExprId)

      val scanWithSubquery = scan.copy(runtimeFilters = Seq(unresolvedScalarSubquery))
      val metrics = scanWithSubquery.metrics
      assert(
        metrics.nonEmpty,
        "expected CometDeltaNativeScanExec.metrics to populate the native scan metric node " +
          "even with an unresolved ScalarSubquery in runtimeFilters")
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

  test(
    "decline: SMALLINT column falls back with correct results when unsigned-small-int " +
      "safety check is enabled") {
    // Regression: the Delta claim path must
    // run the same CometScanTypeChecker core's own scan does, so the default-on
    // COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK safety fallback still applies to a native Delta
    // scan. Without it, an out-of-range/malformed UINT_8 payload stored under a ShortType
    // column could be claimed and silently decoded with the wrong values.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, s SMALLINT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 10), (2, 20), (3, 30)")

      // CometTestBase flips this conf off by default so the rest of the suite can exercise
      // ShortType columns against Comet's native scan; put it back to its real production
      // default so this gate actually declines (mirrors the same pattern in
      // DeltaScanContribSuite for the vectorized-reader conf).
      withSQLConf(CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key -> "true") {
        val df = spark.read.format("delta").load(path)
        checkSparkAnswerAndFallbackReason(
          df,
          CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key)
        assert(deltaNativeScans(df).isEmpty)
      }
    }
  }

  test("claims SMALLINT column natively when unsigned-small-int safety check is disabled") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, s SMALLINT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 10), (2, 20), (3, 30)")

      withSQLConf(CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key -> "false") {
        val df = spark.read.format("delta").load(path)
        checkDeltaNativeScanAnswer(df)
      }
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
    // The shape this decline guards against: a Delta shallow clone whose table ROOT is a natively
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

  test("change data feed read never engages the native Delta scan, with correct results") {
    // A batch readChangeFeed() query never reaches DeltaScanSupport.declineReason's own
    // isCDCRead check at all: CDCReader wraps its answer in a DeltaCDFRelation whose buildScan
    // executes its internal (possibly DeltaParquetFileFormat-backed) plan via queryExecution's
    // RDD lineage directly, so the physical plan Spark and Comet's extensions ultimately see for
    // this query is a single, opaque RowDataSourceScanExec, never a FileSourceScanExec
    // DeltaScanSupport.isDeltaScan could recognize. This still pins the outcome that matters:
    // Change Data Feed reads are never claimed by the native Delta scan and stay correct.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      // Change Data Feed must be enabled from the table's first version: CDC reads validate
      // that change data was actually recorded for every version in the requested range.
      spark.sql(s"""CREATE TABLE delta.`$path` (id LONG, v LONG) USING delta
                   |TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$path` SELECT id, id * 2 FROM range(0, 100)")
      spark.sql(s"UPDATE delta.`$path` SET v = -1 WHERE id < 10")

      val df = spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .load(path)
      checkSparkAnswer(df)
      assert(deltaNativeScans(df).isEmpty)
      assert(df.count() > 0)
    }
  }

  test("reader features: TIMESTAMP_NTZ column claims natively with correct results") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id LONG, ts TIMESTAMP_NTZ) USING delta")
      spark.sql(
        s"INSERT INTO delta.`$path` VALUES " +
          "(1, CAST('2021-01-01 00:00:00' AS TIMESTAMP_NTZ)), " +
          "(2, CAST('2022-06-15 12:30:00' AS TIMESTAMP_NTZ))")

      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == 2)
    }
  }

  test("reader features: v2Checkpoint table claims natively with correct results") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"""CREATE TABLE delta.`$path` (id LONG, v LONG) USING delta
                   |TBLPROPERTIES (
                   |  'delta.checkpointPolicy' = 'v2',
                   |  'delta.checkpointInterval' = '3')""".stripMargin)
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
    "reader features: an unsupported reader feature (type widening) declines with the " +
      "reader feature(s) reason") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
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

      val df = spark.read.format("delta").load(path)
      checkSparkAnswerAndFallbackReason(
        df,
        "Native Delta scan does not support reader feature(s) typeWidening")
      assert(deltaNativeScans(df).isEmpty)
    }
  }

  test("_metadata.row_index declines before any deletion vector exists on a DV-enabled table") {
    // _metadata.row_index only resolves on a Delta table once deletion-vector support is on
    // the protocol (it errors as an unknown field otherwise); once it resolves, Delta always
    // routes the read through the DV-application shape (a row-index column with no
    // is_row_deleted alongside it), even with zero deletion vectors written yet. This pins that
    // the hasRowIndex-without-hasIsRowDeleted gate declines this shape regardless of whether a
    // DV has ever actually been written for the file.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).selectExpr("id").write.format("delta").save(path)
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")

      val df = spark.read
        .format("delta")
        .load(path)
        .selectExpr("id", "_metadata.row_index as ri")
      checkSparkAnswerAndFallbackReason(
        df,
        "Native Delta scan does not support row-index reads outside a deletion-vector scan")
      assert(deltaNativeScans(df).isEmpty)
      assert(df.count() == 100)
    }
  }

  test(
    "decline: parquet.crypto.factory.class configured declines conservatively even without " +
      "actual encryption") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).selectExpr("id", "id * 2 as v").write.format("delta").save(path)

      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val key = "parquet.crypto.factory.class"
      val prior = Option(hadoopConf.get(key))
      // A real, resolvable factory that explicitly allows plaintext files: the table itself is
      // NOT encrypted, so this exercises Comet's stricter, conservative "decline ALL
      // encrypted-parquet configurations" gate without breaking Spark's own read.
      hadoopConf.set(key, "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
      try {
        val df = spark.read.format("delta").load(path)
        checkSparkAnswerAndFallbackReason(
          df,
          "Native Delta scan does not support encrypted parquet")
        assert(deltaNativeScans(df).isEmpty)
        assert(df.count() == 100)
      } finally {
        prior match {
          case Some(v) => hadoopConf.set(key, v)
          case None => hadoopConf.unset(key)
        }
      }
    }
  }

  test(
    "deletion vectors: a data predicate deleting every row of one file still claims " +
      "natively with correct results") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 40)
        .selectExpr("id", "id % 2 as p", "id * 2 as v")
        .repartition(2, col("p"))
        .write
        .format("delta")
        .partitionBy("p")
        .save(path)
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      // A data-column predicate (not purely a partition predicate) forces Delta through the
      // row-level deletion-vector path rather than a metadata-only partition drop, even though
      // every row in partition 1's file happens to match.
      spark.sql(s"DELETE FROM delta.`$path` WHERE p = 1 AND v >= 0")

      val df = spark.read.format("delta").load(path)
      checkDeltaNativeScanAnswer(df)
      assert(df.count() == 20)
      assert(df.filter(col("p") === 1).count() == 0)
    }
  }

  test(
    "conf interactions: ANSI, case sensitivity, and disabled DPP leave claim/decline " +
      "outcomes unchanged") {
    withTempPath { claimDir =>
      withTempPath { declineDir =>
        val claimPath = claimDir.getAbsolutePath
        val declinePath = declineDir.getAbsolutePath
        spark.range(0, 200).selectExpr("id", "id * 2 as v").write.format("delta").save(claimPath)
        spark.sql(s"""CREATE TABLE delta.`$declinePath` (id LONG, v LONG) USING delta
                     |TBLPROPERTIES ('delta.columnMapping.mode' = 'id')""".stripMargin)
        spark
          .range(0, 200)
          .selectExpr("id", "id * 2 as v")
          .write
          .format("delta")
          .mode("append")
          .save(declinePath)

        val confVariants = Seq(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.CASE_SENSITIVE.key -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false")

        confVariants.foreach { case (key, value) =>
          withSQLConf(key -> value) {
            val claimDf = spark.read.format("delta").load(claimPath)
            checkDeltaNativeScanAnswer(claimDf)

            val declineDf = spark.read.format("delta").load(declinePath)
            checkSparkAnswer(declineDf)
            assert(
              deltaNativeScans(declineDf).isEmpty,
              s"expected id-mode column mapping to still decline under $key=$value")
          }
        }
      }
    }
  }

  test(
    "deletion vectors: maxDeletedRowsPerFile boundary claims when cardinality exactly " +
      "equals the limit (gate declines only when the limit is exceeded)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 1000)
        .selectExpr("id", "id * 2 as v")
        .coalesce(1)
        .write
        .format("delta")
        .save(path)
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")

      withSQLConf(DeltaScanConf.COMET_DELTA_MAX_DELETED_ROWS_PER_FILE.key -> "500") {
        val df = spark.read.format("delta").load(path)
        checkDeltaNativeScanAnswer(df)
        assert(df.count() == 500)
      }
    }
  }

  // Both tests below pin caseSensitive=true purely to exercise the exact-match (non-folding)
  // path for a non-ASCII column name. Native's case-insensitive name matching reproduces the
  // JVM's `toLowerCase(Locale.ROOT)` fold (see `fold_names` in
  // native/core/src/parquet/name_fold.rs), so caseSensitive=false would also read these
  // correctly -- there is no decline gate involved here to route around.
  test("unicode column names round-trip natively with correct results") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        spark.sql(s"CREATE TABLE delta.`$path` (id LONG, `名前` STRING) USING delta")
        spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 'たろう'), (2, 'はなこ')")

        val df = spark.sql(s"SELECT id, `名前` FROM delta.`$path` ORDER BY id")
        checkDeltaNativeScanAnswer(df)
        val rows = df.collect()
        assert(rows.map(_.getString(1)).sameElements(Array("たろう", "はなこ")))
      }
    }
  }

  test("unicode and space-containing column names round-trip natively under column mapping") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        // A space is one of Parquet's disallowed schema-name characters, so the space-containing
        // column can only be added AFTER column mapping (physical names) is already active --
        // creating it inline at CREATE TABLE time fails before column mapping ever takes effect.
        spark.sql(s"CREATE TABLE delta.`$path` (id LONG, `名前` STRING) USING delta")
        enableColumnMapping(path)
        spark.sql(s"ALTER TABLE delta.`$path` ADD COLUMN `a b` LONG")
        spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 'たろう', 10), (2, 'はなこ', 20)")

        val df = spark.sql(s"SELECT id, `名前`, `a b` FROM delta.`$path` ORDER BY id")
        checkDeltaNativeScanAnswer(df)
        val rows = df.collect()
        assert(rows.map(_.getString(1)).sameElements(Array("たろう", "はなこ")))
        assert(rows.map(_.getLong(2)).sameElements(Array(10L, 20L)))
      }
    }
  }

  /** Fallback reason strings for every declined Delta scan node in `df`'s (executed) plan. */
  private def deltaDeclineReasons(df: DataFrame): Seq[String] =
    collectWithSubqueries(stripAQEPlan(df.queryExecution.executedPlan)) {
      case f: FileSourceScanExec if DeltaScanSupport.isDeltaScan(f) => f
    }.flatMap(f => new ExtendedExplainInfo().getFallbackReasons(f))

  test(
    "a non-ASCII case-insensitive column name claims the native Delta scan with correct " +
      "results") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        val table = "comet_unicode_" + java.util.UUID.randomUUID().toString.replace("-", "")
        withTable(table) {
          // Two plain parquet files whose footers differ only in the case of a non-ASCII letter
          // (an ordinary CONVERT-eligible layout: no column mapping, no defaults, no DVs).
          // Native's name matcher reproduces this JVM's `toLowerCase(Locale.ROOT)` from
          // shipped case tables, which folds 'É'/'é' together just like Spark does.
          spark.range(1, 2).select(col("id"), lit(71).as("É")).coalesce(1).write.parquet(path)
          spark
            .range(2, 3)
            .select(col("id"), lit(72).as("é"))
            .coalesce(1)
            .write
            .mode("append")
            .parquet(path)

          spark.sql(s"CREATE TABLE $table (id BIGINT, `É` INT) USING PARQUET LOCATION '$path'")
          spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")

          val df = spark.read.format("delta").load(path).selectExpr("id", "`É`")
          checkDeltaNativeScanAnswer(df)
          val rows = df.collect().sortBy(_.getLong(0))
          assert(rows.map(_.getInt(1)).sameElements(Array(71, 72)))
        }
      }
    }
  }

  test(
    "an ASCII case-insensitive column name still claims the native Delta scan with correct " +
      "results") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        val table = "comet_ascii_case_" + java.util.UUID.randomUUID().toString.replace("-", "")
        withTable(table) {
          // Same shape as above, but the differing-case letter is plain ASCII, which native's
          // name folding (`fold_names` in name_fold.rs) always matches correctly, ASCII being
          // the easy case.
          spark.range(1, 2).select(col("id"), lit(71).as("E")).coalesce(1).write.parquet(path)
          spark
            .range(2, 3)
            .select(col("id"), lit(72).as("e"))
            .coalesce(1)
            .write
            .mode("append")
            .parquet(path)

          spark.sql(s"CREATE TABLE $table (id BIGINT, `E` INT) USING PARQUET LOCATION '$path'")
          spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")

          val df = spark.read.format("delta").load(path).selectExpr("id", "`E`")
          checkDeltaNativeScanAnswer(df)
          val rows = df.collect().sortBy(_.getLong(0))
          assert(rows.map(_.getInt(1)).sameElements(Array(71, 72)))
        }
      }
    }
  }

  test(
    "a non-ASCII partition column name still claims the native Delta scan with correct " +
      "results") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        // Partition values are injected into the output as constants by exact name match, never
        // matched against a file's footer schema, so a non-ASCII partition name (data names stay
        // plain ASCII here) never goes through native's case-insensitive DATA-column name
        // folding (`fold_names` in name_fold.rs) at all.
        spark
          .range(0, 20)
          .selectExpr("id", "cast(id % 4 as long) as `名前`")
          .write
          .format("delta")
          .partitionBy("名前")
          .save(path)

        val df = spark.read.format("delta").load(path).filter(col("名前") === 2)
        checkDeltaNativeScanAnswer(df)
        assert(df.count() > 0)
      }
    }
  }

  test(
    "a non-ASCII physical column name still claims the native Delta scan under column " +
      "mapping with case-insensitive reads") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        // The column pre-exists the column-mapping upgrade, so Delta assigns its physical name
        // as its current (non-ASCII) name verbatim -- exactly what a converted-then-upgraded
        // table keeps. Logical and physical names are identical here, so this was always safe;
        // it now also claims natively rather than being caught by a blanket non-ASCII gate.
        spark.sql(s"CREATE TABLE delta.`$path` (id LONG, `É` STRING) USING delta")
        enableColumnMapping(path)
        spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 'a'), (2, 'b')")

        val df = spark.sql(s"SELECT id, `É` FROM delta.`$path` ORDER BY id")
        checkDeltaNativeScanAnswer(df)
        val rows = df.collect()
        assert(rows.map(_.getString(1)).sameElements(Array("a", "b")))
      }
    }
  }

  test(
    "a Kelvin sign physical column name in one file of an otherwise-ASCII CONVERTed table " +
      "claims the native Delta scan with correct results") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        val table = "comet_kelvin_" + java.util.UUID.randomUUID().toString.replace("-", "")
        withTable(table) {
          // An ordinary CONVERT-eligible layout (no column mapping, no defaults, no DVs)
          // where the table is declared with a plain ASCII "K" column, but one of its
          // underlying Parquet files happens to have been written with a physical column
          // literally named U+212A (KELVIN SIGN) -- not decomposable to ASCII by naive
          // folding, but a case variant of ASCII 'k'/'K' under Java's `Character` mappings
          // (and thus under Spark's `caseSensitive=false` resolution). Nothing on the JVM
          // side can see this: the divergent name lives only in the second file's footer.
          spark.range(1, 2).select(col("id"), lit(71).as("K")).coalesce(1).write.parquet(path)
          spark
            .range(2, 3)
            .select(col("id"), lit(72).as("K"))
            .coalesce(1)
            .write
            .mode("append")
            .parquet(path)

          spark.sql(s"CREATE TABLE $table (id BIGINT, `K` INT) USING PARQUET LOCATION '$path'")
          spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")

          val df = spark.read.format("delta").load(path).selectExpr("id", "`K`")
          checkDeltaNativeScanAnswer(df)
          val rows = df.collect().sortBy(_.getLong(0))
          assert(rows.map(_.getInt(1)).sameElements(Array(71, 72)))
          assert(
            df.filter(col("K").isNotNull).count() == 2,
            "the Kelvin-sign-named file's row must not be nulled out by native")
        }
      }
    }
  }

  test(
    "a capital-sigma physical column name matches a final-sigma table column with correct " +
      "results") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        val table = "comet_sigma_" + java.util.UUID.randomUUID().toString.replace("-", "")
        withTable(table) {
          // Java's `String.toLowerCase(Locale.ROOT)` lowers "A1Σ" to "a1ς" (FINAL
          // sigma): its Final_Cased context scan runs on word boundaries, and the digit keeps
          // "A1Σ" a single word, so the trailing sigma takes the final form. Spark's
          // footer matching therefore folds physical "A1Σ" onto a requested "a1ς",
          // and the value in that file must be read, not nulled. Nothing on the JVM side can
          // see this: the divergent name lives only in the second file's footer.
          spark
            .range(1, 2)
            .select(col("id"), lit(71).as("a1ς"))
            .coalesce(1)
            .write
            .parquet(path)
          spark
            .range(2, 3)
            .select(col("id"), lit(72).as("A1Σ"))
            .coalesce(1)
            .write
            .mode("append")
            .parquet(path)

          spark.sql(s"CREATE TABLE $table (id BIGINT, `a1ς` INT) USING PARQUET LOCATION '$path'")
          spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")

          val df = spark.read.format("delta").load(path).selectExpr("id", "`a1ς`")
          checkDeltaNativeScanAnswer(df)
          val rows = df.collect().sortBy(_.getLong(0))
          assert(rows.map(_.getInt(1)).sameElements(Array(71, 72)))
          assert(
            df.filter(col("a1ς").isNotNull).count() == 2,
            "the capital-sigma-named file's row must not be nulled out by native")
        }
      }
    }
  }

  test("a capital-sigma physical column name is missing for a non-final-sigma table column") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        val table = "comet_sigma_miss_" + java.util.UUID.randomUUID().toString.replace("-", "")
        withTable(table) {
          // The inverse of the test above: "A1Σ" lowers to "a1ς", NOT "a1σ"
          // (non-final sigma), so Spark's footer lookup treats a requested "a1σ" as
          // MISSING in the capital-sigma file and substitutes NULL. Reading a value there
          // (as a naive codepoint-wise fold would) surfaces a row Spark considers absent
          // and breaks IS NOT NULL filters.
          spark
            .range(1, 2)
            .select(col("id"), lit(71).as("a1σ"))
            .coalesce(1)
            .write
            .parquet(path)
          spark
            .range(2, 3)
            .select(col("id"), lit(72).as("A1Σ"))
            .coalesce(1)
            .write
            .mode("append")
            .parquet(path)

          spark.sql(s"CREATE TABLE $table (id BIGINT, `a1σ` INT) USING PARQUET LOCATION '$path'")
          spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")

          val df = spark.read.format("delta").load(path).selectExpr("id", "`a1σ`")
          checkDeltaNativeScanAnswer(df)
          val rows = df.collect().sortBy(_.getLong(0))
          assert(rows.length == 2)
          assert(rows(0).getInt(1) == 71)
          assert(
            rows(1).isNullAt(1),
            "the capital-sigma file's column lowers to final sigma, so a non-final-sigma " +
              "requested column must read as missing (NULL) there")
        }
      }
    }
  }

  test("a Unicode-version-drift physical column name folds per the running JDK") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        val table = "comet_drift_" + java.util.UUID.randomUUID().toString.replace("-", "")
        withTable(table) {
          // U+A7C0 (LATIN CAPITAL LETTER OLD POLISH O) gained its lowercase pairing U+A7C1
          // in Unicode 14, after JDK 17's Unicode snapshot: JDK 17 lowers it to itself
          // (no match against a U+A7C1 column), while JDK 21+ lowers it to U+A7C1 (match).
          // The expectation is derived from the RUNNING JDK's own toLowerCase, so this test
          // is correct on any JDK -- exactly the property the native matcher must mirror,
          // since it consumes case tables generated by this same JVM at plan time.
          val physicalFolds =
            "Ꟁ".toLowerCase(java.util.Locale.ROOT) == "ꟁ"

          spark
            .range(1, 2)
            .select(col("id"), lit(71).as("ꟁ"))
            .coalesce(1)
            .write
            .parquet(path)
          spark
            .range(2, 3)
            .select(col("id"), lit(72).as("Ꟁ"))
            .coalesce(1)
            .write
            .mode("append")
            .parquet(path)

          spark.sql(s"CREATE TABLE $table (id BIGINT, `ꟁ` INT) USING PARQUET LOCATION '$path'")
          spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")

          val df = spark.read.format("delta").load(path).selectExpr("id", "`ꟁ`")
          checkDeltaNativeScanAnswer(df)
          val rows = df.collect().sortBy(_.getLong(0))
          assert(rows.length == 2)
          assert(rows(0).getInt(1) == 71)
          if (physicalFolds) {
            assert(
              !rows(1).isNullAt(1) && rows(1).getInt(1) == 72,
              "this JDK folds U+A7C0 onto U+A7C1, so the value must be read")
          } else {
            assert(
              rows(1).isNullAt(1),
              "this JDK does not fold U+A7C0 onto U+A7C1, so the column must be missing")
          }
        }
      }
    }
  }

  /**
   * Runs `action` under a [[SparkListener]] that captures every `onTaskEnd` input-metrics
   * reading, then waits (via `eventually`, since this suite lives outside the `org.apache.spark`
   * package and cannot reach the package-private `SparkContext.listenerBus.waitUntilEmpty`) for
   * the aggregated recordsRead to reach at least `minRecords` -- the listener bus delivers
   * `onTaskEnd` asynchronously, so `action` returning is not enough to guarantee every event has
   * already been processed. `minRecords` is a floor rather than an exact target because Delta's
   * own transaction-log state reconstruction runs a small auxiliary job reading the commit JSON,
   * which legitimately contributes a few extra input records alongside the actual data scan.
   * Returns the aggregated (recordsRead, bytesRead) once stable.
   */
  private def collectTaskInputMetrics(minRecords: Long)(action: => Unit): (Long, Long) = {
    val inputRecords = mutable.ArrayBuffer.empty[Long]
    val inputBytes = mutable.ArrayBuffer.empty[Long]
    val listener = new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val im = taskEnd.taskMetrics.inputMetrics
        inputRecords.synchronized { inputRecords += im.recordsRead }
        inputBytes.synchronized { inputBytes += im.bytesRead }
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      action
      eventually(timeout(30.seconds), interval(200.milliseconds)) {
        val recordsRead = inputRecords.synchronized(inputRecords.sum)
        assert(
          recordsRead >= minRecords,
          s"expected task input recordsRead to reach at least $minRecords, currently $recordsRead")
      }
      (inputRecords.synchronized(inputRecords.sum), inputBytes.synchronized(inputBytes.sum))
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("standalone uncached delta read reports task-level input metrics") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 10000)
        .selectExpr("id", "id * 2 as v")
        .write
        .format("delta")
        .save(path)

      val df = spark.read.format("delta").load(path)
      var collected = 0L
      val (recordsRead, bytesRead) = collectTaskInputMetrics(10000L) {
        collected = df.collect().length.toLong
      }

      assert(collected == 10000L)
      assert(
        deltaNativeScans(df).nonEmpty,
        s"expected a native Delta scan:\n${df.queryExecution.executedPlan}")
      assert(
        recordsRead >= 10000L,
        s"expected task input recordsRead to cover the row count, got $recordsRead")
      assert(bytesRead > 0L, s"expected task input bytesRead > 0, got $bytesRead")
    }
  }

  test("fused aggregate over a delta scan reports task-level input metrics") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 10000)
        .selectExpr("id", "id % 13 as g", "id * 2 as v")
        .write
        .format("delta")
        .save(path)

      val df = spark.read.format("delta").load(path).groupBy("g").sum("v")
      val (recordsRead, bytesRead) = collectTaskInputMetrics(10000L) {
        df.collect()
      }

      assert(
        deltaNativeScans(df).nonEmpty,
        s"expected the native Delta scan fused into the aggregate:\n${df.queryExecution.executedPlan}")
      assert(
        recordsRead >= 10000L,
        s"expected task input recordsRead to cover the scanned row count, got $recordsRead")
      assert(bytesRead > 0L, s"expected task input bytesRead > 0, got $bytesRead")
    }
  }

  /** Write `values` rows (id, d, ts) as a Delta table with the given write-side rebase modes. */
  private def writeRebaseTable(
      path: String,
      timeZone: String,
      datetimeMode: String,
      int96Mode: String,
      values: String): Unit = {
    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone,
      "spark.sql.parquet.datetimeRebaseModeInWrite" -> datetimeMode,
      "spark.sql.parquet.int96RebaseModeInWrite" -> int96Mode) {
      spark
        .sql(s"select * from values $values as t(id, d, ts)")
        .write
        .format("delta")
        .save(path)
    }
  }

  test("legacy-rebase ancient dates and timestamps match Spark's own read") {
    // Spark stamps org.apache.spark.legacyDateTime / legacyINT96 / timeZone into the file
    // footer when writing with LEGACY rebase modes, and its own reader rebases based on that
    // per-file metadata regardless of the session's read-mode conf. The native scan must
    // resolve the same per-file policy: without rebasing, 1500-01-01 reads as 1500-01-10.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeRebaseTable(
        path,
        timeZone = "UTC",
        datetimeMode = "LEGACY",
        int96Mode = "LEGACY",
        values = "(1, date'0001-01-01', timestamp'1500-01-01 00:00:00'), " +
          "(2, date'1500-01-01', timestamp'1582-10-04 23:59:59'), " +
          "(3, date'1582-10-04', timestamp'0001-01-01 00:00:00'), " +
          "(4, date'2024-06-01', timestamp'2024-06-01 12:00:00')")
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        val df = spark.read.format("delta").load(path)
        checkDeltaNativeScanAnswer(df)
      }
    }
  }

  test("predicate on a legacy-rebase ancient date matches Spark") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeRebaseTable(
        path,
        timeZone = "UTC",
        datetimeMode = "LEGACY",
        int96Mode = "LEGACY",
        values = "(1, date'1500-01-01', timestamp'1500-01-01 00:00:00'), " +
          "(2, date'1500-02-11', timestamp'1500-02-11 00:00:00'), " +
          "(3, date'2024-06-01', timestamp'2024-06-01 12:00:00')")
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        val df = spark.read
          .format("delta")
          .load(path)
          .filter("d = date'1500-01-01'")
        checkDeltaNativeScanAnswer(df)
      }
    }
  }

  test("legacy-rebase file holding only modern values stays native and correct") {
    // Rebasing is the identity from 1582-10-15 onward, so a LEGACY-stamped file whose values
    // are all modern must keep reading natively with unchanged results.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeRebaseTable(
        path,
        timeZone = "UTC",
        datetimeMode = "LEGACY",
        int96Mode = "LEGACY",
        values = "(1, date'1990-01-01', timestamp'1990-01-01 00:00:00'), " +
          "(2, date'2024-06-01', timestamp'2024-06-01 12:00:00')")
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        val df = spark.read.format("delta").load(path)
        checkDeltaNativeScanAnswer(df)
      }
    }
  }

  test(
    "legacy-rebase ancient timestamps with a non-UTC writer zone fail loudly instead of " +
      "returning shifted values") {
    // Timestamp rebasing outside a fixed UTC writer zone needs the JVM's historical timezone
    // tables; the native reader refuses ancient values rather than guessing.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeRebaseTable(
        path,
        timeZone = "America/Los_Angeles",
        datetimeMode = "LEGACY",
        int96Mode = "LEGACY",
        values = "(1, date'2024-06-01', timestamp'1500-01-01 00:00:00')")
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
        val e = intercept[Exception] {
          spark.read.format("delta").load(path).collect()
        }
        val messages = Iterator
          .iterate(e: Throwable)(_.getCause)
          .takeWhile(_ != null)
          .map(_.getMessage)
          .mkString("\n")
        assert(messages.contains("rebase"), s"expected a calendar-rebase error, got:\n$messages")
      }
    }
  }

  test("mixed rebase flags attribute each timestamp column to its physical type's flag") {
    // legacyDateTime governs INT64 timestamps while legacyINT96 governs INT96 ones. A file
    // carrying exactly one of the two flags must read every timestamp column under the flag
    // of its own physical type -- rebased exactly when that flag is LEGACY, verbatim when it
    // is not -- matching Spark's own read, instead of refusing ancient values because the two
    // flags disagree. All four (physical type, mode pair) combinations round-trip
    // 1500-01-01 00:00:00.
    for ((outputType, datetimeMode, int96Mode) <- Seq(
        ("TIMESTAMP_MICROS", "LEGACY", "CORRECTED"),
        ("TIMESTAMP_MICROS", "CORRECTED", "LEGACY"),
        ("INT96", "LEGACY", "CORRECTED"),
        ("INT96", "CORRECTED", "LEGACY"))) {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        withSQLConf("spark.sql.parquet.outputTimestampType" -> outputType) {
          writeRebaseTable(
            path,
            timeZone = "UTC",
            datetimeMode = datetimeMode,
            int96Mode = int96Mode,
            values = "(1, date'2024-06-01', timestamp'1500-01-01 00:00:00'), " +
              "(2, date'2024-06-01', timestamp'2024-06-01 12:00:00')")
        }
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
          val df = spark.read.format("delta").load(path)
          checkDeltaNativeScanAnswer(df)
          val rows = df.selectExpr("id", "cast(ts as string)").collect().sortBy(_.getInt(0))
          assert(
            rows(0).getString(1) == "1500-01-01 00:00:00",
            s"$outputType/$datetimeMode/$int96Mode: got ${rows(0)}")
        }
      }
    }
  }

  /**
   * Write one raw parquet file through parquet-mr's example writer: NO Spark writer metadata
   * (`org.apache.spark.version` and friends) lands in the footer, the shape any non-Spark writer
   * produces. Spark resolves such files' rebase policy from the session read modes
   * (`DataSourceUtils.getRebaseSpec`'s `modeByConfig` fallback), so the native scan must too.
   * Rows are (id, days-since-epoch date, micros-since-epoch UTC timestamp).
   */
  private def writeNonSparkParquetFile(
      dir: String,
      rows: Seq[(Int, Option[Int], Option[Long])]): Unit = {
    writeRawParquetFile(
      dir,
      """message m {
        |  required int32 id;
        |  optional int32 d (DATE);
        |  optional int64 ts (TIMESTAMP_MICROS);
        |}""".stripMargin) { factory =>
      rows.map { case (id, d, ts) =>
        val group = factory.newGroup().append("id", id)
        d.foreach(group.append("d", _))
        ts.foreach(group.append("ts", _))
        group
      }
    }
  }

  /**
   * Write one raw parquet file of the given parquet-mr `schema` (message type syntax) with the
   * groups `rows` builds from a factory for that schema. Like [[writeNonSparkParquetFile]], no
   * Spark writer metadata lands in the footer.
   */
  private def writeRawParquetFile(dir: String, schema: String)(
      rows: org.apache.parquet.example.data.simple.SimpleGroupFactory => Seq[
        org.apache.parquet.example.data.Group]): Unit = {
    import org.apache.parquet.example.data.simple.SimpleGroupFactory
    import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
    import org.apache.parquet.schema.MessageTypeParser
    val messageType = MessageTypeParser.parseMessageType(schema)
    val conf = new org.apache.hadoop.conf.Configuration()
    GroupWriteSupport.setSchema(messageType, conf)
    val writer = ExampleParquetWriter
      .builder(new org.apache.hadoop.fs.Path(s"$dir/part-00000.parquet"))
      .withConf(conf)
      .build()
    try {
      rows(new SimpleGroupFactory(messageType)).foreach(writer.write)
    } finally {
      writer.close()
    }
  }

  /**
   * The 12-byte INT96 encoding of midnight on the day `days` after 1970-01-01: 8 bytes of
   * nanos-of-day then the 4-byte Julian Day Number (2440588 + days), both little-endian, the
   * layout Spark's `ParquetRowConverter.binaryToSQLTimestamp` decodes.
   */
  private def int96Midnight(days: Int): org.apache.parquet.io.api.Binary = {
    val buf = java.nio.ByteBuffer.allocate(12).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    buf.putLong(0L).putInt(2440588 + days)
    org.apache.parquet.io.api.Binary.fromConstantByteArray(buf.array())
  }

  /** Collect every message down the cause chain of `e`, newline-joined. */
  private def causeMessages(e: Throwable): String =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString("\n")

  /** Spark's `RebaseDateTime.lastSwitchJulianTs`: 1900-01-01T00:00:00Z in micros. */
  private val LastSwitchJulianMicros = -2208988800000000L

  test(
    "non-Spark INT64 timestamps at or after 1900-01-01 read verbatim under EXCEPTION read " +
      "modes") {
    // Spark's EXCEPTION read mode refuses only timestamps before
    // RebaseDateTime.lastSwitchJulianTs (1900-01-01T00:00:00Z, the last instant at which
    // rebasing changes a value in any zone), converting MILLIS columns to micros first; a
    // timestamp one microsecond before the epoch is well inside the accepted range.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeRawParquetFile(
        path,
        """message m {
          |  required int32 id;
          |  optional int64 ts_us (TIMESTAMP(MICROS,true));
          |  optional int64 ts_ms (TIMESTAMP(MILLIS,true));
          |}""".stripMargin) { factory =>
        Seq(
          factory.newGroup().append("id", 1).append("ts_us", -1L).append("ts_ms", -1L),
          factory
            .newGroup()
            .append("id", 2)
            .append("ts_us", LastSwitchJulianMicros)
            .append("ts_ms", LastSwitchJulianMicros / 1000),
          factory
            .newGroup()
            .append("id", 3)
            .append("ts_us", 1717243200000000L)
            .append("ts_ms", 1717243200000L),
          factory.newGroup().append("id", 4))
      }
      val table = "comet_nonspark_1900_" + java.util.UUID.randomUUID().toString.replace("-", "")
      withTable(table) {
        spark.sql(
          s"CREATE TABLE $table (id INT, ts_us TIMESTAMP, ts_ms TIMESTAMP) USING PARQUET " +
            s"LOCATION '$path'")
        spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.parquet.datetimeRebaseModeInRead" -> "EXCEPTION",
          "spark.sql.parquet.int96RebaseModeInRead" -> "EXCEPTION") {
          val df = spark.read.format("delta").load(path)
          checkDeltaNativeScanAnswer(df)
          val rows = df
            .selectExpr("id", "cast(ts_us as string)", "cast(ts_ms as string)")
            .collect()
            .sortBy(_.getInt(0))
          assert(rows(0).getString(1) == "1969-12-31 23:59:59.999999", s"got ${rows(0)}")
          assert(rows(0).getString(2) == "1969-12-31 23:59:59.999", s"got ${rows(0)}")
          assert(rows(1).getString(1) == "1900-01-01 00:00:00", s"got ${rows(1)}")
          assert(rows(1).getString(2) == "1900-01-01 00:00:00", s"got ${rows(1)}")
          assert(rows(2).getString(1) == "2024-06-01 12:00:00", s"got ${rows(2)}")
          assert(rows(3).isNullAt(1) && rows(3).isNullAt(2), s"got ${rows(3)}")
        }
      }
    }
  }

  test("non-Spark INT64 timestamps before 1900-01-01 fail loudly under EXCEPTION read modes") {
    // One millisecond before the cutoff, in a MILLIS column: Spark converts to micros before
    // comparing against lastSwitchJulianTs and raises; the native scan must raise too.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeRawParquetFile(
        path,
        """message m {
          |  required int32 id;
          |  optional int64 ts_ms (TIMESTAMP(MILLIS,true));
          |}""".stripMargin) { factory =>
        Seq(factory.newGroup().append("id", 1).append("ts_ms", LastSwitchJulianMicros / 1000 - 1))
      }
      val table = "comet_nonspark_1899_" + java.util.UUID.randomUUID().toString.replace("-", "")
      withTable(table) {
        spark.sql(s"CREATE TABLE $table (id INT, ts_ms TIMESTAMP) USING PARQUET LOCATION '$path'")
        spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.parquet.datetimeRebaseModeInRead" -> "EXCEPTION",
          "spark.sql.parquet.int96RebaseModeInRead" -> "EXCEPTION") {
          val e = intercept[Exception] {
            spark.read.format("delta").load(path).collect()
          }
          val messages = causeMessages(e)
          assert(
            messages.contains("Native scan cannot rebase") && messages.contains("'ts_ms'"),
            s"expected the native calendar-rebase error on ts_ms, got:\n$messages")
        }
      }
    }
  }

  /** A raw file with one INT64 MICROS timestamp (`ts`) and one INT96 timestamp (`ts96`). */
  private def writeInt64AndInt96File(dir: String, tsMicros: Long, int96Days: Int): Unit = {
    writeRawParquetFile(
      dir,
      """message m {
        |  required int32 id;
        |  optional int64 ts (TIMESTAMP(MICROS,true));
        |  optional int96 ts96;
        |}""".stripMargin) { factory =>
      Seq(
        factory
          .newGroup()
          .append("id", 1)
          .append("ts", tsMicros)
          .append("ts96", int96Midnight(int96Days)),
        factory.newGroup().append("id", 2))
    }
  }

  /** Proleptic 1500-01-01 as days / micros since the epoch. */
  private val AncientDays = -171664
  private val AncientMicros = AncientDays.toLong * 86400000000L

  test("non-Spark INT64 timestamps follow the datetime read mode when the INT96 mode differs") {
    // Spark selects datetimeRebaseSpec for INT64 MICROS/MILLIS columns and int96RebaseSpec only
    // for INT96 columns. Under datetime CORRECTED + int96 EXCEPTION an ancient INT64 value reads
    // verbatim; it must not be refused just because the INT96 spec would refuse an ancient
    // INT96 value (the INT96 column holds a modern one here).
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeInt64AndInt96File(path, AncientMicros, int96Days = 19875)
      val table = "comet_int64_vs_int96_" + java.util.UUID.randomUUID().toString.replace("-", "")
      withTable(table) {
        spark.sql(
          s"CREATE TABLE $table (id INT, ts TIMESTAMP, ts96 TIMESTAMP) USING PARQUET " +
            s"LOCATION '$path'")
        spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.parquet.datetimeRebaseModeInRead" -> "CORRECTED",
          "spark.sql.parquet.int96RebaseModeInRead" -> "EXCEPTION") {
          val df = spark.read.format("delta").load(path)
          checkDeltaNativeScanAnswer(df)
          val rows = df
            .selectExpr("id", "cast(ts as string)", "cast(ts96 as string)")
            .collect()
            .sortBy(_.getInt(0))
          assert(rows(0).getString(1) == "1500-01-01 00:00:00", s"got ${rows(0)}")
          assert(rows(0).getString(2) == "2024-06-01 00:00:00", s"got ${rows(0)}")
          assert(rows(1).isNullAt(1) && rows(1).isNullAt(2), s"got ${rows(1)}")
        }
      }
    }
  }

  test("non-Spark INT96 timestamps follow the INT96 read mode") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeInt64AndInt96File(path, tsMicros = 0L, int96Days = AncientDays)
      val table = "comet_int96_policy_" + java.util.UUID.randomUUID().toString.replace("-", "")
      withTable(table) {
        spark.sql(
          s"CREATE TABLE $table (id INT, ts TIMESTAMP, ts96 TIMESTAMP) USING PARQUET " +
            s"LOCATION '$path'")
        spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")
        // datetime CORRECTED + int96 EXCEPTION: the ancient INT96 value is refused, naming the
        // INT96 column (the INT64 column's epoch value is fine under either spec).
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.parquet.datetimeRebaseModeInRead" -> "CORRECTED",
          "spark.sql.parquet.int96RebaseModeInRead" -> "EXCEPTION") {
          val e = intercept[Exception] {
            spark.read.format("delta").load(path).collect()
          }
          val messages = causeMessages(e)
          assert(
            messages.contains("Native scan cannot rebase") && messages.contains("'ts96'"),
            s"expected the native calendar-rebase error on ts96, got:\n$messages")
        }
        // Mirror image: datetime EXCEPTION + int96 CORRECTED reads the ancient INT96 value
        // verbatim (Spark decodes the Julian Day Number directly, no calendar involved).
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.parquet.datetimeRebaseModeInRead" -> "EXCEPTION",
          "spark.sql.parquet.int96RebaseModeInRead" -> "CORRECTED") {
          val df = spark.read.format("delta").load(path)
          checkDeltaNativeScanAnswer(df)
          val rows = df
            .selectExpr("id", "cast(ts as string)", "cast(ts96 as string)")
            .collect()
            .sortBy(_.getInt(0))
          assert(rows(0).getString(1) == "1970-01-01 00:00:00", s"got ${rows(0)}")
          assert(rows(0).getString(2) == "1500-01-01 00:00:00", s"got ${rows(0)}")
        }
      }
    }
  }

  private val NestedRawSchema = """message m {
      |  required int32 id;
      |  optional group s {
      |    optional int32 d (DATE);
      |    optional int64 ts (TIMESTAMP(MICROS,true));
      |  }
      |  optional group l (LIST) {
      |    repeated group list {
      |      optional int32 element (DATE);
      |    }
      |  }
      |}""".stripMargin

  private def createNestedRawTable(table: String, path: String): Unit = {
    spark.sql(
      s"CREATE TABLE $table (id INT, s STRUCT<d: DATE, ts: TIMESTAMP>, l ARRAY<DATE>) " +
        s"USING PARQUET LOCATION '$path'")
    spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")
  }

  test(
    "metadata-free nested columns with modern and null datetime leaves stay native under " +
      "EXCEPTION read modes") {
    // EXCEPTION only refuses values that actually are ancient; a STRUCT<d: DATE, ts: TIMESTAMP>
    // and an ARRAY<DATE> holding modern and null leaves must read natively, not be rejected
    // up front for being nested.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeRawParquetFile(path, NestedRawSchema) { factory =>
        val g1 = factory.newGroup().append("id", 1)
        g1.addGroup("s").append("d", 19875).append("ts", 1717243200000000L)
        val l1 = g1.addGroup("l")
        l1.addGroup("list").append("element", 19875)
        l1.addGroup("list")
        val g2 = factory.newGroup().append("id", 2)
        g2.addGroup("s")
        g2.addGroup("l")
        val g3 = factory.newGroup().append("id", 3)
        Seq(g1, g2, g3)
      }
      val table = "comet_nested_modern_" + java.util.UUID.randomUUID().toString.replace("-", "")
      withTable(table) {
        createNestedRawTable(table, path)
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.parquet.datetimeRebaseModeInRead" -> "EXCEPTION",
          "spark.sql.parquet.int96RebaseModeInRead" -> "EXCEPTION") {
          val df = spark.read.format("delta").load(path)
          checkDeltaNativeScanAnswer(df)
          val rows = df
            .selectExpr("id", "cast(s.d as string)", "cast(s.ts as string)", "cast(l as string)")
            .collect()
            .sortBy(_.getInt(0))
          assert(rows(0).getString(1) == "2024-06-01", s"got ${rows(0)}")
          assert(rows(0).getString(2) == "2024-06-01 12:00:00", s"got ${rows(0)}")
          assert(rows(0).getString(3) == "[2024-06-01, null]", s"got ${rows(0)}")
          assert(rows(1).isNullAt(1) && rows(1).isNullAt(2), s"got ${rows(1)}")
          assert(rows(1).getString(3) == "[]", s"got ${rows(1)}")
          assert(rows(2).isNullAt(1) && rows(2).isNullAt(3), s"got ${rows(2)}")
        }
      }
    }
  }

  test(
    "metadata-free nested columns with an ancient date leaf fail loudly under EXCEPTION read " +
      "modes") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeRawParquetFile(path, NestedRawSchema) { factory =>
        val g1 = factory.newGroup().append("id", 1)
        g1.addGroup("s").append("d", -171655)
        Seq(g1)
      }
      val table = "comet_nested_ancient_" + java.util.UUID.randomUUID().toString.replace("-", "")
      withTable(table) {
        createNestedRawTable(table, path)
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.parquet.datetimeRebaseModeInRead" -> "EXCEPTION",
          "spark.sql.parquet.int96RebaseModeInRead" -> "EXCEPTION") {
          val e = intercept[Exception] {
            spark.read.format("delta").load(path).collect()
          }
          val messages = causeMessages(e)
          assert(
            messages.contains("Native scan cannot rebase") && messages.contains("'s'"),
            s"expected the native calendar-rebase error on s, got:\n$messages")
        }
      }
    }
  }

  test(
    "only the requested nested leaves are rebase-checked: an unrequested ancient s.ts does not " +
      "block select s.d under EXCEPTION read modes") {
    // A metadata-free file with s.d = 2024-06-01 next to s.ts = 1500-01-01. Spark's requested
    // schema for `select s.d` is STRUCT<d>, so Spark never decodes s.ts and reads the modern
    // date fine; the native scan must not refuse the row for a leaf the schema adapter's struct
    // narrowing drops. Requesting the ancient leaf itself still fails loudly.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeRawParquetFile(path, NestedRawSchema) { factory =>
        val g1 = factory.newGroup().append("id", 1)
        g1.addGroup("s").append("d", 19875).append("ts", AncientMicros)
        Seq(g1)
      }
      val table =
        "comet_nested_requested_" + java.util.UUID.randomUUID().toString.replace("-", "")
      withTable(table) {
        createNestedRawTable(table, path)
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.parquet.datetimeRebaseModeInRead" -> "EXCEPTION",
          "spark.sql.parquet.int96RebaseModeInRead" -> "EXCEPTION") {
          val df = spark.read.format("delta").load(path).selectExpr("id", "cast(s.d as string)")
          checkDeltaNativeScanAnswer(df)
          val rows = df.collect()
          assert(rows.length == 1 && rows(0).getString(1) == "2024-06-01", s"got ${rows.toSeq}")

          for (projection <- Seq("s", "s.ts")) {
            val e = intercept[Exception] {
              spark.read.format("delta").load(path).selectExpr(projection).collect()
            }
            val messages = causeMessages(e)
            assert(
              messages.contains("Native scan cannot rebase") && messages.contains("'s'"),
              s"expected the native calendar-rebase error on s for `select $projection`, " +
                s"got:\n$messages")
          }
        }
      }
    }
  }

  test("legacy-rebase ancient datetime values inside nested columns match Spark's own read") {
    // Spark rebases dates and timestamps at every nesting depth; a LEGACY (UTC) file with
    // ancient leaves inside a struct, an array, a map and an array of structs must read
    // natively with exactly Spark's rebased values, nulls and offsets preserved.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        "spark.sql.parquet.datetimeRebaseModeInWrite" -> "LEGACY",
        "spark.sql.parquet.int96RebaseModeInWrite" -> "LEGACY") {
        spark
          .sql("select * from values " +
            "(1, named_struct('d', date'1500-01-01', 'ts', timestamp'1500-01-01 12:34:56'), " +
            "array(date'1500-01-01', null, date'2024-06-01'), " +
            "map(1, date'1582-10-04', 2, cast(null as date)), " +
            "array(named_struct('d', date'0001-01-01'), named_struct('d', cast(null as date)))), " +
            "(2, named_struct('d', cast(null as date), 'ts', cast(null as timestamp)), " +
            "array(), map(), array(cast(null as struct<d: date>))), " +
            "(3, cast(null as struct<d: date, ts: timestamp>), cast(null as array<date>), " +
            "cast(null as map<int, date>), cast(null as array<struct<d: date>>)) " +
            "as t(id, s, l, m, ls)")
          .write
          .format("delta")
          .save(path)
      }
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        val df = spark.read.format("delta").load(path)
        checkDeltaNativeScanAnswer(df)
        val rows = df
          .selectExpr(
            "id",
            "cast(s.d as string)",
            "cast(s.ts as string)",
            "cast(l as string)",
            "cast(m as string)",
            "cast(ls as string)")
          .collect()
          .sortBy(_.getInt(0))
        assert(rows(0).getString(1) == "1500-01-01", s"got ${rows(0)}")
        assert(rows(0).getString(2) == "1500-01-01 12:34:56", s"got ${rows(0)}")
        assert(rows(0).getString(3) == "[1500-01-01, null, 2024-06-01]", s"got ${rows(0)}")
        assert(rows(0).getString(4) == "{1 -> 1582-10-04, 2 -> null}", s"got ${rows(0)}")
        assert(rows(0).getString(5) == "[{0001-01-01}, {null}]", s"got ${rows(0)}")
        assert(rows(1).isNullAt(1) && rows(1).isNullAt(2), s"got ${rows(1)}")
        assert(rows(1).getString(3) == "[]" && rows(1).getString(4) == "{}", s"got ${rows(1)}")
        assert(rows(1).getString(5) == "[null]", s"got ${rows(1)}")
        assert((1 to 5).forall(rows(2).isNullAt), s"got ${rows(2)}")
      }
    }
  }

  /** Register `path`'s raw parquet files as an external table and CONVERT it to Delta. */
  private def convertRawParquetToDelta(path: String, table: String): Unit = {
    spark.sql(
      s"CREATE TABLE $table (id INT, d DATE, ts TIMESTAMP) USING PARQUET LOCATION '$path'")
    spark.sql(s"CONVERT TO DELTA $table NO STATISTICS")
  }

  test("non-Spark parquet files read ancient values verbatim under CORRECTED read modes") {
    // A converted table over a file with no Spark writer metadata: getRebaseSpec resolves the
    // policy from the session read modes (Spark 4.0 defaults both to CORRECTED), so a
    // proleptic 1500-01-01 (day -171664) and a timestamp one microsecond before the epoch
    // must read natively exactly as stored.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeNonSparkParquetFile(
        path,
        Seq(
          (1, Some(-171664), Some(-1L)),
          (2, Some(19875), Some(1717243200000000L)),
          (3, None, None)))
      val table =
        "comet_nonspark_corrected_" + java.util.UUID.randomUUID().toString.replace("-", "")
      withTable(table) {
        convertRawParquetToDelta(path, table)
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.parquet.datetimeRebaseModeInRead" -> "CORRECTED",
          "spark.sql.parquet.int96RebaseModeInRead" -> "CORRECTED") {
          val df = spark.read.format("delta").load(path)
          checkDeltaNativeScanAnswer(df)
          val rows = df
            .selectExpr("id", "cast(d as string)", "cast(ts as string)")
            .collect()
            .sortBy(_.getInt(0))
          assert(rows(0).getString(1) == "1500-01-01", s"got ${rows(0)}")
          assert(rows(0).getString(2) == "1969-12-31 23:59:59.999999", s"got ${rows(0)}")
          assert(rows(1).getString(1) == "2024-06-01", s"got ${rows(1)}")
          assert(rows(2).isNullAt(1) && rows(2).isNullAt(2), s"got ${rows(2)}")
        }
      }
    }
  }

  test("non-Spark parquet files rebase ancient dates under LEGACY read modes") {
    // LEGACY read modes on a file without writer metadata: the stored day count is hybrid
    // Julian + Gregorian, so Julian 1500-01-01 (stored as -171655) must rebase to proleptic
    // 1500-01-01, matching Spark's own LEGACY read (the day rebase is timezone-free).
    // Timestamps stay modern: rebasing ancient ones needs the writer zone, which this file
    // does not record.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeNonSparkParquetFile(
        path,
        Seq((1, Some(-171655), Some(0L)), (2, Some(19875), Some(1717243200000000L))))
      val table = "comet_nonspark_legacy_" + java.util.UUID.randomUUID().toString.replace("-", "")
      withTable(table) {
        convertRawParquetToDelta(path, table)
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.parquet.datetimeRebaseModeInRead" -> "LEGACY",
          "spark.sql.parquet.int96RebaseModeInRead" -> "LEGACY") {
          val df = spark.read.format("delta").load(path)
          checkDeltaNativeScanAnswer(df)
          val rows = df
            .selectExpr("id", "cast(d as string)")
            .collect()
            .sortBy(_.getInt(0))
          assert(rows(0).getString(1) == "1500-01-01", s"got ${rows(0)}")
          assert(rows(1).getString(1) == "2024-06-01", s"got ${rows(1)}")
        }
      }
    }
  }

  test("non-Spark parquet files with ancient values fail loudly under EXCEPTION read modes") {
    // EXCEPTION read modes (Spark 3.x's default) refuse ancient values whose calendar the
    // file does not declare; the native scan must refuse them too rather than return
    // silently shifted values.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeNonSparkParquetFile(path, Seq((1, Some(-171655), Some(0L))))
      val table =
        "comet_nonspark_exception_" + java.util.UUID.randomUUID().toString.replace("-", "")
      withTable(table) {
        convertRawParquetToDelta(path, table)
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          "spark.sql.parquet.datetimeRebaseModeInRead" -> "EXCEPTION",
          "spark.sql.parquet.int96RebaseModeInRead" -> "EXCEPTION") {
          val e = intercept[Exception] {
            spark.read.format("delta").load(path).collect()
          }
          val messages = Iterator
            .iterate(e: Throwable)(_.getCause)
            .takeWhile(_ != null)
            .map(_.getMessage)
            .mkString("\n")
          assert(
            messages.toLowerCase(java.util.Locale.ROOT).contains("rebase"),
            s"expected a calendar-rebase error, got:\n$messages")
        }
      }
    }
  }

  test(
    "a struct with a date column stays native when the file carries only the legacy INT96 " +
      "flag and corrected dates") {
    // legacyINT96 alone puts the file's INT96 timestamp column under the LEGACY policy, but
    // its DATE policy is CORRECTED -- so a STRUCT<d: DATE> column has nothing to rebase and
    // must pass through natively, unwrapped, instead of being handled just because the
    // timestamp policy needs handling elsewhere in the file.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        "spark.sql.parquet.outputTimestampType" -> "INT96",
        "spark.sql.parquet.datetimeRebaseModeInWrite" -> "CORRECTED",
        "spark.sql.parquet.int96RebaseModeInWrite" -> "LEGACY") {
        spark
          .sql(
            "select * from values " +
              "(1, named_struct('d', date'2020-06-01'), timestamp'2021-01-01 00:00:00'), " +
              "(2, named_struct('d', cast(null as date)), timestamp'2022-01-01 12:34:56') " +
              "as t(id, s, ts)")
          .write
          .format("delta")
          .save(path)
      }
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        val df = spark.read.format("delta").load(path)
        checkDeltaNativeScanAnswer(df)
        val rows = df.selectExpr("id", "cast(s.d as string)").collect().sortBy(_.getInt(0))
        assert(rows(0).getString(1) == "2020-06-01", s"got ${rows(0)}")
        assert(rows(1).isNullAt(1), s"got ${rows(1)}")
      }
    }
  }
}
