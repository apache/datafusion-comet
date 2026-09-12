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

import java.io.{File, FileInputStream}
import java.util.Properties

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame, QueryTest, Row}
import org.apache.spark.sql.comet.{CometHashAggregateExec, CometIcebergChangelogExec, CometIcebergNativeScanExec}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, Trigger}

class CometIcebergStreamingSuite
    extends CometTestBase
    with CometIcebergTestBase
    with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

  private def withCatalog(f: (File, String, String) => Unit): Unit = {
    assume(icebergAvailable, "Compatible Iceberg runtime required")
    withTempIcebergDir { dir =>
      val catalog = "stream_cat_" + dir.getName.replace("-", "_")
      val namespace = "ns_" + dir.getName.replace("-", "_")
      // Run the same assertions against a disposable REST catalog during integration testing.
      val properties = sys.env.get("COMET_TEST_ICEBERG_CATALOG_PROPERTIES") match {
        case Some(path) =>
          val values = new Properties()
          Using.resource(new FileInputStream(path))(values.load)
          values.stringPropertyNames().asScala.toSeq.map(k => k -> values.getProperty(k))
        case None =>
          Seq("type" -> "hadoop", "warehouse" -> new File(dir, "warehouse").toString)
      }
      val configs = properties.map { case (key, value) =>
        s"spark.sql.catalog.$catalog.$key" -> value
      } ++ Seq(
        s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_CHANGELOG_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_STREAMING_ENABLED.key -> "true")
      withSQLConf(configs: _*) {
        sql(s"CREATE NAMESPACE $catalog.$namespace")
        try f(dir, catalog, namespace)
        finally sql(s"DROP NAMESPACE $catalog.$namespace")
      }
    }
  }

  // StreamingQueryWrapper moved packages in Spark 4.1. The public accessors are unchanged.
  private def executedPlan(query: StreamingQuery): SparkPlan = {
    val execution = query.getClass.getMethod("streamingQuery").invoke(query)
    execution.getClass
      .getMethod("lastExecution")
      .invoke(execution)
      .asInstanceOf[QueryExecution]
      .executedPlan
  }

  private def runAvailable(source: DataFrame, checkpoint: File, outputMode: String = "append")(
      process: (DataFrame, Long) => Unit): StreamingQuery = {
    val query = source.writeStream
      .outputMode(outputMode)
      .option("checkpointLocation", checkpoint.toString)
      .trigger(Trigger.AvailableNow())
      .foreachBatch(process)
      .start()
    try {
      assert(query.awaitTermination(60000), "AvailableNow query did not terminate")
      query
    } finally {
      if (query.isActive) query.stop()
    }
  }

  test(
    "offset-bounded native batches preserve admission limits, progress, and checkpoint restart") {
    withCatalog { (dir, catalog, namespace) =>
      val table = s"$catalog.$namespace.events"
      withTable(table) {
        sql(s"CREATE TABLE $table (id BIGINT) USING iceberg")
        (0 until 3).foreach { batch =>
          spark.range(batch * 2L, batch * 2L + 2).coalesce(1).writeTo(table).append()
        }
        val checkpoint = new File(dir, "checkpoint")
        def source = spark.readStream
          .option("streaming-max-files-per-micro-batch", "1")
          .table(table)
        // Iceberg 1.10 falls back from AvailableNow to a single batch. 1.11 supports the
        // trigger and its admission limit. Preserve the source runtime's own batch boundaries.
        val expected = ArrayBuffer.empty[(Long, Seq[Long])]
        val baseline = withSQLConf(CometConf.COMET_ICEBERG_STREAMING_ENABLED.key -> "false") {
          runAvailable(source, new File(dir, "baseline")) { (batch, id) =>
            expected += id -> batch.collect().toSeq.map(_.getLong(0)).sorted
          }
        }
        val batches = ArrayBuffer.empty[(Long, Seq[Long])]
        val query = runAvailable(source, checkpoint) { (batch, id) =>
          batches += id -> batch.collect().toSeq.map(_.getLong(0)).sorted
        }
        assert(batches == expected)
        assert(batches.flatMap(_._2).sorted == (0L until 6L))
        assert(
          query.recentProgress.map(_.numInputRows).toSeq ==
            baseline.recentProgress.map(_.numInputRows).toSeq)
        if (icebergVersionAtLeast(1, 11)) {
          assert(batches.map(_._2) == Seq(Seq(0L, 1L), Seq(2L, 3L), Seq(4L, 5L)))
        }
        val native = executedPlan(query).collect { case s: CometIcebergNativeScanExec => s }
        assert(native.size == 1, executedPlan(query).toString)
        assert(native.head.getStream.isDefined)
        assert(native.head.metrics("numOutputRows") eq native.head.metrics("output_rows"))

        spark.range(6, 8).coalesce(1).writeTo(table).append()
        val resumed = ArrayBuffer.empty[(Long, Seq[Long])]
        val restart = runAvailable(source, checkpoint) { (batch, id) =>
          resumed += id -> batch.collect().toSeq.map(_.getLong(0)).sorted
        }
        assert(resumed.toSeq == Seq((batches.last._1 + 1) -> Seq(6L, 7L)))
        assert(restart.recentProgress.map(_.numInputRows).sum == 2)
        assert(executedPlan(restart).exists(_.isInstanceOf[CometIcebergNativeScanExec]))
      }
    }
  }

  test("streaming scan opt-in and native execution gates retain Spark source and progress") {
    withCatalog { (dir, catalog, namespace) =>
      val table = s"$catalog.$namespace.fallback_events"
      withTable(table) {
        sql(s"CREATE TABLE $table (id INT) USING iceberg")
        sql(s"INSERT INTO $table VALUES (1), (2)")
        Seq(
          CometConf.COMET_ICEBERG_STREAMING_ENABLED.key,
          CometConf.COMET_ICEBERG_NATIVE_ENABLED.key,
          CometConf.COMET_NATIVE_SCAN_ENABLED.key,
          CometConf.COMET_EXEC_ENABLED.key).zipWithIndex.foreach { case (config, index) =>
          withSQLConf(config -> "false") {
            val rows = ArrayBuffer.empty[Row]
            val query =
              runAvailable(spark.readStream.table(table), new File(dir, s"fallback-$index")) {
                (batch, _) =>
                  rows ++= batch.collect()
              }
            assert(rows.toSet == Set(Row(1), Row(2)))
            assert(query.recentProgress.map(_.numInputRows).sum == 2)
            assert(executedPlan(query).exists(_.isInstanceOf[MicroBatchScanExec]))
            assert(!executedPlan(query).exists(_.isInstanceOf[CometIcebergNativeScanExec]))
          }
        }
      }
    }
  }

  test("failed foreachBatch is replayed without replaying an earlier committed batch") {
    withCatalog { (dir, catalog, namespace) =>
      val table = s"$catalog.$namespace.retry_events"
      withTable(table) {
        sql(s"CREATE TABLE $table (id BIGINT) USING iceberg")
        spark.range(0, 1).coalesce(1).writeTo(table).append()
        def source = spark.readStream
          .option("streaming-max-files-per-micro-batch", "1")
          .table(table)
        val checkpoint = new File(dir, "retry")
        val committed = ArrayBuffer.empty[Long]
        runAvailable(source, checkpoint) { (batch, id) =>
          assert(id == 0)
          committed ++= batch.collect().map(_.getLong(0))
        }
        assert(committed.toSeq == Seq(0L))
        spark.range(1, 2).coalesce(1).writeTo(table).append()
        val error = intercept[StreamingQueryException] {
          runAvailable(source, checkpoint) { (batch, id) =>
            assert(id == 1)
            assert(batch.collect().map(_.getLong(0)).toSeq == Seq(1L))
            throw new IllegalStateException("injected callback failure")
          }
        }
        assert(error.getMessage.contains("injected callback failure"))
        assert(committed.toSeq == Seq(0L))
        val retried = ArrayBuffer.empty[Long]
        val query = runAvailable(source, checkpoint) { (batch, id) =>
          val rows = batch.collect().map(_.getLong(0)).toSeq
          assert(rows == Seq(id))
          retried ++= rows
        }
        assert(retried.toSeq == Seq(1L))
        assert(executedPlan(query).exists(_.isInstanceOf[CometIcebergNativeScanExec]))
      }
    }
  }

  test("unsupported Iceberg files and other streaming sources retain their Spark readers") {
    withCatalog { (dir, catalog, namespace) =>
      val table = s"$catalog.$namespace.avro_events"
      withTable(table) {
        sql(s"""CREATE TABLE $table (id INT) USING iceberg
          TBLPROPERTIES ('write.format.default' = 'avro')""")
        sql(s"INSERT INTO $table VALUES (1), (2)")
        val rows = ArrayBuffer.empty[Row]
        val query = runAvailable(spark.readStream.table(table), new File(dir, "avro")) {
          (batch, _) => rows ++= batch.collect()
        }
        assert(rows.toSet == Set(Row(1), Row(2)))
        assert(executedPlan(query).exists(_.isInstanceOf[MicroBatchScanExec]))
        assert(!executedPlan(query).exists(_.isInstanceOf[CometIcebergNativeScanExec]))
      }
      val query = runAvailable(
        spark.readStream.format("rate-micro-batch").option("rowsPerBatch", 2).load(),
        new File(dir, "other-source")) { (batch, _) =>
        assert(batch.count() == 2)
      }
      assert(executedPlan(query).exists(_.isInstanceOf[MicroBatchScanExec]))
      assert(!executedPlan(query).exists(_.isInstanceOf[CometIcebergNativeScanExec]))
    }
  }

  test("append streaming rejects overwrite and delete snapshots instead of losing changes") {
    withCatalog { (dir, catalog, namespace) =>
      Seq("overwrite", "delete").foreach { operation =>
        val table = s"$catalog.$namespace.mutations_$operation"
        withTable(table) {
          sql(s"""CREATE TABLE $table (id INT, target STRING) USING iceberg
            PARTITIONED BY (id) TBLPROPERTIES ('format-version' = '2',
            'write.update.mode' = 'copy-on-write')""")
          sql(s"INSERT INTO $table VALUES (1, 'old'), (2, 'removed')")
          val checkpoint = new File(dir, operation)
          def source = spark.readStream.table(table)
          val initial = runAvailable(source, checkpoint) { (batch, _) =>
            assert(batch.collect().toSet == Set(Row(1, "old"), Row(2, "removed")))
          }
          assert(executedPlan(initial).exists(_.isInstanceOf[CometIcebergNativeScanExec]))
          if (operation == "overwrite") {
            sql(s"UPDATE $table SET target = 'new' WHERE id = 1")
          } else {
            sql(s"DELETE FROM $table WHERE id = 2")
          }
          val snapshot = sql(s"SELECT operation FROM $table.snapshots ORDER BY committed_at DESC")
          assert(snapshot.head().getString(0) == operation)
          val error = intercept[StreamingQueryException] {
            runAvailable(source, checkpoint) { (batch, _) => batch.collect() }
          }
          assert(error.getMessage.toLowerCase.contains(operation), error.getMessage)
        }
      }
    }
  }

  test("Iceberg changelog computes actual mutation images within snapshot bounds") {
    withCatalog { (_, catalog, namespace) =>
      val table = s"$catalog.$namespace.changelog_mappings"
      withTable(table) {
        sql(s"""CREATE TABLE $table (tenant INT, id INT, target STRING) USING iceberg
          TBLPROPERTIES ('format-version' = '2', 'write.update.mode' = 'copy-on-write',
          'write.delete.mode' = 'copy-on-write')""")
        sql(s"INSERT INTO $table VALUES (1, 1, 'old'), (1, 2, 'stable'), (1, 3, 'removed')")
        def latestSnapshot: Long =
          sql(s"SELECT snapshot_id FROM $table.snapshots ORDER BY committed_at DESC")
            .head()
            .getLong(0)
        val start = latestSnapshot
        sql(s"UPDATE $table SET target = 'new' WHERE id = 1")
        val update = latestSnapshot
        sql(s"DELETE FROM $table WHERE id = 3")
        val delete = latestSnapshot
        sql(s"INSERT INTO $table VALUES (1, 4, 'added')")
        val end = latestSnapshot
        // Prove that an explicitly bounded read excludes a later commit.
        sql(s"INSERT INTO $table VALUES (1, 5, 'too-late')")
        withTempView("actual_changes") {
          sql(s"""CALL $catalog.system.create_changelog_view(
            table => '$namespace.changelog_mappings', changelog_view => 'actual_changes',
            options => map('start-snapshot-id', '$start', 'end-snapshot-id', '$end'),
            identifier_columns => array('tenant', 'id'), compute_updates => true)""")
          val changes = spark
            .table("actual_changes")
            .select("id", "target", "_change_type", "_commit_snapshot_id")
          val expected = Seq(
            Row(1, "old", "UPDATE_BEFORE", update),
            Row(1, "new", "UPDATE_AFTER", update),
            Row(3, "removed", "DELETE", delete),
            Row(4, "added", "INSERT", end))
          checkAnswer(changes, expected)
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            checkAnswer(
              spark
                .table("actual_changes")
                .select("id", "target", "_change_type", "_commit_snapshot_id"),
              expected)
          }
          // The standard Iceberg procedure remains a batch API, with native data processing.
          assert(!changes.isStreaming)
          assert(
            collect(changes.queryExecution.executedPlan) {
              case scan: CometIcebergNativeScanExec => scan
            }.nonEmpty,
            changes.queryExecution.executedPlan.toString)
          assert(
            collect(changes.queryExecution.executedPlan) { case node: CometIcebergChangelogExec =>
              node
            }.nonEmpty,
            changes.queryExecution.executedPlan.toString)
        }
      }
    }
  }

  private def checkNativeChangelog(query: => DataFrame, processed: Boolean): Unit = {
    val expected = withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      query.collect().toSeq
    }
    val native = query
    // The procedure sorts within partitions; it promises no global output order.
    QueryTest.sameRows(expected, native.collect().toSeq, false).foreach(message => fail(message))
    val plan = native.queryExecution.executedPlan
    assert(collect(plan) { case s: CometIcebergNativeScanExec => s }.nonEmpty, plan.toString)
    if (processed) {
      assert(collect(plan) { case c: CometIcebergChangelogExec => c }.nonEmpty, plan.toString)
    }
  }

  test("native raw changelog, carryovers and net changes preserve bounds and duplicates") {
    withCatalog { (_, catalog, namespace) =>
      for (version <- Seq(1, 2, 3) if version < 3 || icebergVersionAtLeast(1, 9)) {
        val table = s"$catalog.$namespace.cdc_v$version"
        withTable(table) {
          sql(s"""CREATE TABLE $table (id INT, value STRING) USING iceberg
            TBLPROPERTIES ('format-version' = '$version',
            'write.update.mode' = 'copy-on-write', 'write.delete.mode' = 'copy-on-write')""")
          sql(s"""INSERT INTO $table SELECT /*+ COALESCE(1) */ * FROM VALUES
            (1, 'old'), (2, NULL), (2, NULL)""")
          assert(sql(s"SELECT * FROM $table.data_files").count() == 1)
          def snapshot = sql(s"""SELECT snapshot_id, committed_at FROM $table.snapshots
            ORDER BY committed_at DESC""").head()
          val start = snapshot
          sql(s"UPDATE $table SET value = 'middle' WHERE id = 1")
          sql(s"UPDATE $table SET value = 'final' WHERE id = 1")
          sql(s"INSERT INTO $table VALUES (3, 'temporary')")
          sql(s"DELETE FROM $table WHERE id = 3")
          val end = snapshot
          sql(s"INSERT INTO $table VALUES (4, 'outside')")
          val bounds = s"""map('start-snapshot-id', '${start.getLong(0)}',
            'end-snapshot-id', '${end.getLong(0)}')"""
          def raw = spark.read
            .option("start-snapshot-id", start.getLong(0))
            .option("end-snapshot-id", end.getLong(0))
            .table(s"$table.changes")
          checkNativeChangelog(raw, processed = false)
          // Iceberg 1.11's JVM changelog reader misprojects noncontiguous metadata fields.
          // Derive this projection from complete JVM rows instead of exercising that bug.
          val rawRows = withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            raw.collect().toSeq
          }
          checkAnswer(
            raw.select("_change_type", "_commit_snapshot_id"),
            rawRows.map(r => Row(r.getString(2), r.getLong(4))))
          checkNativeChangelog(raw.groupBy("_change_type").count(), processed = false)
          checkNativeChangelog(raw.filter("id = 1"), processed = false)
          assert(raw.filter("id = 2").count() > 0, "Fixture must contain actual carryovers")
          withTempView("cdc_default", "cdc_net", "cdc_time") {
            sql(s"""CALL $catalog.system.create_changelog_view(
              table => '$namespace.cdc_v$version', changelog_view => 'cdc_default',
              options => $bounds)""")
            checkNativeChangelog(spark.table("cdc_default"), processed = true)
            checkNativeChangelog(
              spark.table("cdc_default").filter("id = 1 AND _change_type = 'DELETE'"),
              processed = true)
            checkNativeChangelog(
              spark.table("cdc_default").select("_change_type"),
              processed = true)
            assert(spark.table("cdc_default").filter("id = 2 OR id = 4").count() == 0)
            sql(s"""CALL $catalog.system.create_changelog_view(
              table => '$namespace.cdc_v$version', changelog_view => 'cdc_net',
              options => $bounds, net_changes => true)""")
            checkNativeChangelog(spark.table("cdc_net"), processed = true)
            checkAnswer(
              spark.table("cdc_net").select("id", "value", "_change_type").orderBy("value"),
              Seq(Row(1, "final", "INSERT"), Row(1, "old", "DELETE")))
            sql(s"""CALL $catalog.system.create_changelog_view(
              table => '$namespace.cdc_v$version', changelog_view => 'cdc_time',
              options => map('start-timestamp', '${start.getTimestamp(1).getTime}',
                'end-timestamp', '${end.getTimestamp(1).getTime}'), net_changes => true)""")
            checkNativeChangelog(spark.table("cdc_time"), processed = true)
            checkAnswer(spark.table("cdc_time"), spark.table("cdc_net"))
          }
        }
      }
    }
  }

  test("native update images use table identifiers and preserve complex values") {
    withCatalog { (_, catalog, namespace) =>
      val table = s"$catalog.$namespace.cdc_complex"
      withTable(table) {
        sql(s"""CREATE TABLE $table (id INT NOT NULL, value STRING, tags MAP<STRING, INT>,
          details STRUCT<items: ARRAY<INT>>, score DOUBLE) USING iceberg
          TBLPROPERTIES ('format-version' = '2', 'write.update.mode' = 'copy-on-write')""")
        sql(s"ALTER TABLE $table SET IDENTIFIER FIELDS id")
        sql(s"""INSERT INTO $table SELECT /*+ COALESCE(1) */ * FROM VALUES
          (1, 'stable', map('b', 2, 'a', 1), named_struct('items', array(1, NULL)), double('NaN')),
          (2, 'old', NULL, NULL, -0.0D)""")
        assert(sql(s"SELECT * FROM $table.data_files").count() == 1)
        val start = sql(s"SELECT snapshot_id FROM $table.snapshots").head().getLong(0)
        sql(s"UPDATE $table SET value = 'new' WHERE id = 2")
        withTempView("cdc_identifiers") {
          // Explicit compute_updates uses the table's identifier fields when columns are omitted.
          sql(s"""CALL $catalog.system.create_changelog_view(
            table => '$namespace.cdc_complex', changelog_view => 'cdc_identifiers',
            options => map('start-snapshot-id', '$start'), compute_updates => true)""")
          checkNativeChangelog(spark.table("cdc_identifiers"), processed = true)
          checkAnswer(
            spark.table("cdc_identifiers").select("id", "value", "_change_type"),
            Seq(Row(2, "old", "UPDATE_BEFORE"), Row(2, "new", "UPDATE_AFTER")))
          // Explicit columns imply compute_updates=true when that option is omitted.
          sql(s"""CALL $catalog.system.create_changelog_view(
            table => '$namespace.cdc_complex', changelog_view => 'cdc_identifiers',
            options => map('start-snapshot-id', '$start'), identifier_columns => array('id'))""")
          checkNativeChangelog(spark.table("cdc_identifiers"), processed = true)
          checkNativeChangelog(
            spark.table("cdc_identifiers").selectExpr("id + 10 AS id", "upper(value) AS value"),
            processed = true)
          withSQLConf(CometConf.COMET_ICEBERG_CHANGELOG_ENABLED.key -> "false") {
            val fallback = spark.table("cdc_identifiers")
            assert(fallback.count() == 2)
            assert(collect(fallback.queryExecution.executedPlan) {
              case c: CometIcebergChangelogExec => c
            }.isEmpty)
          }
        }
      }
    }
  }

  test("native changelog rejects ambiguous update identifiers and keeps procedure validation") {
    withCatalog { (_, catalog, namespace) =>
      val table = s"$catalog.$namespace.cdc_duplicates"
      withTable(table) {
        sql(s"""CREATE TABLE $table (id INT, value STRING) USING iceberg
          TBLPROPERTIES ('write.update.mode' = 'copy-on-write')""")
        sql(s"INSERT INTO $table VALUES (1, 'a'), (1, 'b')")
        val start = sql(s"SELECT snapshot_id FROM $table.snapshots").head().getLong(0)
        sql(s"UPDATE $table SET value = concat(value, '_new')")
        withTempView("cdc_ambiguous") {
          def causeContains(error: Throwable, message: String): Boolean =
            Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists { cause =>
              Option(cause.getMessage).exists(_.contains(message))
            }
          Seq("true", "false").foreach { enabled =>
            withSQLConf(CometConf.COMET_ENABLED.key -> enabled) {
              sql(s"""CALL $catalog.system.create_changelog_view(
                table => '$namespace.cdc_duplicates', changelog_view => 'cdc_ambiguous',
                options => map('start-snapshot-id', '$start'), identifier_columns => array('id'))""")
              val error = intercept[Exception](spark.table("cdc_ambiguous").collect())
              assert(
                causeContains(error, "multiple rows with the same identifier"),
                error.toString)
            }
          }
          val error = intercept[Exception] {
            sql(s"""CALL $catalog.system.create_changelog_view(
              table => '$namespace.cdc_duplicates', identifier_columns => array('id'),
              net_changes => true)""")
          }
          assert(causeContains(error, "Not support net changes with update images"))
        }
      }
    }
  }

  test("vanilla Iceberg batch changelog rejects merge-on-read delete files") {
    withCatalog { (_, catalog, namespace) =>
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val table = s"$catalog.$namespace.mor_changes"
        withTable(table) {
          sql(s"""CREATE TABLE $table (id INT, target STRING) USING iceberg
            TBLPROPERTIES ('format-version' = '2', 'write.update.mode' = 'merge-on-read')""")
          sql(s"INSERT INTO $table VALUES (1, 'old'), (2, 'stable')")
          sql(s"UPDATE $table SET target = 'new' WHERE id = 1")
          assert(sql(s"SELECT * FROM $table.delete_files").count() > 0)
          val error = intercept[Exception] {
            spark.table(s"$table.changes").collect()
          }
          assert(
            Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).exists { cause =>
              Option(cause.getMessage).exists(
                _.contains("Delete files are currently not supported in changelog scans"))
            },
            error.toString)
        }
      }
    }
  }

  test("append event micro-batches filter postimages and detect historical mapping conflicts") {
    // The application supplies change events; the append source does not derive them from updates.
    withCatalog { (dir, catalog, namespace) =>
      val mappings = s"$catalog.$namespace.mappings"
      val events = s"$catalog.$namespace.mapping_events"
      withTable(mappings, events) {
        sql(s"""CREATE TABLE $mappings (
          tenant INT, before_key STRING, target STRING, observed TIMESTAMP) USING iceberg""")
        sql(s"""INSERT INTO $mappings VALUES
          (1, 'a', 'old', TIMESTAMP '2020-01-01'),
          (1, 'a', 'new', TIMESTAMP '2020-01-02'),
          (1, 'b', 'unchanged', TIMESTAMP '2020-01-01'),
          (1, 'c', 'inserted', TIMESTAMP '2020-01-01'),
          (2, 'b', 'different-tenant', TIMESTAMP '2020-01-01')""")
        val records = sql("""SELECT * FROM VALUES
          (1, 'a', 'old', 'update_preimage', TIMESTAMP '2020-01-02'),
          (1, 'a', 'new', 'update_postimage', TIMESTAMP '2020-01-02'),
          (1, 'b', 'discarded-preimage', 'update_preimage', TIMESTAMP '2020-01-02'),
          (1, 'b', 'unchanged', 'update_postimage', TIMESTAMP '2020-01-02'),
          (1, 'c', 'discarded-delete', 'delete', TIMESTAMP '2020-01-02'),
          (1, 'c', 'inserted', 'insert', TIMESTAMP '2020-01-02')
          AS changes(tenant, before_key, target, _change_type, observed)""")
        records.coalesce(1).writeTo(events).using("iceberg").create()
        val query =
          runAvailable(spark.readStream.table(events), new File(dir, "mapping-events")) {
            (batch, _) =>
              val batchSession = batch.sparkSession
              batch.createOrReplaceTempView("mapping_events_batch")
              try {
                val violations = batchSession.sql(s"""
            WITH current_mappings AS (
              SELECT tenant, before_key, target, observed FROM mapping_events_batch
              WHERE _change_type IN ('insert', 'update_postimage')
            ), historical_mappings AS (
              SELECT r.* FROM $mappings r
              JOIN (SELECT DISTINCT tenant, before_key FROM current_mappings) k
                ON r.tenant = k.tenant AND r.before_key = k.before_key
              WHERE r.observed < (SELECT MIN(observed) FROM current_mappings)
            ), all_mappings AS (
              SELECT * FROM current_mappings UNION ALL SELECT * FROM historical_mappings
            )
            SELECT tenant, before_key, COUNT(DISTINCT target), SORT_ARRAY(COLLECT_SET(target))
            FROM all_mappings GROUP BY tenant, before_key HAVING COUNT(DISTINCT target) > 1
          """)
                assert(violations.collect().toSeq == Seq(Row(1, "a", 2L, Seq("new", "old"))))
                val plan = violations.queryExecution.executedPlan
                assert(
                  collect(plan) { case s: CometIcebergNativeScanExec => s }.nonEmpty,
                  plan.toString)
                assert(
                  collect(plan) {
                    case p if p.nodeName.contains("Comet") && p.nodeName.contains("Join") => p
                  }.nonEmpty,
                  plan.toString)
              } finally {
                batchSession.catalog.dropTempView("mapping_events_batch")
              }
          }
        assert(executedPlan(query).exists(_.isInstanceOf[CometIcebergNativeScanExec]))
      }
    }
  }

  test(
    "foreachBatch integrity checks read references, repeat actions, and append Iceberg results") {
    withCatalog { (dir, catalog, namespace) =>
      val events = s"$catalog.$namespace.integrity_events"
      val refs = s"$catalog.$namespace.integrity_references"
      val result = s"$catalog.$namespace.integrity_violations"
      withTable(events, refs, result) {
        sql(s"""CREATE TABLE $events (
          tenant INT, entity STRING, session STRUCT<id: STRING>, observed TIMESTAMP)
          USING iceberg PARTITIONED BY (tenant)""")
        sql(s"CREATE TABLE $refs (tenant INT, entity STRING, session_id STRING) USING iceberg")
        sql(s"""CREATE TABLE $result (
          kind STRING, tenant INT, entity STRING, events BIGINT, hard BOOLEAN) USING iceberg""")
        sql(
          s"INSERT INTO $refs VALUES (1, 'known', 'known-session'), (2, 'missing', 'missing-session')")
        sql(s"""INSERT INTO $events VALUES
          (1, 'known', named_struct('id', 'known-session'), TIMESTAMP '2020-01-01 00:00:00'),
          (1, 'known', named_struct('id', 'missing-session'), TIMESTAMP '2020-01-01 00:00:00'),
          (1, 'missing', named_struct('id', 'missing-session'), TIMESTAMP '2020-01-01 00:00:00'),
          (1, 'missing', named_struct('id', 'missing-session'), TIMESTAMP '2020-01-02 00:00:00'),
          (1, 'recent', named_struct('id', 'recent-session'), TIMESTAMP '2100-01-01 00:00:00')""")
        val query = runAvailable(spark.readStream.table(events), new File(dir, "integrity")) {
          (batch, _) =>
            assert(!batch.isEmpty)
            val batchSession = batch.sparkSession
            import org.apache.spark.sql.functions._
            val minTime = batch.select(min("observed")).head().getTimestamp(0)
            assert(minTime.toString == "2020-01-01 00:00:00.0")
            val tenants = batch.select("tenant").distinct().collect().map(_.getInt(0))
            batch.createOrReplaceTempView("stream_events")
            val references =
              batchSession.table(refs).filter(col("tenant").isin(tenants.toSeq: _*))
            references.createOrReplaceTempView("stream_references")
            try {
              // Check entity and nested session references independently, as separate callbacks
              // would. A known entity with a missing session must fail only the session check.
              Seq(("entity", "entity", "entity"), ("session", "session.id", "session_id"))
                .foreach { case (kind, sourceKey, referenceKey) =>
                  val violations = batchSession.sql(s"""
                    SELECT '$kind' AS kind, e.tenant, e.$sourceKey AS entity, COUNT(*) AS events,
                      MIN(e.observed) < current_timestamp() - INTERVAL 15 MINUTES AS hard
                    FROM stream_events e LEFT ANTI JOIN stream_references r
                      ON e.tenant = r.tenant AND e.$sourceKey = r.$referenceKey
                    GROUP BY e.tenant, e.$sourceKey""")
                  assert(violations.count() == 2)
                  assert(violations.filter("hard").count() == 1)
                  assert(violations.filter("NOT hard").count() == 1)
                  val expected = if (kind == "entity") {
                    Set(Row(kind, 1, "missing", 2L, true), Row(kind, 1, "recent", 1L, false))
                  } else {
                    Set(
                      Row(kind, 1, "missing-session", 3L, true),
                      Row(kind, 1, "recent-session", 1L, false))
                  }
                  assert(violations.collect().toSet == expected)
                  val plan = violations.queryExecution.executedPlan
                  assert(
                    collect(plan) { case s: CometIcebergNativeScanExec => s }.nonEmpty,
                    plan.toString)
                  assert(
                    collect(plan) {
                      case p if p.nodeName.contains("Comet") && p.nodeName.contains("Join") => p
                    }.nonEmpty,
                    plan.toString)
                  violations.writeTo(result).append()
                }
            } finally {
              batchSession.catalog.dropTempView("stream_events")
              batchSession.catalog.dropTempView("stream_references")
            }
        }
        assert(executedPlan(query).exists(_.isInstanceOf[CometIcebergNativeScanExec]))
        assert(query.recentProgress.map(_.numInputRows).sum >= 5)
        // foreachBatch committed through its own session/catalog instance.
        spark.catalog.refreshTable(result)
        checkAnswer(
          spark.table(result),
          Seq(
            Row("entity", 1, "missing", 2L, true),
            Row("entity", 1, "recent", 1L, false),
            Row("session", 1, "missing-session", 3L, true),
            Row("session", 1, "recent-session", 1L, false)))
      }
    }
  }

  test("native streaming aggregate merges persisted state and restarts with Spark checkpoints") {
    withCatalog { (dir, catalog, namespace) =>
      val table = s"$catalog.$namespace.state_events"
      withTable(table) {
        sql(s"CREATE TABLE $table (tenant INT, value BIGINT) USING iceberg")
        sql(s"INSERT INTO $table VALUES (1, 10), (1, 20), (2, NULL)")
        val checkpoint = new File(dir, "state-checkpoint")
        def source = spark.readStream
          .table(table)
          .groupBy("tenant")
          .agg(count(lit(1)).as("n"), sum("value").as("total"))
        def run(native: Boolean, expected: Set[Row]): StreamingQuery = {
          withSQLConf(CometConf.COMET_STREAMING_EXEC_ENABLED.key -> native.toString) {
            val rows = ArrayBuffer.empty[Row]
            val query = runAvailable(source, checkpoint, "complete") { (batch, _) =>
              rows.clear()
              rows ++= batch.collect()
            }
            assert(rows.toSet == expected)
            val plan = executedPlan(query)
            val aggregates = plan.collect { case a: CometHashAggregateExec => a }
            if (native) {
              assert(
                aggregates.exists(_.aggregateExpressions.exists(
                  _.mode == org.apache.spark.sql.catalyst.expressions.aggregate.PartialMerge)),
                plan.toString)
              assert(plan.exists(_.getClass.getSimpleName == "StateStoreRestoreExec"))
              assert(query.lastProgress.stateOperators.map(_.numRowsTotal).sum == expected.size)
            } else assert(aggregates.isEmpty, plan.toString)
            query
          }
        }
        run(false, Set(Row(1, 2L, 30L), Row(2, 1L, null)))
        sql(s"INSERT INTO $table VALUES (1, 5), (2, 7), (3, 9)")
        run(true, Set(Row(1, 3L, 35L), Row(2, 2L, 7L), Row(3, 1L, 9L)))
        sql(s"INSERT INTO $table VALUES (3, 1)")
        run(false, Set(Row(1, 3L, 35L), Row(2, 2L, 7L), Row(3, 2L, 10L)))
      }
    }
  }

  test("native state merge replays a failed batch without applying it twice") {
    withCatalog { (dir, catalog, namespace) =>
      val table = s"$catalog.$namespace.state_retry"
      withTable(table) {
        sql(s"CREATE TABLE $table (id INT, value BIGINT) USING iceberg")
        sql(s"INSERT INTO $table VALUES (1, 10)")
        val checkpoint = new File(dir, "state-retry")
        def source = spark.readStream.table(table).groupBy("id").agg(sum("value"))
        withSQLConf(CometConf.COMET_STREAMING_EXEC_ENABLED.key -> "true") {
          runAvailable(source, checkpoint, "update") { (batch, _) =>
            assert(batch.collect().toSeq == Seq(Row(1, 10L)))
          }
          sql(s"INSERT INTO $table VALUES (1, 5)")
          val failure = intercept[StreamingQueryException] {
            runAvailable(source, checkpoint, "update") { (batch, _) =>
              assert(batch.collect().toSeq == Seq(Row(1, 15L)))
              throw new IllegalStateException("state callback failure")
            }
          }
          assert(failure.getMessage.contains("state callback failure"))
          val replay = runAvailable(source, checkpoint, "update") { (batch, _) =>
            assert(batch.collect().toSeq == Seq(Row(1, 15L)))
          }
          assert(executedPlan(replay).exists(_.isInstanceOf[CometHashAggregateExec]))
        }
      }
    }
  }

  test("native window state preserves late-event filtering and watermark eviction") {
    withCatalog { (dir, catalog, namespace) =>
      val table = s"$catalog.$namespace.window_events"
      withTable(table) {
        sql(s"CREATE TABLE $table (tenant INT, time TIMESTAMP) USING iceberg")
        def source = spark.readStream
          .table(table)
          .withWatermark("time", "10 seconds")
          .groupBy(window(col("time"), "10 seconds"), col("tenant"))
          .count()
        val actual = ArrayBuffer.empty[Row]
        val expected = ArrayBuffer.empty[Row]
        for ((second, index) <- Seq(1, 25, 2, 45).zipWithIndex) {
          sql(s"INSERT INTO $table VALUES (1, TIMESTAMP '2026-01-01 00:00:$second')")
          for (native <- Seq(false, true)) {
            withSQLConf(CometConf.COMET_STREAMING_EXEC_ENABLED.key -> native.toString) {
              val query = runAvailable(source, new File(dir, s"window-$native")) { (batch, _) =>
                val rows = batch.collect()
                if (native) actual ++= rows else expected ++= rows
              }
              if (native) {
                assert(
                  executedPlan(query).exists(_.isInstanceOf[CometHashAggregateExec]),
                  executedPlan(query).toString)
              }
            }
          }
          assert(actual.toSeq == expected.toSeq, s"window mismatch after input $index")
        }
        // Iceberg 1.10 falls back to one batch per trigger, so it does not run the extra
        // no-data batch that advances eviction before the late event arrives. Both engines
        // must follow that runtime's schedule, as checked after every input above.
        assert(actual.size == (if (icebergVersionAtLeast(1, 11)) 2 else 1))
        assert(actual.forall(_.getLong(2) == (if (icebergVersionAtLeast(1, 11)) 1L else 2L)))
      }
    }
  }

  test("streaming aggregates with incompatible checkpoint buffers retain Spark state") {
    withCatalog { (dir, catalog, namespace) =>
      val table = s"$catalog.$namespace.collected_state"
      withTable(table) {
        sql(s"CREATE TABLE $table (id INT, value STRING) USING iceberg")
        sql(s"INSERT INTO $table VALUES (1, 'a')")
        def source = spark.readStream.table(table).groupBy("id").agg(collect_set("value"))
        val checkpoint = new File(dir, "collected-state")
        for (native <- Seq(true, false)) {
          if (!native) sql(s"INSERT INTO $table VALUES (1, 'b')")
          withSQLConf(CometConf.COMET_STREAMING_EXEC_ENABLED.key -> native.toString) {
            val query = runAvailable(source, checkpoint, "complete") { (batch, _) =>
              assert(
                batch.collect().head.getSeq[String](1).toSet ==
                  (if (native) Set("a") else Set("a", "b")))
            }
            assert(!executedPlan(query).exists(_.isInstanceOf[CometHashAggregateExec]))
          }
        }
      }
    }
  }

  test("streaming execution keeps continuous sources and disabled no-data batches in Spark") {
    val attr = org.apache.spark.sql.catalyst.expressions
      .AttributeReference("id", org.apache.spark.sql.types.IntegerType)()
    val empty = org.apache.spark.sql.execution.LocalTableScanExec(Seq(attr), Seq.empty, None)
    empty.setLogicalLink(
      org.apache.spark.sql.catalyst.plans.logical.LocalRelation(Seq(attr), isStreaming = true))
    withSQLConf(
      CometConf.COMET_STREAMING_EXEC_ENABLED.key -> "false",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      assert(org.apache.comet.rules.CometExecRule(spark).apply(empty) eq empty)
    }
    val continuousClass = classOf[org.apache.spark.sql.connector.read.streaming.ContinuousStream]
    val continuous = java.lang.reflect.Proxy
      .newProxyInstance(
        continuousClass.getClassLoader,
        Array(continuousClass),
        (_, _, _) =>
          throw new AssertionError("Continuous source must not be executed by the rule"))
      .asInstanceOf[org.apache.spark.sql.connector.read.streaming.ContinuousStream]
    val scan = org.apache.spark.sql.execution.RDDScanExec(
      Seq(attr),
      spark.sparkContext.emptyRDD[org.apache.spark.sql.catalyst.InternalRow],
      "continuous",
      stream = Some(continuous))
    withSQLConf(
      CometConf.COMET_STREAMING_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true") {
      assert(org.apache.comet.rules.CometExecRule(spark).apply(scan) eq scan)
    }
  }
}
