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
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.comet.{CometIcebergWriteExec, IcebergCommitExec, IcebergWriteExec}
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark41Plus}

private case class WriteSnapshot(snapshotDelta: Long, plans: Seq[SparkPlan])

class CometIcebergWriteActionSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometIcebergTestBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key, "true")
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  }

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

  test("AppendData from a zero-partition RDD still runs one write task and commits") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "zero_part", partitionSpec = "")
      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("region", StringType),
          StructField("amount", DoubleType)))
      val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      assert(emptyDf.rdd.getNumPartitions == 0, "test requires a genuinely zero-partition input")

      val snapshot = captureWrite("zero_part") {
        emptyDf.writeTo(s"$catalog.$ns.zero_part").append()
      }
      assertExactlyOneCommit(snapshot)
      val commitNodes = snapshot.plans.flatMap { plan =>
        collectWithSubqueries(plan) { case c: IcebergCommitExec => c }
      }
      assert(
        commitNodes.exists(_.metrics("numCommittedMessages").value == 1),
        "expected the dummy single-partition write task to produce one commit message")
      assertRows("zero_part", expectedIds = Seq.empty)
    }
  }

  test("AQE re-plan of the writer subtree writes and commits exactly once") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "aqe_replan", partitionSpec = "")
      val session = spark
      import session.implicits._
      (1 to 100)
        .map(i => (i, s"r${i % 4}", i.toDouble))
        .toDF("id", "region", "amount")
        .createOrReplaceTempView("aqe_replan_left")
      (1 to 100)
        .map(i => (i, i * 10.0))
        .toDF("id", "bonus")
        .createOrReplaceTempView("aqe_replan_right")

      // Broadcast is disabled at static planning time, so the initial plan under the writer
      // joins with a shuffle. AQE's runtime stats then re-plan it to a broadcast join, which
      // re-emits the writer subtree via IcebergWriteLogical mid-execution.
      val snapshot = captureWrite("aqe_replan") {
        withSQLConf(
          "spark.sql.adaptive.enabled" -> "true",
          "spark.sql.autoBroadcastJoinThreshold" -> "-1",
          "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "10m") {
          spark.sql(
            "INSERT INTO cat.db.aqe_replan " +
              "SELECT l.id, l.region, l.amount + r.bonus " +
              "FROM aqe_replan_left l JOIN aqe_replan_right r ON l.id = r.id")
        }
      }
      assertExactlyOneCommit(snapshot)
      val broadcastJoins = snapshot.plans.flatMap { plan =>
        collectWithSubqueries(plan) {
          case j if j.nodeName.contains("BroadcastHashJoin") => j
        }
      }
      assert(
        broadcastJoins.nonEmpty,
        "expected AQE to re-plan the static shuffle join to a broadcast join. Plans:\n" +
          snapshot.plans.mkString("\n--\n"))
      assertRows("aqe_replan", expectedIds = 1 to 100)
    }
  }

  test("partitioned write under AQE keeps the clustered writer working across the shuffle") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "aqe_part", partitionSpec = "PARTITIONED BY (region)")
      val session = spark
      import session.implicits._
      (1 to 500)
        .map(i => (i, s"r${i % 8}", i.toDouble))
        .toDF("id", "region", "amount")
        .createOrReplaceTempView("aqe_part_src")

      val snapshot = captureWrite("aqe_part") {
        withSQLConf(
          "spark.sql.adaptive.enabled" -> "true",
          "spark.sql.shuffle.partitions" -> "8") {
          spark.sql(s"INSERT INTO $catalog.$ns.aqe_part SELECT * FROM aqe_part_src")
        }
      }
      assertExactlyOneCommit(snapshot)
      val exchanges = snapshot.plans.flatMap { plan =>
        collectWithSubqueries(plan) { case e if e.nodeName.contains("Exchange") => e }
      }
      assert(
        exchanges.nonEmpty,
        "expected the clustered partitioned write to shuffle its input. Plans:\n" +
          snapshot.plans.mkString("\n--\n"))
      assertRows("aqe_part", expectedIds = 1 to 500)
    }
  }

  test("multi-partition partitioned write collects one commit message per task") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "multi_task", partitionSpec = "PARTITIONED BY (region)")
      val session = spark
      import session.implicits._
      (1 to 100)
        .map(i => (i, s"r${i % 4}", i.toDouble))
        .toDF("id", "region", "amount")
        .createOrReplaceTempView("multi_task_src")

      val snapshot = captureWrite("multi_task") {
        withSQLConf(
          "spark.sql.adaptive.coalescePartitions.enabled" -> "false",
          "spark.sql.shuffle.partitions" -> "4") {
          spark.sql(s"INSERT INTO $catalog.$ns.multi_task SELECT * FROM multi_task_src")
        }
      }
      assertExactlyOneCommit(snapshot)
      val commitNodes = snapshot.plans.flatMap { plan =>
        collectWithSubqueries(plan) { case c: IcebergCommitExec => c }
      }
      assert(
        commitNodes.exists(_.metrics("numCommittedMessages").value >= 2),
        "expected multiple task commit messages, got " +
          s"${commitNodes.map(_.metrics("numCommittedMessages").value)}")
      assertRows("multi_task", expectedIds = 1 to 100)
    }
  }

  // Spark 4.1 recaches v2 catalog tables by name, so a cached table must pick up an append
  // that follows a schema-changing DDL; plan-based recaching on older Sparks cannot match the
  // pre-DDL cache entry, matching stock behaviour there.
  test("cached table reflects an append that follows a schema change on Spark 4.1+") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(isSpark41Plus, "name-based cache refresh needs Spark 4.1+")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "cache_refresh", partitionSpec = "")
      spark.sql(s"INSERT INTO $catalog.$ns.cache_refresh VALUES (1, 'us-east', 10.0)")
      spark.sql(s"CACHE TABLE $catalog.$ns.cache_refresh")
      assert(spark.sql(s"SELECT * FROM $catalog.$ns.cache_refresh").count() == 1)

      try {
        spark.sql(s"ALTER TABLE $catalog.$ns.cache_refresh ADD COLUMN extra INT")
        spark.sql(s"INSERT INTO $catalog.$ns.cache_refresh VALUES (2, 'eu', 20.0, -1)")
        // Sorted client-side: ORDER BY over an InMemoryTableScan trips an unrelated,
        // pre-existing Comet shuffle-planning bug regardless of this config.
        val ids = spark
          .sql(s"SELECT id FROM $catalog.$ns.cache_refresh")
          .collect()
          .map(_.getInt(0))
          .sorted
          .toSeq
        assert(ids == Seq(1, 2), s"cached table missed the post-DDL append, got $ids")
      } finally {
        spark.sql(s"UNCACHE TABLE IF EXISTS $catalog.$ns.cache_refresh")
      }
    }
  }

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

  test("OverwritePartitionsDynamic replaces only touched partitions") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "overwrite_dynamic", partitionSpec = "PARTITIONED BY (region)")
      spark.sql(
        "INSERT INTO cat.db.overwrite_dynamic VALUES " +
          "(1, 'us-east', 1.0), (2, 'us-west', 2.0), (3, 'eu', 3.0)")

      val snapshot = captureWrite("overwrite_dynamic") {
        withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "DYNAMIC") {
          spark.sql("INSERT OVERWRITE cat.db.overwrite_dynamic VALUES (10, 'us-east', 100.0)")
        }
      }
      assertExactlyOneCommit(snapshot)
      val ids = spark
        .sql("SELECT id FROM cat.db.overwrite_dynamic ORDER BY id")
        .collect()
        .map(_.getInt(0))
        .toSeq
      assert(ids == Seq(2, 3, 10), s"expected (2,3,10), got $ids")
    }
  }

  test("ReplaceData (CoW DELETE) on a row predicate goes through two-op") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "cow_delete",
        partitionSpec = "",
        properties = Some("'write.delete.mode'='copy-on-write'"))
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

  test("ReplaceData (CoW MERGE) with matched and unmatched legs routes through two-op") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "cow_merge",
        partitionSpec = "",
        properties = Some("'write.merge.mode'='copy-on-write'"))
      coalesceInsert("cow_merge", Seq((1, "us-east", 10.0), (2, "us-west", 20.0)))

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

  // Spark 4.1 hands MERGE metrics to `BatchWrite.commit(messages, summary)` and Iceberg 1.11+
  // records them in the snapshot summary.
  test("CoW MERGE records merge metrics in the snapshot summary on Spark 4.1+") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(isSpark41Plus, "MERGE write summaries need Spark 4.1+")
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "merge_summary",
        partitionSpec = "",
        properties = Some("'write.merge.mode'='copy-on-write'"))
      coalesceInsert("merge_summary", Seq((1, "us-east", 10.0), (2, "us-west", 20.0)))

      spark.sql(s"""
        |MERGE INTO $catalog.$ns.merge_summary t
        |USING (SELECT 2 AS id, 'us-west' AS region, 200.0 AS amount UNION ALL
        |       SELECT 3 AS id, 'eu' AS region, 30.0 AS amount) s
        |ON t.id = s.id
        |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
        |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
        |""".stripMargin)

      val summary = spark
        .sql(s"SELECT summary FROM $catalog.$ns.merge_summary.snapshots " +
          "ORDER BY committed_at DESC LIMIT 1")
        .collect()(0)
        .getMap[String, String](0)
      val expected = Map(
        "spark.merge-into.num-target-rows-copied" -> "1",
        "spark.merge-into.num-target-rows-updated" -> "1",
        "spark.merge-into.num-target-rows-inserted" -> "1")
      expected.foreach { case (key, value) =>
        assert(
          summary.get(key).contains(value),
          s"expected $key=$value in snapshot summary, got $summary")
      }
    }
  }

  test("failed write job aborts and leaves the table unchanged") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "task_fail", partitionSpec = "")
      coalesceInsert("task_fail", Seq((1, "us-east", 10.0)))
      val session = spark
      import session.implicits._
      (1 to 10)
        .map(i => (i, s"r$i", i.toDouble))
        .toDF("id", "region", "amount")
        .createOrReplaceTempView("task_fail_src")
      spark.udf.register(
        "boom_on_seven",
        (id: Int) => {
          if (id == 7) throw new RuntimeException("boom")
          id
        })

      val before = countSnapshots("task_fail")
      val e = intercept[Exception] {
        spark.sql(
          s"INSERT INTO $catalog.$ns.task_fail " +
            "SELECT boom_on_seven(id), region, amount FROM task_fail_src")
      }
      assert(
        exceptionChain(e).exists(_.getMessage != null) &&
          exceptionChain(e).exists(t => Option(t.getMessage).exists(_.contains("boom"))),
        s"expected the injected task failure to surface, got $e")
      assert(countSnapshots("task_fail") == before, "failed write must not create a snapshot")
      assertRows("task_fail", expectedIds = Seq(1))
    }
  }

  test("commit-time validation still sees a conflicting concurrent append") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "conflict",
        partitionSpec = "",
        properties = Some(
          "'write.delete.mode'='copy-on-write','write.delete.isolation-level'='serializable'"))
      coalesceInsert("conflict", Seq((1, "us-east", 10.0), (2, "us-west", 20.0), (3, "eu", 30.0)))

      ConflictGate.reset()
      spark.udf.register(
        "conflict_gate",
        (id: Int) => {
          ConflictGate.enter()
          id
        })

      val before = countSnapshots("conflict")
      // The gate UDF blocks the DELETE's tasks after its scan snapshot is pinned, so the
      // append below is guaranteed to land between the scan and the commit. Runtime group
      // filtering is disabled so the conflict is detected by Iceberg's commit-time
      // validation rather than aborted earlier by the runtime file filter.
      withSQLConf("spark.sql.optimizer.runtime.rowLevelOperationGroupFilter.enabled" -> "false") {
        val delete = Future {
          spark.sql(s"DELETE FROM $catalog.$ns.conflict WHERE conflict_gate(id) = 2")
        }
        assert(
          ConflictGate.awaitScanStarted(2, TimeUnit.MINUTES),
          "DELETE never started scanning; gate UDF was not invoked")
        spark.sql(s"INSERT INTO $catalog.$ns.conflict VALUES (2, 'us-west', 99.0)")
        ConflictGate.releaseWrite()

        val e = intercept[Exception] {
          Await.result(delete, 2.minutes)
        }
        assert(
          exceptionChain(e).exists(t =>
            Option(t.getMessage).exists(_.toLowerCase.contains("conflict"))),
          s"expected Iceberg commit-time validation to fail the DELETE, got $e")
      }
      assert(
        countSnapshots("conflict") == before + 1,
        "only the concurrent append may commit; the DELETE must not")
      val ids = spark
        .sql(s"SELECT id FROM $catalog.$ns.conflict ORDER BY id")
        .collect()
        .map(_.getInt(0))
        .toSeq
      assert(ids == Seq(1, 2, 2, 3), s"expected (1,2,2,3), got $ids")
    }
  }

  test("non-Iceberg V2 write plans through Spark unchanged with the config on") {
    withSQLConf(
      "spark.sql.catalog.testcat" -> classOf[InMemoryTableCatalog].getName,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      spark.sql("CREATE TABLE testcat.tbl (id INT, region STRING, amount DOUBLE)")
      try {
        val plans = capturePlans {
          spark.sql("INSERT INTO testcat.tbl VALUES (1, 'us-east', 10.5)")
        }
        val (commits, writes) = collectIcebergWriteOps(plans)
        assert(commits.isEmpty, s"unexpected IcebergCommitExec on a non-Iceberg write: $commits")
        assert(writes.isEmpty, s"unexpected IcebergWriteExec on a non-Iceberg write: $writes")
        val ids = spark.sql("SELECT id FROM testcat.tbl").collect().map(_.getInt(0)).toSeq
        assert(ids == Seq(1), s"expected (1), got $ids")
      } finally {
        spark.sql("DROP TABLE IF EXISTS testcat.tbl")
      }
    }
  }

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
      val (commits, writes) = collectIcebergWriteOps(snapshot.plans)
      assert(commits.isEmpty, s"unexpected IcebergCommitExec: $commits")
      assert(writes.isEmpty, s"unexpected IcebergWriteExec: $writes")
      assertRows("disabled_conf", expectedIds = Seq(1))
    }
  }

  // --- Round-trip parity vs Spark default path ---------------------------------------------------

  // --- Native acceleration --------------------------------------------------------------------

  test("native acceleration: AppendData INSERT FROM SELECT") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "native_source", partitionSpec = "")
      createTable(warehouseDir, "native_target", partitionSpec = "")
      spark.sql(
        "INSERT INTO cat.db.native_source VALUES " +
          "(1, 'us-east', 10.5), (2, 'us-west', 20.3), (3, 'eu', 30.7)")
      assertNativeWriteEngages("native_target", Seq(1, 2, 3)) {
        spark.sql(
          "INSERT INTO cat.db.native_target SELECT id, region, amount FROM cat.db.native_source")
      }
    }
  }

  test("native acceleration: AppendData unpartitioned VALUES") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "native_append_values", partitionSpec = "")
      assertNativeWriteEngages("native_append_values", Seq(1, 2, 3)) {
        spark.sql(
          "INSERT INTO cat.db.native_append_values VALUES " +
            "(1, 'us-east', 10.5), (2, 'us-west', 20.3), (3, 'eu', 30.7)")
      }
    }
  }

  // What Iceberg's Spark writer stamps for `sort_order_id` on appended files changed across
  // releases: through 1.10 `SparkWrite$WriterFactory` never wires the table sort order (files
  // get 0 even on a sorted table); 1.11 added `SparkWriteConf.outputSortOrderId` and stamps the
  // resolved order id. The native path reflects the resolver when present and defaults to 0
  // otherwise, so pin parity against the JVM writer on the same runtime instead of a literal.
  test("native acceleration: appended files carry the same sort_order_id as the JVM writer") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      // WRITE ORDERED BY (provided by IcebergSparkSessionExtensions, enabled in this suite)
      // bumps the table's sort order id to a non-default value (1).
      Seq("sorted_native", "sorted_jvm").foreach { t =>
        createTable(warehouseDir, t, partitionSpec = "")
        spark.sql(s"ALTER TABLE cat.db.$t WRITE ORDERED BY id")
      }
      val insert = (t: String) =>
        spark.sql(
          s"INSERT INTO cat.db.$t VALUES " +
            "(3, 'eu', 30.7), (1, 'us-east', 10.5), (2, 'us-west', 20.3)")
      assertNativeWriteEngages("sorted_native", Seq(1, 2, 3))(insert("sorted_native"))
      insert("sorted_jvm")

      def sortOrderIds(t: String): Set[Int] = spark
        .sql(s"SELECT DISTINCT sort_order_id FROM cat.db.$t.data_files")
        .collect()
        .map(_.getInt(0))
        .toSet
      val nativeIds = sortOrderIds("sorted_native")
      val jvmIds = sortOrderIds("sorted_jvm")
      assert(nativeIds == jvmIds, s"native sort_order_ids $nativeIds != JVM $jvmIds")
      assert(nativeIds.size == 1, s"expected one distinct sort_order_id, got $nativeIds")
    }
  }

  test("native acceleration: AppendData partitioned by identity") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "native_append_part", partitionSpec = "PARTITIONED BY (region)")
      assertNativeWriteEngages("native_append_part", Seq(1, 2, 3)) {
        spark.sql(
          "INSERT INTO cat.db.native_append_part VALUES " +
            "(1, 'us-east', 10.5), (2, 'us-east', 20.3), (3, 'eu', 30.7)")
      }
    }
  }

  test("native acceleration: OverwriteByExpression (INSERT OVERWRITE STATIC)") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "native_overwrite_static", partitionSpec = "")
      spark.sql(
        "INSERT INTO cat.db.native_overwrite_static VALUES " +
          "(1, 'old', 1.0), (2, 'old', 2.0), (3, 'old', 3.0)")
      assertNativeWriteEngages("native_overwrite_static", Seq(10, 11)) {
        withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "STATIC") {
          spark.sql(
            "INSERT OVERWRITE cat.db.native_overwrite_static VALUES " +
              "(10, 'new', 100.0), (11, 'new', 110.0)")
        }
      }
    }
  }

  test("native acceleration: OverwritePartitionsDynamic") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "native_overwrite_dyn", partitionSpec = "PARTITIONED BY (region)")
      spark.sql(
        "INSERT INTO cat.db.native_overwrite_dyn VALUES " +
          "(1, 'us-east', 1.0), (2, 'us-west', 2.0), (3, 'eu', 3.0)")
      assertNativeWriteEngages("native_overwrite_dyn", Seq(2, 3, 10)) {
        withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "DYNAMIC") {
          spark.sql("INSERT OVERWRITE cat.db.native_overwrite_dyn VALUES (10, 'us-east', 100.0)")
        }
      }
    }
  }

  test("native acceleration: ReplaceData (CoW DELETE)") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "native_cow_delete",
        partitionSpec = "",
        properties = Some("'write.delete.mode'='copy-on-write'"))
      // Seed via the JVM path so the assertion isolates native engagement to the DELETE.
      withSQLConf(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "false") {
        coalesceInsert(
          "native_cow_delete",
          Seq((1, "us-east", 10.0), (2, "us-west", 20.0), (3, "eu", 30.0), (4, "us-east", 40.0)))
      }
      assertNativeWriteEngages("native_cow_delete", Seq(1, 3, 4)) {
        spark.sql("DELETE FROM cat.db.native_cow_delete WHERE id = 2")
      }
    }
  }

  test("native acceleration: ReplaceData (CoW UPDATE)") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "native_cow_update",
        partitionSpec = "",
        properties = Some("'write.update.mode'='copy-on-write'"))
      withSQLConf(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "false") {
        coalesceInsert(
          "native_cow_update",
          Seq((1, "us-east", 10.0), (2, "us-west", 20.0), (3, "eu", 30.0)))
      }
      // Engage natively; expected rows are the ids 1..3 (UPDATE keeps cardinality).
      assertNativeWriteEngages("native_cow_update", Seq(1, 2, 3)) {
        spark.sql("UPDATE cat.db.native_cow_update SET amount = amount * 2 WHERE id = 2")
      }
      // Spot-check the UPDATE actually rewrote row 2 (cardinality unchanged + value flipped).
      val r =
        spark.sql("SELECT id, amount FROM cat.db.native_cow_update WHERE id = 2").collect()
      assert(r.length == 1 && r(0).getDouble(1) == 40.0, s"got ${r.toSeq}")
    }
  }

  test("native acceleration: ReplaceData (CoW MERGE) falls back (MergeRowsExec not Comet)") {
    // TODO(comet-merge-rows): native MERGE engagement requires a Comet equivalent of Iceberg's
    // `MergeRowsExec` (the per-row dispatch operator that assigns __row_operation codes from
    // MATCHED/NOT MATCHED clauses). Without it, `MergeRowsExec` stays JVM, the upstream chain
    // breaks Comet-native partway, and `requiresNativeChildren=true` declines the
    // `IcebergWriteExec -> CometIcebergWriteExec` conversion. Until that lands, MERGE
    // falls back to the JVM two-op path -- this test pins that contract. Native `MergeRowsExec`
    // is being added in https://github.com/apache/datafusion-comet/pull/5318; when that lands
    // this test will start failing and needs to flip to `assertNativeWriteEngages`.
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "native_cow_merge",
        partitionSpec = "",
        properties = Some("'write.merge.mode'='copy-on-write'"))
      withSQLConf(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key -> "false") {
        coalesceInsert("native_cow_merge", Seq((1, "us-east", 10.0), (2, "us-west", 20.0)))
      }
      assertNativeWriteDoesNotEngage("native_cow_merge", Seq(1, 2, 3)) {
        spark.sql("""
          |MERGE INTO cat.db.native_cow_merge t
          |USING (SELECT 2 AS id, 'us-west' AS region, 200.0 AS amount UNION ALL
          |       SELECT 3 AS id, 'eu' AS region, 30.0 AS amount) s
          |ON t.id = s.id
          |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
          |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
          |""".stripMargin)
      }
    }
  }

  test("native acceleration: complex types (struct, array, map) round-trip with field IDs") {
    assumeNativeAcceleration()
    withIcebergCatalog { _ =>
      // Three nested kinds in one schema so the recursive PARQUET_FIELD_ID_META_KEY decoration
      // is exercised end-to-end. Reading back via Iceberg's reader is the proof point: Iceberg
      // matches columns by field id, not by name, so a row that round-trips means every nested
      // field id made it into the Parquet metadata.
      spark.sql(s"""
        CREATE TABLE $catalog.$ns.native_complex (
          id INT,
          addr STRUCT<city: STRING, zip: INT>,
          tags ARRAY<STRING>,
          attrs MAP<STRING, INT>
        ) USING iceberg
      """)
      val snapshot = withNativeEnabled {
        captureWrite("native_complex") {
          spark.sql("""
            INSERT INTO cat.db.native_complex VALUES
              (1, named_struct('city', 'NYC', 'zip', 10001), array('a', 'b'), map('k1', 1, 'k2', 2)),
              (2, named_struct('city', 'SF',  'zip', 94016), array('c'),      map('k3', 3))
          """)
        }
      }
      assert(snapshot.snapshotDelta == 1L)
      val nativeExecs = snapshot.plans.flatMap { p =>
        collectWithSubqueries(p) { case e: CometIcebergWriteExec => e }
      }
      assert(nativeExecs.nonEmpty, "expected native write exec in captured plans")
      val rows = spark
        .sql(s"SELECT id, addr.city, addr.zip, tags, attrs FROM $catalog.$ns.native_complex" +
          " ORDER BY id")
        .collect()
      assert(rows.length == 2)
      assert(rows(0).getInt(0) == 1)
      assert(rows(0).getString(1) == "NYC")
      assert(rows(0).getInt(2) == 10001)
      assert(rows(1).getString(1) == "SF")
      assert(rows(1).getInt(2) == 94016)
    }
  }

  // Java sources float/double manifest metrics from writer-tracked state (FloatFieldMetrics):
  // NaN counted separately, bounds computed over non-NaN values, bounds dropped when every
  // value is NaN. The native path must reproduce those decisions via the JVM-side metrics
  // rebuild, so write identical data through both paths and pin the aggregated per-column
  // manifest metrics against each other.
  test("native acceleration: NaN float/double manifest metrics match the JVM writer") {
    assumeNativeAcceleration()
    withIcebergCatalog { _ =>
      Seq("nan_native", "nan_jvm").foreach { t =>
        spark.sql(s"""
          CREATE TABLE $catalog.$ns.$t (
            id INT,
            f FLOAT,
            d DOUBLE,
            all_nan FLOAT
          ) USING iceberg
        """)
      }
      // `f`'s zero is deliberately -0.0: parquet-rs normalises zero lower bounds to -0.0 while
      // Java preserves the sign it saw (an accepted divergence), so -0.0 is the one zero where
      // both paths agree bit-for-bit and Row equality below stays exact.
      def insert(t: String): Unit = {
        spark.sql(s"""
          INSERT INTO $catalog.$ns.$t VALUES
            (1, CAST('NaN' AS FLOAT), 1.5D, CAST('NaN' AS FLOAT)),
            (2, 2.5, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)),
            (3, -0.0, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)),
            (4, NULL, 0.25D, NULL)
        """)
      }

      val snapshot = withNativeEnabled { captureWrite("nan_native")(insert("nan_native")) }
      assert(snapshot.snapshotDelta == 1L)
      val nativeExecs = snapshot.plans.flatMap { p =>
        collectWithSubqueries(p) { case e: CometIcebergWriteExec => e }
      }
      assert(nativeExecs.nonEmpty, "expected the NaN write to engage the native path")
      insert("nan_jvm")

      // Aggregate across data files so the assertion is robust to how the insert splits into
      // tasks; both paths run the identical upstream plan, so the file split is the same.
      def columnMetrics(t: String): Seq[Row] = {
        spark
          .sql(s"""
            SELECT
              sum(readable_metrics.f.nan_value_count),
              min(readable_metrics.f.lower_bound),
              max(readable_metrics.f.upper_bound),
              sum(readable_metrics.d.nan_value_count),
              min(readable_metrics.d.lower_bound),
              max(readable_metrics.d.upper_bound),
              sum(readable_metrics.all_nan.nan_value_count),
              min(readable_metrics.all_nan.lower_bound),
              max(readable_metrics.all_nan.upper_bound)
            FROM $catalog.$ns.$t.data_files
          """)
          .collect()
          .toSeq
      }

      val native = columnMetrics("nan_native")
      val jvm = columnMetrics("nan_jvm")
      assert(
        native == jvm,
        s"native manifest metrics ${native.mkString} != JVM manifest metrics ${jvm.mkString}")

      val row = native.head
      assert(row.getLong(0) == 1L, "f has exactly one NaN")
      assert(row.getFloat(1) == -0.0f && row.getFloat(2) == 2.5f, "f bounds skip NaN")
      assert(row.getLong(3) == 2L, "d has exactly two NaNs")
      assert(row.getLong(6) == 3L, "all_nan counts every non-null value as NaN")
      assert(row.isNullAt(7) && row.isNullAt(8), "all-NaN column has no bounds")
    }
  }

  test("native acceleration: CTAS runs its inner append through the native writer") {
    assumeNativeAcceleration()
    assume(isSpark35Plus, "CTAS re-plans its inner append only on Spark 3.5+")
    withIcebergCatalog { _ =>
      // A brand-new table has no metadata file yet, so this also pins the empty
      // metadata-location path in the proto builder.
      val snapshot = withNativeEnabled {
        captureWrite("ctas_native") {
          spark.sql(s"""
            CREATE TABLE $catalog.$ns.ctas_native USING iceberg AS
            SELECT * FROM VALUES (1, 'us', 1.0), (2, 'eu', 2.0) AS t(id, region, amount)
          """)
        }
      }
      assert(snapshot.snapshotDelta == 1L)
      val nativeExecs = snapshot.plans.flatMap { p =>
        collectWithSubqueries(p) { case e: CometIcebergWriteExec => e }
      }
      assert(nativeExecs.nonEmpty, "expected CTAS's inner append to engage the native writer")
      assertRows("ctas_native", Seq(1, 2))
    }
  }

  test("native acceleration: RTAS replaces table contents through the native writer") {
    assumeNativeAcceleration()
    assume(isSpark35Plus, "RTAS re-plans its inner append only on Spark 3.5+")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "rtas_native", partitionSpec = "")
      coalesceInsert("rtas_native", Seq((1, "old", 1.0)))
      val snapshot = withNativeEnabled {
        captureWrite("rtas_native") {
          spark.sql(s"""
            REPLACE TABLE $catalog.$ns.rtas_native USING iceberg AS
            SELECT * FROM VALUES (10, 'new', 10.0), (11, 'new', 11.0) AS t(id, region, amount)
          """)
        }
      }
      val nativeExecs = snapshot.plans.flatMap { p =>
        collectWithSubqueries(p) { case e: CometIcebergWriteExec => e }
      }
      assert(nativeExecs.nonEmpty, "expected RTAS's inner append to engage the native writer")
      assertRows("rtas_native", Seq(10, 11))
    }
  }

  test("native acceleration: fanout writer handles unsorted partitioned input") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "native_fanout",
        partitionSpec = "PARTITIONED BY (region)",
        properties = Some("'write.spark.fanout.enabled'='true'"))
      // Fanout writes skip Spark's partition-local sort, so partition values arrive
      // interleaved -- the mode the clustered writer would reject.
      assertNativeWriteEngages("native_fanout", Seq(1, 2, 3, 4)) {
        spark.sql(
          "INSERT INTO cat.db.native_fanout VALUES " +
            "(1, 'us-east', 1.0), (2, 'eu', 2.0), (3, 'us-east', 3.0), (4, 'eu', 4.0)")
      }
      val byRegion = spark
        .sql("SELECT region, count(*) FROM cat.db.native_fanout GROUP BY region ORDER BY region")
        .collect()
        .map(r => r.getString(0) -> r.getLong(1))
        .toSeq
      assert(byRegion == Seq("eu" -> 2L, "us-east" -> 2L), s"got $byRegion")
      val files = spark
        .sql("SELECT count(*) FROM cat.db.native_fanout.data_files")
        .collect()
        .head
        .getLong(0)
      assert(files >= 2L, s"expected at least one data file per partition, got $files")
    }
  }

  test("native acceleration: target-file-size rolls one task across multiple files") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "native_roll",
        partitionSpec = "",
        properties = Some("'write.target-file-size-bytes'='1'"))
      val values = (1 to 300).map(i => s"($i, 'r', $i.0)").mkString(", ")
      // One upstream slice + small Comet batches: the rolling writer checks the target size
      // per batch, so three batches against a 1-byte target must roll into multiple files.
      withSQLConf(
        "spark.sql.leafNodeDefaultParallelism" -> "1",
        CometConf.COMET_BATCH_SIZE.key -> "100") {
        assertNativeWriteEngages("native_roll", 1 to 300) {
          spark.sql(s"INSERT INTO cat.db.native_roll VALUES $values")
        }
      }
      val fileRows = spark
        .sql("SELECT record_count FROM cat.db.native_roll.data_files")
        .collect()
        .map(_.getLong(0))
      assert(fileRows.length >= 2, s"expected a multi-file roll, got ${fileRows.length} file(s)")
      assert(fileRows.sum == 300L, s"rows across rolled files must sum to 300, got $fileRows")
    }
  }

  test("native acceleration: empty append commits exactly once with zero data files") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "native_empty", partitionSpec = "")
      val snapshot = withNativeEnabled {
        captureWrite("native_empty") {
          spark.sql(
            "INSERT INTO cat.db.native_empty SELECT id, region, amount " +
              "FROM (SELECT 1 AS id, 'r' AS region, 1.0 AS amount) WHERE id < 0")
        }
      }
      assert(
        snapshot.snapshotDelta == 1L,
        s"expected exactly 1 commit for the empty append, got ${snapshot.snapshotDelta}")
      val nativeExecs = snapshot.plans.flatMap { p =>
        collectWithSubqueries(p) { case e: CometIcebergWriteExec => e }
      }
      assert(nativeExecs.nonEmpty, "expected the empty append to engage the native writer")
      val files = spark
        .sql("SELECT count(*) FROM cat.db.native_empty.data_files")
        .collect()
        .head
        .getLong(0)
      assert(files == 0L, s"expected no data files from an empty append, got $files")
      assertRows("native_empty", Seq.empty)
    }
  }

  test("native acceleration: empty append to a partitioned table commits with zero data files") {
    assumeNativeAcceleration()
    withIcebergCatalog { warehouseDir =>
      // The unpartitioned test above ends in UnpartitionedWriter::close() with nothing written;
      // these end in ClusteredWriter::close() (the partitioned default) and FanoutWriter::close()
      // respectively, with the partition splitter never invoked at all.
      Seq(
        ("native_empty_clustered", None),
        ("native_empty_fanout", Some("'write.spark.fanout.enabled'='true'"))).foreach {
        case (table, props) =>
          createTable(warehouseDir, table, partitionSpec = "PARTITIONED BY (region)", props)
          val snapshot = withNativeEnabled {
            captureWrite(table) {
              spark.sql(s"INSERT INTO $catalog.$ns.$table SELECT id, region, amount " +
                "FROM (SELECT 1 AS id, 'r' AS region, 1.0 AS amount) WHERE id < 0")
            }
          }
          assert(
            snapshot.snapshotDelta == 1L,
            s"expected exactly 1 commit for the empty append to $table, " +
              s"got ${snapshot.snapshotDelta}")
          val nativeExecs = snapshot.plans.flatMap { p =>
            collectWithSubqueries(p) { case e: CometIcebergWriteExec => e }
          }
          assert(
            nativeExecs.nonEmpty,
            s"expected the empty append to $table to engage the native writer")
          val files = spark
            .sql(s"SELECT count(*) FROM $catalog.$ns.$table.data_files")
            .collect()
            .head
            .getLong(0)
          assert(
            files == 0L,
            s"expected no data files from an empty append to $table, got $files")
          assertRows(table, Seq.empty)
      }
    }
  }

  // --- Manifest metadata parity ---------------------------------------------------------------
  //
  // The JVM-side metrics rebuild must reproduce iceberg-java's manifest judgment calls exactly:
  // metrics modes, truncate(N) bound adjustment, per-column overrides, and the sorted-column
  // promotion inside MetricsConfig.forTable. Each test writes identical data through the native
  // and JVM paths and compares the readable per-column manifest metrics.

  /**
   * Create `nameNative`/`nameJvm` twins via `createSql`, run `insertSql` through the native path
   * (asserting it engaged) and the JVM path, then return both tables' aggregated
   * `readable_metrics` rows produced by `metricsSql` for comparison.
   */
  private def manifestMetricsParity(
      baseName: String,
      createSql: String => Seq[String],
      insertSql: String => String,
      metricsSql: String => String): (Row, Row) = {
    val nativeTable = s"${baseName}_native"
    val jvmTable = s"${baseName}_jvm"
    Seq(nativeTable, jvmTable).foreach(t => createSql(s"$catalog.$ns.$t").foreach(spark.sql))
    val snapshot = withNativeEnabled {
      captureWrite(nativeTable)(spark.sql(insertSql(s"$catalog.$ns.$nativeTable")))
    }
    assert(snapshot.snapshotDelta == 1L)
    val nativeExecs = snapshot.plans.flatMap { p =>
      collectWithSubqueries(p) { case e: CometIcebergWriteExec => e }
    }
    assert(nativeExecs.nonEmpty, s"expected the $baseName write to engage the native path")
    spark.sql(insertSql(s"$catalog.$ns.$jvmTable"))

    def collectMetrics(t: String): Row = {
      val rows = spark.sql(metricsSql(s"$catalog.$ns.$t.data_files")).collect()
      assert(rows.length == 1, s"expected one aggregated metrics row for $t")
      rows.head
    }
    (collectMetrics(nativeTable), collectMetrics(jvmTable))
  }

  test("native acceleration: string bounds are truncated like the JVM writer") {
    assumeNativeAcceleration()
    withIcebergCatalog { _ =>
      // Both values exceed Iceberg's default truncate(16): the manifest lower bound must be the
      // 16-code-point prefix and the upper bound the prefix with its last code point incremented.
      val (native, jvm) = manifestMetricsParity(
        "trunc",
        t => Seq(s"CREATE TABLE $t (id INT, s STRING) USING iceberg"),
        t => s"""
          INSERT INTO $t VALUES
            (1, 'aaaaaaaaaaaaaaaaAAAA'),
            (2, 'zzzzzzzzzzzzzzzzZZZZ')
        """,
        f => s"""
          SELECT
            min(readable_metrics.s.lower_bound),
            max(readable_metrics.s.upper_bound),
            sum(readable_metrics.s.value_count)
          FROM $f
        """)
      assert(native == jvm, s"native $native != jvm $jvm")
      assert(native.getString(0) == "aaaaaaaaaaaaaaaa", "lower bound is the truncated prefix")
      assert(
        native.getString(1) == "zzzzzzzzzzzzzzz{",
        "upper bound is the truncated prefix with the last code point incremented")
    }
  }

  test("native acceleration: metrics mode none drops counts and bounds like the JVM writer") {
    assumeNativeAcceleration()
    withIcebergCatalog { _ =>
      // `id` is overridden to full and must keep everything; `s` follows the `none` default and
      // must lose counts and bounds entirely.
      val (native, jvm) = manifestMetricsParity(
        "mode_none",
        t => Seq(s"""
          CREATE TABLE $t (id INT, s STRING) USING iceberg
          TBLPROPERTIES (
            'write.metadata.metrics.default'='none',
            'write.metadata.metrics.column.id'='full'
          )
        """),
        t => s"INSERT INTO $t VALUES (1, 'aaa'), (7, 'zzz')",
        f => s"""
          SELECT
            min(readable_metrics.id.lower_bound),
            max(readable_metrics.id.upper_bound),
            sum(readable_metrics.id.value_count),
            min(readable_metrics.s.lower_bound),
            max(readable_metrics.s.upper_bound),
            sum(readable_metrics.s.value_count),
            sum(readable_metrics.s.null_value_count)
          FROM $f
        """)
      assert(native == jvm, s"native $native != jvm $jvm")
      assert(native.getInt(0) == 1 && native.getInt(1) == 7 && native.getLong(2) == 2L)
      assert((3 to 6).forall(native.isNullAt), s"expected no metrics for s, got $native")
    }
  }

  test("native acceleration: counts mode with a sort order promotes sorted-column bounds") {
    assumeNativeAcceleration()
    withIcebergCatalog { _ =>
      // Under `counts`, plain columns keep counts but no bounds; MetricsConfig.forTable promotes
      // columns of the table sort order to truncate(16), so `s` regains bounds. Pins that the
      // rebuild resolves the config from the table (with sort order), not from raw properties.
      val (native, jvm) = manifestMetricsParity(
        "mode_counts",
        t =>
          Seq(
            s"""
          CREATE TABLE $t (id INT, s STRING) USING iceberg
          TBLPROPERTIES ('write.metadata.metrics.default'='counts')
        """,
            s"ALTER TABLE $t WRITE ORDERED BY s"),
        t => s"INSERT INTO $t VALUES (1, 'aaa'), (7, 'zzz')",
        f => s"""
          SELECT
            sum(readable_metrics.id.value_count),
            min(readable_metrics.id.lower_bound),
            max(readable_metrics.id.upper_bound),
            min(readable_metrics.s.lower_bound),
            max(readable_metrics.s.upper_bound)
          FROM $f
        """)
      assert(native == jvm, s"native $native != jvm $jvm")
      assert(native.getLong(0) == 2L, "counts mode keeps value counts")
      assert(native.isNullAt(1) && native.isNullAt(2), "counts mode drops unsorted bounds")
      assert(
        native.getString(3) == "aaa" && native.getString(4) == "zzz",
        "sorted column is promoted to truncate(16) and keeps bounds")
    }
  }

  test("native acceleration: wide primitive types keep JVM-parity values and manifest metrics") {
    assumeNativeAcceleration()
    withIcebergCatalog { _ =>
      // A non-UTC session zone is the case where a silent timestamp shift would hide: `ts`
      // (timestamptz) must be normalised through the zone while `ts_ntz` must pass through
      // untouched. The remaining columns widen the type surface the native writer is pinned on:
      // both decimal encodings (int64-backed and fixed-16-backed), date, binary, boolean, bigint,
      // and float.
      withSQLConf("spark.sql.session.timeZone" -> "America/New_York") {
        val (native, jvm) = manifestMetricsParity(
          "wide",
          t => Seq(s"""
            CREATE TABLE $t (
              id BIGINT,
              flag BOOLEAN,
              f FLOAT,
              dec9 DECIMAL(9, 2),
              dec38 DECIMAL(38, 10),
              d DATE,
              ts TIMESTAMP,
              ts_ntz TIMESTAMP_NTZ,
              bin BINARY
            ) USING iceberg
          """),
          t => s"""
            INSERT INTO $t VALUES
              (1, true, 1.5, 12345.67, 1234567890.1234567890,
               DATE'2024-03-15', TIMESTAMP'2024-03-15 10:30:00',
               TIMESTAMP_NTZ'2024-03-15 10:30:00', X'0102'),
              (2, false, -2.25, -0.01, -0.0000000001,
               DATE'1969-12-31', TIMESTAMP'1969-12-31 23:59:59',
               TIMESTAMP_NTZ'1969-12-31 23:59:59', X'FF00')
          """,
          f => s"""
            SELECT
              min(readable_metrics.ts.lower_bound), max(readable_metrics.ts.upper_bound),
              min(readable_metrics.ts_ntz.lower_bound), max(readable_metrics.ts_ntz.upper_bound),
              min(readable_metrics.d.lower_bound), max(readable_metrics.d.upper_bound),
              min(readable_metrics.dec9.lower_bound), max(readable_metrics.dec9.upper_bound),
              min(readable_metrics.dec38.lower_bound), max(readable_metrics.dec38.upper_bound),
              min(readable_metrics.f.lower_bound), max(readable_metrics.f.upper_bound),
              min(readable_metrics.bin.lower_bound), max(readable_metrics.bin.upper_bound),
              sum(readable_metrics.flag.value_count), sum(readable_metrics.id.value_count)
            FROM $f
          """)
        assert(native == jvm, s"native $native != jvm $jvm")

        def rows(t: String): Seq[Row] =
          spark.table(s"$catalog.$ns.$t").orderBy("id").collect().toSeq
        assert(rows("wide_native") == rows("wide_jvm"))
        // Absolute semantics, not just twin parity: the timestamptz value must read back as the
        // same zoned instant it was written as, and the ntz value must be zone-independent.
        val matched = spark
          .sql(s"""
            SELECT count(*) FROM $catalog.$ns.wide_native
            WHERE ts = TIMESTAMP'2024-03-15 10:30:00'
              AND ts_ntz = TIMESTAMP_NTZ'2024-03-15 10:30:00'
          """)
          .collect()
          .head
          .getLong(0)
        assert(matched == 1L, "timestamp values shifted on the native write path")
      }
    }
  }

  test("native acceleration: fixed(N) column round-trips like the JVM writer") {
    assumeNativeAcceleration()
    withIcebergCatalog { _ =>
      // Spark DDL cannot declare `fixed(N)`, so evolve the schema through the Iceberg API. Spark
      // plans the column as BinaryType; the native writer casts Binary -> FixedSizeBinary(4),
      // which only holds when every value is exactly 4 bytes -- pinned here.
      Seq("fx_native", "fx_jvm").foreach { t =>
        spark.sql(s"CREATE TABLE $catalog.$ns.$t (id INT) USING iceberg")
        addIcebergColumn(loadIcebergTable(spark, catalog, ns, t), "fx", icebergFixedType(4))
        spark.sql(s"REFRESH TABLE $catalog.$ns.$t")
      }
      def insert(t: String): Unit =
        spark.sql(s"INSERT INTO $catalog.$ns.$t VALUES (1, X'DEADBEEF'), (2, X'00FF10AB')")
      val snapshot = withNativeEnabled {
        captureWrite("fx_native")(insert("fx_native"))
      }
      assert(snapshot.snapshotDelta == 1L)
      val nativeExecs = snapshot.plans.flatMap { p =>
        collectWithSubqueries(p) { case e: CometIcebergWriteExec => e }
      }
      assert(nativeExecs.nonEmpty, "expected the fixed(N) write to engage the native path")
      insert("fx_jvm")

      def rows(t: String): Seq[Row] =
        spark.table(s"$catalog.$ns.$t").orderBy("id").collect().toSeq
      assert(rows("fx_native") == rows("fx_jvm"))
      def metrics(t: String): Row = spark
        .sql(s"""
          SELECT
            min(readable_metrics.fx.lower_bound),
            max(readable_metrics.fx.upper_bound),
            sum(readable_metrics.fx.value_count)
          FROM $catalog.$ns.$t.data_files
        """)
        .collect()
        .head
      assert(metrics("fx_native") == metrics("fx_jvm"))
    }
  }

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

  test("write custom metrics are registered on the committer only") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "metrics_once", partitionSpec = "")
      val snapshot = captureWrite("metrics_once") {
        spark.sql(s"INSERT INTO $catalog.$ns.metrics_once VALUES (1, 'us-east', 10.5)")
      }
      assertExactlyOneCommit(snapshot)
      val (commits, writes) = collectIcebergWriteOps(snapshot.plans)
      writes.foreach { w =>
        assert(
          w.metrics.keySet == Set("numOutputRows"),
          s"IcebergWriteExec must not re-register the write's custom metrics, got ${w.metrics.keySet}")
      }
      // Iceberg publishes write custom metrics from 1.9 on; older versions expose none.
      commits.head.metrics.get("addedDataFiles").foreach { m =>
        assert(m.value == 1L, s"expected addedDataFiles=1 on the committer, got ${m.value}")
      }
    }
  }

  test("AppendData round-trips all primitive types unchanged") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { _ =>
      spark.sql(s"""
        CREATE TABLE $catalog.$ns.prim_types (
          b BOOLEAN, ti TINYINT, si SMALLINT, i INT, bi BIGINT,
          f FLOAT, d DOUBLE, dec DECIMAL(18,4),
          s STRING, bin BINARY, dt DATE, ts TIMESTAMP
        ) USING iceberg""")
      val literals =
        "(true, CAST(1 AS TINYINT), CAST(2 AS SMALLINT), 3, CAST(4 AS BIGINT), " +
          "CAST(1.5 AS FLOAT), 2.5D, CAST(12.3456 AS DECIMAL(18,4)), " +
          "'hello', X'0102', DATE'2024-01-15', TIMESTAMP'2024-01-15 10:00:00'), " +
          "(false, CAST(NULL AS TINYINT), CAST(NULL AS SMALLINT), CAST(NULL AS INT), " +
          "CAST(NULL AS BIGINT), CAST(NULL AS FLOAT), CAST(NULL AS DOUBLE), " +
          "CAST(NULL AS DECIMAL(18,4)), CAST(NULL AS STRING), CAST(NULL AS BINARY), " +
          "CAST(NULL AS DATE), CAST(NULL AS TIMESTAMP))"
      val snapshot = captureWrite("prim_types") {
        spark.sql(s"INSERT INTO $catalog.$ns.prim_types VALUES $literals")
      }
      assertExactlyOneCommit(snapshot)
      checkAnswer(
        spark.sql(s"SELECT * FROM $catalog.$ns.prim_types"),
        spark.sql(s"SELECT * FROM VALUES $literals"))
    }
  }

  test("AppendData writes NULLs in every column") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(warehouseDir, "nulls_all", partitionSpec = "")
      val snapshot = captureWrite("nulls_all") {
        spark.sql(s"""INSERT INTO $catalog.$ns.nulls_all VALUES
          (CAST(NULL AS INT), CAST(NULL AS STRING), CAST(NULL AS DOUBLE)),
          (1, NULL, 1.0),
          (2, 'r', NULL)""")
      }
      assertExactlyOneCommit(snapshot)
      checkAnswer(
        spark.sql(s"SELECT id, region, amount FROM $catalog.$ns.nulls_all"),
        Seq(Row(null, null, null), Row(1, null, 1.0), Row(2, "r", null)))
    }
  }

  test("AppendData round-trips STRUCT, ARRAY, and MAP columns") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { _ =>
      spark.sql(s"""
        CREATE TABLE $catalog.$ns.nested_types (
          id INT,
          s STRUCT<a: INT, b: STRING>,
          arr ARRAY<INT>,
          m MAP<STRING, INT>
        ) USING iceberg""")
      val snapshot = captureWrite("nested_types") {
        spark.sql(s"""INSERT INTO $catalog.$ns.nested_types VALUES
          (1, named_struct('a', 10, 'b', 'x'), array(1, 2, 3), map('k1', 1, 'k2', 2)),
          (2, named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS STRING)),
              CAST(NULL AS ARRAY<INT>), CAST(NULL AS MAP<STRING, INT>))""")
      }
      assertExactlyOneCommit(snapshot)
      checkAnswer(
        spark.sql(s"SELECT id, s.a, s.b, arr, m FROM $catalog.$ns.nested_types"),
        Seq(
          Row(1, 10, "x", Seq(1, 2, 3), Map("k1" -> 1, "k2" -> 2)),
          Row(2, null, null, null, null)))
    }
  }

  test("CoW MERGE with only a matched DELETE leg routes through two-op") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "merge_del",
        partitionSpec = "",
        properties = Some("'write.merge.mode'='copy-on-write'"))
      coalesceInsert("merge_del", Seq((1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)))

      val snapshot = captureWrite("merge_del") {
        spark.sql(s"""
          |MERGE INTO $catalog.$ns.merge_del t
          |USING (SELECT 2 AS id UNION ALL SELECT 3 AS id) s
          |ON t.id = s.id
          |WHEN MATCHED THEN DELETE
          |""".stripMargin)
      }
      assertExactlyOneCommit(snapshot)
      assertRows("merge_del", expectedIds = Seq(1))
    }
  }

  test("CoW MERGE with a NOT MATCHED BY SOURCE leg routes through two-op") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    // Iceberg 1.5.x (the Spark 3.4 pairing) rejects the clause in its extensions parser.
    assume(isSpark35Plus, "NOT MATCHED BY SOURCE needs Iceberg 1.8+")
    withIcebergCatalog { warehouseDir =>
      createTable(
        warehouseDir,
        "merge_nmbs",
        partitionSpec = "",
        properties = Some("'write.merge.mode'='copy-on-write'"))
      coalesceInsert("merge_nmbs", Seq((1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)))

      val snapshot = captureWrite("merge_nmbs") {
        spark.sql(s"""
          |MERGE INTO $catalog.$ns.merge_nmbs t
          |USING (SELECT 2 AS id) s
          |ON t.id = s.id
          |WHEN MATCHED THEN UPDATE SET t.amount = t.amount + 100
          |WHEN NOT MATCHED BY SOURCE THEN DELETE
          |""".stripMargin)
      }
      assertExactlyOneCommit(snapshot)
      assertRows("merge_nmbs", expectedIds = Seq(2))
      checkAnswer(spark.sql(s"SELECT amount FROM $catalog.$ns.merge_nmbs"), Seq(Row(102.0)))
    }
  }

  test("AppendData to a bucket- and truncate-partitioned table routes through two-op") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { _ =>
      spark.sql(s"""
        CREATE TABLE $catalog.$ns.bucketed (
          id INT, region STRING, amount DOUBLE
        ) USING iceberg PARTITIONED BY (bucket(4, id), truncate(2, region))""")
      val snapshot = captureWrite("bucketed") {
        spark.sql(
          s"INSERT INTO $catalog.$ns.bucketed VALUES " +
            "(1, 'aa', 1.0), (2, 'ab', 2.0), (3, 'bb', 3.0), (4, 'bc', 4.0), (5, 'cc', 5.0)")
      }
      assertExactlyOneCommit(snapshot)
      assertRows("bucketed", expectedIds = Seq(1, 2, 3, 4, 5))
    }
  }

  test("AppendData to a days()-partitioned table routes each day to its own partition") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { _ =>
      spark.sql(s"""
        CREATE TABLE $catalog.$ns.by_day (
          id INT, ts TIMESTAMP, amount DOUBLE
        ) USING iceberg PARTITIONED BY (days(ts))""")
      val snapshot = captureWrite("by_day") {
        spark.sql(
          s"INSERT INTO $catalog.$ns.by_day VALUES " +
            "(1, TIMESTAMP'2024-01-15 10:00:00', 1.0), " +
            "(2, TIMESTAMP'2024-01-15 10:00:00', 2.0), " +
            "(3, TIMESTAMP'2024-02-15 10:00:00', 3.0)")
      }
      assertExactlyOneCommit(snapshot)
      val partitions = spark
        .sql(s"SELECT count(*) FROM $catalog.$ns.by_day.partitions")
        .collect()
        .head
        .getLong(0)
      assert(partitions == 2, s"expected 2 day partitions, got $partitions")
      val ids = spark
        .sql(s"SELECT id FROM $catalog.$ns.by_day ORDER BY id")
        .collect()
        .map(_.getInt(0))
        .toSeq
      assert(ids == Seq(1, 2, 3), s"expected Seq(1, 2, 3), got $ids")
    }
  }

  // On Spark 3.5+ the staged CTAS/RTAS operators run their inner append as its own
  // `AppendData` QueryExecution, which IcebergWriteStrategy intercepts like any other append.
  // Spark 3.4 writes inline inside the exec (no re-planning), so nothing is intercepted there.
  test("CTAS and RTAS write through the split operators on Spark 3.5+") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withIcebergCatalog { _ =>
      val session = spark
      import session.implicits._
      (1 to 5)
        .map(i => (i, s"r${i % 2}", i.toDouble))
        .toDF("id", "region", "amount")
        .createOrReplaceTempView("ctas_src")

      def assertSplitUsage(plans: Seq[SparkPlan], statement: String): Unit = {
        val (commits, writes) = collectIcebergWriteOps(plans)
        if (isSpark35Plus) {
          assert(
            commits.nonEmpty && writes.nonEmpty,
            s"expected the $statement inner append to plan through the split operators: $plans")
        } else {
          assert(
            commits.isEmpty && writes.isEmpty,
            s"expected the $statement write to stay inside Spark's staged exec: $plans")
        }
      }

      val ctasPlans = capturePlans {
        spark.sql(s"CREATE TABLE $catalog.$ns.ctas_tgt USING iceberg AS SELECT * FROM ctas_src")
      }
      assertSplitUsage(ctasPlans, "CTAS")
      assert(countSnapshots("ctas_tgt") == 1L, "CTAS must land exactly one snapshot")
      assertRows("ctas_tgt", expectedIds = Seq(1, 2, 3, 4, 5))

      val rtasPlans = capturePlans {
        (1 to 2)
          .map(i => (i, s"r$i", i.toDouble))
          .toDF("id", "region", "amount")
          .writeTo(s"$catalog.$ns.ctas_tgt")
          .using("iceberg")
          .createOrReplace()
      }
      assertSplitUsage(rtasPlans, "RTAS")
      assertRows("ctas_tgt", expectedIds = Seq(1, 2))
    }
  }

  private val catalog = "cat"
  private val ns = "db"

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

  private def coalesceInsert(tableName: String, rows: Seq[(Int, String, Double)]): Unit = {
    val session = spark
    import session.implicits._
    rows
      .toDF("id", "region", "amount")
      .coalesce(1)
      .writeTo(s"$catalog.$ns.$tableName")
      .append()
  }

  private def capturePlans(action: => Unit): Seq[SparkPlan] = {
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
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.listenerManager.unregister(listener)
    }
    captured.toSeq
  }

  private def captureWrite(tableName: String)(action: => Unit): WriteSnapshot = {
    val before = countSnapshots(tableName)
    val plans = capturePlans(action)
    WriteSnapshot(countSnapshots(tableName) - before, plans)
  }

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

  /** Native acceleration shared assumption -- currently just the Iceberg-on-classpath check. */
  private def assumeNativeAcceleration(): Unit = {
    assume(icebergAvailable, "Iceberg not available in classpath")
  }

  /**
   * Flip [[CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED]] for the duration of `action`.
   *
   * We also enable [[CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED]] (default off) so VALUES-
   * driven INSERTs have a Comet-native upstream -- `requiresNativeChildren = true` would
   * otherwise short-circuit the conversion to `CometIcebergWriteExec` because Spark emits a bare
   * `LocalTableScanExec` for inline `VALUES`. Using `sessionState.conf.setConfString` directly
   * (rather than `withSQLConf`) keeps the override visible to the columnar rule across some Spark
   * version / session-state combinations where `withSQLConf` loses the override before the rule
   * fires.
   */
  private def withNativeEnabled[T](action: => T): T = {
    val session = spark
    session.sessionState.conf
      .setConfString(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key, "true")
    session.sessionState.conf
      .setConfString(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key, "true")
    try action
    finally {
      session.sessionState.conf.unsetConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key)
      session.sessionState.conf.unsetConf(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key)
    }
  }

  /**
   * Strongest invariant we can pin on a single native write: commit-count advanced by exactly one
   * (same as the JVM-path assertion -- AQE re-planning never duplicates commits) AND at least one
   * [[CometIcebergWriteExec]] appears in some captured plan AND the resulting row set matches.
   */
  private def assertNativeWriteEngages(tableName: String, expectedIds: Seq[Int])(
      action: => Unit): Unit = {
    val snapshot = withNativeEnabled { captureWrite(tableName)(action) }
    assert(
      snapshot.snapshotDelta == 1L,
      s"expected 1 commit via native path, got ${snapshot.snapshotDelta}. Plans:\n" +
        snapshot.plans.mkString("\n--\n"))
    val nativeExecs = snapshot.plans.flatMap { p =>
      collectWithSubqueries(p) { case e: CometIcebergWriteExec => e }
    }
    assert(
      nativeExecs.nonEmpty,
      "expected >= 1 CometIcebergWriteExec in captured plans, got 0. Plans:\n" +
        snapshot.plans.mkString("\n--\n"))
    assertRows(tableName, expectedIds)
  }

  /**
   * For writes that *should* fall back even when the native conf is on (e.g. CoW MERGE): exactly
   * one commit, no `CometIcebergWriteExec`, but the JVM two-op pair still present and rows
   * correct. Catches regressions where a trigger silently stops firing and we accidentally run
   * native for an unsupported case.
   */
  private def assertNativeWriteDoesNotEngage(tableName: String, expectedIds: Seq[Int])(
      action: => Unit): Unit = {
    val snapshot = withNativeEnabled { captureWrite(tableName)(action) }
    assertExactlyOneCommit(snapshot)
    val nativeExecs = snapshot.plans.flatMap { p =>
      collectWithSubqueries(p) { case e: CometIcebergWriteExec => e }
    }
    assert(
      nativeExecs.isEmpty,
      s"expected NO CometIcebergWriteExec, got ${nativeExecs.size}. Plans:\n" +
        snapshot.plans.mkString("\n--\n"))
    assertRows(tableName, expectedIds)
  }

  private def exceptionChain(t: Throwable): Seq[Throwable] = {
    val chain = mutable.Buffer.empty[Throwable]
    var current = t
    while (current != null && !chain.contains(current)) {
      chain += current
      current = current.getCause
    }
    chain.toSeq
  }

}

/**
 * Blocks the DELETE's write job between its scan-snapshot pin and its commit so the test can
 * inject a conflicting commit. Top-level so the UDF closure doesn't capture the suite.
 */
private object ConflictGate {
  @volatile private var scanStarted = new CountDownLatch(1)
  @volatile private var writeReleased = new CountDownLatch(1)

  def reset(): Unit = {
    scanStarted = new CountDownLatch(1)
    writeReleased = new CountDownLatch(1)
  }

  def enter(): Unit = {
    scanStarted.countDown()
    if (!writeReleased.await(2, TimeUnit.MINUTES)) {
      throw new IllegalStateException("ConflictGate was never released")
    }
  }

  def awaitScanStarted(timeout: Long, unit: TimeUnit): Boolean = scanStarted.await(timeout, unit)

  def releaseWrite(): Unit = writeReleased.countDown()
}
