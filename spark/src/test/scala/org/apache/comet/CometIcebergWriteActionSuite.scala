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
import org.apache.spark.sql.comet.{IcebergCommitExec, IcebergWriteExec}
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometSparkSessionExtensions.isSpark41Plus

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
