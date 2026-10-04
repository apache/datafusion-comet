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

package org.apache.comet.exec

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{CometListenerBusUtils, SparkConf, TaskContext}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows
import org.apache.spark.sql.comet.{CometMergeRowsExec, CometMetricNode}
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark42Plus
import org.apache.comet.shims.{MergeRowsMetricsShim, ShimCometMergeRows}

/**
 * Spark 4.1+ stock V2 writers build MergeSummary by locating the concrete Spark MergeRowsExec.
 * Native MergeRows is therefore kept on Spark when the enclosing writer remains on Spark.
 */
class CometMergeRowsSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val catalog = "generic_rowlevel"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(s"spark.sql.catalog.$catalog", classOf[InMemoryRowLevelOperationTableCatalog].getName)
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.shuffle.partitions", "4")
      .setMaster("local[5,2]")
  }

  test("native MergeRows semantic counters match Spark for Keep, Discard and Split") {
    withTempPath { path =>
      spark
        .range(1, 9)
        .selectExpr(
          "CAST(id AS INT) AS id",
          "id <= 6 AS source_present",
          "id != 6 AS target_present")
        .repartition(2)
        .write
        .parquet(path.getCanonicalPath)
      val input = spark.read.parquet(path.getCanonicalPath).queryExecution.analyzed
      val id = input.output(0)
      def is(value: Int) = EqualTo(id, Literal(value))
      def plan = MergeRows(
        input.output(1),
        input.output(2),
        Seq(
          MergeRows.Keep(MergeRows.Copy, is(1), Seq(id)),
          MergeRows.Discard(is(2)),
          MergeRows.Keep(MergeRows.Update, is(3), Seq(id)),
          MergeRows.Split(is(4), Seq(id), Seq(id)),
          MergeRows.Keep(MergeRows.Delete, is(5), Seq(id))),
        Seq(MergeRows.Keep(MergeRows.Insert, Literal(true), Seq(id))),
        Seq(MergeRows.Keep(MergeRows.Update, is(7), Seq(id)), MergeRows.Discard(is(8))),
        checkCardinality = false,
        output = Seq(id),
        child = input)
      val expected = Map(
        "numTargetRowsCopied" -> 1L,
        "numTargetRowsDeleted" -> 3L,
        "numTargetRowsUpdated" -> 3L,
        "numTargetRowsInserted" -> 1L,
        "numTargetRowsMatchedUpdated" -> 2L,
        "numTargetRowsMatchedDeleted" -> 2L,
        "numTargetRowsNotMatchedBySourceUpdated" -> 1L,
        "numTargetRowsNotMatchedBySourceDeleted" -> 1L)

      Seq(true, false).foreach { adaptiveEnabled =>
        withSQLConf("spark.sql.adaptive.enabled" -> adaptiveEnabled.toString) {
          val baseline = withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            val df = datasetOfRows(spark, ShimCometMergeRows.withNativeMergeSummary(plan))
            val rows = df.collect().map(_.toString).sorted.toSeq
            val merge = find(df.queryExecution.executedPlan)(_.isInstanceOf[MergeRowsExec]).get
            assert(expected.map { case (key, _) =>
              key -> MergeRowsMetricsShim.value(merge.metrics(key))
            } == expected)
            rows
          }
          withSQLConf(CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true") {
            val df = datasetOfRows(spark, ShimCometMergeRows.withNativeMergeSummary(plan))
            assert(df.collect().map(_.toString).sorted.toSeq == baseline)
            val merge = find(df.queryExecution.executedPlan)(_.isInstanceOf[CometMergeRowsExec])
              .getOrElse(
                fail(s"native MergeRows did not engage: ${df.queryExecution.executedPlan}"))
            assert(expected.map { case (key, _) =>
              key -> MergeRowsMetricsShim.value(merge.metrics(key))
            } == expected)
          }
        }
      }
    }
  }

  test("Spark 4.2 semantic metrics retain only successful task attempt updates") {
    assume(isSpark42Plus, "last-attempt MERGE metrics require Spark 4.2")
    val metrics = MergeRowsMetricsShim.metrics(spark.sparkContext)
    val node = CometMetricNode(metrics, Seq.empty)
    val names = metrics.keys.toSeq
    def runAttempt(): Unit = {
      val attempts = spark.sparkContext
        .parallelize(0 until 4, 4)
        .mapPartitions { input =>
          val count = input.size.toLong
          val attempt = TaskContext.get().attemptNumber()
          if (attempt == 0) {
            names.foreach(node.set(_, 100L))
            throw new IllegalStateException("injected failed MERGE metric attempt")
          }
          // JNI publishes cumulative snapshots, so repeated sets must not add the old value.
          names.foreach(node.set(_, count + 10L))
          names.foreach(node.set(_, count))
          Iterator(attempt)
        }
        .collect()
      assert(attempts.toSeq == Seq.fill(4)(1))
      metrics.foreach { case (name, metric) =>
        assert(MergeRowsMetricsShim.value(metric) == 4L, s"incorrect last-attempt $name")
      }
    }
    runAttempt()
    // AQE can replan an operator into a newer RDD. Select that execution's contributions.
    runAttempt()
  }

  Seq(true, false).foreach { adaptiveEnabled =>
    test(s"stock V2 writer retains Spark MergeRowsExec for MergeSummary, AQE=$adaptiveEnabled") {
      withSQLConf("spark.sql.adaptive.enabled" -> adaptiveEnabled.toString) {
        val target = s"$catalog.default.rowlevel_target"
        val source = s"$catalog.default.rowlevel_source"

        sql(s"DROP TABLE IF EXISTS $target")
        sql(s"DROP TABLE IF EXISTS $source")
        sql(s"CREATE TABLE $target (id INT, amount DOUBLE) USING parquet")
        sql(s"CREATE TABLE $source (id INT, amount DOUBLE) USING parquet")
        sql(s"INSERT INTO $target VALUES (1, 10.0), (2, 20.0)")
        sql(s"INSERT INTO $source VALUES (2, 200.0), (3, 300.0)")

        val captured = ArrayBuffer[QueryExecution]()
        val listener = new QueryExecutionListener {
          override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
            captured += qe
          override def onFailure(
              funcName: String,
              qe: QueryExecution,
              exception: Exception): Unit =
            ()
        }

        spark.listenerManager.register(listener)
        try {
          withSQLConf(CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true") {
            sql(s"""MERGE INTO $target t USING $source s ON t.id = s.id
               |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
               |WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)
               |""".stripMargin)
          }
          CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)

          val executedPlans = captured.map(_.executedPlan)
          val sparkMergeRows = executedPlans.exists(plan =>
            collectWithSubqueries(plan) { case e: MergeRowsExec =>
              e
            }.nonEmpty)
          val cometMergeRows = executedPlans.exists(plan =>
            collectWithSubqueries(plan) { case e: CometMergeRowsExec =>
              e
            }.nonEmpty)

          assert(
            sparkMergeRows,
            s"stock V2 writer must retain MergeRowsExec. Plans: ${executedPlans.mkString("\n")}")
          assert(
            !cometMergeRows,
            "CometMergeRowsExec must not reach a stock Spark V2 writer that requires MergeRowsExec")

          val rows =
            sql(s"SELECT id, amount FROM $target ORDER BY id").collect().map(_.toString).toSeq
          assert(rows == Seq("[1,10.0]", "[2,200.0]", "[3,300.0]"))
        } finally {
          spark.listenerManager.unregister(listener)
        }
      }
    }
  }
}
