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

package org.apache.spark.sql.comet

import java.util.concurrent.{FutureTask, TimeUnit}

import org.apache.spark.sql.{CometTestBase, Observation, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.execution.{CollectMetricsExec, MapPartitionsExec, QueryExecution, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus

class CometCollectMetricsSuite extends CometTestBase {

  import testImplicits._

  private def observedMetrics(observation: Observation): Map[String, Any] = {
    // A missing observation in the executed plan must fail instead of blocking in get forever.
    // Observation.get is available on every supported Spark version, including Spark 3.4.
    val result = new FutureTask[Map[String, Any]](() => observation.get)
    val reader = new Thread(result, "comet-observation-reader")
    reader.setDaemon(true)
    reader.start()
    try {
      result.get(10, TimeUnit.SECONDS)
    } finally {
      result.cancel(true)
    }
  }

  private def withCometEnabled[T](enabled: Boolean)(body: => T): T = {
    // Spark 3.x withSQLConf returns Unit, so capture the result explicitly.
    var result: Option[T] = None
    withSQLConf(CometConf.COMET_ENABLED.key -> enabled.toString) {
      result = Some(body)
    }
    result.get
  }

  for (adaptive <- Seq(false, true)) {
    test(s"observation preserves counters and native consumers, AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        CometConf.COMET_SHUFFLE_MODE.key -> "native") {
        val data = (0 until 12).map(i => (i, i % 3, if (i % 4 == 0) None else Some(i)))
        withParquetTable(data, "observed_input") {
          def run(comet: Boolean): (Seq[Row], Map[String, Any], SparkPlan) = {
            withCometEnabled(comet) {
              val observation = Observation("source_counts")
              val result = spark
                .table("observed_input")
                .observe(
                  observation,
                  count(lit(1)).as("total_rows"),
                  count(when($"_2" === 1, 1)).as("selected_rows"),
                  sum($"_3").as("total_value"))
                .filter($"_1" >= 4)
                .select($"_2".as("k"), ($"_1" + 1).as("v"))
                .groupBy("k")
                .agg(sum("v").as("total"))
              val rows = result.collect().toSeq
              (rows, observedMetrics(observation), result.queryExecution.executedPlan)
            }
          }

          val (sparkRows, sparkMetrics, _) = run(comet = false)
          val (cometRows, cometMetrics, plan) = run(comet = true)
          assert(cometRows.toSet == sparkRows.toSet)
          assert(cometMetrics == sparkMetrics)
          assert(
            cometMetrics == Map("total_rows" -> 12L, "selected_rows" -> 4L, "total_value" -> 54L))
          assert(collect(plan) { case _: CometCollectMetricsExec => true }.size == 1)
          assert(collect(plan) {
            case project: CometProjectExec if collect(project) {
                  case _: CometCollectMetricsExec => true
                }.nonEmpty =>
              project
          }.nonEmpty)
          assert(collect(plan) { case _: CometHashAggregateExec => true }.nonEmpty)
          if (adaptive) {
            assert(plan.asInstanceOf[AdaptiveSparkPlanExec].isFinalPlan)
          }
        }
      }
    }
  }

  test("observation preserves reader-owned structs across batches") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> "2") {
      val values = (0 until 10).map(i => s"(named_struct('v', $i))").mkString(",")
      def run(comet: Boolean): (Seq[Long], Map[String, Any], SparkPlan) = {
        withCometEnabled(comet) {
          val observation = Observation("struct_batches")
          val result = sql(s"SELECT * FROM VALUES $values AS t(a)")
            .observe(observation, count(lit(1)).as("rows"))
            .selectExpr("xxhash64(a)")
            .repartition(2)
          val hashes = result.collect().map(_.getLong(0)).sorted.toSeq
          (hashes, observedMetrics(observation), result.queryExecution.executedPlan)
        }
      }

      val (expected, expectedMetrics, _) = run(comet = false)
      val (actual, actualMetrics, plan) = run(comet = true)
      assert(actual == expected)
      assert(actual.size == 10)
      assert(actualMetrics == expectedMetrics)
      assert(actualMetrics == Map("rows" -> 10L))
      val scan = collect(plan) { case op: CometLocalTableScanExec => op }.head
      assert(collect(plan) { case _: CometCollectMetricsExec => true }.size == 1)
      assert(collect(plan) { case _: CometProjectExec => true }.nonEmpty)
      // Spark's row conversion may close individual columns instead of the batch.
      val sourceValues = scan
        .executeColumnar()
        .mapPartitions { batches =>
          batches.flatMap { batch =>
            val values = (0 until batch.numRows()).map(batch.column(0).getStruct(_).getInt(0))
            batch.column(0).close()
            values
          }
        }
        .collect()
      assert(sourceValues.sorted.toSeq == (0 until 10))
    }
  }

  test("observation collects structs containing calendar intervals") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation",
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true") {
      def run(comet: Boolean): (Map[String, List[Row]], SparkPlan) = {
        withCometEnabled(comet) {
          val observation = Observation("interval_structs")
          val result = sql("""
              |SELECT * FROM VALUES
              |  (0, named_struct('i', make_interval(0, 1), 'id', 0)),
              |  (1, named_struct('i', make_interval(0, 1), 'id', 1)) AS t(id, v)
              |""".stripMargin)
            .observe(observation, collect_list($"v").as("list"), collect_set($"v").as("set"))
            .select("id")
            .repartition(2)
          assert(result.collect().map(_.getInt(0)).sorted.toSeq == Seq(0, 1))
          val metrics = observedMetrics(observation).map { case (name, values) =>
            name -> values.asInstanceOf[scala.collection.Seq[Row]].sortBy(_.getInt(1)).toList
          }
          (metrics, result.queryExecution.executedPlan)
        }
      }

      val (expected, _) = run(comet = false)
      val (actual, plan) = run(comet = true)
      assert(actual == expected)
      assert(actual("list").size == 2)
      assert(actual("set") == actual("list"))
      assert(collect(plan) { case _: CometLocalTableScanExec => true }.nonEmpty)
      assert(collect(plan) { case _: CometCollectMetricsExec => true }.size == 1)
    }
  }

  test("observation publishes collect_list state without an executor merge") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempPath { path =>
        spark.range(0, 100, 1, 1).write.parquet(path.getCanonicalPath)

        def run(comet: Boolean): (Seq[Long], Long, SparkPlan) = {
          withCometEnabled(comet) {
            val executorMergedValues = spark.sparkContext.longAccumulator
            val values = getColumnFromExpression(
              new MergeTrackingCollectList(UnresolvedAttribute("id"), 0, 0, executorMergedValues)
                .toAggregateExpression())
            val observation = Observation("collected_values")
            val result = spark.read
              .parquet(path.getCanonicalPath)
              .observe(observation, values.as("values"))
              .repartition(1)
            assert(result.collect().map(_.getLong(0)).sorted.toSeq == (0L until 100L))
            val observed = observedMetrics(observation)("values")
              .asInstanceOf[scala.collection.Seq[Long]]
              .sorted
              .toList
            (observed, executorMergedValues.value, result.queryExecution.executedPlan)
          }
        }

        val (sparkValues, sparkMergedValues, _) = run(comet = false)
        val (cometValues, cometMergedValues, plan) = run(comet = true)
        assert(collect(plan) { case _: CometCollectMetricsExec => true }.size == 1)
        assert(sparkValues == (0L until 100L))
        assert(cometValues == sparkValues)
        assert(sparkMergedValues == 0L)
        assert(cometMergedValues == sparkMergedValues)
      }
    }
  }

  test("empty observation publishes aggregate defaults") {
    withParquetTable(Seq((1, 2), (3, 4)), "observed_input") {
      val observation = Observation("empty")
      val result = spark
        .table("observed_input")
        .filter($"_1" < 0)
        .observe(observation, count(lit(1)).as("rows"), sum($"_2").as("total"))
        .repartition(1)
        .select($"_1" + 1)
      assert(result.collect().isEmpty)
      assert(observedMetrics(observation) == Map("rows" -> 0L, "total" -> null))
      assert(collect(result.queryExecution.executedPlan) { case _: CometCollectMetricsExec =>
        true
      }.nonEmpty)
    }
  }

  test("named observation preserves Spark producer reexecution and reset behavior") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable(Seq((1, 2), (3, 4)), "observed_input") {
        def run(comet: Boolean): Seq[Row] = {
          withCometEnabled(comet) {
            val result = spark
              .table("observed_input")
              .observe("repeated", count(lit(1)).as("rows"), sum($"_2").as("total"))
              .repartition(1)
            val plan = result.queryExecution.executedPlan
            val collector = collect(plan) { case op: CollectMetricsExec => op }.head
            if (comet) {
              assert(collector.isInstanceOf[CometCollectMetricsExec])
            }
            // Reexecute the actual producer: collecting the whole query would reuse its shuffle.
            val metrics = (0 until 2).map { _ =>
              assert(collector.executeCollect().map(_.getInt(0)).toSet == Set(1, 3))
              CollectMetricsExec.collect(plan)("repeated")
            }
            plan.resetMetrics()
            metrics :+ CollectMetricsExec.collect(plan)("repeated")
          }
        }
        val expected = run(comet = false)
        assert(run(comet = true) == expected)
      }
    }
  }

  for (fraction <- Seq(0.0, 1.0)) {
    test(s"observation below replacement sampling preserves row demand, fraction=$fraction") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "false",
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
        withTempPath { path =>
          spark.range(0, 100, 1, 1).write.parquet(path.getCanonicalPath)

          def run(comet: Boolean): (Seq[Long], Map[String, Any], SparkPlan) = {
            withCometEnabled(comet) {
              val observation = Observation("sampled_input")
              val result = spark.read
                .parquet(path.getCanonicalPath)
                .observe(observation, count(lit(1)).as("rows"))
                .select(($"id" + 1).as("k"))
                .sample(withReplacement = true, fraction = fraction, seed = 42L)
                .repartition(2)
              val rows = result.collect().map(_.getLong(0)).sorted.toSeq
              (rows, observedMetrics(observation), result.queryExecution.executedPlan)
            }
          }

          val (sparkRows, sparkMetrics, _) = run(comet = false)
          val (cometRows, cometMetrics, plan) = run(comet = true)
          assert(collect(plan) { case _: CometNativeScanExec => true }.nonEmpty)
          assert(cometRows == sparkRows)
          assert(sparkMetrics == Map("rows" -> (if (fraction == 0.0) 0L else 100L)))
          assert(cometMetrics == sparkMetrics)
          assert(
            collect(plan) { case _: CometCollectMetricsExec => true }.nonEmpty ==
              (fraction > 0.0))
        }
      }
    }
  }

  test("observation below a limit preserves Spark row demand") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable((0 until 100).map(i => (i, i)), "observed_input") {
        def run(comet: Boolean): (Map[String, Any], SparkPlan) = {
          withCometEnabled(comet) {
            val observation = Observation("limited")
            val result = spark
              .table("observed_input")
              .observe(observation, count(lit(1)).as("rows"))
              .limit(1)
            assert(result.collect().length == 1)
            (observedMetrics(observation), result.queryExecution.executedPlan)
          }
        }
        val (expected, _) = run(comet = false)
        val (actual, plan) = run(comet = true)
        assert(actual == expected)
        assert(actual == Map("rows" -> 1L))
        assert(collect(plan) { case _: CometCollectMetricsExec => true }.isEmpty)
        assert(collect(plan) { case _: CollectMetricsExec => true }.nonEmpty)
      }
    }
  }

  test("observation below a window group limit preserves Spark row demand") {
    assume(isSpark35Plus, "WindowGroupLimit was added in Spark 3.5")
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      "spark.sql.optimizer.windowGroupLimitThreshold" -> "1000",
      "spark.sql.execution.topKSortFallbackThreshold" -> "0",
      CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true") {
      def run(comet: Boolean): (Map[String, Any], SparkPlan) = {
        withCometEnabled(comet) {
          val observation = Observation("window_limited")
          // Ordered single-partition input avoids a sort consuming the complete observation.
          val result = spark
            .range(0, 100, 1, 1)
            .filter($"id" % 2 === 0)
            .observe(observation, count(lit(1)).as("rows"))
            .withColumn("rn", row_number().over(Window.orderBy("id")))
            .filter($"rn" <= 1)
            .select("id")
          assert(result.collect().toSeq == Seq(Row(0L)))
          (observedMetrics(observation), result.queryExecution.executedPlan)
        }
      }
      val (expected, _) = run(comet = false)
      val (actual, plan) = run(comet = true)
      assert(expected("rows").asInstanceOf[Long] < 50L)
      assert(actual == expected)
      assert(collect(plan) { case _: CometCollectMetricsExec => true }.isEmpty)
      assert(collect(plan) { case op if op.nodeName == "WindowGroupLimit" => op }.nonEmpty)
    }
  }

  test("observation below a sort merge join preserves Spark row demand") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      CometConf.COMET_FORCE_SHJ.key -> "false",
      CometConf.COMET_EXEC_RANGE_ENABLED.key -> "true") {
      def run(comet: Boolean): (Map[String, Any], SparkPlan) = {
        withCometEnabled(comet) {
          val observation = Observation("join_limited")
          val left = spark
            .range(0, 100, 1, 1)
            .filter($"id" % 2 === 0)
            .observe(observation, count(lit(1)).as("rows"))
          val right = spark.range(0, 1, 1, 1)
          val result = left.join(right, Seq("id"))
          assert(result.collect().toSeq == Seq(Row(0L)))
          (observedMetrics(observation), result.queryExecution.executedPlan)
        }
      }
      val (expected, sparkPlan) = run(comet = false)
      val (actual, cometPlan) = run(comet = true)
      assert(expected("rows").asInstanceOf[Long] < 50L)
      assert(actual == expected)
      for (plan <- Seq(sparkPlan, cometPlan)) {
        assert(collect(plan) { case _: SortMergeJoinExec => true }.nonEmpty)
        assert(collect(plan) { case _: CometCollectMetricsExec => true }.isEmpty)
        assert(collect(plan) { case _: CollectMetricsExec => true }.nonEmpty)
      }
    }
  }

  for (sortBeforeTake <- Seq(false, true)) {
    test(s"observation below mapPartitions preserves row demand, sort=$sortBeforeTake") {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        withTempPath { path =>
          spark.range(0, 100, 1, 1).write.parquet(path.getCanonicalPath)

          def run(comet: Boolean): (Seq[Long], Map[String, Any], SparkPlan) = {
            withCometEnabled(comet) {
              val observation = Observation("iterator_rows")
              val observed = spark.read
                .parquet(path.getCanonicalPath)
                .observe(observation, count(lit(1)).as("rows"))
              // A local sort drains the observation before the iterator takes its first row.
              val input =
                if (sortBeforeTake) observed.sortWithinPartitions($"id".desc) else observed
              val result = input.as[Long].mapPartitions(_.take(1)).repartition(2)
              val rows = result.collect().toSeq
              (rows, observedMetrics(observation), result.queryExecution.executedPlan)
            }
          }

          val (sparkRows, sparkMetrics, sparkPlan) = run(comet = false)
          val (cometRows, cometMetrics, cometPlan) = run(comet = true)
          assert(sparkRows == Seq(if (sortBeforeTake) 99L else 0L))
          assert(sparkMetrics == Map("rows" -> (if (sortBeforeTake) 100L else 1L)))
          assert(cometRows == sparkRows)
          assert(cometMetrics == sparkMetrics)
          for (plan <- Seq(sparkPlan, cometPlan)) {
            assert(collect(plan) { case _: MapPartitionsExec => true }.nonEmpty)
            assert(collect(plan) {
              case node if node.nodeName.contains("Limit") || node.nodeName.contains("Join") =>
                node
            }.isEmpty)
            assert(collect(plan) { case _: CollectMetricsExec => true }.size == 1)
          }
          assert(collect(cometPlan) { case _: CometNativeScanExec => true }.nonEmpty)
          assert(collect(cometPlan) { case _: CometCollectMetricsExec => true }.isEmpty)
        }
      }
    }
  }

  for ((consumeRows, sortBeforeConsume) <- Seq((1, false), (0, true))) {
    test(s"public foreachPartition preserves observation row demand, rows=$consumeRows") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        withTempPath { path =>
          spark.range(0, 100, 1, 1).write.parquet(path.getCanonicalPath)

          def run(comet: Boolean): (Map[String, Any], SparkPlan) = {
            withCometEnabled(comet) {
              val observation = Observation("foreach_rows")
              val observed = spark.read
                .parquet(path.getCanonicalPath)
                .observe(observation, count(lit(1)).as("rows"))
              // Spark's code-generated sort does not consume input until its output is requested.
              val result =
                if (sortBeforeConsume) observed.sortWithinPartitions($"id".desc) else observed
              val consumed = spark.sparkContext.longAccumulator
              @volatile var executedPlan: Option[SparkPlan] = None
              // foreachPartition executes its own deserializing QueryExecution.
              val listener = new QueryExecutionListener {
                override def onSuccess(
                    funcName: String,
                    qe: QueryExecution,
                    durationNs: Long): Unit = {
                  if (funcName == "foreachPartition") executedPlan = Some(qe.executedPlan)
                }

                override def onFailure(
                    funcName: String,
                    qe: QueryExecution,
                    exception: Exception): Unit = ()
              }
              spark.listenerManager.register(listener)
              try {
                result.foreachPartition { rows: Iterator[Row] =>
                  rows.take(consumeRows).foreach(_ => consumed.add(1L))
                }
                spark.sparkContext.listenerBus.waitUntilEmpty()
                assert(consumed.value == consumeRows.toLong)
                assert(executedPlan.nonEmpty)
                (observedMetrics(observation), executedPlan.get)
              } finally {
                spark.listenerManager.unregister(listener)
              }
            }
          }

          val (expected, sparkPlan) = run(comet = false)
          val (actual, cometPlan) = run(comet = true)
          assert(expected == Map("rows" -> consumeRows.toLong))
          assert(actual == expected)
          for (plan <- Seq(sparkPlan, cometPlan)) {
            assert(collect(plan) { case _: MapPartitionsExec => true }.isEmpty)
            if (sortBeforeConsume) {
              assert(collect(plan) { case _: SortExec | _: CometSortExec => true }.nonEmpty)
            }
            assert(collect(plan) {
              case node if node.nodeName.contains("Limit") || node.nodeName.contains("Join") =>
                node
            }.isEmpty)
          }
          assert(collect(cometPlan) { case _: CometNativeScanExec => true }.nonEmpty)
        }
      }
    }
  }

  test("observation preserves row demand in QueryExecution.toRdd.take") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempPath { path =>
        spark.range(0, 100, 1, 1).write.parquet(path.getCanonicalPath)

        def run(comet: Boolean): (Long, SparkPlan) = {
          withCometEnabled(comet) {
            val result = spark.read
              .parquet(path.getCanonicalPath)
              .observe("rdd_rows", count(lit(1)).as("rows"))
              .filter($"id" >= 0)
              .select(($"id" + 1).as("next_id"))
            val execution = result.queryExecution
            assert(execution.toRdd.take(1).map(_.getLong(0)).toSeq == Seq(1L))
            val plan = execution.executedPlan
            // RDD actions do not publish Observation listener results.
            (CollectMetricsExec.collect(plan)("rdd_rows").getLong(0), plan)
          }
        }

        val (expected, sparkPlan) = run(comet = false)
        val (actual, cometPlan) = run(comet = true)
        assert(expected == 1L)
        assert(actual == expected)
        for (plan <- Seq(sparkPlan, cometPlan)) {
          assert(collect(plan) { case _: MapPartitionsExec => true }.isEmpty)
          assert(collect(plan) {
            case node if node.nodeName.contains("Limit") || node.nodeName.contains("Join") =>
              node
          }.isEmpty)
        }
        assert(collect(cometPlan) { case _: CometNativeScanExec => true }.nonEmpty)
      }
    }
  }

  test("transition reversion restores Spark observation with its child") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_SORT_ENABLED.key -> "false",
      CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
      withParquetTable(Seq((1, 2), (3, 4)), "observed_input") {
        for (revert <- Seq(false, true)) {
          withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> revert.toString) {
            val observation = Observation("reverted")
            val result = spark
              .table("observed_input")
              .select(($"_1" + 1).as("value"))
              .observe(observation, count(lit(1)).as("rows"), sum($"value").as("total"))
              .sortWithinPartitions($"value".desc)
              .repartition(1)
            assert(result.collect().toSet == Set(Row(2), Row(4)))
            assert(observedMetrics(observation) == Map("rows" -> 2L, "total" -> 6L))
            val plan = result.queryExecution.executedPlan
            assert(collect(plan) { case _: SortExec => true }.nonEmpty)
            assert(collect(plan) { case _: CollectMetricsExec => true }.size == 1)
            assert(collect(plan) { case _: CometCollectMetricsExec => true }.nonEmpty == !revert)
          }
        }
      }
    }
  }

  test("disabled columnar observation keeps Spark collection") {
    withSQLConf(CometConf.COMET_EXEC_COLLECT_METRICS_ENABLED.key -> "false") {
      withParquetTable(Seq((1, 2), (3, 4)), "observed_input") {
        val observation = Observation("disabled")
        val result = spark
          .table("observed_input")
          .observe(observation, count(lit(1)).as("rows"))
          .repartition(1)
          .select($"_1" + 1)
        assert(result.collect().toSet == Set(Row(2), Row(4)))
        assert(observedMetrics(observation) == Map("rows" -> 2L))
        assert(collect(result.queryExecution.executedPlan) { case _: CometCollectMetricsExec =>
          true
        }.isEmpty)
      }
    }
  }
}
