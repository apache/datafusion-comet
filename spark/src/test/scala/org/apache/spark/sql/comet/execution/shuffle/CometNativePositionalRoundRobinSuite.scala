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

package org.apache.spark.sql.comet.execution.shuffle

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Positional round robin places rows by their ordinal within the map task rather than by hashing
 * their contents, which is what Spark's own round robin does. Placement is reproducible only when
 * the map task replays rows in the same order, so the gating that decides where it is used is as
 * much the feature as the placement itself, and most of what is tested here.
 *
 * Lives in the `execution.shuffle` package so it can reach
 * `CometShuffleExchangeExec.usesPositionalRoundRobin` directly rather than inferring the decision
 * from output.
 */
class CometNativePositionalRoundRobinSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val numPartitions = 8

  private def withPositionalRoundRobin(extra: (String, String)*)(f: => Unit): Unit =
    withSQLConf(
      Seq(
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_POSITIONAL_ENABLED.key -> "true",
        // Keep AQE from coalescing the round robin away, so the exchange under test survives
        // into the executed plan.
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") ++ extra: _*)(f)

  /** Whether the round-robin exchange in `df`'s executed plan chose positional placement. */
  private def isPositional(df: DataFrame): Boolean = {
    val exchanges = collect(df.queryExecution.executedPlan) {
      case e: CometShuffleExchangeExec
          if e.shuffleType == CometNativeShuffle &&
            e.outputPartitioning.isInstanceOf[RoundRobinPartitioning] =>
        e
    }
    assert(
      exchanges.size == 1,
      s"expected one native round-robin exchange in\n${df.queryExecution.executedPlan}")
    CometShuffleExchangeExec.usesPositionalRoundRobin(
      exchanges.head.outputPartitioning,
      exchanges.head.child)
  }

  private def withParquetTable(rows: Int)(f: String => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(rows)
        .selectExpr("id", "cast(id % 7 as string) as s", "id % 3 as g")
        .write
        .parquet(path)
      withTempView("t") {
        spark.read.parquet(path).createOrReplaceTempView("t")
        f("t")
      }
    }
  }

  test("a scan under projections and filters takes positional placement") {
    withPositionalRoundRobin() {
      withParquetTable(1000) { t =>
        assert(isPositional(spark.table(t).repartition(numPartitions)))
        assert(isPositional(spark.table(t).filter("id > 10").repartition(numPartitions)))
        assert(isPositional(
          spark.table(t).filter("id > 10").selectExpr("id + 1 as id").repartition(numPartitions)))
      }
    }
  }

  test("a plan whose replay order Comet cannot establish keeps content-hash placement") {
    withPositionalRoundRobin() {
      withParquetTable(1000) { t =>
        // An aggregate under memory pressure emits groups in an order that depends on how many
        // times it spilled, which differs between attempts.
        assert(!isPositional(spark.table(t).groupBy("g").count().repartition(numPartitions)))
        // A sort is not excluded because it reorders, but because Comet has not established that
        // its tie-breaking survives a differing spill count.
        assert(!isPositional(spark.table(t).sort("s").repartition(numPartitions)))
      }
    }
  }

  test("positional placement is off unless its own config is on") {
    withParquetTable(100) { t =>
      withPositionalRoundRobin(
        CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_POSITIONAL_ENABLED.key -> "false") {
        assert(!isPositional(spark.table(t).repartition(numPartitions)))
      }
    }
  }

  test("a hash repartition is never positional") {
    withPositionalRoundRobin() {
      withParquetTable(100) { t =>
        val df = spark.table(t).repartition(numPartitions, spark.table(t)("g"))
        val exchanges = collect(df.queryExecution.executedPlan) {
          case e: CometShuffleExchangeExec => e
        }
        assert(exchanges.nonEmpty)
        exchanges.foreach { e =>
          assert(
            !CometShuffleExchangeExec.usesPositionalRoundRobin(e.outputPartitioning, e.child))
        }
      }
    }
  }

  test("positional placement keeps every row exactly once") {
    withPositionalRoundRobin() {
      withParquetTable(5000) { t =>
        checkSparkAnswer(spark.table(t).repartition(numPartitions).selectExpr("id", "s", "g"))
      }
    }
  }

  test("a group size larger than a batch still keeps every row") {
    // Exercises the whole-batch fast path, where a run covers an entire input batch and is handed
    // through without a copy.
    withPositionalRoundRobin(
      CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_POSITIONAL_GROUP_ROWS.key -> "8192",
      CometConf.COMET_BATCH_SIZE.key -> "128") {
      withParquetTable(5000) { t =>
        checkSparkAnswer(spark.table(t).repartition(numPartitions))
      }
    }
  }

  /**
   * Rows per output partition, in partition order.
   *
   * Taken off the RDD rather than by grouping on `spark_partition_id()`, because the aggregate
   * that grouping introduces brings its own exchange and the planner drops the repartition under
   * it, leaving the measurement describing the scan's partitioning instead.
   */
  private def partitionSizes(df: DataFrame): Seq[Int] =
    df.rdd.mapPartitions(rows => Iterator(rows.size)).collect().toSeq

  test("duplicate rows spread evenly, where hashing sends them all to one partition") {
    // The behaviour that makes positional placement round robin rather than hash partitioning:
    // `pmod(murmur3(row), n)` is a function of the row's contents, so a column of one repeated
    // value collapses onto a single reducer. Spark's round robin spreads it.
    withParquetTable(100) { t =>
      val duplicated = spark.table(t).select(lit(1).as("c"))
      val distinct = spark.table(t).select(col("id"))

      // An explicit small group, because the derived default is batchSize / numPartitions and
      // would put this whole 100-row fixture in one group.
      withPositionalRoundRobin(
        CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_POSITIONAL_GROUP_ROWS.key -> "8") {
        val duplicatedSizes = partitionSizes(duplicated.repartition(numPartitions))
        assert(duplicatedSizes.sum == 100)
        assert(
          duplicatedSizes.count(_ > 0) > 1,
          s"expected the rows to spread across partitions, got $duplicatedSizes")
        // Placement ignores row contents entirely, so a column of one repeated value partitions
        // exactly like a column of distinct ones.
        assert(duplicatedSizes == partitionSizes(distinct.repartition(numPartitions)))
      }

      withPositionalRoundRobin(
        CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_POSITIONAL_ENABLED.key -> "false") {
        val sizes = partitionSizes(duplicated.repartition(numPartitions))
        assert(
          sizes.count(_ > 0) == 1,
          s"content hashing should collapse identical rows onto one partition, got $sizes")
      }
    }
  }

  /**
   * Rows each reducer receives when `mapTasks` map tasks of `rowsPerTask` rows each place
   * positionally into `partitions` output partitions, starting where `start` says. Counted a
   * group at a time, which is exact because a group's rows all land together.
   */
  private def stageSpread(
      mapTasks: Int,
      rowsPerTask: Int,
      partitions: Int,
      groupRows: Int,
      start: Int => Int): Array[Int] = {
    val counts = Array.fill(partitions)(0)
    val groups = (rowsPerTask + groupRows - 1) / groupRows
    for (mapPartitionId <- 0 until mapTasks; group <- 0 until groups) {
      val rows = math.min(groupRows, rowsPerTask - group * groupRows)
      counts(((start(mapPartitionId).toLong + group) % partitions).toInt) += rows
    }
    counts
  }

  test("map tasks start on decorrelated partitions, so the stage leaves no reducer empty") {
    // Each task walks ceil(rowsPerTask / groupRows) consecutive partitions from its start, so
    // distinct starts are not enough: consecutive starts make every task's run overlap its
    // neighbours' and the partitions past mapTasks + groupsPerTask never get a row. This is the
    // correlation SPARK-21782 fixed, and `positionalStartPartition` fixes it the same way.
    val (mapTasks, rowsPerTask, partitions, groupRows) = (10, 5000, 200, 64)

    val adjacent = stageSpread(mapTasks, rowsPerTask, partitions, groupRows, identity)
    assert(
      adjacent.count(_ == 0) == 112,
      "the hazard this scrambling exists to avoid should still be reachable with adjacent starts")

    val scrambled = stageSpread(
      mapTasks,
      rowsPerTask,
      partitions,
      groupRows,
      CometShuffleExchangeExec.positionalStartPartition(_, partitions))
    assert(scrambled.sum == mapTasks * rowsPerTask)
    assert(
      scrambled.count(_ == 0) == 0,
      s"every reducer should get rows, got ${scrambled.count(_ == 0)} empty of $partitions")
  }

  test("stage-wide balance needs many more groups per task than there are partitions") {
    // Why the group size defaults to batchSize / numPartitions rather than to the batch size,
    // even though a batch-sized group is far cheaper to flush: the per-task bound does not
    // compose. A reducer sees the sum over every map task, and that sum only evens out once each
    // task has wrapped the partition space several times.
    val (mapTasks, rowsPerTask, partitions) = (50, 1000000, 200)
    def spread(groupRows: Int): Double = {
      val counts = stageSpread(
        mapTasks,
        rowsPerTask,
        partitions,
        groupRows,
        CometShuffleExchangeExec.positionalStartPartition(_, partitions))
      assert(counts.sum == mapTasks.toLong * rowsPerTask)
      counts.max.toDouble / counts.min
    }

    // 64 rows per group is 15625 groups per task, so each task wraps 78 times and the sums
    // converge. 8192 is 123 groups, fewer than there are partitions, so a task cannot even cover
    // the space once and where the gaps fall is down to the starts.
    assert(spread(64) < 1.01, s"expected an even stage at a small group, got ${spread(64)}")
    assert(spread(8192) > 1.2, s"expected a batch-sized group to skew, got ${spread(8192)}")
  }
}
