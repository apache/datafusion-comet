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

import java.util.Properties

import org.apache.spark.{Partition, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.comet.CometMetricNode
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometRuntimeException}

/**
 * Retry safety for the batch-granular (positional) native round-robin strategy.
 *
 * That strategy assigns a whole Arrow batch to an output partition from a per-task counter, so a
 * row's destination depends on the order and the framing of the batches the upstream operator
 * produced rather than on the row itself. Re-executing a map task can therefore write a different
 * partitioning of the same rows, which duplicates and drops rows on the reduce side once any of
 * the replaced output has been fetched. Two defences are covered here: declaring the shuffle
 * input RDD indeterminate so the DAGScheduler rolls the stage back instead of re-running one
 * task, and refusing to run a retried map task at all.
 *
 * Lives in the `execution.shuffle` package so it can construct the `private[shuffle]`
 * [[CometNativeShuffleInputRDD]] and the `private[spark]` [[TaskContextImpl]] directly.
 */
class CometNativeRoundRobinRetrySuite extends CometTestBase {

  private val batchGranularKey =
    CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_BATCH_GRANULAR.key
  private val failOnRetryKey =
    CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_FAIL_ON_RETRY.key

  /** An input RDD that reports exactly `level`, standing in for a real upstream subtree. */
  private def parentWithLevel(level: DeterministicLevel.Value): RDD[AnyRef] =
    new RDD[AnyRef](spark.sparkContext, Nil) {
      override protected def getOutputDeterministicLevel: DeterministicLevel.Value = level
      override protected def getPartitions: Array[Partition] = Array.empty
      override def compute(split: Partition, context: TaskContext): Iterator[AnyRef] =
        Iterator.empty
    }

  private def shuffleInput(
      parent: RDD[AnyRef],
      positionalRoundRobin: Boolean): CometNativeShuffleInputRDD =
    new CometNativeShuffleInputRDD(
      spark.sparkContext,
      Seq(parent),
      0,
      Set.empty,
      CometMetricNode(Map.empty),
      positionalRoundRobin = positionalRoundRobin)

  /** Mirrors `TaskContext.empty()`, which hard-codes both attempt numbers to zero. */
  private def taskContextFor(stageAttempt: Int, taskAttempt: Int): TaskContext =
    new TaskContextImpl(
      0,
      stageAttempt,
      0,
      0L,
      taskAttempt,
      1,
      null,
      new Properties,
      null,
      TaskMetrics.empty,
      1)

  private def writerFor(
      outputPartitioning: Partitioning,
      taskContext: TaskContext): CometNativeShuffleWriter[Int, Any] =
    new CometNativeShuffleWriter[Int, Any](
      NativeShuffleSpec(null, CometMetricNode(Map.empty), null),
      outputPartitioning,
      Nil,
      Map.empty,
      4,
      0,
      0L,
      taskContext,
      null)

  /**
   * `write` is driven with a plain iterator rather than a [[CometNativeShuffleInputIterator]], so
   * anything that gets past the retry guard fails soon after on that. Returning the message lets
   * a caller assert on which of the two happened without depending on the unrelated failure's
   * type.
   */
  private def writeFailureMessage(writer: CometNativeShuffleWriter[Int, Any]): String = {
    val failure = intercept[Throwable](writer.write(Iterator.empty))
    Option(failure.getMessage).getOrElse("")
  }

  private val refusalPrefix = "Refusing to re-execute a Comet native round-robin"

  test("positional round robin is claimed only for round robin with the config enabled") {
    val partitionings = Seq(
      RoundRobinPartitioning(4),
      SinglePartition,
      HashPartitioning(Seq(Literal(1)), 4),
      RangePartitioning(Nil, 4))
    withSQLConf(batchGranularKey -> "true") {
      val claimed = partitionings.filter(CometShuffleExchangeExec.usesPositionalRoundRobin)
      assert(claimed == Seq(RoundRobinPartitioning(4)))
    }
    withSQLConf(batchGranularKey -> "false") {
      assert(!partitionings.exists(CometShuffleExchangeExec.usesPositionalRoundRobin))
    }
  }

  test("positional round robin declares indeterminate output unless its parent is determinate") {
    // A determinate parent (a plain scan) replays identically, so positional assignment is
    // reproducible and per-task retry stays cheap. Anything below another exchange is unordered,
    // which is where Spark's own round robin flips to indeterminate too.
    Seq(
      DeterministicLevel.DETERMINATE -> DeterministicLevel.DETERMINATE,
      DeterministicLevel.UNORDERED -> DeterministicLevel.INDETERMINATE,
      DeterministicLevel.INDETERMINATE -> DeterministicLevel.INDETERMINATE).foreach {
      case (parentLevel, expected) =>
        val input = shuffleInput(parentWithLevel(parentLevel), positionalRoundRobin = true)
        assert(input.outputDeterministicLevel == expected, s"parent was $parentLevel")
        assert(
          input.copyForLocalShuffle().outputDeterministicLevel == expected,
          s"local fallback lost the declaration for parent $parentLevel")
    }
  }

  test("content-hash round robin keeps inheriting its parent's determinism") {
    // Hash placement is a pure function of the rows, so nothing here should be indeterminate on
    // its own account. This is the default path and must stay as retryable as it is today.
    Seq(
      DeterministicLevel.DETERMINATE,
      DeterministicLevel.UNORDERED,
      DeterministicLevel.INDETERMINATE).foreach { level =>
      val input = shuffleInput(parentWithLevel(level), positionalRoundRobin = false)
      assert(input.outputDeterministicLevel == level)
    }
  }

  test("positional round robin refuses to run a retried map task") {
    withSQLConf(batchGranularKey -> "true") {
      // A task re-run inside the current stage attempt, and a re-submitted stage, which is what
      // losing an executor produces. Neither counter subsumes the other.
      Seq(("task attempt", 0, 1), ("stage attempt", 1, 0), ("both", 2, 3)).foreach {
        case (label, stageAttempt, taskAttempt) =>
          val writer =
            writerFor(RoundRobinPartitioning(4), taskContextFor(stageAttempt, taskAttempt))
          val failure = intercept[CometRuntimeException](writer.write(Iterator.empty))
          assert(failure.getMessage.startsWith(refusalPrefix), label)
          // The message has to say which knob to turn; an operator hitting this mid-job has no
          // other signal that positional placement is what failed them.
          assert(failure.getMessage.contains(batchGranularKey), label)
          assert(failure.getMessage.contains(failOnRetryKey), label)
      }
    }
  }

  test("positional round robin lets a first attempt through") {
    withSQLConf(batchGranularKey -> "true") {
      val writer = writerFor(RoundRobinPartitioning(4), taskContextFor(0, 0))
      assert(!writeFailureMessage(writer).startsWith(refusalPrefix))
    }
  }

  test("the retry guard is scoped to positional round robin") {
    // Hash and range partitioning place rows by content, and content-hash round robin does too,
    // so a retry of any of those is safe and must not be turned into a job failure.
    withSQLConf(batchGranularKey -> "true") {
      Seq(SinglePartition, HashPartitioning(Seq(Literal(1)), 4), RangePartitioning(Nil, 4))
        .foreach { partitioning =>
          val writer = writerFor(partitioning, taskContextFor(1, 1))
          assert(
            !writeFailureMessage(writer).startsWith(refusalPrefix),
            s"$partitioning should not be treated as positional round robin")
        }
    }
    withSQLConf(batchGranularKey -> "false") {
      val writer = writerFor(RoundRobinPartitioning(4), taskContextFor(1, 1))
      assert(!writeFailureMessage(writer).startsWith(refusalPrefix))
    }
  }

  test("failOnRetry=false hands retry handling back to the DAGScheduler") {
    withSQLConf(batchGranularKey -> "true", failOnRetryKey -> "false") {
      val writer = writerFor(RoundRobinPartitioning(4), taskContextFor(1, 1))
      assert(!writeFailureMessage(writer).startsWith(refusalPrefix))
    }
  }

  /**
   * The determinism level the DAGScheduler would read off a planned round-robin exchange.
   * `ShuffleMapStage.rdd` is the dependency's RDD, so this is exactly what
   * `Stage.isIndeterminate` sees.
   */
  private def roundRobinStageLevel(df: DataFrame): DeterministicLevel.Value = {
    val plan = df.queryExecution.executedPlan
    val exchanges = plan.collect {
      case exchange: CometShuffleExchangeExec
          if exchange.outputPartitioning.isInstanceOf[RoundRobinPartitioning] =>
        exchange
    }
    assert(exchanges.size == 1, s"expected exactly one round robin exchange in\n$plan")
    assert(exchanges.head.shuffleType == CometNativeShuffle, s"not native shuffle in\n$plan")
    exchanges.head.shuffleDependency.rdd.outputDeterministicLevel
  }

  test("a planned positional round robin exchange carries the declaration to its stage") {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_ENABLED.key -> "true",
      batchGranularKey -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable((0 until 100).map(i => (i % 10, i)), "tbl") {
        // Straight over a scan. The scan replays identically, so positional assignment is
        // reproducible and per-task retry stays as cheap as it is for hash placement.
        assert(
          roundRobinStageLevel(sql("SELECT * FROM tbl").repartition(4)) ==
            DeterministicLevel.DETERMINATE)

        // Below an aggregate's exchange. The reduce side sees shuffle blocks in arrival order, so
        // a replay can frame the batches differently and the stage has to be rolled back whole.
        assert(
          roundRobinStageLevel(sql("SELECT _1, sum(_2) FROM tbl GROUP BY _1").repartition(4)) ==
            DeterministicLevel.INDETERMINATE)
      }
    }
  }
}
