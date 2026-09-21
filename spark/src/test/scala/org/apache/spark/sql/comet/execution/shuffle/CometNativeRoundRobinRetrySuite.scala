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

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.DeterministicLevel
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
 * the replaced output has been fetched. This suite covers the writer refusing to run a retried
 * map task. The other defence, declaring the shuffle input RDD indeterminate so the DAGScheduler
 * rolls the stage back, is covered in [[CometNativeShuffleInputRDDSuite]].
 *
 * Lives in the `execution.shuffle` package so it can construct the `private[shuffle]`
 * [[CometNativeShuffleInputRDD]] and the `private[spark]` [[TaskContextImpl]] directly.
 */
class CometNativeRoundRobinRetrySuite extends CometTestBase {

  private val batchGranularKey =
    CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_BATCH_GRANULAR.key
  private val failOnRetryKey =
    CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_FAIL_ON_RETRY.key

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

  test("usesPositionalRoundRobin claims only multi-partition round robin with the config on") {
    val partitionings = Seq(
      RoundRobinPartitioning(4),
      // One output partition puts every row in the same place, so there is no placement to get
      // wrong. `isRoundRobin` in `prepareJVMShuffleDependency` excludes it for the same reason.
      RoundRobinPartitioning(1),
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

  test("the retry guard fires only for positional round robin on a retried attempt") {
    // Everything here must reach the writer. Hash and range partitioning place rows by content,
    // content-hash round robin does too, a first attempt is not a retry, and failOnRetry=false
    // hands retry handling back to the DAGScheduler.
    Seq(
      ("first attempt", "true", "true", RoundRobinPartitioning(4): Partitioning, 0, 0),
      ("batchGranular off", "false", "true", RoundRobinPartitioning(4), 1, 1),
      ("failOnRetry off", "true", "false", RoundRobinPartitioning(4), 1, 1),
      ("single output partition", "true", "true", RoundRobinPartitioning(1), 1, 1),
      ("hash partitioning", "true", "true", HashPartitioning(Seq(Literal(1)), 4), 1, 1),
      ("range partitioning", "true", "true", RangePartitioning(Nil, 4), 1, 1),
      ("single partition", "true", "true", SinglePartition, 1, 1)).foreach {
      case (label, granular, failOnRetry, partitioning, stageAttempt, taskAttempt) =>
        withSQLConf(batchGranularKey -> granular, failOnRetryKey -> failOnRetry) {
          val writer = writerFor(partitioning, taskContextFor(stageAttempt, taskAttempt))
          assert(!writeFailureMessage(writer).startsWith(refusalPrefix), label)
        }
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
