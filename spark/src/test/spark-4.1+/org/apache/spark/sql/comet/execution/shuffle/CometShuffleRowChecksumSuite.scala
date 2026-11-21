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

import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency, SparkEnv}
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Spark 4.1+ can send an order-independent checksum of each map task's output to the driver
 * (SPARK-51756). The driver compares it across attempts of the same task to find a recomputed
 * output that changed. These tests check which shuffle paths report it. See
 * https://github.com/apache/datafusion-comet/issues/6414.
 */
class CometShuffleRowChecksumSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  // Compute and compare checksums, but do not roll back stages on a mismatch, so that a test
  // can recompute a map stage and read what the driver recorded.
  private val checksumConf = Seq(
    SQLConf.SHUFFLE_ORDER_INDEPENDENT_CHECKSUM_ENABLED.key -> "true",
    SQLConf.SHUFFLE_CHECKSUM_MISMATCH_FULL_RETRY_ENABLED.key -> "false",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false")

  private case class ShufflePath(name: String, conf: Seq[(String, String)], kind: String)

  private val sparkShuffle =
    ShufflePath("Spark shuffle", Seq(CometConf.COMET_SHUFFLE_ENABLED.key -> "false"), "Spark")
  private val jvmShuffle = ShufflePath(
    "Comet JVM columnar shuffle",
    Seq(CometConf.COMET_SHUFFLE_MODE.key -> "jvm"),
    CometColumnarShuffle.toString)
  private val nativeShuffle = ShufflePath(
    "Comet native shuffle",
    Seq(CometConf.COMET_SHUFFLE_MODE.key -> "native"),
    CometNativeShuffle.toString)

  private val rows = (0 until 1000).map(i => (i, s"row $i"))

  private def tracker: MapOutputTrackerMaster =
    SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  /**
   * Runs the query's own RDD, as CometNativePositionalRoundRobinSuite does, so that every run
   * goes through the same exchange and shuffle id.
   */
  private def run(df: DataFrame): Unit =
    SQLExecution.withSQLConfPropagated(spark)(df.queryExecution.toRdd.count())

  /** The kind of shuffle the plan uses, and its dependency. */
  private def shuffleOf(df: DataFrame): (String, ShuffleDependency[_, _, _]) = {
    val plan = df.queryExecution.executedPlan
    val shuffles = collect[(String, ShuffleDependency[_, _, _])](plan) {
      case e: CometShuffleExchangeExec => (e.shuffleType.toString, e.shuffleDependency)
      case e: ShuffleExchangeExec => ("Spark", e.shuffleDependency)
    }
    assert(shuffles.size == 1, s"Expected one shuffle in:\n$plan")
    shuffles.head
  }

  private def checksums(shuffleId: Int): Seq[Long] =
    tracker.shuffleStatuses(shuffleId).mapStatuses.map(_.checksumValue).toSeq

  Seq(sparkShuffle, jvmShuffle, nativeShuffle).foreach { path =>
    test(s"${path.name}: every map status carries a checksum") {
      withSQLConf(checksumConf ++ path.conf: _*) {
        withParquetTable(rows, "t") {
          val df = spark.table("t").repartition(10, col("_1"))
          run(df)
          val (kind, dep) = shuffleOf(df)
          assert(kind == path.kind)
          val values = checksums(dep.shuffleId)
          assert(values.nonEmpty && values.forall(_ != 0), s"$kind checksums: $values")
        }
      }
    }
  }

  // Only Spark and JVM columnar shuffle here: a nondeterministic UDF keeps its projection in
  // Spark, and native shuffle needs a native child.
  Seq(sparkShuffle, jvmShuffle).foreach { path =>
    test(s"${path.name}: a recomputed map output that changed is detected") {
      val random = udf(() => scala.util.Random.nextLong()).asNondeterministic()
      withSQLConf(checksumConf ++ path.conf: _*) {
        withParquetTable(rows, "t") {
          // Partitioned by the random value, so a recomputed row can land in another partition:
          // the case where a partial retry loses rows or reads them twice.
          val df = spark.table("t").select(col("_1"), random().as("r")).repartition(10, col("r"))
          run(df)
          val (kind, dep) = shuffleOf(df)
          assert(kind == path.kind)
          val first = checksums(dep.shuffleId)
          // What losing an executor does: drop the map outputs, so the next run recomputes them.
          tracker.unregisterAllMapAndMergeOutput(dep.shuffleId)
          run(df)
          val mismatched = tracker.shuffleStatuses(dep.shuffleId).checksumMismatchIndices
          assert(
            mismatched.nonEmpty,
            s"$kind: no mismatch recorded. Checksums: $first, then ${checksums(dep.shuffleId)}. " +
              s"Static determinacy of the map stage: ${dep.rdd.outputDeterministicLevel}")
        }
      }
    }
  }

  // The same rows on both attempts must give the same checksums. A false mismatch would roll
  // back the consumers for nothing, or abort a job whose result stage already wrote output.
  Seq(sparkShuffle, jvmShuffle).foreach { path =>
    test(s"${path.name}: an unchanged recomputed map output is not flagged") {
      withSQLConf(checksumConf ++ path.conf: _*) {
        withParquetTable(rows, "t") {
          val df = spark.table("t").repartition(10, col("_1"))
          run(df)
          val (kind, dep) = shuffleOf(df)
          assert(kind == path.kind)
          val first = checksums(dep.shuffleId)
          tracker.unregisterAllMapAndMergeOutput(dep.shuffleId)
          run(df)
          assert(checksums(dep.shuffleId) == first)
          assert(tracker.shuffleStatuses(dep.shuffleId).checksumMismatchIndices.isEmpty)
        }
      }
    }
  }
}
