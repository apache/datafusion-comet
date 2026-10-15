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

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter.{SHUFFLE_BYTES_WRITTEN, SHUFFLE_RECORDS_WRITTEN}

import org.apache.comet.CometConf

class CometCelebornShuffleStatisticsSuite extends CometTestBase {
  import testImplicits._

  override protected val shuffleManager: String =
    classOf[CometCelebornFallbackTestShuffleManager].getName

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SHUFFLE_MODE.key, "native")
      .set(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key, "false")
      .set(CometConf.COMET_SHUFFLE_RSS_MAX_FRAME_BYTES.key, "32k")
      .set(CometConf.COMET_EXCHANGE_SIZE_MULTIPLIER.key, "1")
      .set("spark.shuffle.compress", "false")
      .set("spark.celeborn.client.spark.stageRerun.enabled", "true")

  private def manager: CometCelebornFallbackTestShuffleManager =
    SparkEnv.get.shuffleManager.asInstanceOf[CometCelebornFallbackTestShuffleManager]

  for (adaptive <- Seq(false, true)) {
    test(s"fallback publishes only the local destination's output statistics: AQE=$adaptive") {
      withSQLConf("spark.sql.adaptive.enabled" -> adaptive.toString) {
        manager.remoteMapsBeforeFailure.set(0)
        manager.waitForFirstRemoteMap = true
        try {
          val shuffled = spark
            .range(0, 2, 1, 2)
            .selectExpr(
              "CAST(id AS INT) AS key",
              "CASE WHEN id = 0 THEN 'small' ELSE repeat('x', 1048576) END AS payload")
            .repartition(3, $"key")
          val actual = shuffled.collect().map(row => (row.getInt(0), row.getString(1))).toSeq
          val exchange = collect(shuffled.queryExecution.executedPlan) {
            case value: CometShuffleExchangeExec => value
          }.headOption.getOrElse(fail("Expected a native Comet shuffle exchange"))
          val remote = exchange.shuffleDependency.asInstanceOf[CometShuffleDependency[_, _, _]]
          val local = remote.currentShuffleDependency

          assert(actual.sortBy(_._1) == Seq((0, "small"), (1, "x" * (1024 * 1024))))
          assert(manager.remoteMapsBeforeFailure.get() == 1)
          assert(local.useLocalShuffle)
          assert(remote.shuffleWriteMetrics(SHUFFLE_RECORDS_WRITTEN).value == 1L)
          assert(exchange.runtimeStatistics.rowCount.contains(BigInt(2)))
          assert(exchange.metrics(SHUFFLE_RECORDS_WRITTEN).value == 2L)
          assert(remote.shuffleWriteMetrics("dataSize").value > 0L)
          assert(
            exchange.runtimeStatistics.sizeInBytes ==
              BigInt(local.shuffleWriteMetrics("dataSize").value))
          assert(
            exchange.metrics("dataSize").value ==
              local.shuffleWriteMetrics("dataSize").value)
          assert(
            exchange.metrics(SHUFFLE_BYTES_WRITTEN).value ==
              local.shuffleWriteMetrics(SHUFFLE_BYTES_WRITTEN).value)

          // Simulate another accumulator merge from the abandoned remote stage after publication.
          // Resetting the original counters would allow that update to inflate AQE statistics.
          val published = exchange.runtimeStatistics
          val publishedBytes = exchange.metrics(SHUFFLE_BYTES_WRITTEN).value
          remote.shuffleWriteMetrics("dataSize").add(4096L)
          remote.shuffleWriteMetrics(SHUFFLE_RECORDS_WRITTEN).add(1L)
          remote.shuffleWriteMetrics(SHUFFLE_BYTES_WRITTEN).add(4096L)
          assert(exchange.runtimeStatistics == published)
          assert(exchange.metrics(SHUFFLE_RECORDS_WRITTEN).value == 2L)
          assert(exchange.metrics("dataSize").value == published.sizeInBytes.toLong)
          assert(exchange.metrics(SHUFFLE_BYTES_WRITTEN).value == publishedBytes)

          assert(manager.unregisterShuffle(remote.shuffleId))
          assert(manager.unregisterShuffle(local.shuffleId))
        } finally {
          manager.waitForFirstRemoteMap = false
        }
      }
    }
  }

  test("successful remote materialization publishes remote output statistics") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      val query = spark.range(0, 2, 1, 2).repartition(2, $"id")
      val exchange = collect(query.queryExecution.executedPlan) {
        case value: CometShuffleExchangeExec => value
      }.headOption.getOrElse(fail("Expected a native Comet shuffle exchange"))
      val remote = exchange.shuffleDependency.asInstanceOf[CometShuffleDependency[_, _, _]]
      try {
        val statistics = Await.result(exchange.mapOutputStatisticsFuture, 20.seconds)
        assert(remote.currentShuffleDependency eq remote)
        assert(exchange.runtimeStatistics.rowCount.contains(BigInt(2)))
        assert(exchange.runtimeStatistics.sizeInBytes > 0)
        assert(exchange.metrics(SHUFFLE_RECORDS_WRITTEN).value == 2L)
        assert(statistics.bytesByPartitionId.sum > 0L)
        // MapOutputStatistics uses Spark's compressed MapStatus sizes, which are approximate.
        assert(
          exchange.metrics(SHUFFLE_BYTES_WRITTEN).value ==
            remote.shuffleWriteMetrics(SHUFFLE_BYTES_WRITTEN).value)
        assert(
          exchange.metrics("dataSize").value ==
            exchange.runtimeStatistics.sizeInBytes.toLong)
      } finally {
        manager.unregisterShuffle(remote.shuffleId)
      }
    }
  }
}
