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

package org.apache.spark.sql.comet.uniffle

import java.util.function.Function

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{RssShuffleHandle, RssShuffleManager}
import org.apache.spark.shuffle.writer.RssShuffleWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.execution.shuffle.{CometBaseNativeShuffleWriter, NativeShuffleSpec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.uniffle.client.api.ShuffleWriteClient

import org.apache.comet.Native
import org.apache.comet.shuffle.ShufflePartitionPusher

case class CometUniffleShuffleWriter[K, V, C](
    spec: NativeShuffleSpec,
    outputPartitioning: Partitioning,
    outputAttributes: Seq[Attribute],
    numParts: Int,
    nativeMetrics: Map[String, SQLMetric],
    appId: String,
    shuffleId: Int,
    taskId: String,
    rssTaskAttemptId: Long,
    rssShuffleWriteMetrics: ShuffleWriteMetrics,
    shuffleManager: RssShuffleManager,
    sparkConf: SparkConf,
    shuffleWriteClient: ShuffleWriteClient,
    rssHandle: RssShuffleHandle[K, V, C],
    taskFailureCallback: Function[String, java.lang.Boolean],
    context: TaskContext,
    rangePartitionBounds: Option[Seq[InternalRow]] = None)
    extends RssShuffleWriter[K, V, C](
      appId,
      shuffleId,
      taskId,
      rssTaskAttemptId,
      rssShuffleWriteMetrics,
      shuffleManager,
      sparkConf,
      shuffleWriteClient,
      rssHandle,
      taskFailureCallback,
      context)
    with CometBaseNativeShuffleWriter
    with ShufflePartitionPusher {

  private val nativeLib = new Native()
  private var nativeShufflePusherHandle: Long = nativeLib.createRssPartitionPusher(this)

  override def writeImpl(inputs: Iterator[Product2[K, V]]): Unit = {
    val detailedMetrics = Seq(
      "elapsed_compute",
      "encode_time",
      "repart_time",
      "input_batches",
      "spill_count",
      "spilled_bytes")
    val metricsOutputRows = new SQLMetric("outputRows")
    val metricsWriteTime = new SQLMetric("writeTime")
    val shuffleWriterSQLMetrics = Map(
      "output_rows" -> metricsOutputRows,
      "data_size" -> nativeMetrics("dataSize"),
      "write_time" -> metricsWriteTime) ++
      nativeMetrics.filterKeys(detailedMetrics.contains)

    nativeWrite(inputs, shuffleWriterSQLMetrics, nativeShufflePusherHandle)

    rssShuffleWriteMetrics.incRecordsWritten(metricsOutputRows.value)
    rssShuffleWriteMetrics.incWriteTime(metricsWriteTime.value)

    val pushMergedDataTime = System.nanoTime()
    // clear all
    sendRestBlockAndWait()
    sendCommit()
    val writeDurationNanos = System.nanoTime() - pushMergedDataTime
    rssShuffleWriteMetrics.incWriteTime(writeDurationNanos)

  }

  override def sendCommit(): Unit = {
    if (!this.isMemoryShuffleEnabled) {
      super.sendCommit()
    }
  }

  override def pushPartitionData(partitionId: Int, bytes: Array[Byte], length: Int): Int = {
    val shuffleBlockInfos =
      super.getBufferManager.addPartitionData(
        partitionId,
        bytes,
        length,
        System.currentTimeMillis)
    super.processShuffleBlockInfos(shuffleBlockInfos)
    // fast fail or resend data// fast fail or resend data
    super.checkDataIfAnyFailure()
    length
  }

  private var stopped = false
  override def stop(success: Boolean): Option[MapStatus] = {
    if (!stopped) {
      stopped = true
      releaseShufflePusher()
      val result = super.stop(success)
      result
    } else {
      Option.empty
    }
  }

  // release native shuffle pusher
  private def releaseShufflePusher(): Unit = {
    if (nativeShufflePusherHandle != -1) {
      nativeLib.releaseRssPartitionPusher(nativeShufflePusherHandle)
      nativeShufflePusherHandle = -1
    }
  }

  private def sendRestBlockAndWait(): Unit = {
    val shuffleBlockInfos = super.getBufferManager.clear(1.0)
    if (!shuffleBlockInfos.isEmpty) {
      super.processShuffleBlockInfos(shuffleBlockInfos)
    }
    super.internalCheckBlockSendResult();
  }

}
