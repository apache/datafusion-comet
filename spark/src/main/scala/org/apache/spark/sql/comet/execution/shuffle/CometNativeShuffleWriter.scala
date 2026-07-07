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

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Paths}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * Drives the native shuffle write in a single [[CometExecIterator]] per partition. The plan is
 * `ShuffleWriter(child = childNativeOp)`; leaf iterators come from a
 * [[CometNativeShuffleInputIterator]]. `childNativeOp` is either a rich Comet native subtree
 * (when fed by [[CometShuffleExchangeExec]] with a [[org.apache.spark.sql.comet.CometNativeExec]]
 * child) or a synthetic `Scan("ShuffleWriterInput")` placeholder (the
 * [[CometShuffleExchangeExec.prepareShuffleDependency]] convenience overload). Same handling
 * either way.
 */
case class CometNativeShuffleWriter[K, V](
    spec: NativeShuffleSpec,
    outputPartitioning: Partitioning,
    outputAttributes: Seq[Attribute],
    metrics: Map[String, SQLMetric],
    numParts: Int,
    shuffleId: Int,
    mapId: Long,
    context: TaskContext,
    metricsReporter: ShuffleWriteMetricsReporter,
    rangePartitionBounds: Option[Seq[InternalRow]] = None)
    extends ShuffleWriter[K, V]
    with CometBaseNativeShuffleWriter
    with Logging {

  private val OFFSET_LENGTH = 8

  var partitionLengths: Array[Long] = _
  var mapStatus: MapStatus = _

  override def write(inputs: Iterator[Product2[K, V]]): Unit = {
    val shuffleBlockResolver =
      SparkEnv.get.shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
    val dataFile = shuffleBlockResolver.getDataFile(shuffleId, mapId)
    val indexFile = shuffleBlockResolver.getIndexFile(shuffleId, mapId)
    val tempDataFilename = dataFile.getPath.replace(".data", ".data.tmp")
    val tempIndexFilename = indexFile.getPath.replace(".index", ".index.tmp")
    val tempDataFilePath = Paths.get(tempDataFilename)
    val tempIndexFilePath = Paths.get(tempIndexFilename)

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
      "data_size" -> metrics("dataSize"),
      "write_time" -> metricsWriteTime) ++
      metrics.filterKeys(detailedMetrics.contains)

    nativeWrite(inputs, shuffleWriterSQLMetrics, tempDataFilename, tempIndexFilename)

    // get partition lengths from shuffle write output index file
    var offset = 0L
    partitionLengths = Files
      .readAllBytes(tempIndexFilePath)
      .grouped(OFFSET_LENGTH)
      .drop(1) // first partition offset is always 0
      .map(indexBytes => {
        val partitionOffset =
          ByteBuffer.wrap(indexBytes).order(ByteOrder.LITTLE_ENDIAN).getLong
        val partitionLength = partitionOffset - offset
        offset = partitionOffset
        partitionLength
      })
      .toArray
    Files.delete(tempIndexFilePath)

    // Total written bytes at native
    metricsReporter.incBytesWritten(Files.size(tempDataFilePath))
    metricsReporter.incRecordsWritten(metricsOutputRows.value)
    metricsReporter.incWriteTime(metricsWriteTime.value)

    // Report spill metrics to Spark's task metrics so they appear in
    // Spark UI task summaries (not just SQL metrics)
    val spilledBytes = shuffleWriterSQLMetrics.get("spilled_bytes").map(_.value).getOrElse(0L)
    if (spilledBytes > 0) {
      context.taskMetrics().incMemoryBytesSpilled(spilledBytes)
      context.taskMetrics().incDiskBytesSpilled(spilledBytes)
    }

    // commit
    shuffleBlockResolver.writeMetadataFileAndCommit(
      shuffleId,
      mapId,
      partitionLengths,
      Array.empty, // TODO: add checksums
      tempDataFilePath.toFile)
    mapStatus =
      MapStatus.apply(SparkEnv.get.blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (success) {
      Some(mapStatus)
    } else {
      None
    }
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths
}
