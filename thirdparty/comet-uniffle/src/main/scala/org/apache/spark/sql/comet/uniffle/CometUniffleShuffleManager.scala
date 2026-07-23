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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.executor.{ShuffleReadMetrics, ShuffleWriteMetrics}
import org.apache.spark.shuffle.{RssShuffleHandle, RssShuffleManager, RssSparkConfig, RssSparkShuffleUtils, ShuffleHandle, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.handle.{ShuffleHandleInfo, SimpleShuffleHandleInfo}
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleDependency}
import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking
import org.apache.uniffle.common.ShuffleServerInfo
import org.apache.uniffle.common.exception.{RssException, RssFetchFailedException}
import org.apache.uniffle.common.util.RssUtils
import org.apache.uniffle.shaded.com.google.common.collect.Sets
import org.apache.uniffle.shaded.org.roaringbitmap.longlong.Roaring64NavigableMap

import org.apache.comet.CometConf

private[uniffle] object CometUniffleShuffleManager {
  private[uniffle] class ReadMetrics(reporter: ShuffleReadMetricsReporter)
      extends ShuffleReadMetrics {
    override def incRemoteBytesRead(v: Long): Unit = reporter.incRemoteBytesRead(v)

    override def incFetchWaitTime(v: Long): Unit = reporter.incFetchWaitTime(v)

    override def incRecordsRead(v: Long): Unit = reporter.incRecordsRead(v)
  }

  private[uniffle] def cometRssSparkConf(sparkConf: SparkConf): SparkConf = sparkConf.clone
    .set(RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssSparkConfig.RSS_ROW_BASED.key, "false")
    .set("spark.rss.client.io.compression.codec", "NONE")
}

class CometUniffleShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends RssShuffleManager(conf, isDriver) {

  import CometUniffleShuffleManager._

  assert(
    conf.get(CometConf.COMET_SHUFFLE_MODE.key, "") == "native",
    "CometUniffleShuffleManager only supports native shuffle mode")

  override def getReaderImpl[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter,
      taskIdBitmap: Roaring64NavigableMap): ShuffleReader[K, C] = {
    if (!handle.isInstanceOf[RssShuffleHandle[K, _, C]]) {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass.getName)
    }
    val rssShuffleHandle =
      handle.asInstanceOf[RssShuffleHandle[K, _, C]]

    rssShuffleHandle.getDependency match {
      case dependency: CometShuffleDependency[_, _, _]
          if dependency.shuffleType == CometNativeShuffle =>
        val partitionNum = rssShuffleHandle.getDependency.partitioner.numPartitions
        val shuffleId = rssShuffleHandle.getShuffleId
        var shuffleHandleInfo: ShuffleHandleInfo = null
        if (this.shuffleManagerRpcServiceEnabled && this.rssStageRetryForWriteFailureEnabled) {
          shuffleHandleInfo = this.getRemoteShuffleHandleInfoWithStageRetry(
            context.stageId,
            context.stageAttemptNumber,
            shuffleId,
            false)
        } else if (this.shuffleManagerRpcServiceEnabled && this.partitionReassignEnabled) {
          shuffleHandleInfo = this.getRemoteShuffleHandleInfoWithBlockRetry(
            context.stageId,
            context.stageAttemptNumber,
            shuffleId,
            false)
        } else {
          shuffleHandleInfo = new SimpleShuffleHandleInfo(
            shuffleId,
            rssShuffleHandle.getPartitionToServers,
            rssShuffleHandle.getRemoteStorage)
        }
        val serverToPartitions =
          getPartitionDataServers(shuffleHandleInfo, startPartition, endPartition)
        val blockIdBitmap = this.getShuffleResultForMultiPart(
          this.clientType,
          serverToPartitions,
          rssShuffleHandle.getAppId,
          shuffleId,
          context.stageAttemptNumber,
          shuffleHandleInfo.createPartitionReplicaTracking)
        val readMetrics =
          if (metrics != null) new ReadMetrics(metrics)
          else context.taskMetrics.shuffleReadMetrics
        val rssSparkConf = cometRssSparkConf(sparkConf)
        val shuffleRemoteStorageInfo = rssShuffleHandle.getRemoteStorage
        val shuffleRemoteStoragePath = shuffleRemoteStorageInfo.getPath
        val readerHadoopConf = RssSparkShuffleUtils.getRemoteStorageHadoopConf(
          this.sparkConf,
          shuffleRemoteStorageInfo)
        new CometUniffleShuffleReader[K, C](
          startPartition,
          endPartition,
          startMapIndex,
          endMapIndex,
          context,
          rssShuffleHandle,
          shuffleRemoteStoragePath,
          readerHadoopConf,
          partitionNum,
          RssUtils.generatePartitionToBitmap(
            blockIdBitmap,
            startPartition,
            endPartition,
            this.blockIdLayout),
          taskIdBitmap,
          readMetrics,
          this.managerClientSupplier,
          RssSparkConfig.toRssConf(rssSparkConf),
          this.dataDistributionType,
          shuffleHandleInfo.getAllPartitionServersForReader)
      case _ =>
        super.getReaderImpl(
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics,
          taskIdBitmap)
    }
  }

  private def getShuffleResultForMultiPart(
      clientType: String,
      serverToPartitions: util.Map[ShuffleServerInfo, util.Set[Integer]],
      appId: String,
      shuffleId: Int,
      stageAttemptId: Int,
      replicaRequirementTracking: PartitionDataReplicaRequirementTracking) = {
    val failedPartitions: util.Set[Integer] = Sets.newHashSet[Integer]()
    try {
      this.shuffleWriteClient.getShuffleResultForMultiPart(
        clientType,
        serverToPartitions,
        appId,
        shuffleId,
        failedPartitions,
        replicaRequirementTracking)
    } catch {
      case e: RssFetchFailedException =>
        throw RssSparkShuffleUtils.reportRssFetchFailedException(
          this.managerClientSupplier,
          e,
          this.sparkConf,
          appId,
          shuffleId,
          stageAttemptId,
          failedPartitions)
    }
  }

  private def getPartitionDataServers(
      shuffleHandleInfo: ShuffleHandleInfo,
      startPartition: Int,
      endPartition: Int): util.Map[ShuffleServerInfo, util.Set[Integer]] = {
    val allPartitionToServers = shuffleHandleInfo.getAllPartitionServersForReader.asScala
    val requirePartitionToServers =
      allPartitionToServers.filter(x => x._1 >= startPartition && x._1 < endPartition).asJava
    val serverToPartitions = RssUtils.generateServerToPartitions(requirePartitionToServers)
    serverToPartitions
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    if (!handle.isInstanceOf[RssShuffleHandle[_, _, _]]) {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass.getName)
    }
    val rssHandle: RssShuffleHandle[K, V, V] = handle.asInstanceOf[RssShuffleHandle[K, V, V]]

    rssHandle.getDependency match {
      case dependency: CometShuffleDependency[_, _, _]
          if dependency.shuffleType == CometNativeShuffle =>
        setPusherAppId(rssHandle)
        val taskId: String = context.taskAttemptId + "_" + context.attemptNumber
        var writeMetrics: ShuffleWriteMetrics = null
        if (metrics != null) {
          writeMetrics = new RssShuffleManager.WriteMetrics(metrics)
        } else {
          writeMetrics = context.taskMetrics.shuffleWriteMetrics
        }
        val conf = cometRssSparkConf(sparkConf)
        val rssTaskAttemptId =
          getTaskAttemptIdForBlockId(context.partitionId(), context.attemptNumber())

        CometUniffleShuffleWriter[K, V, V](
          dependency.nativeShuffleSpec.get,
          dependency.outputPartitioning.get,
          dependency.outputAttributes,
          dependency.numParts,
          dependency.shuffleWriteMetrics,
          rssHandle.getAppId,
          rssHandle.getShuffleId,
          taskId,
          rssTaskAttemptId,
          writeMetrics,
          this,
          conf,
          shuffleWriteClient,
          rssHandle,
          (taskId: String) => markFailedTask(taskId),
          context,
          dependency.rangePartitionBounds)
      case dependency: CometShuffleDependency[_, _, _] =>
        throw new RssException("Unexpected shuffleType:" + dependency.shuffleType)
      case _ =>
        super.getWriter(handle, mapId, context, metrics)
    }

  }

}
