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

import java.nio.{ByteBuffer, ByteOrder}
import java.util
import java.util.{Collections, Optional}
import java.util.function.Supplier

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{InterruptibleIterator, TaskContext}
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{RssShuffleHandle, RssSparkConfig}
import org.apache.spark.shuffle.reader.RssShuffleReader
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleDependency
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.CompletionIterator
import org.apache.uniffle.client.api.{ShuffleManagerClient, ShuffleReadClient}
import org.apache.uniffle.client.factory.ShuffleClientFactory
import org.apache.uniffle.client.response.ShuffleBlock
import org.apache.uniffle.common.{ShuffleDataDistributionType, ShuffleServerInfo}
import org.apache.uniffle.common.compression.Codec
import org.apache.uniffle.common.config.RssConf
import org.apache.uniffle.shaded.org.roaringbitmap.longlong.Roaring64NavigableMap
import org.apache.uniffle.storage.handler.impl.ShuffleServerReadCostTracker

import org.apache.comet.Native
import org.apache.comet.shuffle.{CometNativeShuffleReader, CometShuffleBlockIterator}
import org.apache.comet.vector.NativeUtil

class CometUniffleShuffleReader[K, C](
    startPartition: Int,
    endPartition: Int,
    mapStartIndex: Int,
    mapEndIndex: Int,
    context: TaskContext,
    rssShuffleHandle: RssShuffleHandle[K, _, C],
    basePath: String,
    hadoopConf: Configuration,
    partitionNum: Int,
    partitionToExpectBlocks: util.Map[Integer, Roaring64NavigableMap],
    taskIdBitmap: Roaring64NavigableMap,
    readMetrics: ShuffleReadMetrics,
    managerClientSupplier: Supplier[ShuffleManagerClient],
    rssConf: RssConf,
    dataDistributionType: ShuffleDataDistributionType,
    allPartitionToServers: util.Map[Integer, util.List[ShuffleServerInfo]])
    extends RssShuffleReader[K, C](
      startPartition,
      endPartition,
      mapStartIndex,
      mapEndIndex,
      context,
      rssShuffleHandle,
      basePath,
      hadoopConf,
      partitionNum,
      partitionToExpectBlocks,
      taskIdBitmap,
      readMetrics,
      managerClientSupplier,
      rssConf,
      dataDistributionType,
      allPartitionToServers)
    with CometNativeShuffleReader {

  private val shuffleServerReadCostTracker = new ShuffleServerReadCostTracker
  private val dep = rssShuffleHandle.getDependency.asInstanceOf[CometShuffleDependency[_, _, _]]

  override def read(): Iterator[Product2[K, C]] = {
    val nativeLib = new Native()
    val nativeUtil = new NativeUtil()

    val shuffleBlockIterator = new CometUniffleShuffleBlockIterator()

    context.addTaskCompletionListener[Unit] { _ =>
      shuffleBlockIterator.close()
      nativeUtil.close()
    }

    val recordIter: Iterator[(Int, ColumnarBatch)] = new Iterator[(Int, ColumnarBatch)]
      with AutoCloseable {
      private var currentBatch: ColumnarBatch = null

      // To avoid calling hasNext() multiple times for the same iterator,
      // we cache the result of the last hasNext() call.
      private var lastHasNext: Option[Boolean] = None
      override def hasNext: Boolean = {
        if (lastHasNext.isDefined) {
          return lastHasNext.get
        }
        lastHasNext = Some(shuffleBlockIterator.hasNext != -1)
        lastHasNext.get
      }

      override def next(): (Int, ColumnarBatch) = {
        lastHasNext = None
        if (currentBatch != null) {
          currentBatch.close()
        }

        val dataBuf = shuffleBlockIterator.getBuffer
        val bytesToRead = shuffleBlockIterator.getCurrentBlockLength
        val fieldCount = shuffleBlockIterator.currentBatchFieldCount

        currentBatch = nativeUtil.getNextBatch(
          fieldCount,
          (arrayAddrs, schemaAddrs) => {
            nativeLib.decodeShuffleBlock(
              dataBuf,
              bytesToRead,
              arrayAddrs,
              schemaAddrs,
              tracingEnabled = false)
          }) match {
          case Some(batch) => batch
          case None =>
            throw new IllegalStateException(
              "Unexpected end of shuffle data while reading block of length " + bytesToRead)
        }

        (0, currentBatch)
      }

      override def close(): Unit = {
        if (currentBatch != null) {
          currentBatch.close()
          currentBatch = null
        }
        shuffleBlockIterator.close()
      }
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(record._2.numRows())
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      throw new UnsupportedOperationException("aggregate not allowed")
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(_: Ordering[K]) =>
        throw new UnsupportedOperationException("order not allowed")
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

  private def createShuffleReadClient(partition: Int): ShuffleReadClient = {
    if (!partitionToExpectBlocks.containsKey(partition)
      || partitionToExpectBlocks.get(partition).isEmpty) {
      return null
    }

    val shuffleServerInfoList = allPartitionToServers.get(partition)
    if (shuffleServerInfoList != null && shuffleServerInfoList.size > 1 && rssConf.getBoolean(
        RssSparkConfig.RSS_READ_REORDER_MULTI_SERVERS_ENABLED)) {
      Collections.shuffle(shuffleServerInfoList)
    }

    val isReplicaFilterEnabled =
      rssConf.getInteger("rss.data.replica", 1) > 1 && shuffleServerInfoList.size > 1
    val expectedTaskIdsBitmapFilterEnable =
      mapStartIndex != 0 || mapEndIndex != Integer.MAX_VALUE || isReplicaFilterEnabled
    val retryMax = rssConf.getInteger("rss.client.retry.max", 50)
    val retryIntervalMax = rssConf.getLong("rss.client.retry.interval.max", 10000L)
    val compress = rssConf.getBoolean("spark.shuffle.compress".substring("spark.".length), true)
    val codec =
      if (compress) Codec.newInstance(rssConf)
      else Optional.empty
    val builder = ShuffleClientFactory.newReadBuilder
      .readCostTracker(shuffleServerReadCostTracker)
      .appId(rssShuffleHandle.getAppId)
      .shuffleId(rssShuffleHandle.getDependency.shuffleId)
      .partitionId(partition)
      .basePath(basePath)
      .partitionNumPerRange(1)
      .partitionNum(partitionNum)
      .blockIdBitmap(partitionToExpectBlocks.get(partition))
      .taskIdBitmap(taskIdBitmap)
      .shuffleServerInfoList(shuffleServerInfoList)
      .hadoopConf(hadoopConf)
      .shuffleDataDistributionType(dataDistributionType)
      .expectedTaskIdsBitmapFilterEnable(expectedTaskIdsBitmapFilterEnable)
      .retryMax(retryMax)
      .retryIntervalMax(retryIntervalMax)
      .rssConf(rssConf)
      .taskAttemptId(context.taskAttemptId())
    if (codec.isPresent && rssConf
        .get(RssSparkConfig.RSS_READ_OVERLAPPING_DECOMPRESSION_ENABLED)
        .asInstanceOf[Boolean]) {
      builder
        .overlappingDecompressionEnabled(true)
        .codec(codec.get)
        .overlappingDecompressionThreadNum(
          rssConf.get(RssSparkConfig.RSS_READ_OVERLAPPING_DECOMPRESSION_THREADS))
    }

    ShuffleClientFactory.getInstance.createShuffleReadClient(builder)
  }

  override def readAsShuffleBlockIterator(): CometShuffleBlockIterator = {
    new CometUniffleShuffleBlockIterator()
  }

  class CometUniffleShuffleBlockIterator extends CometShuffleBlockIterator with Logging {
    private var currentPartition: Int = startPartition
    private var current: ByteBuffer = _
    private var currentShuffleReadClient: ShuffleReadClient = createShuffleReadClient(
      currentPartition)

    private val headerBuf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
    private val INITIAL_BUFFER_SIZE = 128 * 1024
    private var dataBuf = ByteBuffer.allocateDirect(INITIAL_BUFFER_SIZE)
    private var currentBlockLength = 0

    var currentBatchFieldCount: Int = _

    override def hasNext: Int = {
      if (current == null || !current.hasRemaining) {
        val nextBlock = nextShuffleBlock()
        if (nextBlock.isEmpty) {
          return -1
        }
        current = nextBlock.get
      }

      val oldLimit = current.limit()
      try {
        current.limit(current.position() + headerBuf.capacity())
        headerBuf.clear()
        headerBuf.put(current)
        headerBuf.flip()
      } finally {
        current.limit(oldLimit)
      }

      val compressedLength = headerBuf.getLong
      currentBatchFieldCount = headerBuf.getLong.toInt

      // Subtract 8 because compressedLength includes the 8-byte field count we already read
      val bytesToRead = compressedLength - 8
      if (bytesToRead > Integer.MAX_VALUE) {
        throw new IllegalStateException(
          "Native shuffle block size of " + bytesToRead + " exceeds maximum of "
            + Integer.MAX_VALUE + ". Try reducing spark.comet.columnar.shuffle.batch.size.")
      }
      currentBlockLength = bytesToRead.toInt

      if (dataBuf.capacity < currentBlockLength) {
        val newCapacity = Math.min(bytesToRead * 2L, Integer.MAX_VALUE).toInt
        dataBuf = ByteBuffer.allocateDirect(newCapacity)
      }
      dataBuf.clear
      dataBuf.limit(currentBlockLength)

      while (dataBuf.hasRemaining) {
        if (!current.hasRemaining) {
          val nextBlock = nextShuffleBlock()
          if (nextBlock.isEmpty) {
            throw new IllegalStateException(
              "Unexpected end of shuffle data while reading block of length " + currentBlockLength)
          }
          current = nextBlock.get
        }
        val readLimit = dataBuf.remaining()
        var oldLimit = -1
        try {
          if (readLimit < current.remaining()) {
            oldLimit = current.limit()
            current.limit(current.position() + readLimit)
          }
          dataBuf.put(current)
        } finally {
          if (oldLimit != -1) {
            current.limit(oldLimit)
          }
        }
      }

      currentBlockLength
    }

    private def nextShuffleBlock(): Option[ByteBuffer] = {
      val shuffleBlock: ShuffleBlock = if (currentShuffleReadClient != null) {
        currentShuffleReadClient.readShuffleBlockData
      } else {
        null
      }
      val rawData = if (shuffleBlock != null) {
        shuffleBlock.getByteBuffer
      } else {
        null
      }
      if (rawData == null) {
        if (currentShuffleReadClient != null) {
          currentShuffleReadClient.checkProcessedBlockIds()
          currentShuffleReadClient.logStatics()
          currentShuffleReadClient.close()
          currentShuffleReadClient = null
        }
        currentPartition += 1
        if (currentPartition >= endPartition) {
          return None
        }

        currentShuffleReadClient = createShuffleReadClient(currentPartition)
        return nextShuffleBlock()
      }
      Some(rawData)
    }

    override def getBuffer: ByteBuffer = dataBuf

    override def getCurrentBlockLength: Int = currentBlockLength

    override def close(): Unit = {
      if (current != null) {
        current = null
      }
      if (dataBuf != null) {
        dataBuf = ByteBuffer.allocateDirect(INITIAL_BUFFER_SIZE)
      }
      if (currentShuffleReadClient != null) {
        currentShuffleReadClient.close()
        currentShuffleReadClient = null
      }
    }
  }
}
