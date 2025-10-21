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

package org.apache.comet

import java.io.FileNotFoundException
import java.lang.management.ManagementFactory

import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.comet.CometMetricNode
import org.apache.spark.sql.vectorized._
import org.apache.spark.util.SerializableConfiguration

import org.apache.comet.CometConf._
import org.apache.comet.Tracing.withTrace
import org.apache.comet.parquet.CometFileKeyUnwrapper
import org.apache.comet.serde.Config.ConfigMap
import org.apache.comet.vector.NativeUtil

/**
 * An iterator class used to execute Comet native query. It takes an input iterator which comes
 * from Comet Scan and is expected to produce batches of Arrow Arrays. During consuming this
 * iterator, it will consume input iterator and pass Arrow Arrays to Comet native engine by
 * addresses. Even after the end of input iterator, this iterator still possibly continues
 * executing native query as there might be blocking operators such as Sort, Aggregate. The API
 * `hasNext` can be used to check if it is the end of this iterator (i.e. the native query is
 * done).
 *
 * @param inputs
 *   The input iterators producing sequence of batches of Arrow Arrays.
 * @param protobufQueryPlan
 *   The serialized bytes of Spark execution plan.
 * @param numParts
 *   The number of partitions.
 * @param partitionIndex
 *   The index of the partition.
 * @param encryptedFilePaths
 *   Paths to encrypted Parquet files that need key unwrapping.
 */
class CometExecIterator(
    val id: Long,
    inputs: Seq[Iterator[ColumnarBatch]],
    numOutputCols: Int,
    protobufQueryPlan: Array[Byte],
    nativeMetrics: CometMetricNode,
    numParts: Int,
    partitionIndex: Int,
    broadcastedHadoopConfForEncryption: Option[Broadcast[SerializableConfiguration]] = None,
    encryptedFilePaths: Seq[String] = Seq.empty)
    extends Iterator[ColumnarBatch]
    with Logging {

  private val tracingEnabled = CometConf.COMET_TRACING_ENABLED.get()
  private val memoryMXBean = ManagementFactory.getMemoryMXBean
  private val nativeLib = new Native()
  private val nativeUtil = new NativeUtil()
  private val taskAttemptId = TaskContext.get().taskAttemptId
  private val cometTaskMemoryManager = new CometTaskMemoryManager(id, taskAttemptId)
  private val cometBatchIterators = inputs.map { iterator =>
    new CometBatchIterator(iterator, nativeUtil)
  }.toArray

  private val plan = {
    val conf = SparkEnv.get.conf
    val localDiskDirs = SparkEnv.get.blockManager.getLocalDiskDirs

    // serialize Comet related Spark configs in protobuf format
    val builder = ConfigMap.newBuilder()
    conf.getAll.filter(_._1.startsWith(CometConf.COMET_PREFIX)).foreach { case (k, v) =>
      builder.putEntries(k, v)
    }
    val protobufSparkConfigs = builder.build().toByteArray

    // Create keyUnwrapper if encryption is enabled
    val keyUnwrapper = if (encryptedFilePaths.nonEmpty) {
      val unwrapper = new CometFileKeyUnwrapper()
      val hadoopConf: Configuration = broadcastedHadoopConfForEncryption.get.value.value

      encryptedFilePaths.foreach(filePath =>
        unwrapper.storeDecryptionKeyRetriever(filePath, hadoopConf))

      unwrapper
    } else {
      null
    }

    val memoryConfig = CometExecIterator.getMemoryConfig(conf)

    nativeLib.createPlan(
      id,
      cometBatchIterators,
      protobufQueryPlan,
      protobufSparkConfigs,
      numParts,
      nativeMetrics,
      metricsUpdateInterval = COMET_METRICS_UPDATE_INTERVAL.get(),
      cometTaskMemoryManager,
      localDiskDirs,
      batchSize = COMET_BATCH_SIZE.get(),
      memoryConfig.offHeapMode,
      memoryConfig.memoryPoolType,
      memoryConfig.memoryLimit,
      memoryConfig.memoryLimitPerTask,
      taskAttemptId,
      keyUnwrapper)
  }

  private var nextBatch: Option[ColumnarBatch] = None
  private var prevBatch: ColumnarBatch = null
  private var currentBatch: ColumnarBatch = null
  private var closed: Boolean = false

  private def getNextBatch: Option[ColumnarBatch] = {
    assert(partitionIndex >= 0 && partitionIndex < numParts)

    if (tracingEnabled) {
      traceMemoryUsage()
    }

    val ctx = TaskContext.get()

    try {
      withTrace(
        s"getNextBatch[JVM] stage=${ctx.stageId()}",
        tracingEnabled, {
          nativeUtil.getNextBatch(
            numOutputCols,
            (arrayAddrs, schemaAddrs) => {
              nativeLib.executePlan(ctx.stageId(), partitionIndex, plan, arrayAddrs, schemaAddrs)
            })
        })
    } catch {
      case e: CometNativeException =>
        // it is generally considered bad practice to log and then rethrow an
        // exception, but it really helps debugging to be able to see which task
        // threw the exception, so we log the exception with taskAttemptId here
        logError(s"Native execution for task $taskAttemptId failed", e)

        val fileNotFoundPattern: Regex =
          ("""^External: Object at location (.+?) not found: No such file or directory """ +
            """\(os error \d+\)$""").r
        val parquetError: Regex =
          """^Parquet error: (?:.*)$""".r
        e.getMessage match {
          case fileNotFoundPattern(filePath) =>
            // See org.apache.spark.sql.errors.QueryExecutionErrors.readCurrentFileNotFoundError
            throw new SparkException(
              errorClass = "_LEGACY_ERROR_TEMP_2055",
              messageParameters = Map("message" -> e.getMessage),
              cause = new FileNotFoundException(filePath)
            ) // Can't use SparkFileNotFoundException because it's private.
          case parquetError() =>
            // See org.apache.spark.sql.errors.QueryExecutionErrors.failedToReadDataError
            // See org.apache.parquet.hadoop.ParquetFileReader for error message.
            throw new SparkException(
              errorClass = "_LEGACY_ERROR_TEMP_2254",
              messageParameters = Map("message" -> e.getMessage),
              cause = new SparkException("File is not a Parquet file.", e))
          case _ =>
            throw e
        }
      case e: Throwable =>
        throw e
    }
  }

  override def hasNext: Boolean = {
    if (closed) return false

    if (nextBatch.isDefined) {
      return true
    }

    // Close previous batch if any.
    // This is to guarantee safety at the native side before we overwrite the buffer memory
    // shared across batches in the native side.
    if (prevBatch != null) {
      prevBatch.close()
      prevBatch = null
    }

    nextBatch = getNextBatch

    logTrace(s"Task $taskAttemptId memory pool usage is ${cometTaskMemoryManager.getUsed} bytes")

    if (nextBatch.isEmpty) {
      close()
      false
    } else {
      true
    }
  }

  override def next(): ColumnarBatch = {
    if (currentBatch != null) {
      // Eagerly release Arrow Arrays in the previous batch
      currentBatch.close()
      currentBatch = null
    }

    if (nextBatch.isEmpty && !hasNext) {
      throw new NoSuchElementException("No more element")
    }

    currentBatch = nextBatch.get
    prevBatch = currentBatch
    nextBatch = None
    currentBatch
  }

  def close(): Unit = synchronized {
    if (!closed) {
      if (currentBatch != null) {
        currentBatch.close()
        currentBatch = null
      }
      nativeUtil.close()
      nativeLib.releasePlan(plan)

      if (tracingEnabled) {
        traceMemoryUsage()
      }

      val memInUse = cometTaskMemoryManager.getUsed
      if (memInUse != 0) {
        logWarning(s"CometExecIterator closed with non-zero memory usage : $memInUse")
      }

      closed = true
    }
  }

  private def traceMemoryUsage(): Unit = {
    nativeLib.logMemoryUsage("jvm_heapUsed", memoryMXBean.getHeapMemoryUsage.getUsed)
    val totalTaskMemory = cometTaskMemoryManager.internal.getMemoryConsumptionForThisTask
    val cometTaskMemory = cometTaskMemoryManager.getUsed
    val sparkTaskMemory = totalTaskMemory - cometTaskMemory
    val threadId = Thread.currentThread().getId
    nativeLib.logMemoryUsage(s"task_memory_comet_$threadId", cometTaskMemory)
    nativeLib.logMemoryUsage(s"task_memory_spark_$threadId", sparkTaskMemory)
  }
}

object CometExecIterator extends Logging {

  def getMemoryConfig(conf: SparkConf): MemoryConfig = {
    val numCores = numDriverOrExecutorCores(conf)
    val coresPerTask = conf.get("spark.task.cpus", "1").toInt
    // there are different paths for on-heap vs off-heap mode
    val offHeapMode = CometSparkSessionExtensions.isOffHeapEnabled(conf)
    if (offHeapMode) {
      // in off-heap mode, Comet uses unified memory management to share off-heap memory with Spark
      val offHeapSize = ByteUnit.MiB.toBytes(conf.getSizeAsMb("spark.memory.offHeap.size"))
      val memoryFraction = CometConf.COMET_OFFHEAP_MEMORY_POOL_FRACTION.get()
      val memoryLimit = (offHeapSize * memoryFraction).toLong
      val memoryLimitPerTask = (memoryLimit.toDouble * coresPerTask / numCores).toLong
      val memoryPoolType = COMET_OFFHEAP_MEMORY_POOL_TYPE.get()
      logInfo(
        s"memoryPoolType=$memoryPoolType, " +
          s"offHeapSize=${toMB(offHeapSize)}, " +
          s"memoryFraction=$memoryFraction, " +
          s"memoryLimit=${toMB(memoryLimit)}, " +
          s"memoryLimitPerTask=${toMB(memoryLimitPerTask)}")
      MemoryConfig(offHeapMode, memoryPoolType = memoryPoolType, memoryLimit, memoryLimitPerTask)
    } else {
      // we'll use the built-in memory pool from DF, and initializes with `memory_limit`
      // and `memory_fraction` below.
      val memoryLimit = CometSparkSessionExtensions.getCometMemoryOverhead(conf)
      // example 16GB maxMemory * 16 cores with 4 cores per task results
      // in memory_limit_per_task = 16 GB * 4 / 16 = 16 GB / 4 = 4GB
      val memoryLimitPerTask = (memoryLimit.toDouble * coresPerTask / numCores).toLong
      val memoryPoolType = COMET_ONHEAP_MEMORY_POOL_TYPE.get()
      logInfo(
        s"memoryPoolType=$memoryPoolType, " +
          s"memoryLimit=${toMB(memoryLimit)}, " +
          s"memoryLimitPerTask=${toMB(memoryLimitPerTask)}")
      MemoryConfig(offHeapMode, memoryPoolType = memoryPoolType, memoryLimit, memoryLimitPerTask)
    }
  }

  private def numDriverOrExecutorCores(conf: SparkConf): Int = {
    def convertToInt(threads: String): Int = {
      if (threads == "*") Runtime.getRuntime.availableProcessors() else threads.toInt
    }

    // If running in local mode, get number of threads from the spark.master setting.
    // See https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
    // for supported formats

    // `local[*]` means using all available cores and `local[2]` means using 2 cores.
    val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r
    // Also handle format `local[num-worker-threads, max-failures]
    val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r

    val master = conf.get("spark.master")
    master match {
      case "local" => 1
      case LOCAL_N_REGEX(threads) => convertToInt(threads)
      case LOCAL_N_FAILURES_REGEX(threads, _) => convertToInt(threads)
      case _ => conf.get("spark.executor.cores", "1").toInt
    }
  }

  private def toMB(n: Long): String = {
    s"${(n.toDouble / 1024.0 / 1024.0).toLong} MB"
  }
}

case class MemoryConfig(
    offHeapMode: Boolean,
    memoryPoolType: String,
    memoryLimit: Long,
    memoryLimitPerTask: Long)
