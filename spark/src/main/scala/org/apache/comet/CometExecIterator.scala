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

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.comet.CometMetricNode
import org.apache.spark.sql.vectorized._

import org.apache.comet.CometConf.{COMET_BATCH_SIZE, COMET_DEBUG_ENABLED, COMET_EXEC_MEMORY_POOL_TYPE, COMET_EXPLAIN_NATIVE_ENABLED, COMET_METRICS_UPDATE_INTERVAL}
import org.apache.comet.Tracing.withTrace
import org.apache.comet.vector.NativeUtil

// scalastyle:off

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
 */
class CometExecIterator(
    val id: Long,
    inputs: Seq[Iterator[ColumnarBatch]],
    numOutputCols: Int,
    protobufQueryPlan: Array[Byte],
    nativeMetrics: CometMetricNode,
    numParts: Int,
    partitionIndex: Int)
    extends Iterator[ColumnarBatch]
    with Logging {

  private val tracingEnabled = CometConf.COMET_TRACING_ENABLED.get()
  private val memoryMXBean = ManagementFactory.getMemoryMXBean
  private val nativeLib = new Native()
  private val nativeUtil = new NativeUtil()
  private val cometTaskMemoryManager = new CometTaskMemoryManager(id)
  private val cometBatchIterators = inputs.map { iterator =>
    new CometBatchIterator(iterator, nativeUtil)
  }.toArray
  private val plan = {
    val conf = SparkEnv.get.conf
    val localDiskDirs = SparkEnv.get.blockManager.getLocalDiskDirs

    val offHeapMode = CometSparkSessionExtensions.isOffHeapEnabled(conf)
    val memoryLimit = if (offHeapMode) {
      // in unified mode we share off-heap memory with Spark
      ByteUnit.MiB.toBytes(conf.getSizeAsMb("spark.memory.offHeap.size"))
    } else {
      // we'll use the built-in memory pool from DF, and initializes with `memory_limit`
      // and `memory_fraction` below.
      CometSparkSessionExtensions.getCometMemoryOverhead(conf)
    }
    nativeLib.createPlan(
      id,
      cometBatchIterators,
      protobufQueryPlan,
      numParts,
      nativeMetrics,
      metricsUpdateInterval = COMET_METRICS_UPDATE_INTERVAL.get(),
      cometTaskMemoryManager,
      localDiskDirs,
      batchSize = COMET_BATCH_SIZE.get(),
      offHeapMode,
      memoryPoolType = COMET_EXEC_MEMORY_POOL_TYPE.get(),
      memoryLimit,
      memoryLimitPerTask = getMemoryLimitPerTask(conf),
      taskAttemptId = TaskContext.get().taskAttemptId,
      debug = COMET_DEBUG_ENABLED.get(),
      explain = COMET_EXPLAIN_NATIVE_ENABLED.get(),
      tracingEnabled)
  }

  private var nextBatch: Option[ColumnarBatch] = None
  private var prevBatch: ColumnarBatch = null
  private var currentBatch: ColumnarBatch = null
  private var closed: Boolean = false

  private def getMemoryLimitPerTask(conf: SparkConf): Long = {
    val numCores = numDriverOrExecutorCores(conf).toFloat
    val maxMemory = CometSparkSessionExtensions.getCometMemoryOverhead(conf)
    val coresPerTask = conf.get("spark.task.cpus", "1").toFloat
    // example 16GB maxMemory * 16 cores with 4 cores per task results
    // in memory_limit_per_task = 16 GB * 4 / 16 = 16 GB / 4 = 4GB
    val limit = (maxMemory.toFloat * coresPerTask / numCores).toLong
    logInfo(
      s"Calculated per-task memory limit of $limit ($maxMemory * $coresPerTask / $numCores)")
    limit
  }

  private def numDriverOrExecutorCores(conf: SparkConf): Int = {
    def convertToInt(threads: String): Int = {
      if (threads == "*") Runtime.getRuntime.availableProcessors() else threads.toInt
    }
    val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r
    val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r
    val master = conf.get("spark.master")
    master match {
      case "local" => 1
      case LOCAL_N_REGEX(threads) => convertToInt(threads)
      case LOCAL_N_FAILURES_REGEX(threads, _) => convertToInt(threads)
      case _ => conf.get("spark.executor.cores", "1").toInt
    }
  }

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
      println("[" + Thread.currentThread.getId + "] CometExecIterator closing batch")
      prevBatch.close()
      prevBatch = null
    }

    nextBatch = getNextBatch

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
      println("[" + Thread.currentThread.getId + "] CometExecIterator closing batch")
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
        println("[" + Thread.currentThread.getId + "] CometExecIterator closing batch")
        currentBatch.close()
        currentBatch = null
      }
      nativeUtil.close()
      nativeLib.releasePlan(plan)

      if (tracingEnabled) {
        traceMemoryUsage()
      }

      // The allocator thoughts the exported ArrowArray and ArrowSchema structs are not released,
      // so it will report:
      // Caused by: java.lang.IllegalStateException: Memory was leaked by query.
      // Memory leaked: (516) Allocator(ROOT) 0/516/808/9223372036854775807 (res/actual/peak/limit)
      // Suspect this seems a false positive leak, because there is no reported memory leak at JVM
      // when profiling. `allocator` reports a leak because it calculates the accumulated number
      // of memory allocated for ArrowArray and ArrowSchema. But these exported ones will be
      // released in native side later.
      // More to clarify it. For ArrowArray and ArrowSchema, Arrow will put a release field into the
      // memory region which is a callback function pointer (C function) that could be called to
      // release these structs in native code too. Once we wrap their memory addresses at native
      // side using FFI ArrowArray and ArrowSchema, and drop them later, the callback function will
      // be called to release the memory.
      // But at JVM, the allocator doesn't know about this fact so it still keeps the accumulated
      // number.
      // Tried to manually do `release` and `close` that can make the allocator happy, but it will
      // cause JVM runtime failure.

      // allocator.close()
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
