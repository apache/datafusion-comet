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

/**
 * Apache DataFusion Comet's primary execution iterator that bridges JVM (Spark) and native
 * (Rust) execution environments. This iterator orchestrates native query execution on Arrow
 * columnar batches while managing sophisticated memory ownership semantics across the JNI boundary.
 *
 * '''Architecture Overview:'''
 * 1. Consumes input ColumnarBatch iterators from Spark operators
 * 2. Transfers Arrow array ownership to native DataFusion execution engine via JNI
 * 3. Executes queries natively using DataFusion's columnar processing
 * 4. Returns results as ColumnarBatch with ownership transferred back to JVM
 *
 * '''Memory Management:'''
 * This class implements a sophisticated ownership transfer pattern to prevent memory leaks
 * and ensure thread safety:
 * - '''Single Ownership''': Only one owner of Arrow array data at any time
 * - '''Transfer Semantics''': Ownership flows JVM → Native → JVM during processing
 * - '''Automatic Cleanup''': Previous batches are automatically released on next() calls
 * - '''Exception Safety''': Resources are cleaned up even during exceptions
 *
 * '''Critical Usage Pattern:'''
 * {{{scala
 * // ALWAYS use try-finally or RAII pattern
 * val iterator = new CometExecIterator(...)
 * try {
 *   while (iterator.hasNext) {
 *     val batch = iterator.next()  // Previous batch auto-released
 *     process(batch)               // Process immediately, don't store
 *   }
 * } finally {
 *   iterator.close()  // CRITICAL: Must call close() for proper cleanup
 * }
 * }}}
 *
 * '''Thread Safety:'''
 * This class is '''NOT thread-safe''' by design. Each iterator instance should only be
 * accessed from a single thread. Concurrent access will cause race conditions and memory
 * corruption.
 *
 * '''Memory Leak Prevention:'''
 * The iterator automatically manages Arrow array lifecycle:
 * - `hasNext()` releases previous batch before preparing next
 * - `next()` releases current batch before returning new one
 * - `close()` ensures all resources are properly cleaned up
 *
 * '''Error Handling:'''
 * Native exceptions are automatically mapped to appropriate Spark exceptions:
 * - File not found errors → SparkException with FileNotFoundException cause
 * - Parquet errors → SparkException with Parquet-specific error details
 *
 * @param id Unique identifier for this execution context (used for native plan tracking)
 * @param inputs Sequence of input ColumnarBatch iterators from upstream Spark operators
 * @param numOutputCols Number of columns in the output schema
 * @param protobufQueryPlan Serialized Spark physical plan as protocol buffer bytes
 * @param nativeMetrics Metrics collection node for native execution statistics
 * @param numParts Total number of partitions in the query
 * @param partitionIndex Zero-based index of the partition this iterator processes
 *
 * @note '''Memory Leak False Positives''': The allocator may report memory leaks for
 *       ArrowArray and ArrowSchema structs. These are false positives because the native
 *       side releases memory through Arrow's C Data Interface release callbacks, which
 *       the JVM allocator doesn't track.
 *
 * @see [[CometBatchIterator]] for the underlying batch iteration mechanism
 * @see [[org.apache.comet.vector.NativeUtil]] for Arrow array import/export utilities
 *
 * @since 0.1.0
 * @author Apache DataFusion Comet Team
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

  /**
   * Checks if there are more batches available from the native execution engine.
   *
   * '''Memory Management:'''
   * This method implements critical memory safety by automatically releasing the previous
   * batch before preparing the next one. This prevents memory leaks and ensures proper
   * ownership transfer across the JNI boundary.
   *
   * '''Execution Flow:'''
   * 1. Returns immediately if iterator is closed
   * 2. Returns true if next batch is already prepared
   * 3. Releases previous batch to prevent memory leaks
   * 4. Calls native execution engine to prepare next batch
   * 5. Automatically closes iterator if no more batches available
   *
   * '''Side Effects:'''
   * - May trigger native query execution for blocking operators (Sort, Aggregate)
   * - Releases memory from previous batch
   * - May close the iterator if execution is complete
   *
   * @return true if more batches are available, false if execution is complete
   *
   * @note This method may trigger expensive native operations for blocking operators.
   *       Consider this when implementing backpressure or timeout mechanisms.
   */
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

    if (nextBatch.isEmpty) {
      close()
      false
    } else {
      true
    }
  }

  /**
   * Returns the next ColumnarBatch from native execution with proper memory ownership transfer.
   *
   * '''Memory Ownership:'''
   * - Releases the current batch before returning the next one
   * - Transfers ownership of the returned batch to the caller
   * - Caller becomes responsible for batch lifecycle management
   *
   * '''Usage Pattern:'''
   * {{{scala
   * while (iterator.hasNext) {
   *   val batch = iterator.next()  // Ownership transferred to caller
   *   process(batch)               // Process immediately
   *   // Batch automatically released on next iteration
   * }
   * }}}
   *
   * '''Memory Safety:'''
   * - Previous batch is eagerly released to prevent memory leaks
   * - Returned batch uses fresh Arrow arrays with clear ownership
   * - No risk of use-after-free as old batch is nullified
   *
   * @return ColumnarBatch containing Arrow arrays with transferred ownership
   * @throws NoSuchElementException if no more elements are available (call hasNext first)
   *
   * @note '''Critical''': Do not store references to returned batches across iterations.
   *       Process each batch immediately to prevent memory leaks.
   */
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

  /**
   * Releases all resources associated with this iterator including native memory and JNI handles.
   *
   * '''Resource Cleanup:'''
   * - Closes current ColumnarBatch and releases Arrow arrays
   * - Closes NativeUtil and associated Arrow allocators
   * - Releases native execution plan and associated memory
   * - Marks iterator as closed to prevent further operations
   *
   * '''Memory Leak Prevention:'''
   * This method is critical for preventing memory leaks. The native side holds references
   * to Arrow arrays and execution context that must be explicitly released.
   *
   * '''False Positive Warnings:'''
   * The JVM allocator may report memory leaks for ArrowArray and ArrowSchema structs.
   * These are false positives because:
   * - Native code releases memory through Arrow's C Data Interface callbacks
   * - JVM allocator doesn't track native-side release operations
   * - Actual memory is properly freed, just not visible to JVM accounting
   *
   * '''Thread Safety:'''
   * This method is synchronized to prevent race conditions during cleanup.
   * Multiple threads calling close() concurrently will be serialized safely.
   *
   * '''Usage:'''
   * {{{scala
   * val iterator = new CometExecIterator(...)
   * try {
   *   // use iterator
   * } finally {
   *   iterator.close()  // ALWAYS call in finally block
   * }
   * }}}
   *
   * @note This method is idempotent - multiple calls are safe but only the first has effect.
   */
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
