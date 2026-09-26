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

import java.lang.management.ManagementFactory
import java.util.Locale
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.arrow.c.ArrowArrayStream
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.comet.CometMetricNode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized._
import org.apache.spark.util.SerializableConfiguration

import org.apache.comet.CometConf._
import org.apache.comet.Tracing.withTrace
import org.apache.comet.exceptions.CometQueryExecutionException
import org.apache.comet.parquet.CometFileKeyUnwrapper
import org.apache.comet.serde.Config.ConfigMap
import org.apache.comet.shuffle.ShufflePartitionPusher
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
 * @param inputObjects
 *   Already-built native input slots, in scan-input order. Each slot is either an
 *   org.apache.arrow.c.ArrowArrayStream (consumed natively via from_raw against its
 *   memoryAddress) or a CometShuffleBlockIterator (consumed via the JNI block-iteration
 *   protocol).
 * @param protobufQueryPlan
 *   The serialized bytes of Spark execution plan.
 * @param numParts
 *   The number of partitions.
 * @param partitionIndex
 *   The index of the partition.
 * @param encryptedFilePaths
 *   Paths to encrypted Parquet files that need key unwrapping.
 * @param shufflePartitionPusher
 *   Optional task-owned callback that receives remote shuffle output.
 * @param capturePartitionOffsets
 *   Whether to read the shuffle writer's partition offsets when the plan reaches the end of its
 *   output, for a plan rooted at a native shuffle writer with a local destination. Remote shuffle
 *   reports its partition lengths through its pusher instead, so it leaves this false.
 */
class CometExecIterator(
    val id: Long,
    inputObjects: Array[Object],
    numOutputCols: Int,
    protobufQueryPlan: Array[Byte],
    nativeMetrics: CometMetricNode,
    numParts: Int,
    partitionIndex: Int,
    broadcastedHadoopConfForEncryption: Option[Broadcast[SerializableConfiguration]] = None,
    encryptedFilePaths: Seq[String] = Seq.empty,
    shuffleBlockIterators: Map[Int, CometShuffleBlockIterator] = Map.empty,
    taskFilePaths: Seq[String] = Seq.empty,
    shufflePartitionPusher: Option[ShufflePartitionPusher] = None,
    capturePartitionOffsets: Boolean = false)
    extends Iterator[ColumnarBatch]
    with Logging {

  private val tracingEnabled = CometConf.COMET_TRACING_ENABLED.get()
  private val memoryMXBean = ManagementFactory.getMemoryMXBean
  private val nativeLib = new Native()
  private val nativeUtil = new NativeUtil()
  private val taskAttemptId = TaskContext.get().taskAttemptId()
  private val taskCPUs = TaskContext.get().cpus()
  private val cometTaskMemoryManager = new CometTaskMemoryManager(id, taskAttemptId)

  private val plan = {
    val conf = SparkEnv.get.conf
    val localDiskDirs = SparkEnv.get.blockManager.getLocalDiskDirs

    // serialize Comet related Spark configs in protobuf format
    val protobufSparkConfigs = CometExecIterator.serializeCometSQLConfs()

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

    val createdPlan = nativeLib.createPlan(
      id,
      inputObjects,
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
      taskAttemptId,
      taskCPUs,
      keyUnwrapper,
      // Propagated to Tokio workers running JVM UDFs so they see this Spark task's
      // TaskContext and context ClassLoader. Read here because this class is only ever
      // constructed on a Spark task thread (see `taskAttemptId` above); a JNI-attached Tokio
      // worker has neither. See CometUdfBridge.evaluate.
      TaskContext.get(),
      Thread.currentThread().getContextClassLoader)

    // Bind task-owned callbacks separately to preserve the existing createPlan JNI signature.
    try {
      shufflePartitionPusher.foreach { pusher =>
        nativeLib.setShufflePartitionPusher(createdPlan, pusher)
      }
      createdPlan
    } catch {
      case failure: Throwable =>
        // The task-completion listener is not installed until iterator construction succeeds.
        try {
          nativeUtil.close()
        } catch {
          case closeFailure: Throwable => failure.addSuppressed(closeFailure)
        }

        // Native only takes ownership of Arrow streams during the first executePlan call.
        inputObjects.foreach {
          case stream: ArrowArrayStream =>
            try {
              stream.release()
            } catch {
              case releaseFailure: Throwable => failure.addSuppressed(releaseFailure)
            }
          case _ =>
        }

        shuffleBlockIterators.values.foreach { iterator =>
          try {
            iterator.close()
          } catch {
            case closeFailure: Throwable => failure.addSuppressed(closeFailure)
          }
        }

        try {
          nativeLib.releasePlan(createdPlan)
        } catch {
          case releaseFailure: Throwable => failure.addSuppressed(releaseFailure)
        }
        throw failure
    }
  }

  /** Set once by [[readPartitionOffsetsBeforeClose]]; `null` until then. */
  private var partitionOffsets: Array[Long] = _

  /**
   * Partition offsets from a native shuffle write, or `null` if this iterator was not built to
   * collect them or has not yet reached the end of its output.
   */
  def shufflePartitionOffsets: Array[Long] = partitionOffsets

  /**
   * Reads the shuffle writer's partition offsets out of the native plan, if this iterator was
   * built to collect them.
   *
   * This has to run at end of stream rather than after iteration finishes. The offsets live in
   * the native execution context; [[close]] releases that context, and [[hasNext]] calls
   * [[close]] as soon as the plan runs out of output. So the final [[hasNext]] is the last point
   * at which they can still be read.
   */
  private def readPartitionOffsetsBeforeClose(): Unit = {
    if (capturePartitionOffsets && partitionOffsets == null) {
      partitionOffsets = nativeLib.getShufflePartitionOffsets(plan)
    }
  }

  private var nextBatch: Option[ColumnarBatch] = None
  private var prevBatch: ColumnarBatch = null
  private var currentBatch: ColumnarBatch = null
  private var closed: Boolean = false

  // Register a task completion listener to ensure native resources are released
  // when the task is done.
  TaskContext.get().addTaskCompletionListener[Unit] { _ =>
    this.close()
  }

  CometExecIterator.startMemoryUsageLog()

  private def getNextBatch: Option[ColumnarBatch] = {
    assert(partitionIndex >= 0 && partitionIndex < numParts)

    val ctx = TaskContext.get()

    try {
      val result = withTrace(
        s"getNextBatch[JVM] stage=${ctx.stageId()}",
        tracingEnabled, {
          nativeUtil.getNextBatch(
            numOutputCols,
            (arrayAddrs, schemaAddrs) => {
              nativeLib.executePlan(ctx.stageId(), partitionIndex, plan, arrayAddrs, schemaAddrs)
            })
        })

      if (tracingEnabled) {
        traceMemoryUsage()
      }

      result
    } catch {
      // Handle CometQueryExecutionException with JSON payload first
      case e: CometQueryExecutionException =>
        logError(s"Native execution for task $taskAttemptId failed", e)
        throw SparkErrorConverter.convertToSparkException(e, taskFilePaths)

      case e: CometNativeException =>
        // it is generally considered bad practice to log and then rethrow an
        // exception, but it really helps debugging to be able to see which task
        // threw the exception, so we log the exception with taskAttemptId here
        logError(s"Native execution for task $taskAttemptId failed", e)
        throw e
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
      readPartitionOffsetsBeforeClose()
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
      closed = true

      // Attempt every resource's cleanup independently, so that one failure does not skip the
      // remaining resources: this close() is the only chance to release them, since `closed` is
      // already set and the task-completion retry is a no-op. The first failure is rethrown with
      // any later ones attached as suppressed exceptions.
      var failure: Throwable = null
      def attempt(cleanup: => Unit): Unit = {
        try {
          cleanup
        } catch {
          case t: Throwable =>
            if (failure == null) failure = t else failure.addSuppressed(t)
        }
      }

      attempt {
        if (currentBatch != null) {
          currentBatch.close()
          currentBatch = null
        }
      }
      attempt(nativeUtil.close())
      shuffleBlockIterators.values.foreach(it => attempt(it.close()))

      // Released last and exactly once, even if the teardown above failed: dropping the native
      // execution context frees this plan's task-shared memory pool reference and several JNI
      // global refs.
      attempt(nativeLib.releasePlan(plan))

      // Run the diagnostics even when teardown failed: a failed teardown is exactly when the
      // non-zero memory usage warning below is most informative.
      attempt {
        if (tracingEnabled) {
          traceMemoryUsage()
        }
      }

      attempt {
        val memInUse = cometTaskMemoryManager.getUsed
        if (memInUse != 0) {
          logWarning(s"CometExecIterator closed with non-zero memory usage : $memInUse")
        }
      }

      if (failure != null) {
        throw failure
      }
    }
  }

  private def traceMemoryUsage(): Unit = {
    nativeLib.logMemoryUsage("jvm_heap_used", memoryMXBean.getHeapMemoryUsage.getUsed)
    Tracing.logArrowMemory()
  }
}

object CometExecIterator extends Logging {

  private val memoryUsageLogStarted = new AtomicBoolean(false)

  /** Native plans running at the previous memory usage log. Only the log's own thread uses it. */
  private var plansAtLastMemoryUsageLog = 0L

  /**
   * Whether the native footprint exceeded the executor's native memory limit at the previous
   * memory usage log. Only the log's own thread uses it.
   */
  private var limitExceededAtLastLog = false

  /**
   * Starts the executor's native memory usage log when the first native plan is created, unless
   * `spark.comet.memory.logInterval` is 0.
   *
   * Both figures the log reports, the bytes the native allocator has handed out and the bytes
   * reserved in Comet's memory pools, are executor-wide, so one daemon thread logs one line per
   * interval for the whole executor, however many tasks are running. It runs on a timer rather
   * than between batches, because a plan can spend its whole run inside one `executePlan` call: a
   * plan rooted at a native shuffle writer consumes all of its input before it returns, and a
   * plan fed directly by native scans parks the task thread until its next batch is ready.
   */
  private def startMemoryUsageLog(): Unit = {
    if (memoryUsageLogStarted.compareAndSet(false, true)) {
      // Read from the executor's configuration rather than the session's, since the one log
      // serves every session on the executor.
      val conf = SparkEnv.get.conf
      val intervalMs = memoryUsageLogInterval(conf.getOption(COMET_MEMORY_LOG_INTERVAL.key))
      // A value set only in the session would otherwise be ignored without a trace. Only the
      // session of the plan that starts the log is checked, which covers the common case of an
      // application with one session.
      Option(SQLConf.get.getConfString(COMET_MEMORY_LOG_INTERVAL.key, null))
        .filterNot(conf.getOption(COMET_MEMORY_LOG_INTERVAL.key).contains)
        .foreach { sessionValue =>
          logWarning(
            s"Ignoring ${COMET_MEMORY_LOG_INTERVAL.key}=$sessionValue set in the session: the " +
              "native memory usage log is executor-wide, so it is read from the executor's " +
              "configuration. Set it when the application is submitted.")
        }
      if (intervalMs > 0) {
        val nativeLib = new Native()
        val limitBytes = nativeMemoryLimit(conf)
        Executors
          .newSingleThreadScheduledExecutor(new ThreadFactory {
            override def newThread(runnable: Runnable): Thread = {
              val thread = new Thread(runnable, "comet-memory-usage-log")
              thread.setDaemon(true)
              thread
            }
          })
          .scheduleWithFixedDelay(
            new Runnable {
              override def run(): Unit = logMemoryUsage(nativeLib, limitBytes)
            },
            intervalMs,
            intervalMs,
            TimeUnit.MILLISECONDS)
      }
    }
  }

  /**
   * The memory usage log interval in milliseconds for the executor's configured value, if any. A
   * value that does not parse, or is negative, disables the log with a warning: it is read when a
   * native plan is created, and a malformed logging setting must not fail every Comet task.
   * Disabling rather than falling back to the default respects an attempt to turn the log off
   * with a value such as `false`.
   */
  def memoryUsageLogInterval(configured: Option[String]): Long =
    configured match {
      case None => COMET_MEMORY_LOG_INTERVAL.defaultValue.get
      case Some(value) =>
        try {
          COMET_MEMORY_LOG_INTERVAL.valueConverter(value)
        } catch {
          case NonFatal(e) =>
            logWarning(
              s"Disabling the native memory usage log: invalid value '$value' for " +
                s"${COMET_MEMORY_LOG_INTERVAL.key}. Expected a non-negative duration such as " +
                s"10s or 500ms, or 0 to disable. ${e.getMessage}")
            0L
        }
    }

  /**
   * The executor's memory overhead in bytes, sized the way Spark sizes the default resource
   * profile's container: `spark.executor.memoryOverhead` if set, otherwise
   * `spark.executor.memoryOverheadFactor` of `spark.executor.memory`, but at least
   * `spark.executor.minMemoryOverhead`. None in local mode, where there is no container, or if
   * the settings do not parse. An executor running a non-default resource profile may have a
   * different overhead.
   */
  def executorMemoryOverhead(conf: SparkConf): Option[Long] = {
    if (conf.get("spark.master", "").startsWith("local")) {
      None
    } else {
      try {
        val overheadMiB = conf.getOption("spark.executor.memoryOverhead") match {
          case Some(_) => conf.getSizeAsMb("spark.executor.memoryOverhead")
          case None =>
            val executorMiB = conf.getSizeAsMb("spark.executor.memory", "1g")
            val factor = conf.getDouble("spark.executor.memoryOverheadFactor", 0.1)
            val minimumMiB = conf.getSizeAsMb("spark.executor.minMemoryOverhead", "384m")
            math.max((executorMiB * factor).toLong, minimumMiB)
        }
        Some(ByteUnit.MiB.toBytes(overheadMiB))
      } catch {
        case NonFatal(_) => None
      }
    }
  }

  /**
   * The memory the executor's container has for native memory: `spark.memory.offHeap.size` plus
   * the memory overhead; see [[executorMemoryOverhead]]. None, so that nothing is compared
   * against it, in local mode, when off-heap memory is disabled (a testing-only mode in which
   * Comet's reservations do not come from Spark's off-heap pool), or if the settings do not
   * parse.
   */
  def nativeMemoryLimit(conf: SparkConf): Option[Long] = {
    if (!CometSparkSessionExtensions.isOffHeapEnabled(conf)) {
      None
    } else {
      executorMemoryOverhead(conf).flatMap { overhead =>
        try {
          Some(overhead + conf.getSizeAsBytes("spark.memory.offHeap.size", "0"))
        } catch {
          case NonFatal(_) => None
        }
      }
    }
  }

  private def logMemoryUsage(nativeLib: Native, limitBytes: Option[Long]): Unit = {
    try {
      val usage = nativeLib.getMemoryUsage()
      memoryUsageMessage(usage, plansAtLastMemoryUsageLog).foreach(logInfo(_))
      plansAtLastMemoryUsageLog = usage(3)
      val warning = limitBytes.flatMap(
        nativeMemoryLimitWarning(usage, CometTaskMemoryManager.sparkOffHeapUsed(), _))
      // Warn when the footprint first exceeds the limit, not at every interval while it stays
      // there: the INFO line above keeps reporting it.
      if (!limitExceededAtLastLog) {
        warning.foreach(logWarning(_))
      }
      limitExceededAtLastLog = warning.isDefined
    } catch {
      case NonFatal(e) =>
        logWarning("Stopping the native memory usage log after a failure", e)
        // Rethrown so that the scheduler stops running the log, rather than having it fail and
        // warn again every interval.
        throw e
    }
  }

  /**
   * The memory usage log line for `usage`, as returned by [[Native.getMemoryUsage]], or None to
   * stay quiet. The log reports while native plans are running, and once more after the last of
   * them finishes, so that allocation that outlives them is visible, then waits for plans to run
   * again.
   */
  def memoryUsageMessage(usage: Array[Long], plansAtLastLog: Long): Option[String] = {
    val (allocated, reserved, pools, plans) = (usage(0), usage(1), usage(2), usage(3))
    if (plans == 0 && plansAtLastLog == 0) {
      None
    } else {
      Some(
        s"Comet native memory usage: allocated ${toMiB(allocated)}, reserved " +
          s"${toMiB(reserved)} ($plans native plans, $pools memory pools)")
    }
  }

  /**
   * A warning if the executor's native footprint exceeds `limitBytes`, the container's memory
   * outside the JVM heap; see [[nativeMemoryLimit]].
   *
   * The footprint is the native memory Comet's pools do not track, `allocated - reserved`, plus
   * `sparkOffHeapUsed`, everything in use in Spark's off-heap pool, which includes Comet's
   * reservations as well as Spark's own off-heap execution and storage memory. Comparing the sum
   * rather than the untracked part against the overhead alone counts the part of
   * `spark.memory.offHeap.size` that nothing has acquired at that moment, which untracked memory
   * can occupy until Spark hands it out. The limit also has to hold the JVM's own non-heap
   * memory, so by the time the footprint exceeds it the executor has likely outgrown its
   * container.
   */
  def nativeMemoryLimitWarning(
      usage: Array[Long],
      sparkOffHeapUsed: Long,
      limitBytes: Long): Option[String] = {
    val untracked = math.max(usage(0) - usage(1), 0L)
    val footprint = untracked + sparkOffHeapUsed
    if (footprint > limitBytes) {
      Some(
        s"Comet native memory not tracked by any memory pool (${toMiB(untracked)}) plus " +
          s"Spark's off-heap memory in use (${toMiB(sparkOffHeapUsed)}, including Comet's " +
          s"reservations) is ${toMiB(footprint)}, more than the ${toMiB(limitBytes)} the " +
          "executor's container has outside the JVM heap (spark.memory.offHeap.size plus the " +
          "memory overhead), which also has to hold the JVM's own non-heap memory. The cluster " +
          "manager may kill this executor for exceeding its container limit. Raise " +
          s"spark.executor.memoryOverhead. ${CometConf.TUNING_GUIDE}.")
    } else {
      None
    }
  }

  private def toMiB(bytes: Long): String =
    "%.1f MiB".formatLocal(Locale.ROOT, bytes / 1024.0 / 1024.0)

  private def cometSqlConfs: Map[String, String] =
    SQLConf.get.getAllConfs.filter(_._1.startsWith(CometConf.COMET_PREFIX))

  def serializeCometSQLConfs(): Array[Byte] = {
    val builder = ConfigMap.newBuilder()
    cometSqlConfs.foreach { case (k, v) =>
      if (k.startsWith(s"${CometConf.COMET_PREFIX}.datafusion.")) {
        if (CometConf.COMET_RESPECT_DATAFUSION_CONFIGS.get(SQLConf.get)) {
          builder.putEntries(k, v)
        }
      } else {
        builder.putEntries(k, v)
      }
    }
    // Inject the resolved executor cores so the native side can use it
    // for tokio runtime thread count
    val executorCores = numDriverOrExecutorCores(SparkEnv.get.conf)
    builder.putEntries("spark.executor.cores", executorCores.toString)

    // Any Comet config that the native side reads must be added here manually, resolved.
    // `cometSqlConfs` only carries values that were explicitly set, exactly as they were
    // written, so defaults from `createWithDefault(...)` would otherwise not cross JNI, and
    // native code, which parses only a bare number or a lowercase boolean, would silently fall
    // back to its own default for a value such as `10g` or `TRUE`.
    Seq[ConfigEntry[_]](
      CometConf.COMET_DEBUG_ENABLED,
      CometConf.COMET_DEBUG_MEMORY_ENABLED,
      CometConf.COMET_EXPLAIN_NATIVE_ENABLED,
      CometConf.COMET_MAX_TEMP_DIRECTORY_SIZE,
      CometConf.COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED,
      CometConf.COMET_TRACING_ENABLED).foreach { entry =>
      builder.putEntries(entry.key, entry.get(SQLConf.get).toString)
    }

    builder.build().toByteArray
  }

  def getMemoryConfig(conf: SparkConf): MemoryConfig = {
    // there are different paths for on-heap vs off-heap mode
    val offHeapMode = CometSparkSessionExtensions.isOffHeapEnabled(conf)
    if (offHeapMode) {
      // in off-heap mode, Comet uses unified memory management to share off-heap memory with Spark
      val offHeapSize = conf.getSizeAsBytes("spark.memory.offHeap.size")
      val memoryFraction = CometConf.COMET_OFFHEAP_MEMORY_POOL_FRACTION.get()
      val memoryLimit = (offHeapSize * memoryFraction).toLong
      val memoryPoolType = COMET_OFFHEAP_MEMORY_POOL_TYPE.get()
      logDebug(
        s"memoryPoolType=$memoryPoolType, " +
          s"offHeapSize=${toMB(offHeapSize)}, " +
          s"memoryFraction=$memoryFraction, " +
          s"memoryLimit=${toMB(memoryLimit)}")
      MemoryConfig(offHeapMode, memoryPoolType, memoryLimit)
    } else {
      // On-heap mode exists only so that the Spark SQL tests can run against Comet without
      // changing Spark's memory configuration, and native memory cannot be charged to Spark's
      // on-heap pool, so nothing is accounted. See the memory management contributor guide.
      logDebug("on-heap mode: native memory is unbounded and unaccounted")
      MemoryConfig(offHeapMode, memoryPoolType = "unbounded", memoryLimit = 0)
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

case class MemoryConfig(offHeapMode: Boolean, memoryPoolType: String, memoryLimit: Long)
