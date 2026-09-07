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

import java.io.{EOFException, FilterInputStream, InputStream, IOException}
import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.{ArrayList, Map => JMap, Set => JSet}
import java.util.concurrent.{ConcurrentHashMap, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.spark.{InterruptibleIterator, SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{FetchFailedException, ShuffleHandle, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{CompletionIterator, Utils}

import org.apache.comet.{CometConf, CometShuffleReadFailureHandler, Native}
import org.apache.comet.serde.{OperatorOuterClass, QueryPlanSerde}
import org.apache.comet.util.ClassLoaders
import org.apache.comet.vector.NativeUtil

/** Reads native shuffle frames from Celeborn without applying Celeborn's row decompressor. */
private[shuffle] final class CometCelebornShuffleReader[K, C](
    dependency: CometShuffleDependency[_, _, _],
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    partitionReader: CelebornRawPartitionReader)
    extends CometShuffleReader[K, C] {

  private val consumed = new AtomicBoolean(false)
  private val metricsMerged = new AtomicBoolean(false)

  private def mergeReadMetrics(): Unit = {
    if (metricsMerged.compareAndSet(false, true)) {
      context.taskMetrics().mergeShuffleReadMetrics()
    }
  }

  override def readAsRawStream(): InputStream = {
    context.killTaskIfInterrupted()
    if (!consumed.compareAndSet(false, true)) {
      throw new IllegalStateException("A Celeborn shuffle reader can only be consumed once")
    }
    val input = new FilterInputStream(
      partitionReader.openPartitions(Some(dependency.outputAttributes.size)))
      with CometShuffleReadFailureHandler {
      override def onShuffleReadFailure(failure: Throwable): Unit =
        partitionReader.onShuffleReadFailure(failure)

      override def read(): Int = {
        val value = in.read()
        if (value < 0) mergeReadMetrics()
        value
      }

      override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
        val count = in.read(buffer, offset, length)
        if (count < 0) mergeReadMetrics()
        count
      }

      override def close(): Unit = {
        Utils.tryWithSafeFinally(super.close())(mergeReadMetrics())
      }
    }
    context.addTaskCompletionListener[Unit](_ => input.close())
    input
  }

  override def read(): Iterator[Product2[K, C]] = {
    if (dependency.aggregator.isDefined) {
      throw new UnsupportedOperationException("aggregate not allowed")
    }
    if (dependency.keyOrdering.isDefined) {
      throw new UnsupportedOperationException("order not allowed")
    }

    // Validate remote logical types before Arrow import, using the same expected types as the
    // native ShuffleScan path. Serialize before opening inputs so unsupported declared types do
    // not acquire shuffle resources or get reported as corrupt remote data.
    val expectedSchema = OperatorOuterClass.ShuffleScan.newBuilder()
    dependency.outputAttributes.foreach { attribute =>
      expectedSchema.addFields(QueryPlanSerde.serializeDataType(attribute.dataType).getOrElse {
        throw new UnsupportedOperationException(
          s"Unsupported Celeborn native shuffle type: ${attribute.dataType}")
      })
    }
    val serializedSchema = expectedSchema.build().toByteArray

    val input = readAsRawStream()
    val nativeUtil =
      try new NativeUtil()
      catch {
        case NonFatal(failure) =>
          Utils.tryWithSafeFinally(throw failure)(input.close())
      }
    val decoder =
      try {
        NativeBatchDecoderIterator(
          input,
          dependency.decodeTime,
          new Native(),
          nativeUtil,
          CometConf.COMET_TRACING_ENABLED.get(),
          Some(serializedSchema))
      } catch {
        case NonFatal(failure) =>
          Utils.tryWithSafeFinally(throw failure) {
            Utils.tryWithSafeFinally(input.close())(nativeUtil.close())
          }
      }
    val resourcesClosed = new AtomicBoolean(false)
    def closeDecoder(): Unit = {
      if (resourcesClosed.compareAndSet(false, true)) {
        Utils.tryWithSafeFinally(decoder.close())(nativeUtil.close())
      }
    }
    // The constructor is lazy. Publish a fully initialized decoder before registering a listener,
    // which Spark invokes immediately if the task has already completed.
    try context.addTaskCompletionListener[Unit](_ => closeDecoder())
    catch {
      case NonFatal(failure) => Utils.tryWithSafeFinally(throw failure)(closeDecoder())
    }

    val rows = decoder.map { batch: ColumnarBatch =>
      readMetrics.incRecordsRead(batch.numRows())
      (0, batch)
    }
    val completed = CompletionIterator[(Int, ColumnarBatch), Iterator[(Int, ColumnarBatch)]](
      rows,
      mergeReadMetrics())
    new InterruptibleIterator[(Int, ColumnarBatch)](context, completed)
      .asInstanceOf[Iterator[Product2[K, C]]]
  }
}

/**
 * Reflects the optional application-owned Celeborn client and keeps one reducer-file-group
 * snapshot for all reducers in an AQE partition.
 */
private[shuffle] final class CelebornRawPartitionReader(
    client: AnyRef,
    sparkShuffleId: Int,
    celebornShuffleId: Int,
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    rpcRetryLimit: Int,
    stageRerunEnabled: Boolean = true)
    extends Logging {

  import CelebornRawPartitionReader._

  require(startMapIndex >= 0, s"Invalid Celeborn start map index: $startMapIndex")
  require(
    startMapIndex <= endMapIndex,
    "Celeborn physical-skew chunk reads are not supported by the native shuffle reader")
  require(
    startPartition >= 0 && startPartition <= endPartition,
    s"Invalid Celeborn reducer range: [$startPartition, $endPartition)")
  require(rpcRetryLimit >= 0, s"Invalid Celeborn RPC retry limit: $rpcRetryLimit")

  private val updateFileGroup =
    client.getClass.getMethod("updateFileGroup", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
  private val readPartitionMethod = client.getClass.getMethods
    .filter(isRawPartitionReader)
    .sortBy(_.getParameterCount)
    .headOption
    .getOrElse {
      throw new IllegalStateException(
        "The Celeborn client does not expose a compatible raw partition reader")
    }
  private val metricsCallback =
    createMetricsCallback(
      readPartitionMethod.getParameterTypes.apply(readPartitionMethod.getParameterCount - 2),
      readMetrics)

  private lazy val fileGroups: AnyRef = loadFileGroups()
  @volatile private var lastReadPartition = startPartition
  private var terminalFetchFailure: Throwable = _

  private[shuffle] def onShuffleReadFailure(failure: Throwable): Nothing =
    reportFetchFailure(lastReadPartition, failure)

  private def loadFileGroups(): AnyRef = {
    val started = System.nanoTime()
    var retries = 0
    try {
      while (true) {
        context.killTaskIfInterrupted()
        val stageEnded =
          try {
            invoke(
              client.getClass.getMethod("isShuffleStageEnd", java.lang.Integer.TYPE),
              client,
              Int.box(celebornShuffleId)).asInstanceOf[Boolean]
          } catch {
            case NonFatal(failure) =>
              logInfo(
                s"Could not check whether Celeborn shuffle $celebornShuffleId ended",
                failure)
              true
          }

        try {
          context.killTaskIfInterrupted()
          return Option(
            invoke(updateFileGroup, client, Int.box(celebornShuffleId), Int.box(startPartition)))
            .getOrElse {
              throw new IllegalStateException(
                "Celeborn returned a null reducer-file-group snapshot")
            }
        } catch {
          case failure if isFileGroupTimeout(failure) && !stageEnded && retries < rpcRetryLimit =>
            retries += 1
            logInfo(
              s"Retrying Celeborn reducer-file-group snapshot $celebornShuffleId " +
                s"($retries/$rpcRetryLimit)",
              failure)
          case failure if isUnreportableFileGroupFailure(failure) =>
            throw failure
          case NonFatal(failure) =>
            reportFetchFailure(startPartition, failure)
        }
      }
      throw new IllegalStateException("The Celeborn file-group retry loop ended unexpectedly")
    } finally {
      readMetrics.incFetchWaitTime(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
    }
  }

  private def snapshotField(snapshot: AnyRef, name: String): AnyRef =
    snapshot.getClass.getField(name).get(snapshot).asInstanceOf[AnyRef]

  private def reducers: Iterator[(Int, ArrayList[AnyRef], AnyRef, Array[Int])] = {
    val snapshot = fileGroups
    val partitionGroups = Option(snapshotField(snapshot, "partitionGroups"))
      .getOrElse {
        throw new IllegalStateException("Celeborn returned null reducer partition groups")
      }
      .asInstanceOf[JMap[Integer, JSet[AnyRef]]]
    val mapAttempts = Option(snapshotField(snapshot, "mapAttempts"))
      .getOrElse {
        throw new IllegalStateException("Celeborn returned null mapper-attempt metadata")
      }
      .asInstanceOf[Array[Int]]
    val pushFailedBatches = snapshotField(snapshot, "pushFailedBatches")

    (startPartition until endPartition).iterator.flatMap { partition =>
      Option(partitionGroups.get(Int.box(partition)))
        .filter(!_.isEmpty)
        .map(locations =>
          (partition, new ArrayList[AnyRef](locations), pushFailedBatches, mapAttempts))
    }
  }

  private def openPartition(
      partition: Int,
      locations: ArrayList[AnyRef],
      pushFailedBatches: AnyRef,
      mapAttempts: Array[Int],
      expectedFieldCount: Option[Int]): InputStream = {
    context.killTaskIfInterrupted()
    lastReadPartition = partition
    val attempt = encodeAttemptNumber(context.stageAttemptNumber(), context.attemptNumber())
    val arguments = Array[AnyRef](
      Int.box(celebornShuffleId),
      Int.box(sparkShuffleId),
      Int.box(partition),
      Int.box(attempt),
      Long.box(context.taskAttemptId()),
      Int.box(startMapIndex),
      Int.box(endMapIndex),
      null,
      locations,
      null,
      pushFailedBatches,
      null) ++
      // Celeborn 0.6/0.7 use 15 arguments. Newer clients insert optional coalesced-partition
      // metadata before the mapper attempts; native reads do not use physical-skew coalescing.
      (if (readPartitionMethod.getParameterCount == 16) Array[AnyRef](null)
       else Array.empty[AnyRef]) ++
      Array[AnyRef](mapAttempts, metricsCallback, java.lang.Boolean.FALSE)
    val stream =
      try {
        Option(invoke(readPartitionMethod, client, arguments: _*))
          .getOrElse {
            throw new IOException(s"Celeborn returned a null stream for reducer $partition")
          }
          .asInstanceOf[InputStream]
      } catch {
        case NonFatal(failure) => reportFetchFailure(partition, failure)
      }

    new FilterInputStream(new NativeFrameInputStream(stream, expectedFieldCount)) {
      override def read(): Int =
        try in.read()
        catch { case failure: IOException => reportFetchFailure(partition, failure) }

      override def read(buffer: Array[Byte], offset: Int, length: Int): Int =
        try in.read(buffer, offset, length)
        catch { case failure: IOException => reportFetchFailure(partition, failure) }

      override def skip(length: Long): Long =
        try in.skip(length)
        catch { case failure: IOException => reportFetchFailure(partition, failure) }
    }
  }

  private def reportFetchFailure(partition: Int, failure: Throwable): Nothing = synchronized {
    if (failure.isInstanceOf[FetchFailedException] || context.isInterrupted() ||
      context.isCompleted()) {
      throw failure
    }
    if (terminalFetchFailure != null) throw terminalFetchFailure

    val invalidated = if (stageRerunEnabled) {
      try {
        invoke(
          client.getClass.getMethod(
            "reportShuffleFetchFailure",
            java.lang.Integer.TYPE,
            java.lang.Integer.TYPE,
            java.lang.Long.TYPE),
          client,
          Int.box(sparkShuffleId),
          Int.box(celebornShuffleId),
          Long.box(context.taskAttemptId())).asInstanceOf[Boolean]
      } catch {
        case NonFatal(reportFailure) =>
          if (reportFailure ne failure) failure.addSuppressed(reportFailure)
          terminalFetchFailure = failure
          throw failure
      }
    } else false
    if (invalidated) {
      terminalFetchFailure = new FetchFailedException(
        null,
        sparkShuffleId,
        -1L,
        -1,
        partition,
        s"Celeborn FetchFailure appShuffleId/shuffleId: $sparkShuffleId/$celebornShuffleId",
        failure)
    } else {
      terminalFetchFailure = failure
    }
    throw terminalFetchFailure
  }

  def openPartitions(expectedFieldCount: Option[Int] = None): InputStream = {
    new InputStream {
      private lazy val partitions = reducers
      private val stateLock = new Object
      @volatile private var current: InputStream = _
      @volatile private var closed = false
      @volatile private var exhausted = false

      private def nextStream(): Boolean = {
        context.killTaskIfInterrupted()
        if (closed || exhausted) {
          false
        } else if (!partitions.hasNext) {
          exhausted = true
          false
        } else {
          val (partition, locations, pushFailedBatches, mapAttempts) = partitions.next()
          if (closed) {
            false
          } else {
            val opened =
              openPartition(
                partition,
                locations,
                pushFailedBatches,
                mapAttempts,
                expectedFieldCount)
            try context.killTaskIfInterrupted()
            catch {
              case NonFatal(failure) =>
                Utils.tryWithSafeFinally(throw failure)(opened.close())
            }
            val accepted = stateLock.synchronized {
              if (closed) false
              else {
                current = opened
                true
              }
            }
            if (!accepted) opened.close()
            accepted
          }
        }
      }

      private def releaseCurrent(stream: InputStream): Unit = {
        val shouldClose = stateLock.synchronized {
          if (current eq stream) {
            current = null
            true
          } else false
        }
        if (shouldClose) stream.close()
      }

      private def currentStream(): InputStream = {
        val active = current
        if (active != null) active
        else if (nextStream()) current
        else null
      }

      override def read(): Int = {
        context.killTaskIfInterrupted()
        if (closed) throw new IOException("Celeborn shuffle input stream is closed")
        var active = currentStream()
        while (active != null && !closed) {
          val value = active.read()
          if (value >= 0) return value
          releaseCurrent(active)
          active = currentStream()
        }
        -1
      }

      override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
        context.killTaskIfInterrupted()
        java.util.Objects.checkFromIndexSize(offset, length, buffer.length)
        if (closed) throw new IOException("Celeborn shuffle input stream is closed")
        if (length == 0) return 0
        var active = currentStream()
        while (active != null && !closed) {
          val count = active.read(buffer, offset, length)
          if (count >= 0) return count
          releaseCurrent(active)
          active = currentStream()
        }
        -1
      }

      override def close(): Unit = {
        val previous = stateLock.synchronized {
          if (closed) null
          else {
            closed = true
            exhausted = true
            val active = current
            current = null
            active
          }
        }
        if (previous != null) previous.close()
      }
    }
  }
}

private[shuffle] object CelebornRawPartitionReader {

  private val SPARK_UTILS_CLASS = "org.apache.spark.shuffle.celeborn.SparkUtils"
  private val SHUFFLE_CLIENT_CLASS = "org.apache.celeborn.client.ShuffleClient"
  private val SHUFFLE_READER_COMPANION_CLASS =
    "org.apache.spark.shuffle.celeborn.CelebornShuffleReader$"
  private val CELEBORN_RUNTIME_EXCEPTION =
    "org.apache.celeborn.common.exception.CelebornRuntimeException"
  private val MAX_STAGE_ATTEMPTS = 1 << 15
  private val MAX_TASK_ATTEMPTS = 1 << 16

  /**
   * Validate frames before concatenating physical reducers. Otherwise a truncated frame can
   * consume the next reducer's header as its body, hiding the fetch failure and corrupting the
   * native input. Only the fixed-size header is buffered; compressed payloads are passed through
   * unchanged.
   */
  private class NativeFrameInputStream(in: InputStream, expectedFieldCount: Option[Int])
      extends FilterInputStream(in) {
    private val header = new Array[Byte](16)
    private val singleByte = new Array[Byte](1)
    private var headerBytes = 0
    private var bodyRemaining = 0L

    override def read(): Int = {
      val count = read(singleByte, 0, 1)
      if (count < 0) -1 else singleByte(0) & 0xff
    }

    override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
      java.util.Objects.checkFromIndexSize(offset, length, buffer.length)
      if (length == 0) return 0
      val readingHeader = bodyRemaining == 0
      val remaining = if (readingHeader) header.length - headerBytes else bodyRemaining
      val count = in.read(buffer, offset, math.min(length.toLong, remaining).toInt)
      if (count < 0) {
        if (headerBytes != 0 || bodyRemaining != 0) {
          throw new EOFException("Truncated native Celeborn shuffle frame at reducer boundary")
        }
      } else if (readingHeader) {
        System.arraycopy(buffer, offset, header, headerBytes, count)
        headerBytes += count
        if (headerBytes == header.length) {
          val fields = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN)
          val compressedLength = fields.getLong()
          val fieldCount = fields.getLong()
          if (compressedLength < 12 || compressedLength - 8 > Int.MaxValue - 8L) {
            throw new IOException(
              s"Invalid native Celeborn shuffle frame length: $compressedLength")
          }
          if (fieldCount < 0 || fieldCount > Int.MaxValue ||
            expectedFieldCount.exists(_ != fieldCount)) {
            throw new IOException(s"Invalid native Celeborn shuffle field count: $fieldCount")
          }
          headerBytes = 0
          bodyRemaining = compressedLength - 8
        }
      } else {
        bodyRemaining -= count
      }
      count
    }

    override def skip(length: Long): Long = {
      if (length <= 0) return 0L
      val scratch = new Array[Byte](math.min(length, 8192L).toInt)
      var remaining = length
      while (remaining > 0) {
        val count = read(scratch, 0, math.min(remaining, scratch.length.toLong).toInt)
        if (count < 0) return length - remaining
        remaining -= count
      }
      length
    }

    override def markSupported(): Boolean = false
    override def mark(readLimit: Int): Unit = ()
    override def reset(): Unit = throw new IOException("Native shuffle frames cannot be reset")
  }

  private def isRawPartitionReader(method: Method): Boolean = {
    val parameters = method.getParameterTypes
    val size = parameters.length
    if (method.getName != "readPartition" || (size != 15 && size != 16) ||
      !classOf[InputStream].isAssignableFrom(method.getReturnType)) {
      return false
    }
    val callback = parameters(size - 2)
    val metricsCompatible = callback.isInterface && Seq("incBytesRead", "incReadTime").forall {
      name =>
        try callback.getMethod(name, java.lang.Long.TYPE).getReturnType == java.lang.Void.TYPE
        catch { case _: NoSuchMethodException => false }
    }
    Seq(0, 1, 2, 3, 5, 6).forall(parameters(_) == java.lang.Integer.TYPE) &&
    parameters(4) == java.lang.Long.TYPE && !parameters(7).isPrimitive &&
    parameters(8) == classOf[ArrayList[_]] && parameters(9) == classOf[ArrayList[_]] &&
    parameters(10) == classOf[JMap[_, _]] && parameters(11) == classOf[JMap[_, _]] &&
    (size == 15 || parameters(12) == classOf[JMap[_, _]]) &&
    parameters(size - 3) == classOf[Array[Int]] && metricsCompatible &&
    parameters(size - 1) == java.lang.Boolean.TYPE
  }

  private[shuffle] def encodeAttemptNumber(stageAttempt: Int, taskAttempt: Int): Int = {
    require(
      stageAttempt >= 0 && stageAttempt < MAX_STAGE_ATTEMPTS,
      s"Celeborn stage attempt must be between 0 and ${MAX_STAGE_ATTEMPTS - 1}: " +
        stageAttempt)
    require(
      taskAttempt >= 0 && taskAttempt < MAX_TASK_ATTEMPTS,
      s"Celeborn task attempt must be between 0 and ${MAX_TASK_ATTEMPTS - 1}: " +
        taskAttempt)
    (stageAttempt << 16) | taskAttempt
  }

  private def isCelebornRuntimeException(failure: Throwable): Boolean = {
    var current: Class[_] = failure.getClass
    while (current != null) {
      if (current.getName == CELEBORN_RUNTIME_EXCEPTION) return true
      current = current.getSuperclass
    }
    false
  }

  private[shuffle] final case class ReadRange(
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int)

  private[shuffle] trait Api {
    def rpcRetryLimit(conf: SparkConf): Int

    def shuffleId(
        client: AnyRef,
        handle: ShuffleHandle,
        context: TaskContext,
        isWriter: Boolean): Int
  }

  private[shuffle] val reflectedApi: Api = new Api {
    override def rpcRetryLimit(conf: SparkConf): Int = {
      val sparkUtilsClass = ClassLoaders.loadClass(SPARK_UTILS_CLASS)
      val celebornConf =
        invoke(sparkUtilsClass.getMethod("fromSparkConf", classOf[SparkConf]), null, conf)
      invoke(celebornConf.getClass.getMethod("clientRpcMaxRetries"), celebornConf)
        .asInstanceOf[Int]
    }

    override def shuffleId(
        client: AnyRef,
        handle: ShuffleHandle,
        context: TaskContext,
        isWriter: Boolean): Int = {
      val sparkUtilsClass = ClassLoaders.loadClass(SPARK_UTILS_CLASS)
      val shuffleClientClass = ClassLoaders.loadClass(SHUFFLE_CLIENT_CLASS)
      val method = sparkUtilsClass.getMethod(
        "celebornShuffleId",
        shuffleClientClass,
        handle.getClass,
        classOf[TaskContext],
        classOf[java.lang.Boolean])
      invoke(method, null, client, handle, context, Boolean.box(isWriter)).asInstanceOf[Int]
    }
  }

  def fromBackendReader(
      conf: SparkConf,
      handle: ShuffleHandle,
      backendReader: ShuffleReader[_, _],
      range: ReadRange,
      context: TaskContext,
      readMetrics: ShuffleReadMetricsReporter,
      onClientAcquired: AnyRef => Unit,
      onGenerationResolved: (AnyRef, Int) => Unit,
      api: Api = reflectedApi): CelebornRawPartitionReader = {
    try {
      context.killTaskIfInterrupted()
      val client = invoke(backendReader.getClass.getMethod("shuffleClient"), backendReader)
      onClientAcquired(client)

      // The delegated reader normally initializes this companion from read(). Its static setup
      // registers the application client's broadcast reducer-file-group deserializer.
      ClassLoaders.loadClass(SHUFFLE_READER_COMPANION_CLASS)

      val stageRerunEnabled =
        invoke(handle.getClass.getMethod("stageRerunEnabled"), handle).asInstanceOf[Boolean]
      if (!stageRerunEnabled) {
        throw new IllegalStateException("Native Celeborn shuffle requires stage reruns")
      }

      val retryLimit = api.rpcRetryLimit(conf)
      val celebornShuffleId = {
        val started = System.nanoTime()
        try {
          context.killTaskIfInterrupted()
          api.shuffleId(client, handle, context, isWriter = false)
        } catch {
          case failure if isCelebornRuntimeException(failure) =>
            throw new FetchFailedException(
              null,
              handle.shuffleId,
              -1L,
              -1,
              range.startPartition,
              s"Celeborn could not resolve shuffle generation ${handle.shuffleId}",
              failure)
        } finally {
          readMetrics.incFetchWaitTime(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
        }
      }

      onGenerationResolved(client, celebornShuffleId)
      new CelebornRawPartitionReader(
        client,
        handle.shuffleId,
        celebornShuffleId,
        range.startMapIndex,
        range.endMapIndex,
        range.startPartition,
        range.endPartition,
        context,
        readMetrics,
        retryLimit,
        stageRerunEnabled)
    } catch {
      case failure: ReflectiveOperationException =>
        throw new IllegalStateException(
          "The Celeborn client does not expose the required native-shuffle reader API",
          failure)
    }
  }

  private def invoke(method: Method, instance: AnyRef, arguments: AnyRef*): AnyRef =
    try method.invoke(instance, arguments: _*)
    catch {
      case failure: InvocationTargetException =>
        throw Option(failure.getCause).getOrElse(failure)
    }

  private def isFileGroupTimeout(failure: Throwable): Boolean =
    Option(failure.getCause).exists(_.isInstanceOf[TimeoutException])

  private def isUnreportableFileGroupFailure(failure: Throwable): Boolean =
    Option(failure.getCause).exists { cause =>
      cause.isInstanceOf[InterruptedException] || cause.isInstanceOf[TimeoutException] ||
      cause.getClass.getName == "org.apache.celeborn.common.exception.CelebornBroadcastException"
    }

  private def createMetricsCallback(
      callbackClass: Class[_],
      reporter: ShuffleReadMetricsReporter): AnyRef = {
    val remoteWorkers = ConcurrentHashMap.newKeySet[String]()
    val unavailableMetrics = ConcurrentHashMap.newKeySet[String]()

    def optionalMetric(name: String, value: Long): Unit = {
      if (!unavailableMetrics.contains(name)) {
        try {
          val method = reporter.getClass.getMethod(name, java.lang.Long.TYPE)
          method.setAccessible(true)
          invoke(method, reporter, Long.box(value))
        } catch {
          case NonFatal(_) => unavailableMetrics.add(name)
        }
      }
      ()
    }

    val callback = new InvocationHandler {
      override def invoke(proxy: AnyRef, method: Method, arguments: Array[AnyRef]): AnyRef = {
        method.getName match {
          case "incBytesRead" =>
            reporter.incRemoteBytesRead(arguments(0).asInstanceOf[Long])
            reporter.incRemoteBlocksFetched(1)
          case "incReadTime" => reporter.incFetchWaitTime(arguments(0).asInstanceOf[Long])
          case "recordRemoteReadWorker" =>
            val worker = arguments(0).asInstanceOf[String]
            if (worker != null && remoteWorkers.add(worker)) {
              optionalMetric("incCelebornDistinctRemoteWorkersRead", 1)
            }
          case "hashCode" => return Int.box(System.identityHashCode(proxy))
          case "equals" => return Boolean.box(proxy eq arguments(0))
          case "toString" => return "CometCelebornShuffleMetricsCallback"
          case name if name.startsWith("inc") =>
            optionalMetric("incCeleborn" + name.substring(3), arguments(0).asInstanceOf[Long])
          case _ =>
        }
        null
      }
    }

    Proxy.newProxyInstance(callbackClass.getClassLoader, Array(callbackClass), callback)
  }
}
