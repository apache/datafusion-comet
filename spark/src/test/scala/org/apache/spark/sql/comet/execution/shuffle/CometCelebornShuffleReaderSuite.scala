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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException, InputStream, IOException}
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.{LinkedHashSet, Set => JSet}
import java.util.concurrent.{CountDownLatch, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters._

import org.apache.arrow.flatbuf.{Int => IpcInt, Message, MessageHeader, Schema => IpcSchema, Type => IpcType}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{FieldVector, IntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector}
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, Field, FieldType}
import org.apache.spark.{HashPartitioner, ShuffleDependency, SparkConf, SparkEnv, TaskContext, TaskKilledException}
import org.apache.spark.shuffle.{FetchFailedException, IndexShuffleBlockResolver, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.comet.{CometExec, CometMetricNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.flatbuffers.Table

import org.apache.comet.{CometConf, CometExecIterator, CometShuffleBlockIterator, Native}
import org.apache.comet.serde.{OperatorOuterClass, QueryPlanSerde}

class CometCelebornShuffleReaderSuite extends CometTestBase {

  import testImplicits._

  private def frameBytes(payload: Byte*): Array[Byte] = ByteBuffer
    .allocate(20 + payload.size)
    .order(ByteOrder.LITTLE_ENDIAN)
    .putLong(12L + payload.size)
    .putLong(0L)
    .put(Array[Byte](78, 79, 78, 69)) // NONE codec; raw-reader tests need only a framed payload.
    .put(payload.toArray)
    .array()

  private final class TrackingInputStream(bytes: Array[Byte])
      extends ByteArrayInputStream(bytes) {
    var closeCalls = 0

    override def close(): Unit = {
      closeCalls += 1
      super.close()
    }
  }

  private final class RecordingReaderBackend(client: RecordingCelebornRawClient)
      extends ShuffleManager {
    var readRange: Option[(Int, Int, Int, Int)] = None
    var unregistered: Option[Int] = None

    override def registerShuffle[K, V, C](
        shuffleId: Int,
        dependency: ShuffleDependency[K, V, C]): ShuffleHandle =
      throw new UnsupportedOperationException("This fixture only reads existing native shuffles")

    override def getWriter[K, V](
        handle: ShuffleHandle,
        mapId: Long,
        context: TaskContext,
        metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] =
      throw new UnsupportedOperationException("This fixture only reads existing native shuffles")

    override def getReader[K, C](
        handle: ShuffleHandle,
        startMapIndex: Int,
        endMapIndex: Int,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext,
        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
      readRange = Some((startMapIndex, endMapIndex, startPartition, endPartition))
      new RecordingCelebornRawClient.BackendReader(client).asInstanceOf[ShuffleReader[K, C]]
    }

    override def shuffleBlockResolver: ShuffleBlockResolver = null

    override def unregisterShuffle(shuffleId: Int): Boolean = {
      unregistered = Some(shuffleId)
      true
    }

    override def stop(): Unit = ()
  }

  private final class RecordingReaderApi extends CelebornRawPartitionReader.Api {
    var resolvedClient: AnyRef = _
    var resolvedHandle: ShuffleHandle = _
    var resolvedContext: TaskContext = _
    var resolvedAsWriter: Option[Boolean] = None
    var retryLimitFailure: Throwable = _
    var generationFailure: Throwable = _

    override def rpcRetryLimit(conf: SparkConf): Int = {
      if (retryLimitFailure != null) throw retryLimitFailure
      2
    }

    override def shuffleId(
        client: AnyRef,
        handle: ShuffleHandle,
        context: TaskContext,
        isWriter: Boolean): Int = {
      resolvedClient = client
      resolvedHandle = handle
      resolvedContext = context
      resolvedAsWriter = Some(isWriter)
      if (generationFailure != null) throw generationFailure
      91
    }
  }

  private def location(values: String*): JSet[Object] = {
    val locations = new LinkedHashSet[Object]()
    values.foreach(locations.add)
    locations
  }

  private def rawReader(
      client: AnyRef,
      context: TaskContext,
      startMap: Int = 0,
      endMap: Int = Int.MaxValue,
      startPartition: Int = 0,
      endPartition: Int = 3,
      retries: Int = 2): CelebornRawPartitionReader =
    new CelebornRawPartitionReader(
      client,
      sparkShuffleId = 17,
      celebornShuffleId = 91,
      startMap,
      endMap,
      startPartition,
      endPartition,
      context,
      context.taskMetrics().createTempShuffleReadMetrics(),
      retries)

  private def simpleDependency(partitions: Int = 3, attributes: Seq[Attribute] = Seq.empty)
      : CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch] =
    new CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      spark.sparkContext.emptyRDD[(Int, ColumnarBatch)],
      new HashPartitioner(partitions),
      decodeTime = SQLMetrics.createMetric(spark.sparkContext, "Celeborn decode time"),
      outputAttributes = attributes)

  private def ownedClients(manager: CometCelebornShuffleManager)
      : java.util.concurrent.ConcurrentHashMap[AnyRef, java.lang.Boolean] = {
    val ownership = classOf[CometCelebornShuffleManager].getDeclaredField("ownedNativeClients")
    ownership.setAccessible(true)
    ownership
      .get(manager)
      .asInstanceOf[java.util.concurrent.ConcurrentHashMap[AnyRef, java.lang.Boolean]]
  }

  private def completionListenerCount(context: TaskContext): Int = {
    val listeners = context.getClass.getDeclaredField("onCompleteCallbacks")
    listeners.setAccessible(true)
    listeners.get(context).asInstanceOf[java.util.Stack[_]].size()
  }

  private def reader(
      dependency: CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
      client: RecordingCelebornRawClient,
      context: TaskContext): CometCelebornShuffleReader[Int, ColumnarBatch] = {
    val metrics = context.taskMetrics().createTempShuffleReadMetrics()
    val partitions = new CelebornRawPartitionReader(
      client,
      dependency.shuffleId,
      91,
      0,
      Int.MaxValue,
      0,
      dependency.partitioner.numPartitions,
      context,
      metrics,
      2)
    new CometCelebornShuffleReader[Int, ColumnarBatch](dependency, context, metrics, partitions)
  }

  private def withNativeFrame(
      run: (CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch], Array[Byte]) => Unit)
      : Unit = {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      "spark.sql.adaptive.enabled" -> "false") {
      withParquetTable((0 until 12).map(i => (i, s"row-$i")), "celeborn_reader_rows") {
        val dependency = sql("SELECT * FROM celeborn_reader_rows")
          .repartition(3, $"_1")
          .queryExecution
          .executedPlan
          .collectFirst { case exchange: CometShuffleExchangeExec =>
            exchange.shuffleDependency
              .asInstanceOf[CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch]]
          }
          .getOrElse(fail("Expected a native Comet shuffle exchange"))

        val frames = spark.sparkContext
          .runJob(
            dependency.rdd,
            (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
              val writer = new CometNativeShuffleWriter[Int, ColumnarBatch](
                dependency.nativeShuffleSpec.get,
                dependency.outputPartitioning.get,
                dependency.outputAttributes,
                dependency.shuffleWriteMetrics,
                dependency.numParts,
                dependency.shuffleId,
                context.taskAttemptId(),
                context,
                context.taskMetrics().shuffleWriteMetrics,
                dependency.rangePartitionBounds)
              writer.write(inputs)
              writer.stop(success = true)
              val dataFile = SparkEnv.get.shuffleManager.shuffleBlockResolver
                .asInstanceOf[IndexShuffleBlockResolver]
                .getDataFile(dependency.shuffleId, context.taskAttemptId())
              val bytes = Files.readAllBytes(dataFile.toPath)
              var offset = 0
              writer.getPartitionLengths().flatMap { length =>
                val end = offset + length.toInt
                val frame = bytes.slice(offset, end)
                offset = end
                if (frame.nonEmpty) Some(frame) else None
              }
            })
          .flatten

        assert(frames.nonEmpty)
        run(dependency, frames.head)
      }
    }
  }

  test("fresh Celeborn readers initialize the broadcast reducer-file-group decoder") {
    assert(!RecordingCelebornRawClient.broadcastDecoderRegistered())
    val context = TaskContext.empty()
    val dependency = simpleDependency()
    val client = new RecordingCelebornRawClient
    client.requiresBroadcastDecoder = true
    client.fileGroups.partitionGroups.put(0, location("worker"))
    client.streams.put(0, new ByteArrayInputStream(frameBytes(9)))
    val backend = new RecordingReaderBackend(client)
    val api = new RecordingReaderApi
    val handle = new org.apache.spark.shuffle.celeborn.CelebornShuffleHandle(17, dependency)
    val manager =
      new CometCelebornShuffleManager(new SparkConf(false), false, (_, _) => backend, api)

    val remote = manager
      .getReader[Int, ColumnarBatch](
        handle,
        0,
        Int.MaxValue,
        0,
        1,
        context,
        context.taskMetrics.createTempShuffleReadMetrics())
      .asInstanceOf[CometShuffleReader[Int, ColumnarBatch]]

    assert(RecordingCelebornRawClient.broadcastDecoderRegistered())
    assert(remote.readAsRawStream().readAllBytes().toSeq == frameBytes(9).toSeq)
    assert(client.updateFileGroupCalls == 1)
    context.markTaskCompleted(None)
  }

  test("the manager routes native Celeborn handles through its reflected raw reader") {
    val context = TaskContext.empty()
    val dependency = simpleDependency()
    val client = new RecordingCelebornRawClient
    client.fileGroups.partitionGroups.put(1, location("worker"))
    client.streams.put(1, new ByteArrayInputStream(frameBytes(3, 4)))
    val backend = new RecordingReaderBackend(client)
    val api = new RecordingReaderApi
    val handle = new org.apache.spark.shuffle.celeborn.CelebornShuffleHandle(17, dependency)
    val manager =
      new CometCelebornShuffleManager(new SparkConf(false), false, (_, _) => backend, api)

    val remote = manager
      .getReader[Int, ColumnarBatch](
        handle,
        2,
        6,
        1,
        3,
        context,
        context.taskMetrics.createTempShuffleReadMetrics())
      .asInstanceOf[CometShuffleReader[Int, ColumnarBatch]]

    assert(backend.readRange.contains((2, 6, 1, 3)))
    assert(api.resolvedClient eq client)
    assert(api.resolvedHandle eq handle)
    assert(api.resolvedContext eq context)
    assert(api.resolvedAsWriter.contains(false))
    assert(remote.readAsRawStream().readAllBytes().toSeq == frameBytes(3, 4).toSeq)
    assert(client.requests.size() == 1)
    assert(!client.requests.get(0).needDecompress)
    assert(client.requests.get(0).startMapIndex == 2)
    assert(client.requests.get(0).endMapIndex == 6)

    assert(ownedClients(manager).containsKey(client))
    assert(manager.unregisterShuffle(17))
    assert(backend.unregistered.contains(17))
    assert(client.cleanupCalls == 1)
    context.markTaskCompleted(None)
  }

  test("the manager fails closed on disabled stage reruns and unavailable optional reader APIs") {
    Seq(false, true).foreach { unavailableApi =>
      val context = TaskContext.empty()
      val dependency = simpleDependency()
      val client = new RecordingCelebornRawClient
      val backend = new RecordingReaderBackend(client)
      val api = new RecordingReaderApi
      if (unavailableApi) api.generationFailure = new NoSuchMethodException("celebornShuffleId")
      val handle = new org.apache.spark.shuffle.celeborn.CelebornShuffleHandle(
        17,
        dependency,
        stageRerunEnabled = unavailableApi)
      val manager =
        new CometCelebornShuffleManager(new SparkConf(false), false, (_, _) => backend, api)

      val failure = intercept[IllegalStateException] {
        manager.getReader[Int, ColumnarBatch](
          handle,
          0,
          Int.MaxValue,
          0,
          1,
          context,
          context.taskMetrics.createTempShuffleReadMetrics())
      }

      if (unavailableApi) {
        assert(failure.getMessage.contains("reader API"))
        assert(failure.getCause.isInstanceOf[NoSuchMethodException])
      } else {
        assert(failure.getMessage.contains("stage reruns"))
        assert(api.resolvedAsWriter.isEmpty)
      }
      assert(ownedClients(manager).containsKey(client))
      assert(client.failureReports == 0)
    }
  }

  test("reader clients remain owned when Celeborn retry configuration cannot be loaded") {
    val context = TaskContext.empty()
    val dependency = simpleDependency()
    val client = new RecordingCelebornRawClient
    val backend = new RecordingReaderBackend(client)
    val api = new RecordingReaderApi
    val expected = new IllegalStateException("Celeborn retry settings are unavailable")
    api.retryLimitFailure = expected
    val handle = new org.apache.spark.shuffle.celeborn.CelebornShuffleHandle(17, dependency)
    val manager =
      new CometCelebornShuffleManager(new SparkConf(false), false, (_, _) => backend, api)

    val failure = intercept[IllegalStateException] {
      manager.getReader[Int, ColumnarBatch](
        handle,
        0,
        Int.MaxValue,
        0,
        1,
        context,
        context.taskMetrics.createTempShuffleReadMetrics())
    }

    assert(failure eq expected)
    assert(ownedClients(manager).containsKey(client))
    assert(api.resolvedAsWriter.isEmpty)
  }

  test("a genuine reducer-generation resolution failure becomes a Spark fetch failure") {
    val context = TaskContext.empty()
    val dependency = simpleDependency()
    val client = new RecordingCelebornRawClient
    val backend = new RecordingReaderBackend(client)
    val api = new RecordingReaderApi
    val expected =
      new org.apache.celeborn.common.exception.CelebornRuntimeException(
        "the reducer generation expired")
    api.generationFailure = expected
    val handle = new org.apache.spark.shuffle.celeborn.CelebornShuffleHandle(17, dependency)
    val manager =
      new CometCelebornShuffleManager(new SparkConf(false), false, (_, _) => backend, api)

    val failure = intercept[FetchFailedException] {
      manager.getReader[Int, ColumnarBatch](
        handle,
        0,
        Int.MaxValue,
        0,
        1,
        context,
        context.taskMetrics.createTempShuffleReadMetrics())
    }

    assert(failure.getCause eq expected)
    assert(api.resolvedAsWriter.contains(false))
    assert(ownedClients(manager).containsKey(client))
  }

  test("reader adapter bugs are never misclassified as retryable generation failures") {
    val context = TaskContext.empty()
    val dependency = simpleDependency()
    val client = new RecordingCelebornRawClient
    val backend = new RecordingReaderBackend(client)
    val api = new RecordingReaderApi
    val expected = new ClassCastException("the optional client exposed an incompatible API")
    api.generationFailure = expected
    val handle = new org.apache.spark.shuffle.celeborn.CelebornShuffleHandle(17, dependency)
    val manager =
      new CometCelebornShuffleManager(new SparkConf(false), false, (_, _) => backend, api)

    val failure = intercept[ClassCastException] {
      manager.getReader[Int, ColumnarBatch](
        handle,
        0,
        Int.MaxValue,
        0,
        1,
        context,
        context.taskMetrics.createTempShuffleReadMetrics())
    }

    assert(failure eq expected)
    assert(ownedClients(manager).containsKey(client))
    assert(client.failureReports == 0)
  }

  test("raw fetch preserves generation, reducer order, map ranges, and one file-group snapshot") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    val first = new TrackingInputStream(frameBytes(1, 2))
    val third = new TrackingInputStream(frameBytes(3, 4, 5))
    client.fileGroups.partitionGroups.put(1, location("worker-b", "worker-c"))
    client.fileGroups.partitionGroups.put(2, location())
    client.fileGroups.partitionGroups.put(3, location("worker-a"))
    client.fileGroups.mapAttempts = Array(3, 1, 4)
    client.fileGroups.pushFailedBatches.put("worker-b", "replayed-batch")
    client.streams.put(1, first)
    client.streams.put(3, third)

    val input = rawReader(client, context, 4, 9, 1, 4).openPartitions()
    assert(client.updateFileGroupCalls == 0)
    assert(input.readAllBytes().toSeq == (frameBytes(1, 2) ++ frameBytes(3, 4, 5)).toSeq)
    assert(client.updateFileGroupCalls == 1)
    assert(client.requests.asScala.map(_.partitionId).toSeq == Seq(1, 3))

    client.requests.asScala.foreach { request =>
      assert(request.shuffleId == 91)
      assert(request.appShuffleId == 17)
      assert(request.startMapIndex == 4)
      assert(request.endMapIndex == 9)
      assert(request.taskId == context.taskAttemptId)
      assert(!request.needDecompress)
      assert(request.exceptionMaker == null)
      assert(request.streamHandlers == null)
      assert(request.chunksRange == null)
      assert(request.coalescedPartitionInfos == null)
      assert(request.pushFailedBatches eq client.fileGroups.pushFailedBatches)
      assert(request.mapAttempts eq client.fileGroups.mapAttempts)
    }
    assert(client.requests.get(0).locations.asScala.toSeq == Seq("worker-b", "worker-c"))
    assert(first.closeCalls == 1)
    assert(third.closeCalls == 1)
    input.close()
    context.markTaskCompleted(None)
  }

  test("raw fetch forwards Celeborn byte, block, and wait metrics") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    client.fileGroups.partitionGroups.put(0, location("worker"))
    client.streams.put(0, new ByteArrayInputStream(frameBytes(7)))
    val input = rawReader(client, context).openPartitions()
    assert(input.skip(20) == 20)
    assert(input.read() == 7)

    val callback = client.requests.get(0).metricsCallback
    callback.incBytesRead(29)
    callback.incBytesRead(11)
    callback.incReadTime(13)
    callback.incRemoteReadRetryCount(1)
    callback.recordRemoteReadWorker("worker:9097")
    callback.recordRemoteReadWorker("worker:9097")
    context.taskMetrics.mergeShuffleReadMetrics()

    assert(context.taskMetrics.shuffleReadMetrics.remoteBytesRead == 40)
    assert(context.taskMetrics.shuffleReadMetrics.remoteBlocksFetched == 2)
    assert(context.taskMetrics.shuffleReadMetrics.fetchWaitTime >= 13)
    input.close()
    context.markTaskCompleted(None)
  }

  test("raw fetch also supports the newer coalesced-metadata argument without decompression") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient.CoalescedClient
    client.delegate.fileGroups.partitionGroups.put(1, location("worker"))
    client.delegate.streams.put(1, new ByteArrayInputStream(frameBytes(3, 4)))
    val input = rawReader(client, context, 2, 6, 1, 3).openPartitions()

    assert(input.readAllBytes().toSeq == frameBytes(3, 4).toSeq)
    val request = client.delegate.requests.get(0)
    assert(request.shuffleId == 91)
    assert(request.appShuffleId == 17)
    assert(request.partitionId == 1)
    assert(request.startMapIndex == 2)
    assert(request.endMapIndex == 6)
    assert(request.mapAttempts eq client.delegate.fileGroups.mapAttempts)
    assert(request.coalescedPartitionInfos == null)
    assert(!request.needDecompress)
    request.metricsCallback.incBytesRead(23)
    context.taskMetrics.mergeShuffleReadMetrics()
    assert(context.taskMetrics.shuffleReadMetrics.remoteBytesRead == 23)
    input.close()
    context.markTaskCompleted(None)
  }

  test("native raw consumption merges task shuffle metrics at EOF and close only once") {
    Seq(false, true).foreach { closeEarly =>
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      client.fileGroups.partitionGroups.put(0, location("worker"))
      client.streams.put(0, new ByteArrayInputStream(frameBytes(7, 8)))
      val input = reader(simpleDependency(), client, context).readAsRawStream()
      assert(input.skip(20) == 20)
      assert(input.read() == 7)
      client.requests.get(0).metricsCallback.incBytesRead(19)

      if (closeEarly) input.close()
      else assert(input.readAllBytes().toSeq == Seq[Byte](8))
      input.close()
      context.markTaskCompleted(None)

      assert(context.taskMetrics.shuffleReadMetrics.remoteBytesRead == 19)
      assert(context.taskMetrics.shuffleReadMetrics.remoteBlocksFetched == 1)
    }
  }

  test("broken optional Celeborn metrics are disabled without failing remote reads") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    client.fileGroups.partitionGroups.put(0, location("worker"))
    client.streams.put(0, new ByteArrayInputStream(frameBytes(7)))
    val underlying = context.taskMetrics.createTempShuffleReadMetrics()
    var optionalUpdates = 0
    val reporter = Proxy
      .newProxyInstance(
        getClass.getClassLoader,
        Array(
          classOf[ShuffleReadMetricsReporter],
          classOf[RecordingCelebornRawClient.OptionalMetricsReporter]),
        new InvocationHandler {
          override def invoke(proxy: AnyRef, method: Method, arguments: Array[AnyRef]): AnyRef = {
            if (method.getName == "incCelebornRemoteReadRetryCount") {
              optionalUpdates += 1
              throw new IllegalStateException("an optional reporter is unavailable")
            }
            method.invoke(underlying, arguments: _*)
          }
        })
      .asInstanceOf[ShuffleReadMetricsReporter]
    val partitions =
      new CelebornRawPartitionReader(client, 17, 91, 0, Int.MaxValue, 0, 1, context, reporter, 2)

    val input = partitions.openPartitions()
    assert(input.skip(20) == 20)
    assert(input.read() == 7)
    client.requests.get(0).metricsCallback.incRemoteReadRetryCount(1)
    client.requests.get(0).metricsCallback.incRemoteReadRetryCount(1)

    assert(optionalUpdates == 1)
    input.close()
    context.markTaskCompleted(None)
  }

  test("coalesced reducer reads retain only one task-completion stream listener") {
    val reducers = 256
    val context = TaskContext.empty()
    val dependency = simpleDependency(reducers)
    val client = new RecordingCelebornRawClient
    val streams = (0 until reducers).map { partition =>
      val stream = new TrackingInputStream(frameBytes(partition.toByte))
      client.fileGroups.partitionGroups.put(partition, location(s"worker-$partition"))
      client.streams.put(partition, stream)
      stream
    }

    val input = reader(dependency, client, context).readAsRawStream()
    assert(completionListenerCount(context) == 1)
    assert(input.readAllBytes().length == reducers * frameBytes(0).length)
    assert(client.requests.size() == reducers)
    assert(completionListenerCount(context) == 1)
    assert(streams.forall(_.closeCalls == 1))

    context.markTaskCompleted(None)
    assert(streams.forall(_.closeCalls == 1))
  }

  test("closing an unread or partially consumed stream never opens another reducer") {
    val untouchedContext = TaskContext.empty()
    val untouchedClient = new RecordingCelebornRawClient
    rawReader(untouchedClient, untouchedContext).openPartitions().close()
    assert(untouchedClient.updateFileGroupCalls == 0)

    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    val first = new TrackingInputStream(frameBytes(1, 2))
    val second = new TrackingInputStream(frameBytes(3))
    client.fileGroups.partitionGroups.put(0, location("worker-a"))
    client.fileGroups.partitionGroups.put(1, location("worker-b"))
    client.streams.put(0, first)
    client.streams.put(1, second)

    val input = rawReader(client, context).openPartitions()
    assert(input.skip(20) == 20)
    assert(input.read() == 1)
    input.close()
    input.close()

    assert(first.closeCalls == 1)
    assert(second.closeCalls == 0)
    assert(client.requests.size() == 1)
    context.markTaskCompleted(None)
    assert(first.closeCalls == 1)
    assert(second.closeCalls == 0)
  }

  test("task completion closes an opened reducer stream") {
    val context = TaskContext.empty()
    val dependency = simpleDependency()
    val client = new RecordingCelebornRawClient
    val stream = new TrackingInputStream(frameBytes(1, 2))
    client.fileGroups.partitionGroups.put(0, location("worker"))
    client.streams.put(0, stream)

    val input = reader(dependency, client, context).readAsRawStream()
    assert(input.skip(20) == 20)
    assert(input.read() == 1)
    context.markTaskCompleted(None)

    assert(stream.closeCalls == 1)
  }

  test("task completion racing reducer-stream creation closes the unpublished stream") {
    val context = TaskContext.empty()
    val dependency = simpleDependency()
    val client = new RecordingCelebornRawClient
    val stream = new TrackingInputStream(frameBytes(1))
    client.fileGroups.partitionGroups.put(0, location("worker"))
    client.streams.put(0, stream)
    client.readPartitionStarted = new CountDownLatch(1)
    client.allowReadPartition = new CountDownLatch(1)
    val input = reader(dependency, client, context).readAsRawStream()
    val result = new AtomicInteger(Int.MinValue)
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(() => {
      try result.set(input.read())
      catch { case caught: Throwable => failure.set(caught) }
    })

    worker.start()
    try {
      assert(client.readPartitionStarted.await(5, TimeUnit.SECONDS))
      context.markTaskCompleted(None)
    } finally {
      client.allowReadPartition.countDown()
      worker.join(5000)
    }

    assert(!worker.isAlive)
    assert(failure.get() == null)
    assert(result.get() == -1)
    assert(stream.closeCalls == 1)
    assert(client.requests.size() == 1)
    assert(client.failureReports == 0)
  }

  test("file-group RPC timeouts retry only while the map stage is still running") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    client.stageEnded = false
    client.timeoutFailures = 2
    client.fileGroups.partitionGroups.put(0, location("worker"))
    client.streams.put(0, new ByteArrayInputStream(frameBytes(9)))

    val input = rawReader(client, context, retries = 2).openPartitions()
    assert(input.skip(20) == 20)
    assert(input.read() == 9)
    assert(client.updateFileGroupCalls == 3)
    assert(client.stageEndChecks == 3)
    assert(client.failureReports == 0)
    input.close()
    context.markTaskCompleted(None)
  }

  test("exhausted file-group timeouts propagate without invalidating a shuffle generation") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    client.stageEnded = false
    client.timeoutFailures = 4

    val failure = intercept[IOException] {
      rawReader(client, context, retries = 2).openPartitions().read()
    }

    assert(failure.getCause.isInstanceOf[TimeoutException])
    assert(client.updateFileGroupCalls == 3)
    assert(client.failureReports == 0)
  }

  test("file-group and partition-open failures invalidate and become Spark fetch failures") {
    Seq(false, true).foreach { failAtOpen =>
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      val expected = new IOException(if (failAtOpen) "worker open failed" else "metadata lost")
      if (failAtOpen) {
        client.fileGroups.partitionGroups.put(2, location("worker"))
        client.readPartitionFailure = expected
      } else {
        client.updateFileGroupFailure = expected
      }

      val failure = intercept[FetchFailedException] {
        rawReader(client, context, startPartition = 2, endPartition = 3).openPartitions().read()
      }

      assert(failure.getCause eq expected)
      assert(failure.getMessage.contains("17/91"))
      assert(client.failureReports == 1)
    }
  }

  test("lazy stream failures invalidate once and interrupted tasks never invalidate") {
    Seq(false, true).foreach { interrupted =>
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      val expected = new IOException("worker failed after stream creation")
      client.fileGroups.partitionGroups.put(1, location("worker"))
      client.streams.put(
        1,
        new InputStream {
          override def read(): Int = throw expected
        })
      val input =
        rawReader(client, context, startPartition = 1, endPartition = 2).openPartitions()
      if (interrupted) context.markInterrupted("the reduce task was cancelled")

      val failure = intercept[Throwable](input.read())
      if (interrupted) {
        assert(failure.isInstanceOf[TaskKilledException])
        assert(client.requests.isEmpty)
        assert(client.failureReports == 0)
      } else {
        assert(failure.isInstanceOf[FetchFailedException])
        assert(failure.getCause eq expected)
        assert(client.failureReports == 1)
      }
    }
  }

  test("physical-skew encoded map ranges fail closed") {
    val failure = intercept[IllegalArgumentException] {
      rawReader(new RecordingCelebornRawClient, TaskContext.empty(), startMap = 9, endMap = 4)
    }
    assert(failure.getMessage.contains("physical-skew"))
  }

  test("cancelled raw tasks do not open streams or retry metadata RPCs") {
    Seq(false, true).foreach { duringMetadata =>
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      client.fileGroups.partitionGroups.put(0, location("worker"))
      client.streams.put(0, new ByteArrayInputStream(frameBytes(1)))
      if (duringMetadata) {
        client.stageEnded = false
        client.timeoutFailures = 2
        client.beforeUpdateFileGroup = () => context.markInterrupted("cancelled during metadata")
      } else {
        context.markInterrupted("cancelled before native scan")
      }
      val input = rawReader(client, context).openPartitions()

      intercept[TaskKilledException](input.read())

      assert(client.updateFileGroupCalls == (if (duringMetadata) 1 else 0))
      assert(client.requests.isEmpty)
      assert(client.failureReports == 0)
      input.close()
      context.markTaskCompleted(None)
    }
  }

  test("cancellation between reducers never opens the next reducer") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    val first = new TrackingInputStream(frameBytes(1))
    val second = new TrackingInputStream(frameBytes(2))
    client.fileGroups.partitionGroups.put(0, location("worker-a"))
    client.fileGroups.partitionGroups.put(1, location("worker-b"))
    client.streams.put(0, first)
    client.streams.put(1, second)
    val input = rawReader(client, context).openPartitions()
    assert(input.readNBytes(frameBytes(1).length).toSeq == frameBytes(1).toSeq)

    context.markInterrupted("cancelled after the first frame")
    intercept[TaskKilledException](input.read())
    input.close()

    assert(client.requests.size() == 1)
    assert(client.failureReports == 0)
    assert(first.closeCalls == 1)
    assert(second.closeCalls == 0)
    context.markTaskCompleted(None)
  }

  test("cancellation during partition open closes the unpublished stream without invalidation") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    val stream = new TrackingInputStream(frameBytes(1))
    client.fileGroups.partitionGroups.put(0, location("worker"))
    client.streams.put(0, stream)
    client.readPartitionStarted = new CountDownLatch(1)
    client.allowReadPartition = new CountDownLatch(1)
    val input = rawReader(client, context).openPartitions()
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(() => {
      try input.read()
      catch { case caught: Throwable => failure.set(caught) }
    })
    worker.start()
    try {
      assert(client.readPartitionStarted.await(5, TimeUnit.SECONDS))
      context.markInterrupted("cancelled while opening a reducer")
    } finally {
      client.allowReadPartition.countDown()
      worker.join(5000)
    }

    assert(!worker.isAlive)
    assert(failure.get().isInstanceOf[TaskKilledException])
    assert(stream.closeCalls == 1)
    assert(client.failureReports == 0)
    input.close()
    context.markTaskCompleted(None)
  }

  test("malformed native frames invalidate once before allocating or opening another reducer") {
    def header(length: Long, fields: Long): Array[Byte] = ByteBuffer
      .allocate(16)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putLong(length)
      .putLong(fields)
      .array()

    val corruptFrames = Seq(
      frameBytes(1).take(3),
      frameBytes(1).dropRight(1),
      header(0L, 0L),
      header(Long.MinValue, 0L),
      header(Long.MaxValue, 0L),
      header(Int.MaxValue.toLong + 1, 0L),
      header(12L, -1L),
      header(12L, Int.MaxValue.toLong + 1),
      header(12L, 1L))
    for (frame <- corruptFrames; raw <- Seq(false, true)) {
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      val broken = new TrackingInputStream(frame)
      val next = new TrackingInputStream(frameBytes(2))
      client.fileGroups.partitionGroups.put(0, location("worker-a"))
      client.fileGroups.partitionGroups.put(1, location("worker-b"))
      client.streams.put(0, broken)
      client.streams.put(1, next)
      val remote = reader(simpleDependency(), client, context)

      val failure = if (raw) {
        val blocks = new CometShuffleBlockIterator(remote.readAsRawStream())
        intercept[FetchFailedException](blocks.hasNext())
      } else {
        intercept[FetchFailedException](remote.read().hasNext)
      }

      assert(failure.getCause.isInstanceOf[IOException])
      assert(client.failureReports == 1)
      assert(client.requests.size() == 1)
      assert(next.closeCalls == 0)
      context.markTaskCompleted(None)
      assert(broken.closeCalls == 1)
    }
  }

  test("failed generation invalidation preserves the original fetch error and is not retried") {
    Seq(false, true).foreach { reportThrows =>
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      val expected = new IOException("worker read failed")
      val reportFailure = new IOException("generation invalidation RPC failed")
      client.fileGroups.partitionGroups.put(0, location("worker"))
      client.readPartitionFailure = expected
      client.invalidateOnFetchFailure = false
      if (reportThrows) client.reportFetchFailureFailure = reportFailure
      val remote = reader(simpleDependency(), client, context)
      val blocks = new CometShuffleBlockIterator(remote.readAsRawStream())

      assert(intercept[IOException](blocks.hasNext()) eq expected)
      assert(client.failureReports == 1)
      assert(
        expected.getSuppressed.toSeq == (if (reportThrows) Seq(reportFailure) else Seq.empty))
      context.markTaskCompleted(None)
    }
  }

  test("raw and JVM consumers preserve metadata timeout and interruption classifications") {
    for (raw <- Seq(false, true); interrupted <- Seq(false, true)) {
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      if (interrupted) {
        client.updateFileGroupFailure =
          new IOException("metadata interrupted", new InterruptedException())
      } else {
        client.stageEnded = false
        client.timeoutFailures = 5
      }
      val remote = reader(simpleDependency(), client, context)

      val failure = if (raw) {
        val blocks = new CometShuffleBlockIterator(remote.readAsRawStream())
        intercept[IOException](blocks.hasNext())
      } else {
        intercept[IOException](remote.read().hasNext)
      }

      assert(failure.getCause.isInstanceOf[InterruptedException] == interrupted)
      assert(failure.getCause.isInstanceOf[TimeoutException] == !interrupted)
      assert(client.updateFileGroupCalls == (if (interrupted) 1 else 3))
      assert(client.failureReports == 0)
      context.markTaskCompleted(None)
    }
  }

  test("JVM readers never treat a truncated header as EOF when invalidation fails") {
    Seq(false, true).foreach { reportThrows =>
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      val reportFailure = new IOException("generation invalidation RPC failed")
      val stream = new TrackingInputStream(frameBytes(1).take(3))
      client.fileGroups.partitionGroups.put(0, location("worker"))
      client.streams.put(0, stream)
      client.invalidateOnFetchFailure = false
      if (reportThrows) client.reportFetchFailureFailure = reportFailure

      val failure =
        intercept[EOFException](reader(simpleDependency(), client, context).read().hasNext)

      assert(failure.getMessage.contains("Truncated"))
      assert(failure.getSuppressed.toSeq == (if (reportThrows) Seq(reportFailure) else Seq.empty))
      assert(client.failureReports == 1)
      context.markTaskCompleted(None)
      assert(stream.closeCalls == 1)
    }
  }

  test("reducer stream cleanup failures do not invalidate successfully fetched data") {
    Seq(false, true).foreach { raw =>
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      val expected = new IOException("reducer cleanup failed")
      client.fileGroups.partitionGroups.put(0, location("worker"))
      client.streams.put(
        0,
        new ByteArrayInputStream(Array.empty[Byte]) {
          override def close(): Unit = throw expected
        })
      val remote = reader(simpleDependency(), client, context)

      val failure = if (raw) {
        val blocks = new CometShuffleBlockIterator(remote.readAsRawStream())
        intercept[IOException](blocks.hasNext())
      } else {
        intercept[IOException](remote.read().hasNext)
      }

      assert(failure eq expected)
      assert(client.failureReports == 0)
      context.markTaskCompleted(None)
    }
  }

  test("JVM native decoding failures invalidate the Celeborn generation") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    val frame = frameBytes(1)
    frame(16) = 0 // Unknown codec, with a complete and correctly sized outer frame.
    client.fileGroups.partitionGroups.put(0, location("worker"))
    val stream = new TrackingInputStream(frame)
    client.streams.put(0, stream)

    val failure =
      intercept[FetchFailedException](reader(simpleDependency(), client, context).read().hasNext)

    assert(failure.getCause.getMessage.contains("invalid compression codec"))
    assert(client.failureReports == 1)
    context.markTaskCompleted(None)
    assert(stream.closeCalls == 1)
  }

  private def readRemoteFrame(
      frame: Array[Byte],
      attributes: Seq[Attribute],
      raw: Boolean): (Seq[InternalRow], Option[String], Int) = {
    val dependency = simpleDependency(1, attributes)
    val scan = OperatorOuterClass.ShuffleScan.newBuilder().setSource("CelebornRawInput")
    attributes.foreach(attribute =>
      scan.addFields(QueryPlanSerde.serializeDataType(attribute.dataType).get))
    val planBytes = OperatorOuterClass.Operator
      .newBuilder()
      .setShuffleScan(scan)
      .build()
      .toByteArray

    val results = spark.sparkContext
      .parallelize(Seq(frame), 1)
      .mapPartitions { frames =>
        val context = TaskContext.get()
        val client = new RecordingCelebornRawClient
        client.fileGroups.partitionGroups.put(
          0,
          java.util.Collections.singleton[Object]("worker"))
        client.streams.put(0, new ByteArrayInputStream(frames.next()))
        val metrics = context.taskMetrics.createTempShuffleReadMetrics()
        val partitions = new CelebornRawPartitionReader(
          client,
          dependency.shuffleId,
          91,
          0,
          Int.MaxValue,
          0,
          1,
          context,
          metrics,
          2)
        val remote = new CometCelebornShuffleReader[Int, ColumnarBatch](
          dependency,
          context,
          metrics,
          partitions)
        val native = if (raw) {
          val blocks = new CometShuffleBlockIterator(remote.readAsRawStream())
          Some(
            new CometExecIterator(
              CometExec.newIterId,
              Array[Object](blocks),
              dependency.outputAttributes.size,
              planBytes,
              CometMetricNode(Map.empty),
              1,
              0,
              shuffleBlockIterators = Map(0 -> blocks)))
        } else {
          None
        }
        val batches: Iterator[ColumnarBatch] = native match {
          case Some(iterator) => iterator
          case None => remote.read().map(_._2)
        }
        var rows = Vector.empty[InternalRow]
        val result =
          try {
            while (batches.hasNext) {
              rows ++= batches.next().rowIterator().asScala.map(_.copy())
            }
            (rows, None, client.failureReports)
          } catch {
            case failure: FetchFailedException =>
              (rows, Some(failure.getCause.getMessage), client.failureReports)
          } finally {
            native.foreach(_.close())
          }
        Iterator.single(result)
      }
      .collect()

    assert(results.length == 1)
    results.head
  }

  private def assertNativeDecodeFailure(
      frame: Array[Byte],
      expectedMessage: String,
      attributes: Seq[Attribute] = Seq.empty): Unit = {
    val (rows, failure, reports) = readRemoteFrame(frame, attributes, raw = true)
    assert(rows.isEmpty)
    assert(
      failure.exists(_.toLowerCase(java.util.Locale.ROOT)
        .contains(expectedMessage.toLowerCase(java.util.Locale.ROOT))))
    assert(reports == 1)
  }

  test("native ShuffleScan decode failures retain Spark fetch-failure identity across JNI") {
    val frame = frameBytes()
    frame(16) = 0
    assertNativeDecodeFailure(frame, "invalid compression codec")
  }

  test("native ShuffleScan rejects IPC column counts inconsistent with the declared schema") {
    withNativeFrame { (_, frame) =>
      val mismatched = frame.clone()
      // Keep a valid two-column IPC payload but claim zero fields in the outer header and plan.
      ByteBuffer.wrap(mismatched).order(ByteOrder.LITTLE_ENDIAN).putLong(8, 0L)
      assertNativeDecodeFailure(mismatched, "column count mismatch")
    }
  }

  private def arrowFrame(
      root: VectorSchemaRoot,
      dictionaries: DictionaryProvider = null): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root, dictionaries, Channels.newChannel(output))
    val payload =
      try {
        writer.start()
        writer.writeBatch()
        writer.end()
        output.toByteArray
      } finally writer.close()
    val frame = frameBytes(payload: _*)
    ByteBuffer
      .wrap(frame)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putLong(8, root.getFieldVectors.size().toLong)
    frame
  }

  private def intFrame(): Array[Byte] = {
    val allocator = new RootAllocator(Long.MaxValue)
    val values = new IntVector("value", allocator)
    val root = VectorSchemaRoot.of(values)
    try {
      values.allocateNew()
      Seq(-1, 0, 1).zipWithIndex.foreach { case (value, index) => values.setSafe(index, value) }
      values.setValueCount(3)
      root.setRowCount(3)
      arrowFrame(root)
    } finally {
      root.close()
      allocator.close()
    }
  }

  private def dictionaryFrame(): Array[Byte] = {
    val allocator = new RootAllocator(Long.MaxValue)
    val encoding = new DictionaryEncoding(0L, false, new ArrowType.Int(32, true))
    val values = new VarCharVector("dictionary", allocator)
    val keys = new IntVector(
      "value",
      new FieldType(true, new ArrowType.Int(32, true), encoding),
      allocator)
    val dictionaries = new MapDictionaryProvider(new Dictionary(values, encoding))
    val root = VectorSchemaRoot.of(keys)
    try {
      values.allocateNew()
      Seq("first", "second").zipWithIndex.foreach { case (value, index) =>
        values.setSafe(index, value.getBytes(StandardCharsets.UTF_8))
      }
      values.setValueCount(2)
      keys.allocateNew()
      Seq(0, 1, 0).zipWithIndex.foreach { case (value, index) => keys.setSafe(index, value) }
      keys.setValueCount(3)
      root.setRowCount(3)
      arrowFrame(root, dictionaries)
    } finally {
      root.close()
      dictionaries.close()
      allocator.close()
    }
  }

  private def listFrame(): Array[Byte] = {
    val allocator = new RootAllocator(Long.MaxValue)
    val values = ListVector.empty("value", allocator)
    values.initializeChildrenFromFields(
      Seq(Field.nullable("element", new ArrowType.Int(32, true))).asJava)
    val root = VectorSchemaRoot.of(values)
    try {
      values.allocateNew()
      val elements = values.getDataVector.asInstanceOf[IntVector]
      var offset = 0
      Seq(Seq(-1, 0), Seq(1), Seq.empty).zipWithIndex.foreach { case (row, index) =>
        values.startNewValue(index)
        row.foreach { value =>
          elements.setSafe(offset, value)
          offset += 1
        }
        values.endValue(index, row.size)
      }
      values.setValueCount(3)
      root.setRowCount(3)
      assert(root.getSchema.getFields.get(0).getChildren.get(0).isNullable)
      arrowFrame(root)
    } finally {
      root.close()
      allocator.close()
    }
  }

  private def mapFrame(nullableKeys: Boolean): Array[Byte] = {
    val allocator = new RootAllocator(Long.MaxValue)
    val intType = new ArrowType.Int(32, true)
    val key = Field.notNullable("key", intType)
    val value = Field.nullable("value", intType)
    val entries = new Field(
      "entries",
      FieldType.notNullable(ArrowType.Struct.INSTANCE),
      Seq(key, value).asJava)
    val field =
      new Field("value", FieldType.nullable(new ArrowType.Map(false)), Seq(entries).asJava)
    val values = field.createVector(allocator).asInstanceOf[MapVector]
    // Keep valid nonnull key buffers, but permit a deliberately invalid IPC key declaration.
    val schemaKey = if (nullableKeys) Field.nullable("key", intType) else key
    val schemaEntries =
      new Field(entries.getName, entries.getFieldType, Seq(schemaKey, value).asJava)
    val schemaField = new Field(field.getName, field.getFieldType, Seq(schemaEntries).asJava)
    val root = new VectorSchemaRoot(Seq(schemaField).asJava, Seq[FieldVector](values).asJava, 0)
    try {
      values.allocateNew()
      val writer = values.getWriter
      Seq(Seq(-1 -> 1), Seq(0 -> 2, 1 -> 3), Seq.empty).zipWithIndex.foreach {
        case (row, index) =>
          writer.setPosition(index)
          writer.startMap()
          row.foreach { case (keyValue, mapValue) =>
            writer.startEntry()
            writer.key().integer().writeInt(keyValue)
            writer.value().integer().writeInt(mapValue)
            writer.endEntry()
          }
          writer.endMap()
      }
      values.setValueCount(3)
      root.setRowCount(3)
      arrowFrame(root)
    } finally {
      root.close()
      allocator.close()
    }
  }

  private final class MutableIpcInt extends Table {
    def clearSignedness(): Int = {
      // Int.is_signed is the second field in the FlatBuffer table (vtable offset 6).
      val offset = __offset(6)
      require(offset != 0 && bb.get(bb_pos + offset) == 1)
      val position = bb_pos + offset
      bb.put(position, 0.toByte)
      position
    }
  }

  Seq(false -> "JVM reader", true -> "native ShuffleScan").foreach { case (raw, consumer) =>
    test(s"$consumer preserves valid negative Int32 shuffle values") {
      val frame = intFrame()
      val attributes = Seq(AttributeReference("value", IntegerType)())
      val (rows, failure, reports) = readRemoteFrame(frame, attributes, raw)
      assert(failure.isEmpty)
      assert(rows.map(_.getInt(0)) == Seq(-1, 0, 1))
      assert(rows.forall(!_.isNullAt(0)))
      assert(reports == 0)
    }

    test(s"$consumer accepts dictionary encoding with the expected logical value type") {
      val attributes = Seq(AttributeReference("value", StringType)())
      val (rows, failure, reports) = readRemoteFrame(dictionaryFrame(), attributes, raw)
      assert(failure.isEmpty)
      assert(rows.map(_.getUTF8String(0).toString) == Seq("first", "second", "first"))
      assert(reports == 0)
    }

    test(s"$consumer accepts array element nullability differences") {
      val attributes = Seq(AttributeReference("value", ArrayType(IntegerType, false))())
      val (rows, failure, reports) = readRemoteFrame(listFrame(), attributes, raw)
      assert(failure.isEmpty)
      assert(rows.map(_.getArray(0).toIntArray().toSeq) == Seq(Seq(-1, 0), Seq(1), Seq.empty))
      assert(reports == 0)
    }

    test(s"$consumer accepts map value nullability differences") {
      val attributes =
        Seq(AttributeReference("value", MapType(IntegerType, IntegerType, false))())
      val (rows, failure, reports) =
        readRemoteFrame(mapFrame(nullableKeys = false), attributes, raw)
      assert(failure.isEmpty)
      val maps = rows.map { row =>
        val map = row.getMap(0)
        map.keyArray().toIntArray().zip(map.valueArray().toIntArray()).toMap
      }
      assert(maps == Seq(Map(-1 -> 1), Map(0 -> 2, 1 -> 3), Map.empty))
      assert(reports == 0)
    }

    test(s"$consumer rejects nullable map keys before cast or import") {
      val attributes =
        Seq(AttributeReference("value", MapType(IntegerType, IntegerType, false))())
      val (rows, failure, reports) =
        readRemoteFrame(mapFrame(nullableKeys = true), attributes, raw)
      assert(rows.isEmpty)
      assert(
        failure.exists(
          _.toLowerCase(java.util.Locale.ROOT).contains("map key field must not be nullable")))
      assert(reports == 1)
    }

    test(s"$consumer rejects an IPC signedness mismatch before cast or import") {
      val pristine = intFrame()
      val corrupted = pristine.clone()
      val buffer = ByteBuffer.wrap(corrupted).order(ByteOrder.LITTLE_ENDIAN)
      buffer.position(20) // Skip the native frame header and NONE codec.
      assert(buffer.getInt() == -1) // IPC continuation marker.
      val schemaLength = buffer.getInt()
      val schemaStart = buffer.position()
      val schemaEnd = schemaStart + schemaLength
      val message = Message.getRootAsMessage(buffer)
      assert(message.headerType() == MessageHeader.Schema)
      val schema = message.header(new IpcSchema).asInstanceOf[IpcSchema]
      assert(schema.fieldsLength() == 1)
      val field = schema.fields(0)
      assert(field.typeType() == IpcType.Int)
      val intType = field.`type`(new IpcInt).asInstanceOf[IpcInt]
      assert(intType.bitWidth() == 32 && intType.isSigned())
      val mutable = field.`type`(new MutableIpcInt).asInstanceOf[MutableIpcInt]
      val signednessPosition = mutable.clearSignedness()
      assert(!intType.isSigned())
      assert(signednessPosition >= schemaStart && signednessPosition < schemaEnd)
      // Only the logical type changes: frame lengths, column count, and all batch buffers survive.
      assert(
        corrupted.indices.filter(index => corrupted(index) != pristine(index)) ==
          Seq(signednessPosition))

      val attributes = Seq(AttributeReference("value", IntegerType)())
      val (rows, failure, reports) = readRemoteFrame(corrupted, attributes, raw)
      assert(rows.isEmpty)
      assert(failure.exists(_.toLowerCase(java.util.Locale.ROOT).contains("type mismatch")))
      assert(reports == 1)
    }
  }

  test("reader attempt identities match writer packing and reject wrapped attempt numbers") {
    assert(CelebornRawPartitionReader.encodeAttemptNumber(0, 0) == 0)
    assert(CelebornRawPartitionReader.encodeAttemptNumber(1, 7) == 65543)
    assert(CelebornRawPartitionReader.encodeAttemptNumber(32767, 65535) == Int.MaxValue)

    Seq((-1, 0), (32768, 0), (0, -1), (0, 65536)).foreach { case (stageAttempt, taskAttempt) =>
      intercept[IllegalArgumentException] {
        CelebornRawPartitionReader.encodeAttemptNumber(stageAttempt, taskAttempt)
      }
    }
  }

  test("both remote consumption paths validate Arrow offsets before exposing decoded arrays") {
    val allocator = new RootAllocator(Long.MaxValue)
    val strings = new VarCharVector("value", allocator)
    val root = VectorSchemaRoot.of(strings)
    val payload =
      try {
        strings.allocateNew()
        strings.setSafe(0, Array[Byte](97, 98, 99))
        strings.setSafe(1, Array[Byte](100, 101, 102))
        strings.setValueCount(2)
        root.setRowCount(2)
        val output = new ByteArrayOutputStream()
        val writer = new ArrowStreamWriter(root, null, Channels.newChannel(output))
        try {
          writer.start()
          writer.writeBatch()
          writer.end()
          output.toByteArray
        } finally writer.close()
      } finally {
        root.close()
        allocator.close()
      }
    val offsets = ByteBuffer
      .allocate(12)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putInt(0)
      .putInt(3)
      .putInt(6)
      .array()
    val positions = payload
      .sliding(offsets.length)
      .zipWithIndex
      .collect {
        case (bytes, position) if bytes.sameElements(offsets) => position
      }
      .toSeq
    assert(positions.size == 1)
    ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN).putInt(positions.head + 8, 2)
    val frame = frameBytes(payload: _*)
    ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN).putLong(8, 1L)
    val attributes = Seq(AttributeReference("value", StringType)())

    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    client.fileGroups.partitionGroups.put(0, location("worker"))
    client.streams.put(0, new ByteArrayInputStream(frame))
    val failure = intercept[FetchFailedException] {
      reader(simpleDependency(1, attributes), client, context).read().hasNext
    }
    assert(failure.getCause.getMessage.toLowerCase(java.util.Locale.ROOT).contains("offset"))
    assert(client.failureReports == 1)
    context.markTaskCompleted(None)

    assertNativeDecodeFailure(frame, "offset", attributes)
  }

  test("empty reducers are skipped and null mapper-attempt metadata fails closed") {
    val emptyContext = TaskContext.empty()
    val emptyClient = new RecordingCelebornRawClient
    assert(rawReader(emptyClient, emptyContext).openPartitions().read() == -1)
    assert(emptyClient.requests.isEmpty)

    val brokenContext = TaskContext.empty()
    val brokenClient = new RecordingCelebornRawClient
    brokenClient.fileGroups.mapAttempts = null
    val failure = intercept[IllegalStateException] {
      rawReader(brokenClient, brokenContext).openPartitions().read()
    }
    assert(failure.getMessage.contains("mapper-attempt"))
    assert(brokenClient.requests.isEmpty)
  }

  test("native ShuffleScan consumes real remote shuffle frames without decompression") {
    withNativeFrame { (dependency, frame) =>
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      client.fileGroups.partitionGroups.put(0, location("worker"))
      client.streams.put(0, new ByteArrayInputStream(frame))
      val iterator =
        new CometShuffleBlockIterator(reader(dependency, client, context).readAsRawStream())

      val expectedLength = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN).getLong.toInt - 8
      assert(iterator.hasNext() == expectedLength)
      val actual = new Array[Byte](expectedLength)
      val buffer = iterator.getBuffer.duplicate()
      buffer.position(0)
      buffer.get(actual)
      assert(actual.toSeq == frame.slice(16, 16 + expectedLength).toSeq)
      assert(iterator.hasNext() == -1)
      assert(!client.requests.get(0).needDecompress)
      context.markTaskCompleted(None)
    }
  }

  test("remote validation is explicit and preserves the existing local decoder JNI entry point") {
    val parameters = Array[Class[_]](
      classOf[ByteBuffer],
      java.lang.Integer.TYPE,
      classOf[Array[Long]],
      classOf[Array[Long]],
      java.lang.Boolean.TYPE)
    assert(
      classOf[Native]
        .getDeclaredMethod("decodeShuffleBlock", parameters: _*)
        .getReturnType == java.lang.Long.TYPE)
    assert(
      classOf[Native]
        .getDeclaredMethod(
          "decodeShuffleBlockWithValidation",
          (parameters :+ classOf[Array[Byte]]): _*)
        .getReturnType == java.lang.Long.TYPE)

    val local = new CometShuffleBlockIterator(new ByteArrayInputStream(Array.empty[Byte]))
    assert(!local.requiresValidation())
    local.close()

    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    val remote =
      new CometShuffleBlockIterator(reader(simpleDependency(), client, context).readAsRawStream())
    assert(remote.requiresValidation())
    assert(client.updateFileGroupCalls == 0)
    remote.close()
    context.markTaskCompleted(None)
  }

  test("JVM consumers decode real remote shuffle frames and count rows") {
    withNativeFrame { (dependency, frame) =>
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      client.fileGroups.partitionGroups.put(0, location("worker"))
      client.streams.put(0, new ByteArrayInputStream(frame))

      val batches = reader(dependency, client, context).read()
      var rows = 0
      while (batches.hasNext) {
        rows += batches.next()._2.numRows()
      }

      assert(rows > 0)
      assert(context.taskMetrics.shuffleReadMetrics.recordsRead == rows)
      assert(!client.requests.get(0).needDecompress)
      context.markTaskCompleted(None)
    }
  }

  test("a remote shuffle reader cannot be consumed through both paths") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    val remote = reader(simpleDependency(), client, context)
    remote.readAsRawStream()

    val failure = intercept[IllegalStateException](remote.readAsRawStream())
    assert(failure.getMessage.contains("only be consumed once"))
    context.markTaskCompleted(None)
  }

  test("decoder cleanup releases a prefetched batch that was not consumed") {
    NativeBatchDecoderIteratorLifecycleChecks.closesPrefetchedBatch()
  }

  test("decoder cleanup releases its delivered batch exactly once") {
    NativeBatchDecoderIteratorLifecycleChecks.closesDeliveredBatch()
  }

  test("decoder cleanup releases remaining resources and preserves suppressed failures") {
    NativeBatchDecoderIteratorLifecycleChecks.preservesCleanupFailures()
  }

  test(
    "decoder reports data errors but does not invalidate shuffle generations on cleanup errors") {
    NativeBatchDecoderIteratorLifecycleChecks.forwardsDecodeFailuresButNotCleanup()
  }

  test("decoder does not invalidate persisted shuffle data on allocation or import failures") {
    NativeBatchDecoderIteratorLifecycleChecks.doesNotReportAllocationOrImportFailures()
  }

  test("decoder propagates input EOF exceptions without reclassifying stream cleanup failures") {
    NativeBatchDecoderIteratorLifecycleChecks.propagatesHeaderAndEofCleanupFailures()
  }

  test("decoder validates remote Arrow payloads without changing the local decode path") {
    NativeBatchDecoderIteratorLifecycleChecks.selectsValidationOnlyForRemoteStreams()
  }

  test("decoder rejects a missing remote schema before consuming or reporting persisted data") {
    NativeBatchDecoderIteratorLifecycleChecks.rejectsRemoteStreamsWithoutExpectedSchema()
  }
}
