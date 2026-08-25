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

import java.io.{ByteArrayInputStream, InputStream, IOException}
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files
import java.util.{LinkedHashSet, Set => JSet}
import java.util.concurrent.{CountDownLatch, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters._

import org.apache.spark.{HashPartitioner, ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.shuffle.{FetchFailedException, IndexShuffleBlockResolver, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.{CometConf, CometShuffleBlockIterator}

class CometCelebornShuffleReaderSuite extends CometTestBase {

  import testImplicits._

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
      client: RecordingCelebornRawClient,
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

  private def simpleDependency(
      partitions: Int = 3): CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch] =
    new CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      spark.sparkContext.emptyRDD[(Int, ColumnarBatch)],
      new HashPartitioner(partitions),
      decodeTime = SQLMetrics.createMetric(spark.sparkContext, "Celeborn decode time"))

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
    client.streams.put(0, new ByteArrayInputStream(Array[Byte](9)))
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
    assert(remote.readAsRawStream().readAllBytes().toSeq == Seq[Byte](9))
    assert(client.updateFileGroupCalls == 1)
    context.markTaskCompleted(None)
  }

  test("the manager routes native Celeborn handles through its reflected raw reader") {
    val context = TaskContext.empty()
    val dependency = simpleDependency()
    val client = new RecordingCelebornRawClient
    client.fileGroups.partitionGroups.put(1, location("worker"))
    client.streams.put(1, new ByteArrayInputStream(Array[Byte](3, 4)))
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
    assert(remote.readAsRawStream().readAllBytes().toSeq == Seq[Byte](3, 4))
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
    val first = new TrackingInputStream(Array[Byte](1, 2))
    val third = new TrackingInputStream(Array[Byte](3, 4, 5))
    client.fileGroups.partitionGroups.put(1, location("worker-b", "worker-c"))
    client.fileGroups.partitionGroups.put(2, location())
    client.fileGroups.partitionGroups.put(3, location("worker-a"))
    client.fileGroups.mapAttempts = Array(3, 1, 4)
    client.fileGroups.pushFailedBatches.put("worker-b", "replayed-batch")
    client.streams.put(1, first)
    client.streams.put(3, third)

    val input = rawReader(client, context, 4, 9, 1, 4).openPartitions()
    assert(client.updateFileGroupCalls == 0)
    assert(input.readAllBytes().toSeq == Seq[Byte](1, 2, 3, 4, 5))
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

  test("raw fetch prefers Apache Celeborn's public 15-argument partition reader") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient.StockApiClient
    client.fileGroups.partitionGroups.put(1, location("worker"))
    client.fileGroups.mapAttempts = Array(2, 4)
    client.streams.put(1, new ByteArrayInputStream(Array[Byte](3, 5)))

    val input = rawReader(client, context, startPartition = 1, endPartition = 2).openPartitions()
    assert(input.readAllBytes().toSeq == Seq[Byte](3, 5))
    assert(client.stockReadPartitionCalls == 1)
    assert(client.requests.size() == 1)
    val request = client.requests.get(0)
    assert(request.shuffleId == 91)
    assert(request.appShuffleId == 17)
    assert(request.coalescedPartitionInfos == null)
    assert(request.mapAttempts eq client.fileGroups.mapAttempts)
    assert(!request.needDecompress)
    input.close()
    context.markTaskCompleted(None)
  }

  test("raw fetch forwards Celeborn byte, block, and wait metrics") {
    val context = TaskContext.empty()
    val client = new RecordingCelebornRawClient
    client.fileGroups.partitionGroups.put(0, location("worker"))
    client.streams.put(0, new ByteArrayInputStream(Array[Byte](7)))
    val input = rawReader(client, context).openPartitions()
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

  test("native raw consumption merges task shuffle metrics at EOF and close only once") {
    Seq(false, true).foreach { closeEarly =>
      val context = TaskContext.empty()
      val client = new RecordingCelebornRawClient
      client.fileGroups.partitionGroups.put(0, location("worker"))
      client.streams.put(0, new ByteArrayInputStream(Array[Byte](7, 8)))
      val input = reader(simpleDependency(), client, context).readAsRawStream()
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
    client.streams.put(0, new ByteArrayInputStream(Array[Byte](7)))
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
      val stream = new TrackingInputStream(Array(partition.toByte))
      client.fileGroups.partitionGroups.put(partition, location(s"worker-$partition"))
      client.streams.put(partition, stream)
      stream
    }

    val input = reader(dependency, client, context).readAsRawStream()
    assert(completionListenerCount(context) == 1)
    assert(input.readAllBytes().length == reducers)
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
    val first = new TrackingInputStream(Array[Byte](1, 2))
    val second = new TrackingInputStream(Array[Byte](3))
    client.fileGroups.partitionGroups.put(0, location("worker-a"))
    client.fileGroups.partitionGroups.put(1, location("worker-b"))
    client.streams.put(0, first)
    client.streams.put(1, second)

    val input = rawReader(client, context).openPartitions()
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
    val stream = new TrackingInputStream(Array[Byte](1, 2))
    client.fileGroups.partitionGroups.put(0, location("worker"))
    client.streams.put(0, stream)

    val input = reader(dependency, client, context).readAsRawStream()
    assert(input.read() == 1)
    context.markTaskCompleted(None)

    assert(stream.closeCalls == 1)
  }

  test("task completion racing reducer-stream creation closes the unpublished stream") {
    val context = TaskContext.empty()
    val dependency = simpleDependency()
    val client = new RecordingCelebornRawClient
    val stream = new TrackingInputStream(Array[Byte](1))
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
    client.streams.put(0, new ByteArrayInputStream(Array[Byte](9)))

    val input = rawReader(client, context, retries = 2).openPartitions()
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
        assert(failure eq expected)
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
}
