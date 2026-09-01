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

package org.apache.comet.shuffle

import java.io.IOException
import java.nio.{ByteBuffer, ByteOrder}
import java.util.{ArrayList => JArrayList, Arrays, List => JList, Optional}
import java.util.concurrent.{AbstractExecutorService, ConcurrentHashMap, CountDownLatch, ExecutorService, LinkedBlockingQueue, RejectedExecutionException, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference, LongAdder}
import java.util.zip.CRC32

import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf

class CelebornShufflePartitionPusherSuite extends AnyFunSuite {

  private val encodedAttemptId = (4 << 16) | 7

  private def pusher(client: AnyRef): CelebornShufflePartitionPusher =
    new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9)

  private def clientArguments: Array[AnyRef] = Array[AnyRef](
    "native-celeborn-application",
    "localhost",
    Int.box(9097),
    new RecordingCelebornClientConf,
    new RecordingCelebornUserIdentifier,
    Array[Byte](1, 2, 3))

  private def frame(bodyLength: Int = java.lang.Long.BYTES): Array[Byte] = {
    val buffer = ByteBuffer
      .allocate(java.lang.Long.BYTES + bodyLength)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putLong(bodyLength.toLong)
    (0 until bodyLength).foreach(index => buffer.put((index + 1).toByte))
    buffer.array()
  }

  private def combinedCrc(frames: Array[Byte]*): Long = {
    frames.foldLeft(0L) { (combined, bytes) =>
      val checksum = new CRC32
      checksum.update(bytes, 0, bytes.length)
      (0 until java.lang.Integer.BYTES).foldLeft(0L) { (result, index) =>
        val shift = index * java.lang.Byte.SIZE
        val next = ((combined >>> shift) & 0xffL) + ((checksum.getValue >>> shift) & 0xffL)
        result | ((next & 0xffL) << shift)
      }
    }
  }

  test("raw push forwards captured task metadata and preserves the complete Comet frame") {
    val client = new RecordingCelebornPushClient
    val bytes = frame()

    pusher(client).pushPartitionData(6, bytes, bytes.length)

    val push = client.lastPush
    assert(push.shuffleId == 19)
    assert(push.mapId == 3)
    assert(push.attemptId == encodedAttemptId)
    assert(push.partitionId == 6)
    assert(push.bytes eq bytes)
    assert(push.offset == 0)
    assert(push.length == bytes.length)
    assert(push.numMappers == 12)
    assert(push.numPartitions == 9)
    assert(push.doPush)
    assert(push.skipCompress)
  }

  test("raw push accepts a complete frame within a larger backing array") {
    val client = new RecordingCelebornPushClient
    val completeFrame = frame()
    val backingArray = Arrays.copyOf(completeFrame, completeFrame.length + 4)

    pusher(client).pushPartitionData(0, backingArray, completeFrame.length)

    assert(client.lastPush.bytes eq backingArray)
    assert(client.lastPush.length == completeFrame.length)
  }

  test("raw push rejects fewer bytes than the frame and Celeborn transport header require") {
    val bytes = frame()

    Seq(0, bytes.length, bytes.length + 15, -1).foreach { accepted =>
      val client = new RecordingCelebornPushClient
      client.acceptedBytes = Some(accepted)

      val failure = intercept[IOException] {
        pusher(client).pushPartitionData(0, bytes, bytes.length)
      }

      assert(failure.getMessage.contains(accepted.toString))
      assert(failure.getMessage.contains((bytes.length + 16).toString))
    }
  }

  test("legacy Celeborn clients without integrity accounting remain compatible") {
    val client = new RecordingCelebornPushClient
    val bytes = frame()

    assert(!client.getClass.getMethods.exists(_.getName == "computeBatchCRC"))
    pusher(client).pushPartitionData(2, bytes, bytes.length)

    assert(client.pushCount == 1)
    assert(client.lastPush.partitionId == 2)
    assert(client.lastPush.skipCompress)
  }

  test("integrity accounting records every complete frame exactly once before each raw push") {
    val client = new IntegrityCheckingCelebornPushClient
    val adapter = pusher(client)
    val first = frame(8)
    val second = frame(11)
    val third = frame(12)

    adapter.pushPartitionData(2, first, first.length)
    adapter.pushPartitionData(5, second, second.length)
    adapter.pushPartitionData(2, third, third.length)

    assert(client.pushCount == 3)
    assert(client.accountedFrames.size == 3)
    assert(client.partitionCrc(2) == combinedCrc(first, third))
    assert(client.partitionBytes(2) == first.length + third.length)
    assert(client.partitionCrc(5) == combinedCrc(second))
    assert(client.partitionBytes(5) == second.length)
    assert(
      client.invocationOrder.toSeq ==
        Seq("crc:2", "push:2", "crc:5", "push:5", "crc:2", "push:2"))
    assert(client.accountedFrames.forall(_.shuffleId == 19))
    assert(client.accountedFrames.forall(_.mapId == 3))
    assert(client.accountedFrames.forall(_.attemptId == encodedAttemptId))
    assert(client.accountedFrames.forall(_.offset == 0))
    assert(
      client.accountedFrames
        .map(_.length)
        .toSeq == Seq(first.length, second.length, third.length))
    assert(client.accountedFrames.head.bytes eq first)
    assert(client.recordedPushes.forall(_.doPush))
    assert(client.recordedPushes.forall(_.skipCompress))
  }

  test("integrity accounting failures prevent raw pushes and preserve the original exception") {
    val bytes = frame()

    Seq[Throwable](
      new IOException("Celeborn integrity accounting rejected the frame"),
      new IllegalStateException("Celeborn integrity accounting state was unavailable"))
      .foreach { expected =>
        val client = new IntegrityCheckingCelebornPushClient
        client.integrityFailure = expected

        val actual = intercept[Throwable] {
          pusher(client).pushPartitionData(0, bytes, bytes.length)
        }

        assert(actual eq expected)
        assert(client.pushCount == 0)
        assert(client.accountedFrames.isEmpty)
        assert(client.invocationOrder.toSeq == Seq("crc:0"))
      }
  }

  test("encrypted native clients are rejected before encoding or raw allocation") {
    val client = new RecordingCelebornPushClient
    val cryptoHandler = new RecordingCelebornCryptoHandler
    client.cryptoHandler = Optional.of(cryptoHandler)

    val failure = intercept[UnsupportedOperationException] {
      pusher(client)
    }

    assert(failure.getMessage.contains("Encrypted native Celeborn shuffle is not supported"))
    assert(client.pushCount == 0)
    assert(cryptoHandler.encryptionCount == 0)
    assert(client.cryptoHandler.get() eq cryptoHandler)
  }

  test("Spark IO encryption is rejected before creating a native task pusher") {
    val client = new RecordingCelebornPushClient
    val conf = new SparkConf(false).set("spark.io.encryption.enabled", "true")
    val failure = intercept[IllegalArgumentException] {
      CelebornShufflePusherFactory.create(conf, client, 19, 12, 9, null)
    }
    assert(failure.getMessage.contains("Encrypted native Celeborn shuffle is not supported"))
    assert(client.pushCount == 0)
  }

  test("crypto-aware client acquisition preserves encryption for ordinary Spark shuffle") {
    val conf = new SparkConf(false).set("spark.io.encryption.enabled", "true")
    val cryptoHandler = new RecordingCelebornCryptoHandler
    val bytes = frame()
    RecordingCryptoAwareCelebornClientFactory.reset()
    var observedConf: SparkConf = null

    val client = CelebornShufflePusherFactory
      .resolveClient(
        conf,
        classOf[RecordingCryptoAwareCelebornClientFactory],
        classOf[RecordingCelebornClientConf],
        classOf[RecordingCelebornUserIdentifier],
        clientArguments,
        sparkConf => {
          observedConf = sparkConf
          Optional.of(cryptoHandler)
        })
      .asInstanceOf[RecordingCelebornPushClient]

    // The shared application client must keep its real crypto handler even though native RSS
    // cannot currently bound that handler's extra allocations and retained high-water buffer.
    client.pushOrMergeData(19, 3, encodedAttemptId, 6, bytes, 0, bytes.length, 12, 9, true, true)

    assert(observedConf eq conf)
    assert(RecordingCryptoAwareCelebornClientFactory.cryptoAwareCalls.get() == 1)
    assert(RecordingCryptoAwareCelebornClientFactory.legacyCalls.get() == 0)
    assert(cryptoHandler.encryptionCount == 1)
    assert(cryptoHandler.plaintext.sameElements(bytes))
    assert(cryptoHandler.encryptedLength == bytes.length + 20)
  }

  test("older Celeborn clients retain their six-argument client acquisition API") {
    val conf = new SparkConf(false)
    var cryptoHandlerResolved = false
    RecordingLegacyCelebornClientFactory.calls.set(0)

    val client = CelebornShufflePusherFactory.resolveClient(
      conf,
      classOf[RecordingLegacyCelebornClientFactory],
      classOf[RecordingCelebornClientConf],
      classOf[RecordingCelebornUserIdentifier],
      clientArguments,
      _ => {
        cryptoHandlerResolved = true
        Optional.empty[AnyRef]()
      })

    assert(client.isInstanceOf[RecordingCelebornPushClient])
    assert(RecordingLegacyCelebornClientFactory.calls.get() == 1)
    assert(!cryptoHandlerResolved)
  }

  test("crypto-handler failures never fall back to an unencrypted Celeborn client") {
    val conf = new SparkConf(false).set("spark.io.encryption.enabled", "true")
    val expected = new IllegalStateException("Spark shuffle encryption key was unavailable")
    RecordingCryptoAwareCelebornClientFactory.reset()

    val actual = intercept[IllegalStateException] {
      CelebornShufflePusherFactory.resolveClient(
        conf,
        classOf[RecordingCryptoAwareCelebornClientFactory],
        classOf[RecordingCelebornClientConf],
        classOf[RecordingCelebornUserIdentifier],
        clientArguments,
        _ => throw expected)
    }

    assert(actual eq expected)
    assert(RecordingCryptoAwareCelebornClientFactory.cryptoAwareCalls.get() == 0)
    assert(RecordingCryptoAwareCelebornClientFactory.legacyCalls.get() == 0)
  }

  test(
    "encryption enabled after binding is rejected before reservation or integrity accounting") {
    val client = new IntegrityCheckingCelebornPushClient
    val cryptoHandler = new RecordingCelebornCryptoHandler
    val bytes = frame(13)
    val adapter = pusher(client)
    client.cryptoHandler = Optional.of(cryptoHandler)

    intercept[UnsupportedOperationException] {
      adapter.reservePartitionData(3 * bytes.length)
    }
    intercept[UnsupportedOperationException] {
      adapter.pushPartitionData(4, bytes, bytes.length)
    }

    assert(client.accountedFrames.isEmpty)
    assert(client.invocationOrder.isEmpty)
    assert(cryptoHandler.encryptionCount == 0)
    assert(client.pushCount == 0)
    assert(client.cryptoHandler.get() eq cryptoHandler)
  }

  test("raw push preserves the exact IOException thrown by the Celeborn client") {
    val client = new RecordingCelebornPushClient
    val expected = new IOException("Celeborn worker rejected the shuffle frame")
    val bytes = frame()
    client.failure = expected

    val actual = intercept[IOException] {
      pusher(client).pushPartitionData(0, bytes, bytes.length)
    }

    assert(actual eq expected)
  }

  test("raw push preserves unchecked exceptions and errors thrown by the Celeborn client") {
    val bytes = frame()

    val runtimeClient = new RecordingCelebornPushClient
    val runtimeFailure = new IllegalStateException("Celeborn client was closed")
    runtimeClient.failure = runtimeFailure
    val actualRuntimeFailure = intercept[IllegalStateException] {
      pusher(runtimeClient).pushPartitionData(0, bytes, bytes.length)
    }
    assert(actualRuntimeFailure eq runtimeFailure)

    val errorClient = new RecordingCelebornPushClient
    val expectedError = new AssertionError("Celeborn client invariant failed")
    errorClient.failure = expectedError
    val actualError = intercept[AssertionError] {
      pusher(errorClient).pushPartitionData(0, bytes, bytes.length)
    }
    assert(actualError eq expectedError)
  }

  test("raw push wraps unexpected checked client failures without losing their cause") {
    val client = new RecordingCelebornPushClient
    val expected = new InterruptedException("Celeborn client was interrupted")
    val bytes = frame()
    client.failure = expected

    val actual = intercept[IOException] {
      pusher(client).pushPartitionData(0, bytes, bytes.length)
    }

    assert(actual.getCause eq expected)
  }

  test("adapter rejects a missing client or an incompatible public Celeborn raw-push API") {
    intercept[IllegalArgumentException] {
      new CelebornShufflePartitionPusher(null, 0, 0, 0, 1, 1)
    }

    val missing = intercept[IllegalArgumentException] {
      new CelebornShufflePartitionPusher(new Object, 0, 0, 0, 1, 1)
    }
    assert(missing.getMessage.contains("public raw-push"))

    val wrongReturnType = intercept[IllegalArgumentException] {
      new CelebornShufflePartitionPusher(new WrongReturnTypeCelebornPushClient, 0, 0, 0, 1, 1)
    }
    assert(wrongReturnType.getMessage.contains("returning an int"))
  }

  test("adapter rejects invalid shuffle, map-attempt, mapper, and partition metadata") {
    val client = new RecordingCelebornPushClient
    val invalidMetadata = Seq(
      (-1, 0, 0, 1, 1),
      (0, -1, 0, 1, 1),
      (0, 0, -1, 1, 1),
      (0, 0, 0, 0, 1),
      (0, 1, 0, 1, 1),
      (0, 0, 0, 1, 0))

    invalidMetadata.foreach { case (shuffleId, mapId, attemptId, numMappers, numPartitions) =>
      intercept[IllegalArgumentException] {
        new CelebornShufflePartitionPusher(
          client,
          shuffleId,
          mapId,
          attemptId,
          numMappers,
          numPartitions)
      }
    }
  }

  test("adapter rejects invalid output partitions and frame lengths before invoking Celeborn") {
    val client = new RecordingCelebornPushClient
    val adapter = pusher(client)
    val bytes = frame()

    Seq(-1, 9).foreach { partitionId =>
      intercept[IOException] {
        adapter.pushPartitionData(partitionId, bytes, bytes.length)
      }
    }

    intercept[IOException] {
      adapter.pushPartitionData(0, null, bytes.length)
    }

    Seq(-1, 0, java.lang.Long.BYTES, bytes.length - 1, bytes.length + 1).foreach { length =>
      intercept[IOException] {
        adapter.pushPartitionData(0, bytes, length)
      }
    }

    val overflow = intercept[IOException] {
      adapter.pushPartitionData(0, bytes, Int.MaxValue)
    }
    assert(overflow.getMessage.contains("transport header"))
    assert(client.pushCount == 0)
  }

  test("adapter rejects truncated, concatenated, and incorrectly encoded shuffle frames") {
    val client = new RecordingCelebornPushClient
    val adapter = pusher(client)

    Seq(-1L, 0L, 7L, 9L, Long.MaxValue).foreach { declaredLength =>
      val bytes = frame()
      ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(declaredLength)

      val failure = intercept[IOException] {
        adapter.pushPartitionData(0, bytes, bytes.length)
      }
      assert(failure.getMessage.contains("body bytes"))
    }

    val wrongByteOrder = frame()
    ByteBuffer.wrap(wrongByteOrder).order(ByteOrder.BIG_ENDIAN).putLong(8L)
    intercept[IOException] {
      adapter.pushPartitionData(0, wrongByteOrder, wrongByteOrder.length)
    }

    assert(client.pushCount == 0)
  }

  test("captured map-task metadata remains available when native invokes a worker thread") {
    val client = new RecordingCelebornPushClient
    val adapter = pusher(client)
    val bytes = frame()
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(new Runnable {
      override def run(): Unit = {
        try adapter.pushPartitionData(4, bytes, bytes.length)
        catch { case error: Throwable => failure.set(error) }
      }
    })

    worker.start()
    worker.join(5000)

    assert(!worker.isAlive)
    assert(failure.get() == null)
    assert(client.lastPush.shuffleId == 19)
    assert(client.lastPush.mapId == 3)
    assert(client.lastPush.attemptId == encodedAttemptId)
    assert(client.lastPush.partitionId == 4)
  }

  test("configured complete-frame and three-copy executor bounds are both enforced") {
    val client = new RecordingCelebornPushClient
    val bounded = new CelebornShufflePartitionPusher(client, 19, 3, 7, 12, 9, 24, 160)
    assert(bounded.maxFrameBytes() == 24)
    assert(bounded.numPartitions() == 9)

    val oversized = frame(17)
    val failure = intercept[IOException] {
      bounded.pushPartitionData(0, oversized, oversized.length)
    }
    assert(failure.getMessage.contains("configured maximum"))
    assert(client.pushCount == 0)

    val budgetClient = new RecordingCelebornPushClient
    val admissionBounded =
      new CelebornShufflePartitionPusher(budgetClient, 19, 3, 7, 12, 9, 1024, 76)
    assert(admissionBounded.maxFrameBytes() == 20)

    intercept[IllegalArgumentException] {
      new CelebornShufflePartitionPusher(new RecordingCelebornPushClient, 19, 3, 7, 12, 9, 15, 76)
    }
    intercept[IllegalArgumentException] {
      new CelebornShufflePartitionPusher(new RecordingCelebornPushClient, 19, 3, 7, 12, 9, 20, 63)
    }
  }

  test("Celeborn 0.7 mapperEnd receives all five task fields and reports Comet partition bytes") {
    val client = new RecordingCelebornPushClient
    val adapter = pusher(client)
    val first = frame(8)
    val second = frame(11)
    adapter.pushPartitionData(2, first, first.length)
    adapter.pushPartitionData(6, second, second.length)

    val sizes = adapter.finish()
    assert(sizes.length == 9)
    assert(sizes(2) == first.length)
    assert(sizes(6) == second.length)
    assert(
      sizes.zipWithIndex
        .filterNot { case (_, index) => index == 2 || index == 6 }
        .forall(_._1 == 0))
    assert(client.mapperEndCalls.get() == 1)
    assert(client.lastMapperEnd == ((19, 3, encodedAttemptId, 12, 9)))
    assert(adapter.finish().sameElements(sizes))
    assert(client.mapperEndCalls.get() == 1)

    intercept[IOException] {
      adapter.pushPartitionData(2, first, first.length)
    }
  }

  test("Celeborn 0.6 four-argument mapperEnd remains supported") {
    val client = new LegacyMapperEndCelebornPushClient
    val adapter = new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9)
    val bytes = frame()
    adapter.pushPartitionData(5, bytes, bytes.length)

    val sizes = adapter.finish()
    assert(sizes(5) == bytes.length)
    assert(client.mapperEndCalls.get() == 1)
    assert(client.lastMapperEnd == ((19, 3, encodedAttemptId, 12)))
  }

  test("mapperEnd preserves its original failure and cancels the failed map exactly once") {
    val client = new RecordingCelebornPushClient
    val expected = new IOException("Celeborn asynchronous worker push failed")
    client.mapperEndFailure = expected

    val actual = intercept[IOException] {
      pusher(client).finish()
    }
    assert(actual eq expected)
    assert(client.mapperEndCalls.get() == 1)
    assert(client.cleanupCalls.get() == 1)
    assert(client.lastCleanup == ((19, 3, encodedAttemptId)))
  }

  test("executor admission remains charged until stock Celeborn completes its async request") {
    val client = new AsyncRecordingCelebornPushClient
    val adapter = new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 64)
    val bytes = frame()
    adapter.pushPartitionData(2, bytes, bytes.length)
    val state = client.currentState(19, 3, encodedAttemptId)
    assert(state.inFlightRequestTracker.totalInflightReqs.sum() == 1)

    val failure = new AtomicReference[Throwable]()
    val second = new Thread(() => {
      try adapter.pushPartitionData(3, bytes, bytes.length)
      catch { case error: Throwable => failure.set(error) }
    })
    second.start()
    second.join(100)
    assert(second.isAlive, "the first async request must retain shared byte admission")
    assert(client.pushCount == 1)

    client.complete(state)
    second.join(5000)
    assert(!second.isAlive)
    assert(failure.get() == null)
    assert(client.pushCount == 2)
    client.complete(state)
    val sizes = adapter.finish()
    assert(sizes(2) == bytes.length)
    assert(sizes(3) == bytes.length)
  }

  test("cleanup does not release pinned request bytes before the cancelled transport completes") {
    val client = new AsyncRecordingCelebornPushClient
    val first = new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 64)
    val bytes = frame()
    first.pushPartitionData(0, bytes, bytes.length)
    val cancelledState = client.currentState(19, 3, encodedAttemptId)
    first.abort()
    first.abort()
    assert(client.cleanupCalls.get() == 0)
    assert(cancelledState.exception.get() == null)
    assert(cancelledState.inFlightRequestTracker.totalInflightReqs.sum() == 1)

    val retry =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId + 1, 12, 9, 64)
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(() => {
      try retry.pushPartitionData(1, bytes, bytes.length)
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    worker.join(100)
    assert(worker.isAlive, "cleanup alone must not free an unfinished network request")
    assert(client.pushCount == 1)

    client.complete(cancelledState)
    worker.join(5000)
    assert(!worker.isAlive)
    assert(failure.get() == null)
    awaitCleanup(client)
    val retryState = client.currentState(19, 3, encodedAttemptId + 1)
    client.complete(retryState)
    assert(retry.finish()(1) == bytes.length)
  }

  test("failed transport after cleanup releases only its completed shared admission") {
    val client = new TransportRecordingCelebornPushClient
    val first = new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 64)
    val bytes = frame()
    first.pushPartitionData(0, bytes, bytes.length)
    val cancelledState = client.currentState(19, 3, encodedAttemptId)
    val unrelatedState = new RecordingCelebornPushState
    client.dataClientFactory.handler.add(unrelatedState)
    first.abort()

    assert(cancelledState.exception.get().getMessage == "Cleaned Up")
    assert(cancelledState.inFlightRequestTracker.totalInflightReqs.sum() == 1)

    val retry =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId + 1, 12, 9, 64)
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(() => {
      try retry.pushPartitionData(1, bytes, bytes.length)
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    worker.join(100)
    assert(worker.isAlive, "cleanup must not release a held transport request")

    client.failTransport(unrelatedState, new IOException("unrelated connection closed"))
    worker.join(100)
    assert(worker.isAlive, "another map's completed request must not release this map's bytes")

    client.failTransport(cancelledState, new IOException("cancelled connection closed"))
    worker.join(5000)
    assert(!worker.isAlive, "failed transport completion after cleanup must release admission")
    assert(failure.get() == null)
    assert(cancelledState.inFlightRequestTracker.totalInflightReqs.sum() == 1)

    val retryState = client.currentState(19, 3, encodedAttemptId + 1)
    client.complete(retryState)
    assert(retry.finish()(1) == bytes.length)
  }

  test("failure callbacks removed before the raw push returns remain observable after cleanup") {
    Seq(false, true).foreach { openConnection =>
      val client = new TransportRecordingCelebornPushClient
      client.openConnectionBeforePush = openConnection
      val first =
        new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 604, 2416)
      val bytes = frame(596)
      val removed = new CountDownLatch(1)
      val resumeCallback = new CountDownLatch(1)
      val callbackFailure = new AtomicReference[Throwable]()
      var callbackThread: Thread = null

      client.beforePushReturns = state => {
        val request = client.dataClientFactory.handler.remove(state)
        callbackThread = new Thread(() => {
          try {
            removed.countDown()
            assert(resumeCallback.await(5, TimeUnit.SECONDS))
            request.callback.onFailure(new IOException("connection closed after cancellation"))
          } catch {
            case failure: Throwable => callbackFailure.set(failure)
          }
        })
        callbackThread.start()
        assert(removed.await(5, TimeUnit.SECONDS))
      }

      var worker: Thread = null
      try {
        first.reservePartitionData(1812)
        first.pushPartitionData(0, bytes, bytes.length)
        first.releasePartitionDataReservation()
        client.beforePushReturns = _ => ()
        val state = client.currentState(19, 3, encodedAttemptId)
        assert(client.dataClientFactory.handler.outstandingPushes.isEmpty)
        first.abort()
        assert(state.exception.get().getMessage == "Cleaned Up")
        assert(state.inFlightRequestTracker.totalInflightReqs.sum() == 1)

        val retry = new CelebornShufflePartitionPusher(
          client,
          19,
          3,
          encodedAttemptId + 1,
          12,
          9,
          604,
          2416)
        val failure = new AtomicReference[Throwable]()
        worker = new Thread(() => {
          try {
            retry.reservePartitionData(1812)
            try retry.pushPartitionData(1, bytes, bytes.length)
            finally retry.releasePartitionDataReservation()
          } catch {
            case error: Throwable => failure.set(error)
          }
        })
        worker.start()
        worker.join(100)
        assert(worker.isAlive, "removal and cleanup must not release the paused callback's bytes")

        resumeCallback.countDown()
        callbackThread.join(5000)
        assert(!callbackThread.isAlive)
        assert(callbackFailure.get() == null)
        worker.join(5000)
        assert(!worker.isAlive, "completion must remain observable after transport-map removal")
        assert(failure.get() == null)
        assert(state.inFlightRequestTracker.totalInflightReqs.sum() == 1)

        val retryState = client.currentState(19, 3, encodedAttemptId + 1)
        client.complete(retryState)
        assert(retry.finish()(1) == bytes.length)
      } finally {
        resumeCallback.countDown()
        if (callbackThread != null) {
          callbackThread.join(5000)
        }
        if (worker != null) {
          worker.interrupt()
          worker.join(5000)
        }
      }
    }
  }

  test("cancelled exact ownership remains sealed after a later bootstrap downgrade") {
    val client = new TransportRecordingCelebornPushClient
    val first =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 604, 2416)
    val bytes = frame(596)
    first.pushPartitionData(0, bytes, bytes.length)
    val state = client.currentState(19, 3, encodedAttemptId)
    val originalHandler = client.dataClientFactory.handler

    first.abort()
    assert(state.exception.get().getMessage == "Cleaned Up")
    client.dataClientFactory.openUninstrumentableConnection()

    assertNextMapWaitsForCompletion(client) {
      originalHandler
        .remove(state)
        .callback
        .onFailure(new IOException("sealed callback failed after global downgrade"))
    }
  }

  test("failed bootstrap instrumentation falls back to push-state completion") {
    val client = new TransportRecordingCelebornPushClient
    client.openUninstrumentableConnectionBeforePush = true
    val bytes = frame()
    val first =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 64)

    first.pushPartitionData(0, bytes, bytes.length)
    val firstState = client.currentState(19, 3, encodedAttemptId)

    def startPush(attemptId: Int)
        : (CelebornShufflePartitionPusher, AtomicReference[Throwable], CountDownLatch, Thread) = {
      val next = new CelebornShufflePartitionPusher(client, 19, 3, attemptId, 12, 9, 64)
      val failure = new AtomicReference[Throwable]()
      val started = new CountDownLatch(1)
      val worker = new Thread(() => {
        try {
          started.countDown()
          next.pushPartitionData(1, bytes, bytes.length)
        } catch { case error: Throwable => failure.set(error) }
      })
      worker.start()
      (next, failure, started, worker)
    }

    val (second, secondFailure, secondStarted, secondWorker) = startPush(encodedAttemptId + 1)
    try {
      assert(secondStarted.await(5, TimeUnit.SECONDS))
      secondWorker.join(100)
      assert(secondWorker.isAlive, "an uninstrumented push must retain counter-backed admission")
      client.complete(firstState)
      secondWorker.join(5000)
      assert(!secondWorker.isAlive)
      assert(secondFailure.get() == null)

      val secondState = client.currentState(19, 3, encodedAttemptId + 1)
      val (third, thirdFailure, thirdStarted, thirdWorker) = startPush(encodedAttemptId + 2)
      try {
        assert(thirdStarted.await(5, TimeUnit.SECONDS))
        thirdWorker.join(100)
        assert(thirdWorker.isAlive, "counter fallback must persist for later pushes")
        client.complete(secondState)
        thirdWorker.join(5000)
        assert(!thirdWorker.isAlive)
        assert(thirdFailure.get() == null)
        val thirdState = client.currentState(19, 3, encodedAttemptId + 2)
        client.complete(thirdState)
        assert(third.finish()(1) == bytes.length)
      } finally {
        thirdWorker.interrupt()
        thirdWorker.join(5000)
      }
      assert(second.finish()(1) == bytes.length)
      assert(first.finish()(0) == bytes.length)
    } finally {
      secondWorker.interrupt()
      secondWorker.join(5000)
    }
  }

  test("aborted fallback push releases admission after its failed transport completes") {
    val client = new TransportRecordingCelebornPushClient
    client.openUninstrumentableConnectionBeforePush = true
    val first =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 604, 2416)
    val bytes = frame(596)

    first.pushPartitionData(0, bytes, bytes.length)
    val state = client.currentState(19, 3, encodedAttemptId)
    first.abort()
    assert(client.cleanupCalls.get() == 0)
    assert(state.exception.get() == null)

    val terminalFailure = new IOException(
      s"Push data to worker failed for shuffle 19 map 3 attempt $encodedAttemptId " +
        "partition 0 batch 1.")
    assertNextMapWaitsForCompletion(client) {
      client.failTransport(state, terminalFailure)
      // Stock Celeborn's failure path leaves this counter charged. The terminal failure itself
      // is the completion signal for the cancelled fallback request.
      assert(state.inFlightRequestTracker.totalInflightReqs.sum() == 1)
    }
    awaitCleanup(client)
    assert(state.exception.get() eq terminalFailure)
  }

  test("aborted fallback push keeps its state until a published request completes") {
    val client = new TransportRecordingCelebornPushClient
    client.openUninstrumentableConnectionBeforePush = true
    val first =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 604, 2416)
    val original = client.getPushState(s"19-3-$encodedAttemptId")
    val published = new AtomicReference[RecordingCelebornPushState]()
    val expected = new IOException("raw fallback push failed after publishing the request")
    client.beforePushBegins = () => first.abort()
    client.beforePushReturns = state => {
      published.set(state)
      throw expected
    }
    val bytes = frame(596)

    val failure = intercept[IOException] {
      first.pushPartitionData(0, bytes, bytes.length)
    }
    assert(failure eq expected)
    client.beforePushBegins = () => ()
    client.beforePushReturns = _ => ()
    assert(published.get() eq original)
    assert(client.cleanupCalls.get() == 0)
    assert(original.exception.get() == null)

    assertNextMapWaitsForCompletion(client) {
      client.complete(published.get())
    }
    awaitCleanup(client)
  }

  test("fallback submissions wait for prior transport completion") {
    val client = new TransportRecordingCelebornPushClient
    val bytes = frame(596)
    val admissionBytes = 5484

    // Disable precise ownership once, then verify later fallback submissions on the same pusher.
    client.openUninstrumentableConnectionBeforePush = true
    val initializer =
      new CelebornShufflePartitionPusher(
        client,
        19,
        3,
        encodedAttemptId,
        12,
        9,
        604,
        admissionBytes)
    initializer.pushPartitionData(0, bytes, bytes.length)
    client.complete(client.currentState(19, 3, encodedAttemptId))
    assert(initializer.finish()(0) == bytes.length)

    val attemptId = encodedAttemptId + 1
    val pusher =
      new CelebornShufflePartitionPusher(client, 19, 3, attemptId, 12, 9, 604, admissionBytes)
    val rawEntries = new AtomicInteger()
    val firstPublished = new CountDownLatch(1)
    val resumeFirst = new CountDownLatch(1)
    val firstFailure = new AtomicReference[Throwable]()
    val secondFailure = new AtomicReference[Throwable]()
    client.beforePushBegins = () => rawEntries.incrementAndGet()
    client.beforePushReturns = _ => {
      if (firstPublished.getCount > 0) {
        firstPublished.countDown()
        if (!resumeFirst.await(5, TimeUnit.SECONDS)) {
          throw new IOException("timed out waiting to resume the first fallback submission")
        }
      }
    }
    val first = new Thread(() => {
      try pusher.pushPartitionData(0, bytes, bytes.length)
      catch { case failure: Throwable => firstFailure.set(failure) }
    })
    val second = new Thread(() => {
      try pusher.pushPartitionData(1, bytes, bytes.length)
      catch { case failure: Throwable => secondFailure.set(failure) }
    })
    try {
      first.start()
      assert(firstPublished.await(5, TimeUnit.SECONDS))
      second.start()
      second.join(100)
      assert(second.isAlive)
      assert(
        rawEntries.get() == 1,
        "a second raw submission must not race the first submission's PushState resolution")

      resumeFirst.countDown()
      first.join(5000)
      assert(!first.isAlive)
      assert(firstFailure.get() == null)
      second.join(100)
      assert(second.isAlive, "a second fallback push must wait for the first callback")
      assert(rawEntries.get() == 1)

      val state = client.currentState(19, 3, attemptId)
      client.complete(state)
      second.join(5000)
      assert(!second.isAlive)
      assert(secondFailure.get() == null)
      assert(rawEntries.get() == 2)

      client.complete(state)
      assert(pusher.finish().take(2).sum == 2L * bytes.length)
    } finally {
      resumeFirst.countDown()
      first.interrupt()
      second.interrupt()
      first.join(5000)
      second.join(5000)
      client.beforePushBegins = () => ()
      client.beforePushReturns = _ => ()
    }
  }

  private def assertNextMapWaitsForCompletion(client: TransportRecordingCelebornPushClient)(
      completePending: => Unit): Unit = {
    val bytes = frame(596)
    val next =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId + 1, 12, 9, 604, 2416)
    val started = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(() => {
      try {
        started.countDown()
        next.reservePartitionData(1812)
        try next.pushPartitionData(1, bytes, bytes.length)
        finally next.releasePartitionDataReservation()
      } catch {
        case error: Throwable => failure.set(error)
      }
    })
    try {
      worker.start()
      assert(started.await(5, TimeUnit.SECONDS))
      worker.join(100)
      assert(worker.isAlive, "unfinished callbacks or retries must retain their admission")
      completePending
      worker.join(5000)
      assert(!worker.isAlive, "completed callbacks and retries must release shared admission")
      assert(failure.get() == null)
      client.complete(client.currentState(19, 3, encodedAttemptId + 1))
      assert(next.finish()(1) == bytes.length)
    } finally {
      worker.interrupt()
      worker.join(5000)
    }
  }

  private def awaitCleanup(client: RecordingCelebornPushClient): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    while (client.cleanupCalls.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(10)
    }
    assert(client.cleanupCalls.get() == 1)
  }

  test("cleanup during raw submission waits until callback ownership is registered") {
    val client = new TransportRecordingCelebornPushClient
    val first =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 604, 2416)
    val original = client.getPushState(s"19-3-$encodedAttemptId")
    val recreated = new AtomicReference[RecordingCelebornPushState]()
    client.beforePushBegins = () => first.abort()
    client.beforePushReturns = state => recreated.set(state)
    val bytes = frame(596)
    intercept[IOException] {
      first.pushPartitionData(0, bytes, bytes.length)
    }
    client.beforePushBegins = () => ()
    client.beforePushReturns = _ => ()
    val state = recreated.get()
    assert(state eq original)
    assert(client.cleanupCalls.get() == 1)
    assert(state.exception.get().getMessage == "Cleaned Up")

    assertNextMapWaitsForCompletion(client) {
      client.failTransport(state, new IOException("connection failed after final cleanup"))
      assert(state.inFlightRequestTracker.totalInflightReqs.sum() == 1)
    }
  }

  test("abort interrupts a raw invocation blocked before publication") {
    val client = new TransportRecordingCelebornPushClient
    val first =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 604, 2416)
    val enteredRawPush = new CountDownLatch(1)
    val blockRawPush = new CountDownLatch(1)
    val pushFailure = new AtomicReference[Throwable]()
    client.beforePushBegins = () => {
      enteredRawPush.countDown()
      try blockRawPush.await()
      catch {
        case interrupted: InterruptedException =>
          Thread.currentThread().interrupt()
          val failure =
            new IOException("Interrupted while waiting for Celeborn admission", interrupted)
          client.currentState(19, 3, encodedAttemptId).exception.compareAndSet(null, failure)
          throw failure
      }
    }
    val bytes = frame(596)
    val worker = new Thread(() => {
      try first.pushPartitionData(0, bytes, bytes.length)
      catch { case error: Throwable => pushFailure.set(error) }
    })

    try {
      worker.start()
      assert(enteredRawPush.await(5, TimeUnit.SECONDS))
      first.abort()
      worker.join(5000)
      assert(!worker.isAlive, "abort must interrupt a raw push waiting on Celeborn admission")
      assert(pushFailure.get().isInstanceOf[IOException])
      assert(client.dataClientFactory.handler.outstandingPushes.isEmpty)
      awaitCleanup(client)
    } finally {
      blockRawPush.countDown()
      worker.interrupt()
      worker.join(5000)
      client.beforePushBegins = () => ()
    }
  }

  test("a raw invocation that throws after publication retains its outstanding callback") {
    val client = new TransportRecordingCelebornPushClient
    val first =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 604, 2416)
    val published = new AtomicReference[RecordingCelebornPushState]()
    val expected = new IOException("raw push failed after publishing the request")
    client.beforePushReturns = state => {
      published.set(state)
      throw expected
    }
    val bytes = frame(596)
    val failure = intercept[IOException] {
      first.pushPartitionData(0, bytes, bytes.length)
    }
    assert(failure eq expected)
    client.beforePushReturns = _ => ()

    assertNextMapWaitsForCompletion(client) {
      client.failTransport(published.get(), new IOException("cancelled transport failed"))
    }
  }

  test("cleanup discards a queued retry without waiting for its executor") {
    val client = new TransportRecordingCelebornPushClient
    client.retriesBeforeFailure = 1
    val first =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 604, 2416)
    val bytes = frame(596)
    first.pushPartitionData(0, bytes, bytes.length)
    val state = client.currentState(19, 3, encodedAttemptId)
    client.failTransport(state, new IOException("retry the failed transport"))
    assert(client.retryExecutor.pendingCount == 1)
    assert(client.dataClientFactory.handler.outstandingPushes.isEmpty)
    first.abort()

    val next =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId + 1, 12, 9, 604, 2416)
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(() => {
      try next.pushPartitionData(1, bytes, bytes.length)
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    worker.join(5000)
    assert(!worker.isAlive, "queued retries must not retain admission after cancellation")
    assert(failure.get() == null)

    // The delegate executor may retain the cancelled wrapper, but it no longer retains or runs
    // the stock retry and its payload.
    client.retryExecutor.runNext()
    assert(client.retryExecutor.pendingCount == 0)
    assert(state.inFlightRequestTracker.totalInflightReqs.sum() == 1)
    val nextState = client.currentState(19, 3, encodedAttemptId + 1)
    client.complete(nextState)
    assert(next.finish()(1) == bytes.length)
  }

  test("retry handoff to another transport keeps admission until its callback returns") {
    val client = new TransportRecordingCelebornPushClient
    client.retriesBeforeFailure = 1
    val first =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 604, 2416)
    val bytes = frame(596)
    first.pushPartitionData(0, bytes, bytes.length)
    val state = client.currentState(19, 3, encodedAttemptId)
    client.retryCallback = callback => client.dataClientFactory.handler.add(state, callback)
    client.failTransport(state, new IOException("retry at another worker"))
    assert(client.retryExecutor.pendingCount == 1)
    client.retryExecutor.runNext()
    assert(client.retryExecutor.pendingCount == 0)
    assert(client.dataClientFactory.handler.outstandingPushes.size() == 1)
    first.abort()

    assertNextMapWaitsForCompletion(client) {
      client.failTransport(state, new IOException("retried transport failed after cancellation"))
      assert(state.inFlightRequestTracker.totalInflightReqs.sum() == 1)
    }
  }

  test("cancelling a queued retry releases ownership without running its body") {
    val client = new TransportRecordingCelebornPushClient
    val tracker = CelebornTransportCallbackTracker.tryCreate(client)
    val push = tracker.beginPush()
    val calls = new AtomicInteger()
    val retry =
      try {
        client.pushDataRetryPool.submit(new Runnable {
          override def run(): Unit = { calls.incrementAndGet(); () }
        })
      } finally {
        push.close()
      }
    assert(!push.isComplete)
    assert(retry.cancel(true))
    assert(push.isComplete)
    client.retryExecutor.runNext()
    assert(calls.get() == 0)
  }

  test("cancelling a running retry retains ownership until its body actually returns") {
    Seq(false, true).foreach { interrupt =>
      val client = new TransportRecordingCelebornPushClient
      val tracker = CelebornTransportCallbackTracker.tryCreate(client)
      val started = new CountDownLatch(1)
      val resume = new CountDownLatch(1)
      val push = tracker.beginPush()
      val retry =
        try {
          client.pushDataRetryPool.submit(new Runnable {
            override def run(): Unit = {
              started.countDown()
              try {
                assert(resume.await(5, TimeUnit.SECONDS))
              } catch {
                // A cancelled Future is done even when the retry ignores interruption.
                case _: InterruptedException => assert(resume.await(5, TimeUnit.SECONDS))
              }
            }
          })
        } finally {
          push.close()
        }
      val worker = new Thread(() => client.retryExecutor.runNext())
      try {
        worker.start()
        assert(started.await(5, TimeUnit.SECONDS))
        assert(retry.cancel(interrupt))
        assert(retry.isDone)
        assert(!push.isComplete, "Future cancellation is not completion of a running retry")
        resume.countDown()
        worker.join(5000)
        assert(!worker.isAlive)
        assert(push.isComplete)
      } finally {
        resume.countDown()
        worker.join(5000)
      }
    }
  }

  test("rejected retry submissions do not leak ownership") {
    val client = new TransportRecordingCelebornPushClient
    val tracker = CelebornTransportCallbackTracker.tryCreate(client)
    val push = tracker.beginPush()
    client.retryExecutor.shutdown()
    try {
      intercept[RejectedExecutionException] {
        client.pushDataRetryPool.submit(new Runnable {
          override def run(): Unit = fail("A rejected retry must not run")
        })
      }
    } finally {
      push.close()
    }
    assert(push.isComplete)
  }

  test("cancelled successful transport does not release another live transport twice") {
    val client = new TransportRecordingCelebornPushClient
    // The complete reservations are 112 + 64 bytes. Neither individual completion can admit
    // the next map's 160-byte reservation, regardless of transport callback ordering.
    val first = new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 176)
    val large = frame(24)
    val small = frame()
    val retryBytes = frame(40)
    first.pushPartitionData(0, large, large.length)
    first.pushPartitionData(1, small, small.length)
    val cancelledState = client.currentState(19, 3, encodedAttemptId)
    first.abort()
    assert(cancelledState.inFlightRequestTracker.totalInflightReqs.sum() == 2)

    val retry =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId + 1, 12, 9, 176)
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(() => {
      try retry.pushPartitionData(2, retryBytes, retryBytes.length)
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    worker.join(100)
    assert(worker.isAlive)

    client.complete(cancelledState)
    worker.join(100)
    assert(worker.isAlive, "one success must not also release the other live request")
    assert(cancelledState.inFlightRequestTracker.totalInflightReqs.sum() == 2)

    client.failTransport(cancelledState, new IOException("cancelled connection closed"))
    worker.join(5000)
    assert(!worker.isAlive)
    assert(failure.get() == null)
    assert(cancelledState.inFlightRequestTracker.totalInflightReqs.sum() == 2)

    val retryState = client.currentState(19, 3, encodedAttemptId + 1)
    client.complete(retryState)
    assert(retry.finish()(2) == retryBytes.length)
  }

  test(
    "terminal async failure without removeBatch preserves its exception and releases one request") {
    val client = new AsyncRecordingCelebornPushClient
    val adapter = new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 64)
    val bytes = frame()
    adapter.pushPartitionData(0, bytes, bytes.length)
    val failedState = client.currentState(19, 3, encodedAttemptId)
    val expected = new IOException(
      "Push data to worker-1 failed for shuffle 19 map 3 attempt 7 partition 0 batch 1.")
    client.failWithoutRemovingRequest(failedState, expected)

    // mapperEnd observes the exception immediately, before the polling reconciler has sampled it.
    val actual = intercept[IOException] {
      adapter.finish()
    }
    assert(actual eq expected)
    assert(failedState.inFlightRequestTracker.totalInflightReqs.sum() == 1)

    val retry =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId + 1, 12, 9, 64)
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(() => {
      try retry.pushPartitionData(4, bytes, bytes.length)
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    worker.join(5000)
    assert(!worker.isAlive, "the one proven terminal failure must release its shared admission")
    assert(failure.get() == null)
    val retryState = client.currentState(19, 3, encodedAttemptId + 1)
    client.complete(retryState)
    assert(retry.finish()(4) == bytes.length)
  }

  test("encoding reservations enforce one thread-owned claim and release unused capacity") {
    val client = new RecordingCelebornPushClient
    val adapter = new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 64)
    val bytes = frame()
    adapter.reservePartitionData(48)
    intercept[IOException] {
      adapter.reservePartitionData(48)
    }
    adapter.releasePartitionDataReservation()
    adapter.reservePartitionData(48)
    adapter.pushPartitionData(2, bytes, bytes.length)
    adapter.releasePartitionDataReservation()
    assert(adapter.finish()(2) == bytes.length)
  }

  test("native and JNI frame admission waits for both retirement and transport completion") {
    Seq(false, true).foreach { nativeRetiresFirst =>
      val client = new TransportRecordingCelebornPushClient
      val bytes = frame(8852)
      val budget = bytes.length * 3 + 16
      val first =
        new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 8860, budget)
      val next =
        new CelebornShufflePartitionPusher(client, 19, 4, encodedAttemptId, 12, 9, 8860, budget)
      assert(first.maxReservationBytes() == budget - 16)
      first.reservePartitionData(budget - 16)
      first.pushPartitionData(0, bytes, bytes.length)
      val state = client.currentState(19, 3, encodedAttemptId)
      val waiting = new CountDownLatch(1)
      val acquired = new CountDownLatch(1)
      val failure = new AtomicReference[Throwable]()
      val worker = new Thread(() => {
        try {
          waiting.countDown()
          next.reservePartitionData(budget - 16)
          try acquired.countDown()
          finally next.releasePartitionDataReservation()
        } catch {
          case error: Throwable => failure.set(error)
        }
      })
      try {
        worker.start()
        assert(waiting.await(5, TimeUnit.SECONDS))
        if (nativeRetiresFirst) first.releasePartitionDataReservation()
        else client.complete(state)
        assert(!acquired.await(100, TimeUnit.MILLISECONDS))
        if (nativeRetiresFirst) client.complete(state)
        else first.releasePartitionDataReservation()
        assert(acquired.await(5, TimeUnit.SECONDS))
        worker.join(5000)
        assert(failure.get() == null)
        assert(first.finish()(0) == bytes.length)
      } finally {
        first.releasePartitionDataReservation()
        worker.interrupt()
        worker.join(5000)
      }
    }
  }

  test("timed-out writes retain ownership until Netty releases their outbound buffer") {
    CelebornTransportOwnershipTestHelper.assertTimedOutWriteRetainsOwnership(true)
    CelebornTransportOwnershipTestHelper.assertTimedOutWriteRetainsOwnership(false)
  }

  test("native queued writes cannot be cancelled before their buffers are released") {
    CelebornTransportOwnershipTestHelper.assertUnflushedWriteCannotBeCancelledEarly()
  }

  test("ordinary Spark transport writes preserve their cancellation behavior") {
    CelebornTransportOwnershipTestHelper.assertUnownedWritesPreserveCancellation()
  }

  test("completed transport callbacks no longer retain captured payloads") {
    CelebornTransportOwnershipTestHelper.assertCompletedCallbacksForgetPayloads()
  }

  test("completed and discarded retry wrappers no longer retain captured payloads") {
    CelebornTransportOwnershipTestHelper.assertCompletedRetriesForgetPayloads()
  }

  test("Celeborn bootstrap failures do not escape shared client creation") {
    CelebornTransportOwnershipTestHelper.assertBootstrapFailureDoesNotEscapeClientCreation()
  }

  test("Celeborn bootstrap hooks remain only while their shared clients are owned") {
    CelebornTransportOwnershipTestHelper.assertBootstrapHookFollowsClientLifetime()
  }

  test("fatal Celeborn bootstrap errors do not leak shared hook registrations") {
    CelebornTransportOwnershipTestHelper.assertFatalBootstrapErrorsDoNotLeakRegistrations()
  }

  test("Celeborn hook release waits for an active bootstrap invocation") {
    CelebornTransportOwnershipTestHelper.assertBootstrapReleaseWaitsForActiveInvocation()
  }

  test("failed push keeps native frame admission until the JNI caller retires its buffers") {
    Seq(false, true).foreach { publishedTransport =>
      val client = new TransportRecordingCelebornPushClient
      val bytes = frame()
      val expected = new IOException("submission failed while native and JNI buffers are live")
      val publishedState = new AtomicReference[RecordingCelebornPushState]()
      if (publishedTransport) {
        client.beforePushReturns = state => {
          publishedState.set(state)
          throw expected
        }
      } else client.failure = expected
      val first =
        new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 64)
      val next =
        new CelebornShufflePartitionPusher(client, 19, 4, encodedAttemptId, 12, 9, 64)
      first.reservePartitionData(48)
      assert(intercept[IOException] {
        first.pushPartitionData(0, bytes, bytes.length)
      } eq expected)
      if (publishedTransport) {
        client.failTransport(publishedState.get(), new IOException("cancelled request failed"))
      }
      val waiting = new CountDownLatch(1)
      val acquired = new CountDownLatch(1)
      val failure = new AtomicReference[Throwable]()
      val worker = new Thread(() => {
        try {
          waiting.countDown()
          next.reservePartitionData(48)
          try acquired.countDown()
          finally next.releasePartitionDataReservation()
        } catch {
          case error: Throwable => failure.set(error)
        }
      })
      try {
        worker.start()
        assert(waiting.await(5, TimeUnit.SECONDS))
        assert(!acquired.await(100, TimeUnit.MILLISECONDS))
        first.releasePartitionDataReservation()
        assert(acquired.await(5, TimeUnit.SECONDS))
        worker.join(5000)
        assert(failure.get() == null)
      } finally {
        first.releasePartitionDataReservation()
        worker.interrupt()
        worker.join(5000)
      }
    }
  }

  test("interruption failures never release another still-live async transport request") {
    val client = new AsyncRecordingCelebornPushClient
    val first = new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9, 64)
    val bytes = frame()
    first.pushPartitionData(0, bytes, bytes.length)
    val heldState = client.currentState(19, 3, encodedAttemptId)
    val interruption = new IOException(
      "Interrupted while limiting Celeborn in-flight requests",
      new InterruptedException("task was interrupted"))
    client.failWithoutRemovingRequest(heldState, interruption)

    val observed = intercept[IOException] {
      first.finish()
    }
    assert(observed eq interruption)

    val retry =
      new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId + 1, 12, 9, 64)
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(() => {
      try retry.pushPartitionData(2, bytes, bytes.length)
      catch { case error: Throwable => failure.set(error) }
    })
    worker.start()
    worker.join(100)
    assert(worker.isAlive, "an interrupted limiter does not prove its older request completed")
    assert(heldState.inFlightRequestTracker.totalInflightReqs.sum() == 1)

    client.complete(heldState)
    worker.join(5000)
    assert(!worker.isAlive)
    assert(failure.get() == null)
    val retryState = client.currentState(19, 3, encodedAttemptId + 1)
    client.complete(retryState)
    assert(retry.finish()(2) == bytes.length)
  }
}

/** Stand-ins for optional Celeborn types used by reflective client-acquisition tests. */
final class RecordingCelebornClientConf
final class RecordingCelebornUserIdentifier

/** Mirrors Celeborn 0.7's modern and legacy static client-acquisition overloads. */
final class RecordingCryptoAwareCelebornClientFactory

object RecordingCryptoAwareCelebornClientFactory {
  val cryptoAwareCalls: AtomicInteger = new AtomicInteger()
  val legacyCalls: AtomicInteger = new AtomicInteger()

  def reset(): Unit = {
    cryptoAwareCalls.set(0)
    legacyCalls.set(0)
  }

  def get(
      appUniqueId: String,
      lifecycleManagerHost: String,
      lifecycleManagerPort: Int,
      conf: RecordingCelebornClientConf,
      userIdentifier: RecordingCelebornUserIdentifier,
      extension: Array[Byte],
      cryptoHandler: Optional[_]): RecordingCelebornPushClient = {
    cryptoAwareCalls.incrementAndGet()
    val client = new RecordingCelebornPushClient
    if (cryptoHandler.isPresent) {
      client.cryptoHandler =
        Optional.of(cryptoHandler.get().asInstanceOf[RecordingCelebornCryptoHandler])
    }
    client
  }

  def get(
      appUniqueId: String,
      lifecycleManagerHost: String,
      lifecycleManagerPort: Int,
      conf: RecordingCelebornClientConf,
      userIdentifier: RecordingCelebornUserIdentifier,
      extension: Array[Byte]): RecordingCelebornPushClient = {
    legacyCalls.incrementAndGet()
    new RecordingCelebornPushClient
  }
}

/** Mirrors Celeborn 0.6's original static client-acquisition API. */
final class RecordingLegacyCelebornClientFactory

object RecordingLegacyCelebornClientFactory {
  val calls: AtomicInteger = new AtomicInteger()

  def get(
      appUniqueId: String,
      lifecycleManagerHost: String,
      lifecycleManagerPort: Int,
      conf: RecordingCelebornClientConf,
      userIdentifier: RecordingCelebornUserIdentifier,
      extension: Array[Byte]): RecordingCelebornPushClient = {
    calls.incrementAndGet()
    new RecordingCelebornPushClient
  }
}

/** Public so the adapter can resolve and invoke the optional client's API using reflection. */
class RecordingCelebornPushClient {

  val mapperEndCalls: AtomicInteger = new AtomicInteger()
  val cleanupCalls: AtomicInteger = new AtomicInteger()
  @volatile var acceptedBytes: Option[Int] = None
  @volatile var cryptoHandler: Optional[RecordingCelebornCryptoHandler] = Optional.empty()
  @volatile var failure: Throwable = _
  @volatile var mapperEndFailure: Throwable = _
  @volatile var cleanupFailure: Throwable = _
  @volatile var lastMapperEnd: (Int, Int, Int, Int, Int) = _
  @volatile var lastCleanup: (Int, Int, Int) = _
  @volatile var lastPush: RecordedCelebornPush = _
  @volatile var pushCount: Int = 0

  @throws[IOException]
  def pushOrMergeData(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int,
      doPush: Boolean,
      skipCompress: Boolean): Int = {
    pushCount += 1
    lastPush = RecordedCelebornPush(
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      bytes,
      offset,
      length,
      numMappers,
      numPartitions,
      doPush,
      skipCompress)

    if (failure != null) {
      throw failure
    }
    val transportPayloadLength =
      if (cryptoHandler.isPresent) cryptoHandler.get().encrypt(bytes, offset, length).length
      else length
    acceptedBytes.getOrElse(transportPayloadLength + 16)
  }

  @throws[IOException]
  def mapperEnd(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      numPartitions: Int): Unit = {
    mapperEndCalls.incrementAndGet()
    lastMapperEnd = (shuffleId, mapId, attemptId, numMappers, numPartitions)
    if (mapperEndFailure != null) {
      throw mapperEndFailure
    }
  }

  @throws[IOException]
  def cleanup(shuffleId: Int, mapId: Int, attemptId: Int): Unit = {
    cleanupCalls.incrementAndGet()
    lastCleanup = (shuffleId, mapId, attemptId)
    if (cleanupFailure != null) {
      throw cleanupFailure
    }
  }
}

/** Mirrors stock Celeborn's private request tracker without requiring its optional dependency. */
final class RecordingCelebornInFlightTracker {
  val totalInflightReqs: LongAdder = new LongAdder()
}

/** Mirrors the public PushState failure slot and its stock private tracker member. */
final class RecordingCelebornPushState {
  val inFlightRequestTracker: RecordingCelebornInFlightTracker =
    new RecordingCelebornInFlightTracker()
  val exception: AtomicReference[IOException] = new AtomicReference[IOException]()
}

/** Exposes the same lifecycle and completion state as the public Apache Celeborn client. */
class AsyncRecordingCelebornPushClient extends RecordingCelebornPushClient {
  val pushStates: ConcurrentHashMap[String, RecordingCelebornPushState] =
    new ConcurrentHashMap[String, RecordingCelebornPushState]()

  def getPushState(mapKey: String): RecordingCelebornPushState =
    pushStates.computeIfAbsent(mapKey, _ => new RecordingCelebornPushState())

  def currentState(shuffleId: Int, mapId: Int, attemptId: Int): RecordingCelebornPushState =
    pushStates.get(s"$shuffleId-$mapId-$attemptId")

  def complete(state: RecordingCelebornPushState): Unit = state.synchronized {
    state.inFlightRequestTracker.totalInflightReqs.decrement()
    state.notifyAll()
  }

  def failWithoutRemovingRequest(state: RecordingCelebornPushState, failure: IOException): Unit =
    state.synchronized {
      state.exception.compareAndSet(null, failure)
      state.notifyAll()
    }

  @throws[IOException]
  override def pushOrMergeData(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int,
      doPush: Boolean,
      skipCompress: Boolean): Int = {
    val state = getPushState(s"$shuffleId-$mapId-$attemptId")
    state.inFlightRequestTracker.totalInflightReqs.increment()
    super.pushOrMergeData(
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      bytes,
      offset,
      length,
      numMappers,
      numPartitions,
      doPush,
      skipCompress)
  }

  @throws[IOException]
  override def mapperEnd(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      numPartitions: Int): Unit = {
    super.mapperEnd(shuffleId, mapId, attemptId, numMappers, numPartitions)
    val key = s"$shuffleId-$mapId-$attemptId"
    val state = pushStates.get(key)
    if (state != null) {
      state.synchronized {
        while (state.exception.get() == null &&
          state.inFlightRequestTracker.totalInflightReqs.sum() > 0) {
          state.wait(25)
        }
        val failure = state.exception.get()
        if (failure != null) {
          throw failure
        }
      }
      pushStates.remove(key, state)
    }
  }

  @throws[IOException]
  override def cleanup(shuffleId: Int, mapId: Int, attemptId: Int): Unit = {
    super.cleanup(shuffleId, mapId, attemptId)
    val removed = pushStates.remove(s"$shuffleId-$mapId-$attemptId")
    if (removed != null) {
      removed.synchronized {
        removed.exception.compareAndSet(null, new IOException("Cleaned Up"))
        removed.notifyAll()
      }
    }
  }
}

/** Reproduces stock Celeborn's client-factory, handler, and callback completion boundaries. */
final class TransportRecordingCelebornPushClient extends AsyncRecordingCelebornPushClient {
  val dataClientFactory: RecordingCelebornTransportClientFactory =
    new RecordingCelebornTransportClientFactory
  val retryExecutor = new RecordingCelebornRetryExecutor
  val pushDataRetryPool: ExecutorService = retryExecutor

  def getDataClientFactory: RecordingCelebornTransportClientFactory = dataClientFactory

  var openConnectionBeforePush: Boolean = false
  var openUninstrumentableConnectionBeforePush: Boolean = false
  var beforePushBegins: () => Unit = () => ()
  var beforePushReturns: RecordingCelebornPushState => Unit = (_: RecordingCelebornPushState) =>
    ()
  var retriesBeforeFailure: Int = 0
  var retryCallback: RecordingCelebornTransportCallbackApi => Unit =
    callback => callback.onFailure(new IOException("revive failed"))

  @throws[IOException]
  override def pushOrMergeData(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int,
      doPush: Boolean,
      skipCompress: Boolean): Int = {
    beforePushBegins()
    val accepted = super.pushOrMergeData(
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      bytes,
      offset,
      length,
      numMappers,
      numPartitions,
      doPush,
      skipCompress)
    if (openConnectionBeforePush) {
      openConnectionBeforePush = false
      dataClientFactory.openConnection()
    }
    if (openUninstrumentableConnectionBeforePush) {
      openUninstrumentableConnectionBeforePush = false
      dataClientFactory.openUninstrumentableConnection()
    }
    val state = currentState(shuffleId, mapId, attemptId)
    dataClientFactory.handler.add(
      state,
      new RecordingCelebornTransportCallback(
        state,
        retriesBeforeFailure,
        callback => {
          pushDataRetryPool.submit(new Runnable {
            override def run(): Unit = retryCallback(callback)
          })
          ()
        }))
    beforePushReturns(state)
    accepted
  }

  override def complete(state: RecordingCelebornPushState): Unit = {
    dataClientFactory.handler.remove(state).callback.onSuccess(ByteBuffer.allocate(0))
  }

  def failTransport(state: RecordingCelebornPushState, failure: IOException): Unit = {
    val request = dataClientFactory.handler.remove(state)
    request.callback.onFailure(failure)
  }
}

final class RecordingCelebornRetryExecutor extends AbstractExecutorService {
  private val pending = new LinkedBlockingQueue[Runnable]()
  @volatile private var stopped = false

  def pendingCount: Int = pending.size()

  def runNext(): Unit = {
    val task = pending.poll(5, TimeUnit.SECONDS)
    require(task != null, "Expected a queued Celeborn retry")
    task.run()
  }

  override def execute(command: Runnable): Unit = {
    if (stopped) {
      throw new RejectedExecutionException("Celeborn retry executor is stopped")
    }
    pending.add(command)
  }

  override def shutdown(): Unit = stopped = true

  override def shutdownNow(): JList[Runnable] = {
    stopped = true
    val tasks = new JArrayList[Runnable]()
    pending.drainTo(tasks)
    tasks
  }

  override def isShutdown: Boolean = stopped

  override def isTerminated: Boolean = stopped && pending.isEmpty

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = isTerminated
}

final class RecordingCelebornTransportClientFactory {
  var handler: RecordingCelebornTransportResponseHandler =
    new RecordingCelebornTransportResponseHandler
  val clientBootstraps: JList[RecordingCelebornTransportClientBootstrap] =
    new JArrayList[RecordingCelebornTransportClientBootstrap]()
  val connectionPool: ConcurrentHashMap[String, RecordingCelebornTransportClientPool] =
    new ConcurrentHashMap[String, RecordingCelebornTransportClientPool]()
  connectionPool.put("worker", new RecordingCelebornTransportClientPool(handler))

  def openConnection(): Unit = {
    openConnection(new io.netty.channel.embedded.EmbeddedChannel())
  }

  def openUninstrumentableConnection(): Unit = {
    openConnection(null)
  }

  private def openConnection(channel: io.netty.channel.Channel): Unit = {
    handler = new RecordingCelebornTransportResponseHandler
    val pool = new RecordingCelebornTransportClientPool(handler, channel)
    val bootstraps = clientBootstraps.iterator()
    while (bootstraps.hasNext) {
      bootstraps.next().doBootstrap(pool.clients(0))
    }
    connectionPool.put("worker", pool)
  }
}

trait RecordingCelebornTransportClientBootstrap {
  def doBootstrap(client: RecordingCelebornTransportClient): Unit
}

final class RecordingCelebornTransportClientPool(
    handler: RecordingCelebornTransportResponseHandler,
    channel: io.netty.channel.Channel = new io.netty.channel.embedded.EmbeddedChannel()) {
  val clients: Array[RecordingCelebornTransportClient] =
    Array(new RecordingCelebornTransportClient(handler, channel))
  val locks: Array[Object] = Array(new Object)
}

final class RecordingCelebornTransportClient(
    handler: RecordingCelebornTransportResponseHandler,
    val channel: io.netty.channel.Channel) {
  def getChannel: io.netty.channel.Channel = channel
  def getHandler: RecordingCelebornTransportResponseHandler = handler
}

final class RecordingCelebornTransportResponseHandler {
  private val nextRequestId = new AtomicLong()
  val outstandingPushes: ConcurrentHashMap[java.lang.Long, RecordingCelebornTransportRequest] =
    new ConcurrentHashMap[java.lang.Long, RecordingCelebornTransportRequest]()

  def add(pushState: RecordingCelebornPushState): Unit =
    add(pushState, new RecordingCelebornTransportCallback(pushState))

  def add(
      pushState: RecordingCelebornPushState,
      callback: RecordingCelebornTransportCallbackApi): Unit = {
    outstandingPushes.put(
      Long.box(nextRequestId.incrementAndGet()),
      new RecordingCelebornTransportRequest(pushState, callback))
  }

  def remove(pushState: RecordingCelebornPushState): RecordingCelebornTransportRequest = {
    val entries = outstandingPushes.entrySet().iterator()
    while (entries.hasNext) {
      val entry = entries.next()
      if (entry.getValue.pushState eq pushState) {
        val removed = outstandingPushes.remove(entry.getKey)
        if (removed != null) {
          return removed
        }
      }
    }
    throw new IllegalStateException("Celeborn transport request is no longer outstanding")
  }
}

final class RecordingCelebornTransportRequest(
    val pushState: RecordingCelebornPushState,
    var callback: RecordingCelebornTransportCallbackApi)

trait RecordingCelebornTransportCallbackApi {
  def onSuccess(response: ByteBuffer): Unit
  def onFailure(failure: Throwable): Unit
}

final class RecordingCelebornTransportCallback(
    val pushState: RecordingCelebornPushState,
    private var retriesRemaining: Int = 0,
    submitRetry: RecordingCelebornTransportCallbackApi => Unit = _ => ())
    extends RecordingCelebornTransportCallbackApi {
  override def onSuccess(response: ByteBuffer): Unit = pushState.synchronized {
    pushState.inFlightRequestTracker.totalInflightReqs.decrement()
    pushState.notifyAll()
  }

  override def onFailure(failure: Throwable): Unit = {
    if (pushState.exception.get() == null) {
      if (retriesRemaining > 0) {
        retriesRemaining -= 1
        submitRetry(this)
      } else {
        val reportedFailure = failure match {
          case io: IOException => io
          case _ => new IOException(failure)
        }
        pushState.exception.compareAndSet(null, reportedFailure)
      }
    }
  }
}

/** Implements the older public Celeborn 0.6 four-argument mapper-completion API. */
final class LegacyMapperEndCelebornPushClient {
  private val delegate = new RecordingCelebornPushClient
  val mapperEndCalls: AtomicInteger = new AtomicInteger()
  @volatile var lastMapperEnd: (Int, Int, Int, Int) = _

  @throws[IOException]
  def pushOrMergeData(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int,
      doPush: Boolean,
      skipCompress: Boolean): Int =
    delegate.pushOrMergeData(
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      bytes,
      offset,
      length,
      numMappers,
      numPartitions,
      doPush,
      skipCompress)

  def mapperEnd(shuffleId: Int, mapId: Int, attemptId: Int, numMappers: Int): Unit = {
    mapperEndCalls.incrementAndGet()
    lastMapperEnd = (shuffleId, mapId, attemptId, numMappers)
  }

  def cleanup(shuffleId: Int, mapId: Int, attemptId: Int): Unit = ()
}

/** Models the Spark crypto wire format's minimum 4-byte length plus 16-byte IV overhead. */
final class RecordingCelebornCryptoHandler {

  @volatile var encryptionCount: Int = 0
  @volatile var plaintext: Array[Byte] = _
  @volatile var encryptedLength: Int = 0

  def encrypt(bytes: Array[Byte], offset: Int, length: Int): Array[Byte] = {
    encryptionCount += 1
    plaintext = Arrays.copyOfRange(bytes, offset, offset + length)
    encryptedLength = length + java.lang.Integer.BYTES + 16
    new Array[Byte](encryptedLength)
  }
}

/** Mirrors Celeborn 0.7 integrity accounting without depending on its optional client classes. */
final class IntegrityCheckingCelebornPushClient extends RecordingCelebornPushClient {

  @volatile var integrityFailure: Throwable = _
  val accountedFrames: mutable.ArrayBuffer[RecordedCelebornAccounting] =
    mutable.ArrayBuffer.empty
  val recordedPushes: mutable.ArrayBuffer[RecordedCelebornPush] = mutable.ArrayBuffer.empty
  val invocationOrder: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty
  private val checksums = mutable.HashMap.empty[Int, Long]
  private val byteTotals = mutable.HashMap.empty[Int, Long]

  def partitionCrc(partitionId: Int): Long = checksums(partitionId)

  def partitionBytes(partitionId: Int): Long = byteTotals(partitionId)

  @throws[IOException]
  def computeBatchCRC(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int): Unit = {
    invocationOrder += s"crc:$partitionId"
    if (integrityFailure != null) {
      throw integrityFailure
    }

    accountedFrames +=
      RecordedCelebornAccounting(shuffleId, mapId, attemptId, partitionId, bytes, offset, length)
    val batchChecksum = new CRC32
    batchChecksum.update(bytes, offset, length)
    val previous = checksums.getOrElse(partitionId, 0L)
    val combined = (0 until java.lang.Integer.BYTES).foldLeft(0L) { (result, index) =>
      val shift = index * java.lang.Byte.SIZE
      val next = ((previous >>> shift) & 0xffL) + ((batchChecksum.getValue >>> shift) & 0xffL)
      result | ((next & 0xffL) << shift)
    }
    checksums.update(partitionId, combined)
    byteTotals.update(partitionId, byteTotals.getOrElse(partitionId, 0L) + length)
  }

  @throws[IOException]
  override def pushOrMergeData(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int,
      doPush: Boolean,
      skipCompress: Boolean): Int = {
    invocationOrder += s"push:$partitionId"
    val accepted = super.pushOrMergeData(
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      bytes,
      offset,
      length,
      numMappers,
      numPartitions,
      doPush,
      skipCompress)
    recordedPushes += lastPush
    accepted
  }
}

final case class RecordedCelebornAccounting(
    shuffleId: Int,
    mapId: Int,
    attemptId: Int,
    partitionId: Int,
    bytes: Array[Byte],
    offset: Int,
    length: Int)

final case class RecordedCelebornPush(
    shuffleId: Int,
    mapId: Int,
    attemptId: Int,
    partitionId: Int,
    bytes: Array[Byte],
    offset: Int,
    length: Int,
    numMappers: Int,
    numPartitions: Int,
    doPush: Boolean,
    skipCompress: Boolean)

/** Mimics an incompatible optional client whose raw-push method does not return an int. */
final class WrongReturnTypeCelebornPushClient {

  def pushOrMergeData(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int,
      doPush: Boolean,
      skipCompress: Boolean): Long = length.toLong
}
