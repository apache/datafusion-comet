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
import java.util.Arrays
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.CRC32

import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite

class CelebornShufflePartitionPusherSuite extends AnyFunSuite {

  private val encodedAttemptId = (4 << 16) | 7

  private def pusher(client: AnyRef): CelebornShufflePartitionPusher =
    new CelebornShufflePartitionPusher(client, 19, 3, encodedAttemptId, 12, 9)

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

  test("raw push accepts frames expanded by Spark IO encryption without pushing twice") {
    val client = new RecordingCelebornPushClient
    val cryptoHandler = new RecordingCelebornCryptoHandler
    val bytes = frame()
    client.cryptoHandler = Some(cryptoHandler)

    pusher(client).pushPartitionData(6, bytes, bytes.length)

    assert(client.pushCount == 1)
    assert(cryptoHandler.encryptionCount == 1)
    assert(cryptoHandler.plaintext.sameElements(bytes))
    assert(cryptoHandler.encryptedLength == bytes.length + 20)
    assert(client.lastPush.bytes eq bytes)
    assert(client.lastPush.skipCompress)
  }

  test("integrity accounting covers plaintext before Spark IO encryption expands the frame") {
    val client = new IntegrityCheckingCelebornPushClient
    val cryptoHandler = new RecordingCelebornCryptoHandler
    val bytes = frame(13)
    client.cryptoHandler = Some(cryptoHandler)

    pusher(client).pushPartitionData(4, bytes, bytes.length)

    assert(client.partitionCrc(4) == combinedCrc(bytes))
    assert(client.partitionBytes(4) == bytes.length)
    assert(client.invocationOrder.toSeq == Seq("crc:4", "push:4"))
    assert(cryptoHandler.plaintext.sameElements(bytes))
    assert(cryptoHandler.encryptedLength == bytes.length + 20)
    assert(client.pushCount == 1)
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
}

/** Public so the adapter can resolve and invoke the optional client's API using reflection. */
class RecordingCelebornPushClient {

  @volatile var acceptedBytes: Option[Int] = None
  @volatile var cryptoHandler: Option[RecordingCelebornCryptoHandler] = None
  @volatile var failure: Throwable = _
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
      cryptoHandler.fold(length)(handler => handler.encrypt(bytes, offset, length).length)
    acceptedBytes.getOrElse(transportPayloadLength + 16)
  }
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
