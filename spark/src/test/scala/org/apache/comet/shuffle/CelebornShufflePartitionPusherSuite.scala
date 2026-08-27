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

  test("raw push requires exactly the frame bytes plus the Celeborn transport header") {
    val bytes = frame()

    Seq(0, bytes.length, bytes.length + 15, bytes.length + 17, -1).foreach { accepted =>
      val client = new RecordingCelebornPushClient
      client.acceptedBytes = Some(accepted)

      val failure = intercept[IOException] {
        pusher(client).pushPartitionData(0, bytes, bytes.length)
      }

      assert(failure.getMessage.contains(accepted.toString))
      assert(failure.getMessage.contains((bytes.length + 16).toString))
    }
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
final class RecordingCelebornPushClient {

  @volatile var acceptedBytes: Option[Int] = None
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
    acceptedBytes.getOrElse(length + 16)
  }
}

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
