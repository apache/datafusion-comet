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
import java.util.concurrent.atomic.AtomicReference

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, TaskContext}

/** Public so the production adapter can invoke its method through ordinary Java reflection. */
final class RecordingCelebornShuffleClient {

  @volatile var acceptedBytes: Option[Int] = None
  @volatile var failure: Throwable = _
  @volatile var observedTaskContext: TaskContext = _
  @volatile var lastPush: RecordedCelebornPush = _

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
    observedTaskContext = TaskContext.get()
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

/** The method exists, but its return type is incompatible with Celeborn's raw-push contract. */
final class WrongReturnTypeCelebornShuffleClient {
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

class CelebornShufflePartitionPusherSuite extends AnyFunSuite {

  private val managerKey = "spark.shuffle.manager"
  private val managerClass = "org.apache.spark.shuffle.celeborn.SparkShuffleManager"
  private val compositeManagerClass =
    "org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager"
  private val pluginKey = "spark.shuffle.sort.io.plugin.class"
  private val pluginClass = "org.apache.spark.shuffle.celeborn.CelebornShuffleDataIO"
  private val endpointsKey = "spark.celeborn.master.endpoints"
  private val enabledKey = "spark.comet.celeborn.enabled"

  private def enabledConf: SparkConf =
    new SparkConf(false).set(endpointsKey, "celeborn-master:9097")

  /** Spark scopes empty() to private[spark] in Scala, but its JVM companion method is public. */
  private def emptyTaskContext(): TaskContext =
    TaskContext.getClass.getMethod("empty").invoke(TaskContext).asInstanceOf[TaskContext]

  private def pusher(client: AnyRef): CelebornShufflePartitionPusher =
    new CelebornShufflePartitionPusher(client, 19, 3, (4 << 16) | 7, 12, 9)

  test("raw Celeborn push forwards captured task metadata and preserves Comet frame bytes") {
    val client = new RecordingCelebornShuffleClient
    val bytes = Array[Byte](1, 2, 3, 4)

    assert(pusher(client).pushPartitionData(6, bytes, 3) == 3)

    val push = client.lastPush
    assert(push.shuffleId == 19)
    assert(push.mapId == 3)
    assert(push.attemptId == ((4 << 16) | 7))
    assert(push.partitionId == 6)
    assert(push.bytes eq bytes)
    assert(push.offset == 0)
    assert(push.length == 3)
    assert(push.numMappers == 12)
    assert(push.numPartitions == 9)
    assert(push.doPush)
    assert(push.skipCompress)
  }

  test("raw Celeborn push requires exactly the payload plus its transport header") {
    val bytes = Array[Byte](1, 2, 3)

    Seq(0, bytes.length, bytes.length + 15, bytes.length + 17, -1).foreach { accepted =>
      val client = new RecordingCelebornShuffleClient
      client.acceptedBytes = Some(accepted)

      val error = intercept[IOException] {
        pusher(client).pushPartitionData(0, bytes, bytes.length)
      }

      assert(error.getMessage.contains(accepted.toString))
      assert(error.getMessage.contains((bytes.length + 16).toString))
    }
  }

  test("raw Celeborn push preserves the exact client IOException") {
    val client = new RecordingCelebornShuffleClient
    val expected = new IOException("worker rejected the shuffle frame")
    client.failure = expected

    val actual = intercept[IOException] {
      pusher(client).pushPartitionData(0, Array[Byte](7), 1)
    }

    assert(actual eq expected)
  }

  test("raw Celeborn push preserves unchecked client failures") {
    val client = new RecordingCelebornShuffleClient
    val expected = new IllegalStateException("client was closed")
    client.failure = expected

    val actual = intercept[IllegalStateException] {
      pusher(client).pushPartitionData(0, Array[Byte](7), 1)
    }

    assert(actual eq expected)
  }

  test("adapter rejects a missing client or incompatible Celeborn raw-push API") {
    intercept[IllegalArgumentException] {
      new CelebornShufflePartitionPusher(null, 0, 0, 0, 1, 1)
    }

    val missing = intercept[IllegalArgumentException] {
      new CelebornShufflePartitionPusher(new Object, 0, 0, 0, 1, 1)
    }
    assert(missing.getMessage.contains("raw-push"))

    intercept[IllegalArgumentException] {
      new CelebornShufflePartitionPusher(new WrongReturnTypeCelebornShuffleClient, 0, 0, 0, 1, 1)
    }
  }

  test("adapter rejects invalid shuffle identity, mapper identity, and partition counts") {
    val client = new RecordingCelebornShuffleClient

    Seq(
      (-1, 0, 0, 1, 1),
      (0, -1, 0, 1, 1),
      (0, 0, -1, 1, 1),
      (0, 0, 0, 0, 1),
      (0, 1, 0, 1, 1),
      (0, 0, 0, 1, 0)).foreach { case (shuffleId, mapId, attemptId, numMappers, numPartitions) =>
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

  test("adapter rejects invalid output partitions and incomplete Comet frames") {
    val client = new RecordingCelebornShuffleClient
    val adapter = pusher(client)

    Seq(-1, 9).foreach { partitionId =>
      intercept[IOException] {
        adapter.pushPartitionData(partitionId, Array[Byte](1), 1)
      }
    }

    intercept[IOException] {
      adapter.pushPartitionData(0, null, 1)
    }

    Seq(-1, 0, 2).foreach { length =>
      intercept[IOException] {
        adapter.pushPartitionData(0, Array[Byte](1), length)
      }
    }

    assert(client.lastPush == null)
  }

  test("factory recognizes the existing Celeborn shuffle manager") {
    val conf = new SparkConf(false).set(managerKey, managerClass)

    assert(CelebornShufflePusherFactory.isEnabled(conf))

    conf.set(managerKey, "org.apache.spark.shuffle.sort.SortShuffleManager")
    assert(!CelebornShufflePusherFactory.isEnabled(conf))
  }

  test("factory recognizes the composite Comet and Celeborn shuffle manager") {
    val conf = new SparkConf(false).set(managerKey, compositeManagerClass)

    assert(CelebornShufflePusherFactory.isEnabled(conf))

    conf.set(enabledKey, "false")
    assert(!CelebornShufflePusherFactory.isEnabled(conf))
  }

  test("factory recognizes the existing Celeborn shuffle data IO plugin") {
    val conf = new SparkConf(false).set(pluginKey, pluginClass)

    assert(CelebornShufflePusherFactory.isEnabled(conf))

    conf.set(pluginKey, "org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO")
    assert(!CelebornShufflePusherFactory.isEnabled(conf))
  }

  test("factory recognizes nonblank existing Celeborn master endpoints") {
    assert(CelebornShufflePusherFactory.isEnabled(enabledConf))

    val conf = new SparkConf(false).set(endpointsKey, "   ")
    assert(!CelebornShufflePusherFactory.isEnabled(conf))
  }

  test("application enable flag alone does not invent an unconfigured Celeborn backend") {
    val conf = new SparkConf(false).set(enabledKey, "true")

    assert(!CelebornShufflePusherFactory.isEnabled(conf))
  }

  test("explicit application opt-out overrides every existing Celeborn selection") {
    Seq(
      managerKey -> managerClass,
      managerKey -> compositeManagerClass,
      pluginKey -> pluginClass,
      endpointsKey -> "celeborn-master:9097").foreach { case (key, value) =>
      val conf = new SparkConf(false).set(key, value).set(enabledKey, "false")

      assert(!CelebornShufflePusherFactory.isEnabled(conf))
    }
  }

  test("disabled factory does not inspect a client, task context, or task metadata") {
    val conf = new SparkConf(false)

    assert(
      CelebornShufflePusherFactory
        .create(
          conf,
          null,
          celebornShuffleId = -1,
          numMappers = 0,
          numPartitions = 0,
          taskContext = null)
        .isEmpty)
  }

  test("enabled factory binds an existing client to captured Spark task metadata") {
    val client = new RecordingCelebornShuffleClient
    val taskContext = emptyTaskContext()
    val adapter = CelebornShufflePusherFactory
      .create(
        enabledConf,
        client,
        celebornShuffleId = 27,
        numMappers = 8,
        numPartitions = 4,
        taskContext = taskContext)
      .get

    assert(adapter.pushPartitionData(2, Array[Byte](3, 4), 2) == 2)
    assert(client.lastPush.shuffleId == 27)
    assert(client.lastPush.mapId == taskContext.partitionId())
    assert(client.lastPush.attemptId == 0)
    assert(client.lastPush.numMappers == 8)
    assert(client.lastPush.numPartitions == 4)
  }

  test("factory encodes the stage and task attempt without loading the Celeborn client jar") {
    assert(CelebornShufflePusherFactory.encodeAttemptNumber(4, 7) == ((4 << 16) | 7))
    assert(CelebornShufflePusherFactory.encodeAttemptNumber(0, 0) == 0)
    assert(CelebornShufflePusherFactory.encodeAttemptNumber(32767, 65535) == Int.MaxValue)
  }

  test("factory rejects attempts that cannot be represented as a nonnegative Celeborn ID") {
    Seq((-1, 0), (32768, 0), (0, -1), (0, 65536)).foreach { case (stageAttempt, taskAttempt) =>
      intercept[IllegalArgumentException] {
        CelebornShufflePusherFactory.encodeAttemptNumber(stageAttempt, taskAttempt)
      }
    }
  }

  test("a captured task-owned pusher works on a worker without Spark's thread-local context") {
    val client = new RecordingCelebornShuffleClient
    val adapter = CelebornShufflePusherFactory
      .create(enabledConf, client, 11, 3, 2, emptyTaskContext())
      .get
    val failure = new AtomicReference[Throwable]()

    val worker = new Thread(() => {
      try {
        assert(TaskContext.get() == null)
        assert(adapter.pushPartitionData(1, Array[Byte](9), 1) == 1)
      } catch {
        case error: Throwable => failure.set(error)
      }
    })

    worker.start()
    worker.join(5000)

    assert(!worker.isAlive, "worker thread did not finish")
    assert(failure.get() == null)
    assert(client.observedTaskContext == null)
    assert(client.lastPush.partitionId == 1)
  }
}
