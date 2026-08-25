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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}

class CometCelebornShuffleManagerSuite extends AnyFunSuite {

  private class RecordingShuffleManager extends ShuffleManager {

    val returnedHandle: ShuffleHandle =
      new BaseShuffleHandle[Any, Any, Any](31, null)

    var registration: Option[(Int, ShuffleDependency[_, _, _])] = None
    var writerCall: Option[(ShuffleHandle, Long)] = None
    var readerCall: Option[(ShuffleHandle, Int, Int, Int, Int)] = None
    var unregisteredShuffleId: Option[Int] = None
    var unregisterResult = true
    var resolverReads = 0
    var stopped = false
    var registrationFailure: RuntimeException = _

    override def registerShuffle[K, V, C](
        shuffleId: Int,
        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
      registration = Some((shuffleId, dependency))
      if (registrationFailure != null) {
        throw registrationFailure
      }
      returnedHandle
    }

    override def getWriter[K, V](
        handle: ShuffleHandle,
        mapId: Long,
        context: TaskContext,
        metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
      writerCall = Some((handle, mapId))
      null
    }

    override def getReader[K, C](
        handle: ShuffleHandle,
        startMapIndex: Int,
        endMapIndex: Int,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext,
        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
      readerCall = Some((handle, startMapIndex, endMapIndex, startPartition, endPartition))
      null
    }

    override def shuffleBlockResolver: ShuffleBlockResolver = {
      resolverReads += 1
      null
    }

    override def unregisterShuffle(shuffleId: Int): Boolean = {
      unregisteredShuffleId = Some(shuffleId)
      unregisterResult
    }

    override def stop(): Unit = stopped = true
  }

  private def manager(
      backend: ShuffleManager,
      conf: SparkConf = new SparkConf(false),
      isDriver: Boolean = true): CometCelebornShuffleManager = {
    new CometCelebornShuffleManager(conf, isDriver, (_, _) => backend)
  }

  test("manager forwards the existing Spark configuration and driver identity") {
    val conf = new SparkConf(false)
      .set("spark.celeborn.master.endpoints", "existing-master:9097")
    val backend = new RecordingShuffleManager
    var observedConf: SparkConf = null
    var observedIsDriver = true

    new CometCelebornShuffleManager(
      conf,
      false,
      (actualConf, actualIsDriver) => {
        observedConf = actualConf
        observedIsDriver = actualIsDriver
        backend
      })

    assert(observedConf eq conf)
    assert(!observedIsDriver)
    assert(conf.get("spark.celeborn.master.endpoints") == "existing-master:9097")
  }

  test("ordinary shuffle registration preserves the existing Celeborn handle and fallback") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val dependency = null.asInstanceOf[ShuffleDependency[Any, Any, Any]]

    val handle = composite.registerShuffle(31, dependency)

    assert(handle eq backend.returnedHandle)
    assert(backend.registration.contains((31, dependency)))
  }

  test("ordinary map writers and reduce readers are owned by the existing Celeborn manager") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle

    assert(composite.getWriter[Any, Any](handle, 93L, null, null) == null)
    assert(backend.writerCall.contains((handle, 93L)))

    assert(composite.getReader[Any, Any](handle, 2, 8, 3, 7, null, null) == null)
    assert(backend.readerCall.contains((handle, 2, 8, 3, 7)))
  }

  test("the inherited all-mapper reader also delegates to the existing Celeborn manager") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle

    assert(composite.getReader[Any, Any](handle, 4, 9, null, null) == null)
    assert(backend.readerCall.contains((handle, 0, Int.MaxValue, 4, 9)))
  }

  test("resolver, shuffle cleanup, and shutdown preserve existing Celeborn behavior") {
    val backend = new RecordingShuffleManager
    backend.unregisterResult = false
    val composite = manager(backend)

    assert(composite.shuffleBlockResolver == null)
    assert(backend.resolverReads == 1)

    assert(!composite.unregisterShuffle(17))
    assert(backend.unregisteredShuffleId.contains(17))

    composite.stop()
    assert(backend.stopped)
  }

  test("backend registration failures propagate without local Comet fallback") {
    val backend = new RecordingShuffleManager
    val expected = new IllegalStateException("remote shuffle registration failed")
    backend.registrationFailure = expected
    val composite = manager(backend)

    val actual = intercept[IllegalStateException] {
      composite.registerShuffle[Any, Any, Any](31, null)
    }

    assert(actual eq expected)
    assert(backend.writerCall.isEmpty)
  }

  test("native and JVM Comet handles never reach the stock Celeborn row path") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val unsupportedHandles = Seq[ShuffleHandle](
      new CometNativeShuffleHandle[Any, Any](41, null),
      new CometBypassMergeSortShuffleHandle[Any, Any](42, null),
      new CometSerializedShuffleHandle[Any, Any](43, null))

    unsupportedHandles.foreach { handle =>
      val writerFailure = intercept[UnsupportedOperationException] {
        composite.getWriter[Any, Any](handle, 0L, null, null)
      }
      assert(writerFailure.getMessage.contains("Comet shuffle over Celeborn is not supported"))

      val readerFailure = intercept[UnsupportedOperationException] {
        composite.getReader[Any, Any](handle, 0, 1, 0, 1, null, null)
      }
      assert(readerFailure.getMessage.contains("Comet shuffle over Celeborn is not supported"))
    }

    assert(backend.writerCall.isEmpty)
    assert(backend.readerCall.isEmpty)
  }

  test("a missing delegated backend fails closed without selecting local shuffle") {
    val error = intercept[IllegalStateException] {
      new CometCelebornShuffleManager(new SparkConf(false), true, (_, _) => null)
    }

    assert(error.getMessage.contains("factory returned null"))
  }

  test("the public constructor rejects an application without the Celeborn client") {
    val celebornManagerAvailable =
      try {
        getClass.getClassLoader.loadClass("org.apache.spark.shuffle.celeborn.SparkShuffleManager")
        true
      } catch {
        case _: ClassNotFoundException => false
      }

    if (celebornManagerAvailable) {
      cancel("The optional Celeborn Spark client is present on this test classpath")
    }

    val error = intercept[IllegalStateException] {
      new CometCelebornShuffleManager(new SparkConf(false), true)
    }

    assert(error.getMessage.contains("Celeborn Spark shuffle manager is not available"))
  }
}
