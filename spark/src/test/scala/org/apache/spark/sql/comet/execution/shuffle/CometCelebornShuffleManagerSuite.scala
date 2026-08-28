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

    val returnedHandle: ShuffleHandle = new BaseShuffleHandle[Any, Any, Any](31, null)

    var registration: Option[(Int, ShuffleDependency[_, _, _])] = None
    var writerCall: Option[(ShuffleHandle, Long)] = None
    var rangedReaderCall: Option[(ShuffleHandle, Int, Int, Int, Int)] = None
    var unregisteredShuffleId: Option[Int] = None
    var unregisterResult = true
    var resolverReads = 0
    var stopped = false
    var registrationFailure: RuntimeException = _
    var writerFailure: RuntimeException = _
    var readerFailure: RuntimeException = _

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
      if (writerFailure != null) {
        throw writerFailure
      }
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
      rangedReaderCall = Some((handle, startMapIndex, endMapIndex, startPartition, endPartition))
      if (readerFailure != null) {
        throw readerFailure
      }
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

  test("manager preserves the application Spark configuration and driver identity") {
    val conf = new SparkConf(false).set("spark.app.name", "existing-celeborn-application")
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
    assert(conf.get("spark.app.name") == "existing-celeborn-application")
  }

  test("ordinary shuffle registration preserves the delegated manager's handle") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val dependency = null.asInstanceOf[ShuffleDependency[Any, Any, Any]]

    val handle = composite.registerShuffle(31, dependency)

    assert(handle eq backend.returnedHandle)
    assert(backend.registration.contains((31, dependency)))
  }

  test("ordinary map writers are owned by the delegated Celeborn manager") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle

    assert(composite.getWriter[Any, Any](handle, 93L, null, null) == null)
    assert(backend.writerCall.contains((handle, 93L)))
  }

  test("mapper-range reads preserve the delegated Celeborn reader's exact range") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle

    assert(composite.getReader[Any, Any](handle, 2, 8, 3, 7, null, null) == null)
    assert(backend.rangedReaderCall.contains((handle, 2, 8, 3, 7)))
  }

  test("all-mapper reads delegate through Spark's inherited complete mapper range") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle

    assert(composite.getReader[Any, Any](handle, 4, 9, null, null) == null)
    assert(backend.rangedReaderCall.contains((handle, 0, Int.MaxValue, 4, 9)))
  }

  test("resolver, shuffle removal, and shutdown retain the delegated lifecycle") {
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

  test("registration failures retain their original exception without local fallback") {
    val backend = new RecordingShuffleManager
    val expected = new IllegalStateException("remote shuffle registration failed")
    backend.registrationFailure = expected

    val actual = intercept[IllegalStateException] {
      manager(backend).registerShuffle[Any, Any, Any](31, null)
    }

    assert(actual eq expected)
    assert(backend.writerCall.isEmpty)
    assert(backend.rangedReaderCall.isEmpty)
  }

  test("delegated writer and reader failures retain their original exception") {
    val backend = new RecordingShuffleManager
    val composite = manager(backend)
    val handle = backend.returnedHandle
    val writerFailure = new IllegalStateException("remote shuffle writer failed")
    val readerFailure = new IllegalStateException("remote shuffle reader failed")
    backend.writerFailure = writerFailure
    backend.readerFailure = readerFailure

    val actualWriterFailure = intercept[IllegalStateException] {
      composite.getWriter[Any, Any](handle, 0L, null, null)
    }
    val actualAllMapperFailure = intercept[IllegalStateException] {
      composite.getReader[Any, Any](handle, 0, 1, null, null)
    }
    val actualRangedFailure = intercept[IllegalStateException] {
      composite.getReader[Any, Any](handle, 0, 1, 0, 1, null, null)
    }

    assert(actualWriterFailure eq writerFailure)
    assert(actualAllMapperFailure eq readerFailure)
    assert(actualRangedFailure eq readerFailure)
  }

  test("Comet native and JVM handles never reach Celeborn's ordinary shuffle paths") {
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
      val allMapperFailure = intercept[UnsupportedOperationException] {
        composite.getReader[Any, Any](handle, 0, 1, null, null)
      }
      val rangedFailure = intercept[UnsupportedOperationException] {
        composite.getReader[Any, Any](handle, 0, 1, 0, 1, null, null)
      }

      assert(writerFailure.getMessage.contains("Comet shuffle over Celeborn is not supported"))
      assert(allMapperFailure.getMessage.contains("Comet shuffle over Celeborn is not supported"))
      assert(rangedFailure.getMessage.contains("Comet shuffle over Celeborn is not supported"))
    }

    assert(backend.writerCall.isEmpty)
    assert(backend.rangedReaderCall.isEmpty)
  }

  test("a null delegated backend fails without selecting a local shuffle manager") {
    val failure = intercept[IllegalStateException] {
      new CometCelebornShuffleManager(new SparkConf(false), true, (_, _) => null)
    }

    assert(failure.getMessage.contains("factory returned null"))
  }

  test("a delegated backend construction failure retains its original exception") {
    val expected = new IllegalStateException("existing Celeborn manager failed to initialize")

    val actual = intercept[IllegalStateException] {
      new CometCelebornShuffleManager(new SparkConf(false), true, (_, _) => throw expected)
    }

    assert(actual eq expected)
  }

  test("the public constructor rejects an application without the optional Celeborn client") {
    val currentThread = Thread.currentThread()
    val originalLoader = currentThread.getContextClassLoader
    val missingCelebornLoader = new ClassLoader(originalLoader) {
      override protected def loadClass(name: String, resolve: Boolean): Class[_] = {
        if (name == "org.apache.spark.shuffle.celeborn.SparkShuffleManager") {
          throw new ClassNotFoundException(name)
        }
        super.loadClass(name, resolve)
      }
    }

    currentThread.setContextClassLoader(missingCelebornLoader)
    try {
      val failure = intercept[IllegalStateException] {
        new CometCelebornShuffleManager(new SparkConf(false), true)
      }

      assert(failure.getMessage.contains("Celeborn Spark shuffle manager is not available"))
      assert(failure.getCause.isInstanceOf[ClassNotFoundException])
    } finally {
      currentThread.setContextClassLoader(originalLoader)
    }
  }
}
