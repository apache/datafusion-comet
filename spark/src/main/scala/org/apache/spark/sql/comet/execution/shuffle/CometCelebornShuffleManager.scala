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

import java.lang.reflect.InvocationTargetException

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle.{ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}

import org.apache.comet.util.ClassLoaders

/**
 * Lets Comet execution coexist with the application's existing Celeborn shuffle manager.
 *
 * Ordinary Spark shuffle dependencies are owned entirely by Celeborn. Native and JVM Comet
 * shuffle dependencies remain unsupported until their remote writer, reader, and task lifecycle
 * are integrated; rejecting them prevents either local-disk fallback or handing a native Comet
 * input iterator to Celeborn's row writer.
 *
 * Celeborn is loaded reflectively because its client is an optional, application-provided
 * dependency rather than part of Comet's compile-time or runtime distribution.
 */
class CometCelebornShuffleManager private[shuffle] (
    conf: SparkConf,
    isDriver: Boolean,
    backendFactory: (SparkConf, Boolean) => ShuffleManager)
    extends ShuffleManager {

  /** Constructor selected by Spark for driver and executor shuffle managers. */
  def this(conf: SparkConf, isDriver: Boolean) =
    this(conf, isDriver, CometCelebornShuffleManager.createBackend)

  private val celebornManager = Option(backendFactory(conf, isDriver)).getOrElse {
    throw new IllegalStateException("Celeborn Spark shuffle manager factory returned null")
  }

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    dependency match {
      case _: CometShuffleDependency[_, _, _] => rejectCometShuffle()
      case _ => celebornManager.registerShuffle(shuffleId, dependency)
    }
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    rejectCometHandle(handle)
    celebornManager.getWriter(handle, mapId, context, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    rejectCometHandle(handle)
    celebornManager.getReader(
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver =
    celebornManager.shuffleBlockResolver

  override def unregisterShuffle(shuffleId: Int): Boolean =
    celebornManager.unregisterShuffle(shuffleId)

  override def stop(): Unit = celebornManager.stop()

  private def rejectCometHandle(handle: ShuffleHandle): Unit = handle match {
    case _: CometNativeShuffleHandle[_, _] => rejectCometShuffle()
    case _: CometBypassMergeSortShuffleHandle[_, _] => rejectCometShuffle()
    case _: CometSerializedShuffleHandle[_, _] => rejectCometShuffle()
    case _ =>
  }

  private def rejectCometShuffle(): Nothing = {
    throw new UnsupportedOperationException(
      "Comet shuffle over Celeborn is not supported yet; its remote writer, reader, " +
        "and task lifecycle must be integrated before Comet shuffle can be enabled")
  }
}

private[shuffle] object CometCelebornShuffleManager {

  private val CELEBORN_MANAGER_CLASS = "org.apache.spark.shuffle.celeborn.SparkShuffleManager"

  private[shuffle] def createBackend(conf: SparkConf, isDriver: Boolean): ShuffleManager = {
    try {
      val managerClass = ClassLoaders.loadClass(CELEBORN_MANAGER_CLASS)
      if (!classOf[ShuffleManager].isAssignableFrom(managerClass)) {
        throw new IllegalStateException(
          "Celeborn Spark shuffle manager does not implement ShuffleManager: " +
            CELEBORN_MANAGER_CLASS)
      }

      val constructor = managerClass.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
      constructor.newInstance(conf, Boolean.box(isDriver)).asInstanceOf[ShuffleManager]
    } catch {
      case error: ClassNotFoundException =>
        throw new IllegalStateException(
          s"Celeborn Spark shuffle manager is not available: $CELEBORN_MANAGER_CLASS. " +
            "Ensure the Celeborn Spark client is present on the application classpath",
          error)
      case error: InvocationTargetException =>
        throw new IllegalStateException(
          s"Could not initialize Celeborn Spark shuffle manager: $CELEBORN_MANAGER_CLASS",
          Option(error.getCause).getOrElse(error))
      case error: ReflectiveOperationException =>
        throw new IllegalStateException(
          s"Could not construct Celeborn Spark shuffle manager: $CELEBORN_MANAGER_CLASS",
          error)
      case error: LinkageError =>
        throw new IllegalStateException(
          s"Could not load Celeborn Spark shuffle manager: $CELEBORN_MANAGER_CLASS",
          error)
    }
  }
}
