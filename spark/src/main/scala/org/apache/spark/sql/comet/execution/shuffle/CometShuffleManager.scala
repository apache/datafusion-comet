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

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config.IO_COMPRESSION_CODEC
import org.apache.spark.io.CompressionCodec
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.{BypassMergeSortShuffleHandle, SerializedShuffleHandle, SortShuffleManager, SortShuffleWriter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.collection.OpenHashSet

import org.apache.comet.CometConf

/**
 * A [[ShuffleManager]] that uses Arrow format to shuffle data.
 */
class CometShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  import CometShuffleManager._
  import SortShuffleManager._

  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  private val sortShuffleManager = new SortShuffleManager(conf)

  /**
   * A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
   */
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  private lazy val shuffleExecutorComponents = loadShuffleExecutorComponents(conf)

  override val shuffleBlockResolver: IndexShuffleBlockResolver = {
    // The patch versions of Spark 3.4 have different constructor signatures:
    // See https://github.com/apache/spark/commit/5180694705be3508bd21dd9b863a59b8cb8ba193
    // We look for proper constructor by reflection.
    classOf[IndexShuffleBlockResolver].getDeclaredConstructors
      .filter(c => List(2, 3).contains(c.getParameterCount()))
      .map { c =>
        c.getParameterCount match {
          case 2 =>
            c.newInstance(conf, null).asInstanceOf[IndexShuffleBlockResolver]
          case 3 =>
            c.newInstance(conf, null, Collections.emptyMap())
              .asInstanceOf[IndexShuffleBlockResolver]
        }
      }
      .head
  }

  /**
   * (override) Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    dependency match {
      case cometShuffleDependency: CometShuffleDependency[_, _, _] =>
        // Comet shuffle dependency, which comes from `CometShuffleExchangeExec`.
        cometShuffleDependency.shuffleType match {
          case CometColumnarShuffle =>
            // Comet columnar shuffle, which uses Arrow format to shuffle data.
            if (shouldBypassMergeSort(conf, dependency) ||
              !SortShuffleManager.canUseSerializedShuffle(dependency)) {
              new CometBypassMergeSortShuffleHandle(
                shuffleId,
                dependency.asInstanceOf[ShuffleDependency[K, V, V]])
            } else {
              new CometSerializedShuffleHandle(
                shuffleId,
                dependency.asInstanceOf[ShuffleDependency[K, V, V]])
            }
          case CometNativeShuffle =>
            new CometNativeShuffleHandle(
              shuffleId,
              dependency.asInstanceOf[ShuffleDependency[K, V, V]])
          case _ =>
            // Unsupported shuffle type.
            throw new UnsupportedOperationException(
              s"Unsupported shuffle type: ${cometShuffleDependency.shuffleType}")
        }
      case _ =>
        // It is a Spark shuffle dependency, so we use Spark Sort Shuffle Manager.
        if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
          // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
          // need map-side aggregation, then write numPartitions files directly and just concatenate
          // them at the end. This avoids doing serialization and deserialization twice to merge
          // together the spilled files, which would happen with the normal code path. The downside
          // is having multiple files open at a time and thus more memory allocated to buffers.
          new BypassMergeSortShuffleHandle[K, V](
            shuffleId,
            dependency.asInstanceOf[ShuffleDependency[K, V, V]])
        } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
          // Otherwise, try to buffer map outputs in a serialized form, since this is more
          // efficient:
          new SerializedShuffleHandle[K, V](
            shuffleId,
            dependency.asInstanceOf[ShuffleDependency[K, V, V]])
        } else {
          // Otherwise, buffer map outputs in a deserialized form:
          new BaseShuffleHandle(shuffleId, dependency)
        }
    }
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, _, C]]
    val (blocksByAddress, canEnableBatchFetch) =
      if (baseShuffleHandle.dependency.shuffleMergeEnabled) {
        val res = SparkEnv.get.mapOutputTracker.getPushBasedShuffleMapSizesByExecutorId(
          handle.shuffleId,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition)
        (res.iter, res.enableBatchFetch)
      } else {
        val address = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
          handle.shuffleId,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition)
        (address, true)
      }

    if (handle.isInstanceOf[CometBypassMergeSortShuffleHandle[_, _]] ||
      handle.isInstanceOf[CometSerializedShuffleHandle[_, _]] ||
      handle.isInstanceOf[CometNativeShuffleHandle[_, _]]) {
      new CometBlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        blocksByAddress,
        context,
        metrics,
        shouldBatchFetch =
          canEnableBatchFetch && canUseBatchFetch(startPartition, endPartition, context))
    } else {
      // It is a Spark shuffle dependency, so we use Spark Sort Shuffle Reader.
      sortShuffleManager.getReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val mapTaskIds =
      taskIdMapsForShuffle.computeIfAbsent(handle.shuffleId, _ => new OpenHashSet[Long](16))
    mapTaskIds.synchronized {
      mapTaskIds.add(context.taskAttemptId())
    }
    val env = SparkEnv.get
    handle match {
      case cometShuffleHandle: CometNativeShuffleHandle[K @unchecked, V @unchecked] =>
        val dep = cometShuffleHandle.dependency.asInstanceOf[CometShuffleDependency[_, _, _]]
        new CometNativeShuffleWriter(
          dep.outputPartitioning.get,
          dep.outputAttributes,
          dep.shuffleWriteMetrics,
          dep.numParts,
          dep.shuffleId,
          mapId,
          context,
          metrics)
      case bypassMergeSortHandle: CometBypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new CometBypassMergeSortShuffleWriter(
          env.blockManager,
          context.taskMemoryManager(),
          context,
          bypassMergeSortHandle,
          mapId,
          env.conf,
          metrics,
          shuffleExecutorComponents)
      case unsafeShuffleHandle: CometSerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new CometUnsafeShuffleWriter(
          env.blockManager,
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf,
          metrics,
          shuffleExecutorComponents)
      case _ =>
        // It is a Spark shuffle dependency, so we use Spark Sort Shuffle Writer.
        sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
      mapTaskIds.iterator.foreach { mapTaskId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapTaskId)
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}

object CometShuffleManager extends Logging {

  /**
   * Loads executor components for shuffle data IO.
   */
  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX).toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    executorComponents
  }

  lazy val compressionCodecForShuffling: CompressionCodec = {
    val sparkConf = SparkEnv.get.conf
    val codecName = CometConf.COMET_EXEC_SHUFFLE_COMPRESSION_CODEC.get(SQLConf.get)

    // only zstd compression is supported at the moment
    if (codecName != "zstd") {
      logWarning(
        s"Overriding config ${IO_COMPRESSION_CODEC}=${codecName} in shuffling, force using zstd")
    }
    CompressionCodec.createCodec(sparkConf, "zstd")
  }

  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    if (dep.mapSideCombine) {
      false
    } else {
      // Condition from Spark:
      // Bypass merge sort if we have fewer than `spark.shuffle.sort.bypassMergeThreshold`
      val partitionCond = SortShuffleWriter.shouldBypassMergeSort(conf, dep)

      // Bypass merge sort if we have partition * cores fewer than
      // `spark.comet.columnar.shuffle.async.max.thread.num`
      val executorCores = conf.get(config.EXECUTOR_CORES)
      val maxThreads = CometConf.COMET_COLUMNAR_SHUFFLE_ASYNC_MAX_THREAD_NUM.get(SQLConf.get)
      val threadCond = dep.partitioner.numPartitions * executorCores <= maxThreads

      // Comet columnar shuffle buffers rows in memory. If too many cores are used with
      // relatively high number of partitions, it may cause OOM when initializing the
      // hash-based shuffle writers at beginning of the task. For example, 10 cores
      // with 100 partitions will allocates 1000 writers. Sort-based shuffle doesn't have
      // this issue because it only allocates one writer per task.
      partitionCond && threadCond
    }
  }
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the bypass merge
 * sort shuffle path.
 */
private[spark] class CometBypassMergeSortShuffleHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V])
    extends BaseShuffleHandle(shuffleId, dependency) {}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the serialized
 * shuffle.
 */
private[spark] class CometSerializedShuffleHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V])
    extends BaseShuffleHandle(shuffleId, dependency) {}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the native shuffle
 * writer.
 */
private[spark] class CometNativeShuffleHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V])
    extends BaseShuffleHandle(shuffleId, dependency) {}
