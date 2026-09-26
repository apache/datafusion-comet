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

package org.apache.spark

import java.{util => ju}
import java.util.Collections

import scala.util.Try

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{EXECUTOR_MEMORY_OVERHEAD, EXECUTOR_MEMORY_OVERHEAD_FACTOR}
import org.apache.spark.sql.internal.StaticSQLConf

import org.apache.comet.{COMET_VERSION, CometSparkSessionExtensions, NativeBase}
import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometConf.{COMET_ICEBERG_WRITE_REPORT_DIR, COMET_METRICS_ENABLED, COMET_ONHEAP_ENABLED}
import org.apache.comet.CometKryoRegistrator
import org.apache.comet.annotation.Public
import org.apache.comet.iceberg.IcebergWriteReportListener

/**
 * Comet driver plugin. This class is loaded by Spark's plugin framework. It will be instantiated
 * on driver side only. It will update the SparkConf with the extra configuration provided by
 * Comet, e.g., the cache serializer and the session extension.
 *
 * Note that `SparkContext.conf` is spark package only. So this plugin must be in spark package.
 * Although `SparkContext.getConf` is public, it returns a copy of the SparkConf, so it cannot
 * actually change Spark configs at runtime.
 *
 * To enable this plugin, set the config "spark.plugins" to `org.apache.spark.CometPlugin`.
 */
class CometDriverPlugin extends DriverPlugin with Logging {

  override def init(sc: SparkContext, pluginContext: PluginContext): ju.Map[String, String] = {
    logInfo("CometDriverPlugin init")

    // Expose the Comet build version as a Spark config so it can be queried at runtime, e.g.
    // `spark.conf.get("spark.comet.version")` or `SET spark.comet.version` in SQL. This is set
    // before the off-heap check below so the version is reported even when Comet is otherwise
    // disabled.
    sc.conf.set(CometDriverPlugin.COMET_VERSION_CONFIG, COMET_VERSION)

    if (!CometSparkSessionExtensions.isOffHeapEnabled(sc.getConf) &&
      !sc.getConf.getBoolean(COMET_ONHEAP_ENABLED.key, false)) {
      logWarning("Comet plugin is disabled because Spark is not running in off-heap mode.")
      return Collections.emptyMap[String, String]
    }

    val extraConfs = new ju.HashMap[String, String]()

    CometDriverPlugin.maybeSetCacheSerializer(sc.conf, extraConfs)
    CometDriverPlugin.warnIfKryoRegistratorMissing(sc.conf)

    // register CometSparkSessionExtensions if it isn't already registered
    CometDriverPlugin.registerCometSessionExtension(sc.conf)

    // Register Comet metrics
    CometDriverPlugin.registerCometMetrics(sc)
    CometDriverPlugin.registerIcebergWriteReport(sc.conf)

    CometDriverPlugin.warnIfExecutorMemoryOverheadUnset(sc.getConf)
    CometDriverPlugin.warnIfMemoryPoolFractionSet(sc.getConf)

    extraConfs
  }

  override def receive(message: Any): AnyRef = super.receive(message)

  override def shutdown(): Unit = {
    logInfo("CometDriverPlugin shutdown")

    NativeBase.releaseNative()

    super.shutdown()
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit =
    super.registerMetrics(appId, pluginContext)

}

object CometDriverPlugin extends Logging {

  /** Spark config key under which the loaded Comet version is exposed at runtime. */
  val COMET_VERSION_CONFIG = "spark.comet.version"

  // Use Comet's cache serializer only for the native in-memory cache path.
  // If the application already set spark.sql.cache.serializer, leave that value
  // unchanged so Comet does not replace a user-selected cache format.
  private[apache] def maybeSetCacheSerializer(
      conf: SparkConf,
      extraConfs: ju.HashMap[String, String]): Unit = {
    if (conf.getBoolean(CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key, false)) {
      val serializerKey = StaticSQLConf.SPARK_CACHE_SERIALIZER.key
      val serializerValue =
        "org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer"
      val defaultSerializer = StaticSQLConf.SPARK_CACHE_SERIALIZER.defaultValueString
      val currentSerializer = conf.get(serializerKey, defaultSerializer)

      if (currentSerializer == defaultSerializer) {
        extraConfs.put(serializerKey, serializerValue)
        conf.set(serializerKey, serializerValue)
        logInfo(s"Auto-set $serializerKey=$serializerValue")
      } else {
        logInfo(s"Not overriding user-provided $serializerKey=$currentSerializer")
      }
    }
  }

  // Comet hands Spark's serializer classes that Kryo has not been told about, so with
  // spark.kryo.registrationRequired=true it rejects them with "Class is not registered", which
  // names neither Comet nor the operation that failed. Two paths reach it: a native broadcast,
  // which broadcasts an Array[ChunkedByteBuffer], and any cached block Spark serializes -- the
  // disk half of MEMORY_AND_DISK, the _SER levels, replication, a cross-executor fetch.
  // CometKryoRegistrator covers both, but spark.kryo.registrator is read when SparkEnv builds the
  // serializer, before any plugin runs, so it cannot be set from here. Say so while the
  // application is still starting up rather than leaving the user to attribute the failure later.
  private[apache] def warnIfKryoRegistratorMissing(conf: SparkConf): Unit = {
    val usingKryo =
      conf.get("spark.serializer", "") == "org.apache.spark.serializer.KryoSerializer"
    val registrationRequired = conf.getBoolean("spark.kryo.registrationRequired", false)
    val registered = conf
      .get("spark.kryo.registrator", "")
      .split(',')
      .map(_.trim)
      .contains(CometKryoRegistrator.CLASS_NAME)

    if (usingKryo && registrationRequired && !registered) {
      logWarning(
        "spark.kryo.registrationRequired=true but spark.kryo.registrator does not include " +
          s"${CometKryoRegistrator.CLASS_NAME}. Comet's native broadcast and its in-memory " +
          "cache format will fail with Kryo's \"Class is not registered\" as soon as their " +
          "payloads are serialized. Add " +
          s"spark.kryo.registrator=${CometKryoRegistrator.CLASS_NAME} before creating the " +
          "SparkContext; it cannot be set later.")
    }
  }

  // Comet's native allocations are made by the Rust global allocator and live in the native heap.
  // In off-heap mode the share that operators reserve is charged against a memory pool, but
  // everything else -- expression kernels and Arrow array builders, decompression buffers, Parquet
  // reader structures, object store buffers, the tokio runtime, allocator overhead -- is covered by
  // no budget at all, and neither is Comet's JVM-side Arrow allocator. In on-heap mode the pool is
  // unbounded and nothing is bounded at all. The only slack the executor container has for that is
  // spark.executor.memoryOverhead, which the JVM's own non-heap usage already draws on.
  //
  // Comet used to add an overhead of its own to it here, but a driver plugin cannot: on Spark
  // 3.4, 3.5 and 4.0, SparkContext builds the default ResourceProfile before it creates the plugin
  // container, and the cluster managers size executors from that profile rather than re-reading
  // the conf, so the new value never reached the container. Say so while the application is still
  // starting up instead, because this has to be set before the SparkContext is created.
  private[apache] def warnIfExecutorMemoryOverheadUnset(conf: SparkConf): Unit = {
    val cometEnabled = getBooleanConf(conf, CometConf.COMET_ENABLED)
    val cometExecEnabled = getBooleanConf(conf, CometConf.COMET_EXEC_ENABLED)
    val cometShuffleEnabled = getBooleanConf(conf, CometConf.COMET_SHUFFLE_ENABLED)
    val cometActive = cometEnabled && (cometExecEnabled || cometShuffleEnabled)
    // Local mode, local-cluster included, has no executor container to size
    val localMode = conf.get("spark.master", "").startsWith("local")

    if (cometActive && !localMode && !isExecutorMemoryOverheadSet(conf)) {
      logWarning(
        s"Neither ${EXECUTOR_MEMORY_OVERHEAD.key} nor ${EXECUTOR_MEMORY_OVERHEAD_FACTOR.key} is " +
          "set. Comet allocates outside the JVM heap, and the part of that which no memory pool " +
          "tracks is not covered by spark.executor.memory or spark.memory.offHeap.size, so " +
          "Spark's default overhead can leave the executor short and the cluster manager may " +
          "kill it. Set one of them before creating the SparkContext; neither can be set later. " +
          s"${CometConf.TUNING_GUIDE}.")
    }
  }

  // Whether the application sized the executor memory overhead itself, as an amount or as a
  // factor of spark.executor.memory, rather than leaving it at Spark's default.
  private def isExecutorMemoryOverheadSet(conf: SparkConf): Boolean =
    conf.contains(EXECUTOR_MEMORY_OVERHEAD.key) ||
      conf.contains(EXECUTOR_MEMORY_OVERHEAD_FACTOR.key) ||
      isKubernetesMemoryOverheadFactorSet(conf)

  // Kubernetes falls back to spark.kubernetes.memoryOverheadFactor when
  // spark.executor.memoryOverheadFactor is unset. In cluster mode spark-submit sets it for the
  // driver even when the application did not, to 0.4 for PySpark and SparkR applications and 0.1
  // for the rest, so only a different value shows that the application set it.
  private def isKubernetesMemoryOverheadFactorSet(conf: SparkConf): Boolean = {
    val submitDefault = conf.get("spark.kubernetes.resource.type", "java") match {
      case "python" | "r" => 0.4
      case _ => 0.1
    }
    conf
      .getOption("spark.kubernetes.memoryOverheadFactor")
      .exists(factor => !Try(factor.toDouble).toOption.contains(submitDefault))
  }

  // spark.comet.exec.memoryPool.fraction was documented as holding back part of the off-heap pool
  // for the native memory that Comet does not reserve. It cannot: Spark hands out the whole pool
  // to the tasks that ask for it, and the fraction only caps each task's consumers under
  // fair_unified. Users who set it for that purpose need to size the memory overhead instead.
  private[apache] def warnIfMemoryPoolFractionSet(conf: SparkConf): Unit = {
    val key = CometConf.COMET_OFFHEAP_MEMORY_POOL_FRACTION.key
    conf.getOption(key).foreach { value =>
      logWarning(
        s"$key=$value is deprecated and will be removed in a future release. It does not leave " +
          "room in spark.memory.offHeap.size for native memory that Comet's memory pools do " +
          "not track, because Spark hands out the whole off-heap pool whatever it is set to. " +
          s"Size ${EXECUTOR_MEMORY_OVERHEAD.key} for that memory instead. " +
          s"${CometConf.TUNING_GUIDE}.")
    }
  }

  private def getBooleanConf(conf: SparkConf, entry: ConfigEntry[Boolean]): Boolean =
    conf.getBoolean(entry.key, entry.defaultValue.get)

  def registerCometMetrics(sc: SparkContext): Unit = {
    if (sc.getConf.getBoolean(
        COMET_METRICS_ENABLED.key,
        COMET_METRICS_ENABLED.defaultValue.get)) {
      sc.env.metricsSystem.registerSource(CometSource)
      registerQueryExecutionListener(sc.conf, "org.apache.comet.CometMetricsListener")
    } else {
      logInfo(
        "Comet metrics reporting is disabled. Set spark.comet.metrics.enabled=true to enable.")
    }
  }

  // Test-only: see COMET_ICEBERG_WRITE_REPORT_DIR. The value may come from the environment, which
  // lets the Iceberg Spark test jobs turn the report on without changing the Iceberg diffs.
  def registerIcebergWriteReport(conf: SparkConf): Unit = {
    if (conf
        .get(COMET_ICEBERG_WRITE_REPORT_DIR.key, COMET_ICEBERG_WRITE_REPORT_DIR.defaultValue.get)
        .nonEmpty) {
      registerQueryExecutionListener(conf, classOf[IcebergWriteReportListener].getName)
    }
  }

  private def registerQueryExecutionListener(conf: SparkConf, listenerClass: String): Unit = {
    val listenerKey = "spark.sql.queryExecutionListeners"
    val listeners = conf.get(listenerKey, "")
    if (listeners.isEmpty) {
      logInfo(s"Setting $listenerKey=$listenerClass")
      conf.set(listenerKey, listenerClass)
    } else {
      val currentListeners = listeners.split(",").map(_.trim)
      if (!currentListeners.contains(listenerClass)) {
        val newValue = s"$listeners,$listenerClass"
        logInfo(s"Setting $listenerKey=$newValue")
        conf.set(listenerKey, newValue)
      }
    }
  }

  def registerCometSessionExtension(conf: SparkConf): Unit = {
    val extensionKey = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
    val extensionClass = classOf[CometSparkSessionExtensions].getName
    val extensions = conf.get(extensionKey, "")
    if (extensions.isEmpty) {
      logInfo(s"Setting $extensionKey=$extensionClass")
      conf.set(extensionKey, extensionClass)
    } else {
      val currentExtensions = extensions.split(",").map(_.trim)
      if (!currentExtensions.contains(extensionClass)) {
        val newValue = s"$extensions,$extensionClass"
        logInfo(s"Setting $extensionKey=$newValue")
        conf.set(extensionKey, newValue)
      }
    }
  }
}

class CometExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: ju.Map[String, String]): Unit = {
    logInfo("CometExecutorPlugin init")

    super.init(ctx, extraConf)
  }

  override def shutdown(): Unit = {
    logInfo("CometExecutorPlugin shutdown")

    NativeBase.releaseNative()

    super.shutdown()
  }

}

/**
 * The Comet plugin for Spark. To enable this plugin, set the config "spark.plugins" to
 * `org.apache.spark.CometPlugin`
 */
@Public
class CometPlugin extends SparkPlugin with Logging {
  override def driverPlugin(): DriverPlugin = new CometDriverPlugin

  override def executorPlugin(): ExecutorPlugin = new CometExecutorPlugin
}
