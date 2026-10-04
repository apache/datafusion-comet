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
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.util.Try

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{EVENT_LOG_ENABLED, EXECUTOR_MEMORY_OVERHEAD, EXECUTOR_MEMORY_OVERHEAD_FACTOR}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer
import org.apache.spark.sql.comet.execution.shuffle.{CometCelebornShuffleManager, CometShuffleManager}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.util.{Clock, SystemClock}

import org.apache.comet.{COMET_VERSION, CometExecIterator, CometExecutorMemoryUsage, CometSparkSessionExtensions, NativeBase}
import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometConf.{COMET_ICEBERG_WRITE_REPORT_DIR, COMET_METRICS_ENABLED, COMET_ONHEAP_ENABLED}
import org.apache.comet.CometExecIterator.MemoryUsageSummary
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
class CometDriverPlugin private[spark] (clock: Clock) extends DriverPlugin with Logging {

  def this() = this(new SystemClock())

  // Set by init, before Spark delivers any message, and read on the RPC thread that delivers them.
  @volatile private var sparkContext: SparkContext = _

  // By executor, the memory usage samples that the event log has yet to record. The RPC thread
  // that delivers samples shares it with the threads that record them.
  private val memoryUsageSummaries = mutable.HashMap.empty[String, MemoryUsageSummary]

  override def init(sc: SparkContext, pluginContext: PluginContext): ju.Map[String, String] = {
    logInfo("CometDriverPlugin init")

    sparkContext = sc
    if (sc.conf.get(EVENT_LOG_ENABLED)) {
      // A queue of its own, so that a slow listener on the shared queue cannot hold the
      // application's end back until the listener bus has stopped, which drops what is posted
      // after it.
      sc.listenerBus.addToQueue(
        new SparkListener {
          // Every executor heartbeat posts one, whether or not the executor is busy, so this ends
          // an idle executor's summary too. The event log does not record the heartbeat itself.
          override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit =
            recordMemoryUsage(memoryUsageSummaries.synchronized {
              memoryUsageSummaries
                .get(event.execId)
                .toList
                .flatMap(_.flushIfDue(clock.nanoTime()))
            })

          // An executor that has gone away sends no more heartbeats to end its summary.
          override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit =
            recordMemoryUsage(memoryUsageSummaries.synchronized {
              memoryUsageSummaries.remove(event.executorId).toList.flatMap(_.flush())
            })

          override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit =
            recordRemainingMemoryUsage()
        },
        "comet")
    }

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
    CometDriverPlugin.warnIfKryoRegistrationsMissing(sc.conf)

    // register CometSparkSessionExtensions if it isn't already registered
    CometDriverPlugin.registerCometSessionExtension(sc.conf)

    // Register Comet metrics
    CometDriverPlugin.registerCometMetrics(sc)
    CometDriverPlugin.registerIcebergWriteReport(sc.conf)

    CometDriverPlugin.warnIfExecutorMemoryOverheadUnset(sc.getConf)
    CometDriverPlugin.warnIfMemoryPoolFractionSet(sc.getConf)

    extraConfs
  }

  override def receive(message: Any): AnyRef = message match {
    // An executor's memory usage sample. A one-way message gets no reply, and Spark logs any
    // reply that is not null.
    case sample: CometExecutorMemoryUsage =>
      memoryUsageSummaries.synchronized {
        memoryUsageSummaries
          .getOrElseUpdate(sample.executorId, new MemoryUsageSummary)
          .add(sample, clock.nanoTime())
      }
      null
    case _ => super.receive(message)
  }

  override def shutdown(): Unit = {
    logInfo("CometDriverPlugin shutdown")

    // From Spark 4.0 the listener bus stops after the plugins, so this records what is left even
    // if the listener has yet to see the application end. Before 4.0 the bus has stopped already.
    recordRemainingMemoryUsage()

    NativeBase.releaseNative()

    super.shutdown()
  }

  // Posting samples to the listener bus is what writes them to the event log.
  private def recordMemoryUsage(samples: Seq[CometExecutorMemoryUsage]): Unit =
    samples.foreach(sparkContext.listenerBus.post)

  private def recordRemainingMemoryUsage(): Unit =
    recordMemoryUsage(memoryUsageSummaries.synchronized {
      val remaining = memoryUsageSummaries.values.flatMap(_.flush()).toList
      memoryUsageSummaries.clear()
      remaining
    })

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit =
    super.registerMetrics(appId, pluginContext)

}

object CometDriverPlugin extends Logging {

  /** Spark config key under which the loaded Comet version is exposed at runtime. */
  val COMET_VERSION_CONFIG = "spark.comet.version"

  // Use Comet's cache serializer only when the native in-memory cache scan can run, which needs
  // Comet and its native execution as well as the cache config. spark.sql.cache.serializer is
  // static, so an application that starts with Comet or native execution off would otherwise
  // store every cache in Comet's format, with only Spark operators to read it. So would one that
  // leaves Comet shuffle enabled without Comet's shuffle manager, since Comet then disables
  // itself.
  // Nor is it used where Kryo requires registration and has not registered Comet's cached batch:
  // caching would then fail the first time Spark serialized a cached block. Where Kryo has
  // registered it, by whatever means, Comet's format is used, since Spark registers its own
  // cached batch only from 4.1.
  // If the application already set spark.sql.cache.serializer, leave that value
  // unchanged so Comet does not replace a user-selected cache format.
  private[apache] def maybeSetCacheSerializer(
      conf: SparkConf,
      extraConfs: ju.HashMap[String, String]): Unit = {
    if (getBooleanConf(conf, CometConf.COMET_ENABLED) &&
      getBooleanConf(conf, CometConf.COMET_EXEC_ENABLED) &&
      getBooleanConf(conf, CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED) &&
      (!getBooleanConf(conf, CometConf.COMET_SHUFFLE_ENABLED) || isCometShuffleManager(conf)) &&
      !unregisteredKryoClasses(conf).contains(ArrowCachedBatchSerializer.cachedBatchClass)) {
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
  private[apache] def warnIfKryoRegistrationsMissing(conf: SparkConf): Unit = {
    val unregistered = unregisteredKryoClasses(conf)
    if (unregistered.nonEmpty) {
      logWarning("spark.kryo.registrationRequired=true but Kryo has not registered " +
        s"${unregistered.map(_.getName).mkString(", ")}, which " +
        s"${CometKryoRegistrator.CLASS_NAME} registers. Comet's native broadcast and in-memory " +
        "cache fail with Kryo's \"Class is not registered\" when they serialize one of them, " +
        "and Comet keeps Spark's cache format while its own cached batch is unregistered. " +
        s"Add spark.kryo.registrator=${CometKryoRegistrator.CLASS_NAME} before creating the " +
        "SparkContext; it cannot be set later.")
    }
  }

  // The classes CometKryoRegistrator registers that Kryo, configured as the application
  // configured it, would reject: none unless it requires registration. They can be registered
  // through CometKryoRegistrator, a registrator of the application's own or
  // spark.kryo.classesToRegister, so ask a Kryo instance built from the conf rather than read the
  // confs. If one cannot be built, take them as registered only if spark.kryo.registrator lists
  // CometKryoRegistrator.
  private[apache] def unregisteredKryoClasses(conf: SparkConf): Seq[Class[_]] = {
    val usingKryo =
      conf.get("spark.serializer", "") == "org.apache.spark.serializer.KryoSerializer"
    if (!usingKryo || !conf.getBoolean("spark.kryo.registrationRequired", false)) {
      Nil
    } else {
      // Qualified, because in this package org.apache.spark.Success, a TaskEndReason, hides an
      // imported scala.util.Success on Scala 2.12.
      Try(new KryoSerializer(conf).newKryo()) match {
        case scala.util.Success(kryo) =>
          CometKryoRegistrator.classes.filter(kryo.getClassResolver.getRegistration(_) == null)
        case scala.util.Failure(e) =>
          logDebug("Could not build Kryo to check Comet's registrations", e)
          val listed = conf
            .get("spark.kryo.registrator", "")
            .split(',')
            .map(_.trim)
            .contains(CometKryoRegistrator.CLASS_NAME)
          if (listed) Nil else CometKryoRegistrator.classes
      }
    }
  }

  // Comet's shuffle managers have no short name, so spark.shuffle.manager names one only by its
  // class name.
  private def isCometShuffleManager(conf: SparkConf): Boolean =
    Set(classOf[CometShuffleManager].getName, classOf[CometCelebornShuffleManager].getName)
      .contains(conf.get("spark.shuffle.manager", ""))

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
    // Only YARN and Kubernetes size executors from the overhead, not local mode or standalone
    val sizedFromOverhead =
      CometExecIterator.isContainerSizedFromOverhead(conf.get("spark.master", ""))

    if (cometActive && sizedFromOverhead && !isExecutorMemoryOverheadSet(conf)) {
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
        s"$key=$value is deprecated and will be removed in a future major release. It does " +
          "not leave room in spark.memory.offHeap.size for native memory that Comet's memory " +
          "pools do not track, because Spark hands out the whole off-heap pool whatever it is " +
          s"set to. Size ${EXECUTOR_MEMORY_OVERHEAD.key} for that memory instead. " +
          s"${CometConf.TUNING_GUIDE}.")
    }
  }

  // Reads a deprecated alternative too, such as spark.comet.exec.shuffle.enabled, as a session
  // would.
  private def getBooleanConf(conf: SparkConf, entry: ConfigEntry[Boolean]): Boolean =
    (entry.key +: entry.alternatives)
      .find(conf.contains)
      .map(conf.getBoolean(_, entry.defaultValue.get))
      .getOrElse(entry.defaultValue.get)

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
      val _ = conf.set(listenerKey, listenerClass)
    } else {
      val currentListeners = listeners.split(",").map(_.trim)
      if (!currentListeners.contains(listenerClass)) {
        val newValue = s"$listeners,$listenerClass"
        logInfo(s"Setting $listenerKey=$newValue")
        val _ = conf.set(listenerKey, newValue)
      }
    }
  }

  def registerCometSessionExtension(conf: SparkConf): Unit = {
    val extensionKey = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
    val extensionClass = classOf[CometSparkSessionExtensions].getName
    val extensions = conf.get(extensionKey, "")
    if (extensions.isEmpty) {
      logInfo(s"Setting $extensionKey=$extensionClass")
      val _ = conf.set(extensionKey, extensionClass)
    } else {
      val currentExtensions = extensions.split(",").map(_.trim)
      if (!currentExtensions.contains(extensionClass)) {
        val newValue = s"$extensions,$extensionClass"
        logInfo(s"Setting $extensionKey=$newValue")
        val _ = conf.set(extensionKey, newValue)
      }
    }
  }
}

class CometExecutorPlugin extends ExecutorPlugin with Logging {

  private var context: PluginContext = _

  override def init(ctx: PluginContext, extraConf: ju.Map[String, String]): Unit = {
    logInfo("CometExecutorPlugin init")

    context = ctx
    CometExecutorPlugin.current.set(ctx)

    super.init(ctx, extraConf)
  }

  override def shutdown(): Unit = {
    logInfo("CometExecutorPlugin shutdown")

    // Unless a later plugin in the same JVM, which local mode starts for each SparkContext, has
    // already replaced it.
    CometExecutorPlugin.current.compareAndSet(context, null)

    NativeBase.releaseNative()

    super.shutdown()
  }

}

object CometExecutorPlugin {

  private val current = new AtomicReference[PluginContext]()

  /**
   * The context of the executor plugin running in this JVM, through which the executor sends its
   * memory usage samples to the driver plugin when the application writes an event log. None
   * without the Comet plugin or an event log, and after the executor has shut the plugin down.
   * The flag is read as the driver reads it, which ignores surrounding whitespace.
   */
  private[apache] def eventLogContext: Option[PluginContext] =
    Option(current.get()).filter(_.conf.get(EVENT_LOG_ENABLED))
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
