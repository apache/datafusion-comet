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

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{EXECUTOR_MEMORY, EXECUTOR_MEMORY_OVERHEAD}
import org.apache.spark.resource.ResourceProfile

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Comet driver plugin. This class is loaded by Spark's plugin framework. It will be instantiated
 * on driver side only. It will update the SparkConf with the extra configuration provided by
 * Comet, e.g., Comet memory configurations.
 *
 * Note that `SparkContext.conf` is spark package only. So this plugin must be in spark package.
 * Although `SparkContext.getConf` is public, it returns a copy of the SparkConf, so it cannot
 * actually change Spark configs at runtime.
 *
 * To enable this plugin, set the config "spark.plugins" to `org.apache.spark.CometPlugin`.
 */
class CometDriverPlugin extends DriverPlugin with Logging {
  import CometDriverPlugin._

  override def init(sc: SparkContext, pluginContext: PluginContext): ju.Map[String, String] = {
    logInfo("CometDriverPlugin init")

    if (shouldOverrideMemoryConf(sc.getConf)) {
      val execMemOverhead = if (sc.getConf.contains(EXECUTOR_MEMORY_OVERHEAD.key)) {
        sc.getConf.getSizeAsMb(EXECUTOR_MEMORY_OVERHEAD.key)
      } else {
        // By default, executorMemory * spark.executor.memoryOverheadFactor, with minimum of 384MB
        val executorMemory = sc.getConf.getSizeAsMb(EXECUTOR_MEMORY.key)
        val memoryOverheadFactor =
          sc.getConf.getDouble(
            EXECUTOR_MEMORY_OVERHEAD_FACTOR,
            EXECUTOR_MEMORY_OVERHEAD_FACTOR_DEFAULT)

        Math.max(
          (executorMemory * memoryOverheadFactor).toInt,
          ResourceProfile.MEMORY_OVERHEAD_MIN_MIB)
      }

      val cometMemOverhead = CometSparkSessionExtensions.getCometMemoryOverheadInMiB(sc.getConf)
      sc.conf.set(EXECUTOR_MEMORY_OVERHEAD.key, s"${execMemOverhead + cometMemOverhead}M")
      val newExecMemOverhead = sc.getConf.getSizeAsMb(EXECUTOR_MEMORY_OVERHEAD.key)

      logInfo(s"""
         Overriding Spark memory configuration for Comet:
           - Spark executor memory overhead: ${execMemOverhead}MB
           - Comet memory overhead: ${cometMemOverhead}MB
           - Updated Spark executor memory overhead: ${newExecMemOverhead}MB
         """)
    }

    Collections.emptyMap[String, String]
  }

  override def receive(message: Any): AnyRef = super.receive(message)

  override def shutdown(): Unit = {
    logInfo("CometDriverPlugin shutdown")

    super.shutdown()
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit =
    super.registerMetrics(appId, pluginContext)

  /**
   * Whether we should override Spark memory configuration for Comet. This only returns true when
   * Comet native execution is enabled and/or Comet shuffle is enabled
   */
  private def shouldOverrideMemoryConf(conf: SparkConf): Boolean = {
    conf.getBoolean(CometConf.COMET_ENABLED.key, true) && (
      conf.getBoolean(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, false) ||
        conf.getBoolean(CometConf.COMET_EXEC_ENABLED.key, false)
    )
  }
}

object CometDriverPlugin {
  // `org.apache.spark.internal.config.EXECUTOR_MEMORY_OVERHEAD_FACTOR` was added since Spark 3.3.0
  val EXECUTOR_MEMORY_OVERHEAD_FACTOR = "spark.executor.memoryOverheadFactor"
  val EXECUTOR_MEMORY_OVERHEAD_FACTOR_DEFAULT = 0.1
}

/**
 * The Comet plugin for Spark. To enable this plugin, set the config "spark.plugins" to
 * `org.apache.spark.CometPlugin`
 */
class CometPlugin extends SparkPlugin with Logging {
  override def driverPlugin(): DriverPlugin = new CometDriverPlugin

  override def executorPlugin(): ExecutorPlugin = null
}
