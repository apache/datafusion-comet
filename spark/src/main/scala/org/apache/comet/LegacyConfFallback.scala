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

package org.apache.comet

import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.shims.ShimLegacyConfFallback

/**
 * Curated set of Spark `spark.sql.legacy.*` configs whose behavior is NOT tied to a specific
 * Comet-supported expression (built-in functions with a legacy dependency are handled
 * per-expression via [[org.apache.comet.serde.CodegenDispatchFallback]] or a native passthrough
 * in the serde). The keys in this list are consumed by analyzer/optimizer rules, data-source
 * readers/writers, or type-system utilities, and Comet's native execution does not replicate
 * their legacy semantics.
 *
 * When [[CometConf.COMET_LEGACY_CONF_FALLBACK_ENABLED]] is true (default), Comet disables itself
 * for the session if any of these keys is set to its non-default value, so Spark's own execution
 * path is used instead. Users can set `spark.comet.legacyConfFallback.enabled=false` to override
 * the fallback and keep Comet enabled (Spark compatibility is not guaranteed in that case).
 *
 * The map of legacy key -> Spark 4 default value comes from [[ShimLegacyConfFallback]]. The 4.x
 * shim derives defaults from live [[org.apache.spark.internal.config.ConfigEntry]] references so
 * additions/removals in Spark 4 are picked up automatically; the 3.x shim hardcodes the same
 * defaults because several of these keys do not exist as ConfigEntry instances in Spark 3.
 */
private[comet] object LegacyConfFallback extends ShimLegacyConfFallback {

  /**
   * Map of legacy config key -> case-insensitive Spark default value. A config triggers the
   * fallback when it is present in the session conf AND its value is not equal (case-insensitive)
   * to the default recorded here.
   */
  val executionAffectingDefaults: Map[String, String] = legacyConfDefaults

  /** Keys in [[executionAffectingDefaults]] that are set to a non-default value on `conf`. */
  def triggeredConfigs(conf: SQLConf): Iterable[String] = {
    executionAffectingDefaults.iterator.collect {
      case (key, safeDefault)
          if conf.contains(key) &&
            !conf.getConfString(key).equalsIgnoreCase(safeDefault) =>
        key
    }.toSeq
  }
}
