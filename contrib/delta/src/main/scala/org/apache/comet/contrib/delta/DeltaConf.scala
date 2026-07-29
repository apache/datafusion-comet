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

package org.apache.comet.contrib.delta

import org.apache.comet.{ConfigBuilder, ConfigEntry}

/**
 * Contrib-local config entries for the Delta integration. Lives in the contrib's package rather
 * than in core's `CometConf` so PR1 stays format-agnostic. Side-effect of object construction is
 * registering the entries with `CometConf.allConfs` (via the `ConfigBuilder` machinery), so they
 * show up in the generated user-guide docs and `SQLConf` resolution works the usual way.
 */
object DeltaConf {

  // CometConf.register asserts every config has a non-empty category — used for grouping
  // entries in the generated user-guide docs. "scan" matches the existing core
  // CATEGORY_SCAN string (CATEGORY_* constants in CometConf are `private val` so contribs
  // can't reference the symbol; the assertion only checks `nonEmpty`).
  private val CATEGORY = "scan"

  val COMET_DELTA_NATIVE_ENABLED: ConfigEntry[Boolean] =
    ConfigBuilder("spark.comet.scan.deltaNative.enabled")
      .category(CATEGORY)
      .doc(
        "Whether to enable native Delta table scans via delta-kernel-rs. When enabled, " +
          "Delta tables are read directly through Comet's tuned ParquetSource + " +
          "DV-filter wrapper, bypassing Spark's Delta reader for better performance.")
      .booleanConf
      .createWithDefault(true)

  val COMET_DELTA_FALLBACK_ON_UNSUPPORTED_FEATURE: ConfigEntry[Boolean] =
    ConfigBuilder("spark.comet.scan.deltaNative.fallbackOnUnsupportedFeature")
      .category(CATEGORY)
      .doc(
        "When true (default), the Delta contrib falls back to Spark's Delta reader on " +
          "any Delta protocol feature it doesn't yet support. When false, the contrib " +
          "raises an error instead -- useful for tests that want to assert the native " +
          "path is reachable for a particular query.")
      .booleanConf
      .createWithDefault(true)

  val COMET_DELTA_DATA_FILE_CONCURRENCY_LIMIT: ConfigEntry[Int] =
    ConfigBuilder("spark.comet.scan.deltaNative.dataFileConcurrencyLimit")
      .category(CATEGORY)
      .doc(
        "Per-Spark-task concurrency when reading Delta data files. Higher values " +
          "improve throughput on tables with many small files at the cost of memory. " +
          "Values between 2 and 8 are typical.")
      .intConf
      .checkValue(v => v > 0, "Data file concurrency limit must be positive")
      .createWithDefault(1)

  val COMET_DELTA_CDF_MAX_PARTITIONS: ConfigEntry[Int] =
    ConfigBuilder("spark.comet.delta.cdf.maxPartitions")
      .category(CATEGORY)
      .doc(
        "Maximum number of Spark partitions a Change Data Feed (readChangeFeed) read is split " +
          "into. The inclusive version range is chunked into up to this many contiguous " +
          "sub-ranges, each read by an independent native delta-kernel TableChanges call, so a " +
          "multi-version CDF read parallelizes across tasks instead of reading the whole range " +
          "on one task. Capped by the number of commits in the range.")
      .intConf
      .checkValue(v => v > 0, "CDF max partitions must be positive")
      .createWithDefault(8)
}
