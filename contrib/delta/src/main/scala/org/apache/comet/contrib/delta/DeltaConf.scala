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
 *
 * Every entry here uses the single `spark.comet.scan.deltaNative.*` prefix -- including entries
 * for sub-features, which nest under it (e.g. `...deltaNative.cdf.maxPartitions`) rather than
 * starting a second top-level namespace. One prefix keeps the whole contrib discoverable from a
 * single `spark.comet.scan.deltaNative` search and groups it in the generated docs.
 */
object DeltaConf {

  /**
   * Every entry defined here, in the order they should appear in the generated docs. Referencing
   * this forces the object to initialise, which is what registers the entries -- see
   * [[org.apache.comet.CometConfigProvider]]. Later parts of the Delta series append their own
   * entries here as they add them.
   */
  def all: Seq[ConfigEntry[_]] = Seq(
    COMET_DELTA_NATIVE_ENABLED,
    COMET_DELTA_FALLBACK_ON_UNSUPPORTED_FEATURE,
    COMET_DELTA_DATA_FILE_CONCURRENCY_LIMIT,
    COMET_DELTA_CDF_MAX_PARTITIONS)

  // CometConf.register asserts every config has a non-empty category -- used for grouping
  // entries in the generated user-guide docs. Deliberately a contrib-specific category rather
  // than reusing a core one ("scan"): `GenerateDocs` fills a page's CONFIG_TABLE markers by
  // category, so sharing core's category would emit these entries into core's `configs.md`,
  // which must describe exactly what a default build ships. `DeltaConfigProvider` points
  // `GenerateDocs` at the contrib's own page for this category.
  private[delta] val CATEGORY = "delta"

  val COMET_DELTA_NATIVE_ENABLED: ConfigEntry[Boolean] =
    ConfigBuilder("spark.comet.scan.deltaNative.enabled")
      .category(CATEGORY)
      .doc(
        "Whether to enable native Delta table scans via delta-kernel-rs. When enabled, " +
          "Delta tables are read directly through Comet's tuned ParquetSource + " +
          "DV-filter wrapper, bypassing Spark's Delta reader for better performance. " +
          "Experimental: defaults to false, so enabling the contrib build does not by " +
          "itself change how any query is read.")
      .booleanConf
      // Off by default while the native Delta path is experimental. The `contrib-delta` build
      // gate already keeps this code out of default builds, but a user who opts into the build
      // should still opt in a second time before their reads change engine.
      .createWithDefault(false)

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
        "Per-Spark-task concurrency when reading Delta data files. The default of 1 reads a " +
          "task's files one at a time, matching Spark's own behaviour and adding no memory " +
          "over it. Raising it improves throughput on tables with many small files at the " +
          "cost of proportionally more in-flight buffers; 2 to 8 is a typical tuned range.")
      .intConf
      .checkValue(v => v > 0, "Data file concurrency limit must be positive")
      .createWithDefault(1)

  // Added here, with the Change Data Feed support it configures, rather than alongside the scan
  // settings above -- a config should not appear before the code that reads it. Nested under the
  // shared `spark.comet.scan.deltaNative.*` prefix (see the note on this object) rather than
  // opening a separate `spark.comet.delta.cdf.*` namespace.
  val COMET_DELTA_CDF_MAX_PARTITIONS: ConfigEntry[Int] =
    ConfigBuilder("spark.comet.scan.deltaNative.cdf.maxPartitions")
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
