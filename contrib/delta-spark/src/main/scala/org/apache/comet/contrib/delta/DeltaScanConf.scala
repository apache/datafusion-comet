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
 * Configuration for the JVM-planned Delta Lake scan contrib. The support is experimental and
 * explicitly opt-in: having the contrib jar on the classpath is not enough, the scan must also be
 * enabled with `spark.comet.scan.delta.enabled`.
 *
 * This is the plain, user-facing flag for enabling native Delta scans, kept under the
 * `spark.comet.scan.delta` namespace. The experimental Rust-kernel-backed scan path is a separate
 * opt-in, defined by the kernel contrib's own `DeltaConf` under the
 * `spark.comet.scan.deltaNative` namespace; the two jars define distinct keys and are not
 * expected to coexist -- see the ownership contract in `CometScanContrib`. Entry construction
 * self-registers with `CometConf.allConfs` via the `ConfigBuilder` machinery.
 */
object DeltaScanConf {

  // Matches the kernel contrib's category so both group onto the same generated-docs table.
  private[delta] val CATEGORY = "delta"

  val COMET_DELTA_NATIVE_ENABLED: ConfigEntry[Boolean] =
    ConfigBuilder("spark.comet.scan.delta.enabled")
      .category(CATEGORY)
      .doc(
        "Whether to enable native Delta table scans. When enabled, DSv1 Delta table reads " +
          "planned by delta-spark are executed through Comet's native Parquet scan, " +
          "inheriting row-group pruning, page-index pruning, and filter pushdown, with " +
          "deletion vectors applied inside the scan. Experimental: defaults to false, so " +
          "adding the contrib jar does not by itself change how any query is read.")
      .booleanConf
      .createWithDefault(false)

  val COMET_DELTA_MAX_DELETED_ROWS_PER_FILE: ConfigEntry[Long] =
    ConfigBuilder("spark.comet.scan.delta.dv.maxDeletedRowsPerFile")
      .category(CATEGORY)
      .doc(
        "Upper bound on a single file's deletion-vector cardinality (deleted row count) the " +
          "native Delta scan will claim. Applying a deletion vector expands it into per-row " +
          "selectors that are retained in memory for the file's scan; this bound is a " +
          "deliberately pessimistic planning-time proxy for that retained memory (deletion " +
          "vector cardinality, not the exact selector count), so a large but contiguous " +
          "deletion is declined the same as a large alternating one. Scans whose deletion " +
          "vectors exceed this bound for any file fall back to Spark's reader.")
      .longConf
      .createWithDefault(1000000)

  /**
   * Every entry defined here, in docs order. Referencing this forces object initialisation, which
   * registers the entries -- see `CometConfigProvider`.
   */
  def all: Seq[ConfigEntry[_]] =
    Seq(COMET_DELTA_MAX_DELETED_ROWS_PER_FILE, COMET_DELTA_NATIVE_ENABLED)

  def scanEnabled: Boolean = COMET_DELTA_NATIVE_ENABLED.get()
}
