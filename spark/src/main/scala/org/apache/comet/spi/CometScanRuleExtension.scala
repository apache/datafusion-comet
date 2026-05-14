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

package org.apache.comet.spi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/**
 * SPI hook that lets a contrib extension intercept scan transformation in `CometScanRule`.
 * Contribs typically use this to recognise a specific table format (Delta, Hudi, etc.) and route
 * it through a contrib-specific native execution path.
 *
 * `CometScanRule` discovers implementations via `CometExtensionRegistry.scanExtensions`
 * (ServiceLoader-backed) and offers each candidate scan to every registered extension in
 * registration order. The first extension whose [[matches]] returns `true` wins -- its
 * [[transformV1]] / [[transformV2]] is called and the returned plan replaces the scan branch. If
 * no extension matches, the core's existing file-format dispatch handles the scan as before.
 *
 * Contribs are discovered via the standard Java ServiceLoader. Each contrib JAR ships a
 * `META-INF/services/org.apache.comet.spi.CometScanRuleExtension` resource listing its extension
 * class.
 *
 * Implementations MUST be safe to invoke from `CometScanRule`'s `apply` method -- specifically:
 * pure, stateless, side-effect-free with respect to the plan tree (any state needed should be
 * derived from `scanExec` / `relation` / the surrounding plan). The registry caches instances
 * across plans, so per-plan state on the implementation will leak between queries.
 */
trait CometScanRuleExtension {

  /** Human-readable name shown in logs and error messages. Should be unique per extension. */
  def name: String

  /**
   * Whether this extension wants to handle the given V1 scan. Implementations should make a cheap
   * decision here (typically file-format class-name probe) so non-matching paths add no per-scan
   * overhead.
   *
   * Default returns false; override `matchesV1` and `transformV1` for V1 scan support.
   */
  def matchesV1(relation: HadoopFsRelation): Boolean = false

  /**
   * Transform the matched V1 scan. Called only when `matchesV1` returned true.
   *
   * Returning `None` means "I matched but ultimately can't accelerate this one" -- the core falls
   * back to its existing file-format dispatch. Returning `Some(plan)` replaces the scan subtree.
   */
  def transformV1(
      plan: SparkPlan,
      scanExec: FileSourceScanExec,
      session: SparkSession): Option[SparkPlan] = None

  /**
   * Whether this extension wants to handle the given V2 batch scan. See `matchesV1`.
   *
   * Default returns false; override `matchesV2` and `transformV2` for V2 scan support.
   */
  def matchesV2(scanExec: BatchScanExec): Boolean = false

  /**
   * Transform the matched V2 scan. Called only when `matchesV2` returned true.
   */
  def transformV2(scanExec: BatchScanExec, session: SparkSession): Option[SparkPlan] = None
}
