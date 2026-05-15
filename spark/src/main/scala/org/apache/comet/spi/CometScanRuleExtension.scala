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
 * registration order. The first extension whose [[matchesV1]] (or [[matchesV2]]) returns true AND
 * whose [[transformV1]] (or [[transformV2]]) returns `Some(_)` wins -- its returned plan replaces
 * the scan subtree. An extension whose `matches` is true but whose `transform` returns `None` is
 * treated as "declined this instance"; dispatch continues to the next matching extension. After
 * every matching extension has declined, core's built-in file-format dispatch handles the scan as
 * before.
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
   * Tree-level pre-pass run once per plan before per-scan dispatch begins. Default: identity.
   *
   * Use this to undo wrapper rewrites that a format's own Catalyst strategy applied. The
   * canonical example is Delta's `PreprocessTableWithDVs` strategy, which wraps every DV-bearing
   * Delta scan in a `Project(Filter(...))` subtree referencing a synthetic
   * `__delta_internal_is_row_deleted` column produced by Delta's own reader. Comet reads via its
   * own parquet path; without unwrapping that subtree, the synthetic column never gets produced
   * and the downstream `Filter` silently drops every row. The Delta contrib's `preTransform`
   * strips the wrapper so the clean scan reaches per-scan dispatch.
   *
   * '''V1-only.''' `preTransform` runs once for the whole plan and the rewritten tree is what
   * later `transformV1` calls see via their `plan` argument. `transformV2` does NOT receive a
   * plan-tree reference -- only the matched `BatchScanExec`. V2 contribs that need
   * wrapper-stripping must do that work inside `transformV2` against `scanExec.scan` /
   * `scanExec.children` directly.
   *
   * '''Disabled when scan conversion is off.''' `CometScanRule` skips the entire preTransform
   * fold when `spark.comet.scan.enabled=false`. A contrib's own wrappers (Delta's DV filter,
   * etc.) are load-bearing in that case; stripping them turns into a correctness bug.
   *
   * '''MUST NOT modify scans the extension does not recognise.''' Multiple registered extensions
   * are folded over the plan in registration order; an extension that rewrites scans outside its
   * format's domain will silently corrupt other formats' plans. `CometScanRule` logs a warning
   * when a `FileSourceScanExec` is replaced by an extension whose `matchesV1` returns false
   * against the original scan's relation -- contribs that trip this warning should narrow their
   * pattern match.
   *
   * '''State sharing.''' Shared state between this pre-pass and later `transformV1` calls is the
   * contrib's problem. The recommended pattern is to attach a Spark `TreeNodeTag` to nodes during
   * `preTransform` and read it during `transformV1`. Spark's tag mechanism is tree-immutable-safe
   * and survives plan transformations -- preferred over external mutable state which leaks across
   * plans.
   */
  def preTransform(plan: SparkPlan, session: SparkSession): SparkPlan = plan

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
   * Returning `None` means "I matched the scan shape but ultimately can't accelerate this
   * specific instance" -- `CometScanRule` then continues to the NEXT registered extension whose
   * `matchesV1` is true, falling back to core's built-in file-format dispatch only after every
   * matching extension has declined. Returning `Some(plan)` ends dispatch and replaces the scan
   * subtree with `plan`.
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
   *
   * Same semantics as `transformV1`: `None` falls through to the next matching extension;
   * `Some(plan)` ends dispatch. Note that unlike `transformV1`, this method does NOT receive a
   * plan-tree reference -- `preTransform` rewrites are not visible here. V2 contribs that need
   * wrapper-stripping must operate on `scanExec.scan` / `scanExec.children` directly.
   */
  def transformV2(scanExec: BatchScanExec, session: SparkSession): Option[SparkPlan] = None
}
