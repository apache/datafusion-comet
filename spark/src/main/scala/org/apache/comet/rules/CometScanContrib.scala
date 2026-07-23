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

package org.apache.comet.rules

import java.util.ServiceLoader

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.comet.serde.CometOperatorSerde

/**
 * Format-agnostic hook that lets an optional, out-of-tree contrib (Delta, Lance, ...) claim a
 * scan before Comet's built-in scan handling runs. This is the scan-rule counterpart to
 * [[org.apache.spark.sql.comet.PlanDataInjector]]: core holds no compile-time reference to any
 * contrib and names none of them -- implementations are discovered at runtime via the JDK
 * [[ServiceLoader]], so default builds (which ship no service file) see an empty registry and
 * zero contrib surface.
 *
 * A contrib ships:
 *   - an implementation of this trait (e.g. `contrib/delta/.../DeltaScanRuleContrib`), and
 *   - a `META-INF/services/org.apache.comet.rules.CometScanContrib` resource naming it.
 *
 * Both hooks default to `None` so a contrib overrides only the scan kind(s) it handles.
 */
trait CometScanContrib {

  /**
   * V1 (`FileSourceScanExec`) hook. Return `Some(plan)` to claim the scan -- either a transformed
   * plan node or an explicit fallback the contrib produced via `withFallbackReason`. Return
   * `None` to pass, letting Comet's generic V1 handling proceed. A claiming contrib is
   * responsible for its own metadata-column handling (the generic guard in [[CometScanRule]] runs
   * only on the pass path).
   */
  def tryTransformV1(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] = None

  /**
   * V2 (`BatchScanExec`) hook. Return `Some(plan)` to claim the scan, `None` to pass.
   */
  def tryTransformV2(scanExec: BatchScanExec): Option[SparkPlan] = None
}

object CometScanContrib extends Logging {

  // Discovered contrib scan handlers. Mirrors `PlanDataInjector.injectors`: the standard JDK
  // ServiceLoader forces the provider iterator here so a misbuilt contrib jar (malformed service
  // file, or a listed provider that can't be instantiated) surfaces as a warning rather than
  // taking down the planner. Default builds carry no service file, so this is empty and there is
  // no contrib surface at runtime. There are no built-in (in-core) contribs today; the format
  // built-ins (Parquet/Iceberg) keep their existing paths in CometScanRule.
  private lazy val contribs: Seq[CometScanContrib] =
    try {
      ServiceLoader.load(classOf[CometScanContrib], getClass.getClassLoader).asScala.toSeq
    } catch {
      // NonFatal covers ServiceConfigurationError (not a LinkageError).
      case NonFatal(e) =>
        logWarning("Failed to load contrib CometScanContrib services", e)
        Seq.empty
    }

  /**
   * Offer a scan to each registered contrib in turn and return the first claim.
   *
   * A contrib that throws is treated as having declined: the failure is logged and the scan
   * continues down Comet's built-in path (ultimately vanilla Spark). A contrib's planning work is
   * speculative and can fail for reasons entirely outside the query -- an unreachable object
   * store, a metadata format newer than the contrib understands, a version-skewed reflective
   * lookup -- and none of those should turn a runnable query into a failed one. Logging (rather
   * than swallowing silently) keeps an unexpectedly-declining contrib diagnosable. `NonFatal`
   * deliberately lets `LinkageError`/`OOM`-class failures through.
   */
  private def firstClaim(hook: CometScanContrib => Option[SparkPlan]): Option[SparkPlan] =
    contribs.view.flatMap { contrib =>
      try hook(contrib)
      catch {
        case NonFatal(e) =>
          logWarning(
            s"Contrib scan handler ${contrib.getClass.getName} threw while examining a scan; " +
              "declining it and continuing with Comet's built-in handling",
            e)
          None
      }
    }.headOption

  /** First contrib to claim the V1 scan, or `None` if none does (including the default build). */
  def tryTransformV1(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] =
    firstClaim(_.tryTransformV1(plan, session, scanExec, relation))

  /** First contrib to claim the V2 scan, or `None` if none does (including the default build). */
  def tryTransformV2(scanExec: BatchScanExec): Option[SparkPlan] =
    firstClaim(_.tryTransformV2(scanExec))
}

/**
 * Marker mixed into a contrib's planning-time scan node so [[CometExecRule]] can route it to the
 * contrib's own serde handler without a compile-time dependency on the contrib. The node carries
 * its handler directly (rather than core resolving it reflectively), so the match in
 * `CometExecRule` is a plain type test:
 * {{{case marker: CometContribScanMarker => convertToComet(marker, marker.scanHandler) ...}}}
 *
 * Type-based dispatch survives node copies / AQE re-planning (a `TreeNodeTag` would not), and a
 * contrib marker typically wraps the original, link-bearing scan as the produced exec's
 * `originalPlan` so no `logicalLink` workaround is needed.
 *
 * Extends `SparkPlan` (rather than using a `this: SparkPlan =>` self-type) so that a value typed
 * as the marker is usable directly as a `SparkPlan` -- e.g. `convertToComet(marker, ...)` in
 * [[CometExecRule]]. A contrib mixes this into its scan-exec node, which already extends
 * `SparkPlan`, so linearization is consistent.
 */
trait CometContribScanMarker extends SparkPlan {

  /** The serde handler that converts this marker into its native Comet exec. */
  def scanHandler: CometOperatorSerde[_ <: SparkPlan]
}
