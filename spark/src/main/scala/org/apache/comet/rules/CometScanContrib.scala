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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.comet.{CometConf, ContribServices}
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
 *
 * ==Ownership contract==
 *
 * An implementation '''MUST''' return `None` for any scan it does not own. Registered contribs
 * are offered a scan one at a time and the '''first''' claim wins, so a contrib that claims a
 * scan belonging to another format does not merely mis-handle that scan -- it hides it from the
 * contrib that could have read it, and the outcome depends on ServiceLoader ordering, which is
 * not specified. Decide ownership from something definitive about the scan itself (the relation's
 * `fileFormat` class, the table's provider, a catalog type) rather than from a heuristic such as
 * a path or table-name pattern, which another format may also match.
 *
 * "Own but cannot handle" is a distinct case from "not mine", and both are expressible: to
 * decline a scan it owns, a contrib returns `Some(withFallbackReason(scanExec, "..."))` --
 * claiming the scan and terminating it with a diagnosable reason -- rather than returning `None`
 * and letting Comet's built-in handling attempt a format it does not understand.
 */
trait CometScanContrib {

  /**
   * V1 (`FileSourceScanExec`) hook. Return `Some(plan)` to claim the scan -- either a transformed
   * plan node or an explicit fallback the contrib produced via `withFallbackReason`. Return
   * `None` to pass, letting Comet's generic V1 handling proceed; see the ownership contract
   * above. A claiming contrib is responsible for its own metadata-column handling (the generic
   * guard in [[CometScanRule]] runs only on the pass path).
   */
  def tryTransformV1(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] = None

  /**
   * V2 (`BatchScanExec`) hook. Return `Some(plan)` to claim the scan, `None` to pass; see the
   * ownership contract above.
   */
  def tryTransformV2(scanExec: BatchScanExec): Option[SparkPlan] = None
}

object CometScanContrib extends Logging {

  // Discovered contrib scan handlers, via the thread context ClassLoader (see
  // `ClassLoaders.contextOrDefault`): Comet usually sits on the driver's extraClassPath while a
  // contrib arrives through `--jars`, i.e. on a CHILD loader, so discovering from Comet's own
  // defining loader would silently miss it. Mirrors `PlanDataInjector.injectors`: the standard JDK
  // ServiceLoader forces the provider iterator here so a misbuilt contrib jar (malformed service
  // file, or a listed provider that can't be instantiated) surfaces as a warning rather than
  // taking down the planner. Default builds carry no service file, so this is empty and there is
  // no contrib surface at runtime. There are no built-in (in-core) contribs today; the format
  // built-ins (Parquet/Iceberg) keep their existing paths in CometScanRule.
  private[comet] lazy val contribs: Seq[CometScanContrib] =
    ContribServices.load(classOf[CometScanContrib], getClass.getClassLoader)

  /**
   * Discover implementations visible to `loader`. Split out from the `contribs` val so tests can
   * drive real discovery against their own classloader without forcing (and permanently caching)
   * the production registry.
   */
  private[comet] def loadContribs(loader: ClassLoader): Seq[CometScanContrib] =
    ContribServices.loadFrom(classOf[CometScanContrib], loader)

  /**
   * Offer a scan to each registered contrib in turn and return the first claim.
   *
   * Contribs are consulted in `ServiceLoader` order and the first `Some` wins; later contribs are
   * not consulted for that scan. Two contribs claiming the same scan is a contract violation, not
   * a supported configuration -- see the ownership contract on [[CometScanContrib]]. Core cannot
   * detect that for free, because a "claim" is opaque: the only way to learn that a second
   * contrib would also have claimed is to ask it, and not asking is the whole point of stopping
   * at the first claim. Setting `spark.comet.scan.contrib.detectConflicts.enabled` trades that
   * saving for visibility -- every contrib is offered the scan and a violation is logged -- while
   * still using the first claim, so behaviour does not change.
   *
   * A contrib that throws is treated as having declined: the failure is logged and the scan
   * continues down Comet's built-in path (ultimately vanilla Spark). A contrib's planning work is
   * speculative and can fail for reasons entirely outside the query -- an unreachable object
   * store, a metadata format newer than the contrib understands, a version-skewed reflective
   * lookup -- and none of those should turn a runnable query into a failed one. Logging (rather
   * than swallowing silently) keeps an unexpectedly-declining contrib diagnosable.
   *
   * `NonFatal` does not match `LinkageError` (`NoSuchMethodError`, `NoClassDefFoundError`, ...),
   * so it is caught separately and contained the same way: a contrib jar built against internals
   * Comet has since moved or removed is a classpath/version skew, not a JVM-corrupting failure,
   * and must not fail a query Spark could otherwise run. Genuinely fatal conditions --
   * `OutOfMemoryError` and the like -- are neither `NonFatal` nor `LinkageError` and always
   * propagate; this is a narrow, deliberate widening for one specific `Error` subtype, not a
   * blanket `catch (Throwable)`.
   */
  private def firstClaim(hook: CometScanContrib => Option[SparkPlan]): Option[SparkPlan] =
    firstClaimFrom(contribs)(hook)

  /**
   * `firstClaim` over an explicit candidate list. Exists so `CometScanContribSuite` can drive the
   * claim / decline / throw-is-a-decline semantics against stub contribs: the production registry
   * is loaded from `getClass.getClassLoader`, which a test cannot add a service file to without
   * polluting every other suite in the JVM. Tests supply contribs discovered through their own
   * `URLClassLoader`, so both real `ServiceLoader` discovery and this dispatch are exercised.
   */
  private[comet] def firstClaimFrom(
      candidates: Seq[CometScanContrib],
      detectConflicts: => Boolean = CometConf.COMET_SCAN_CONTRIB_DETECT_CONFLICTS.get())(
      hook: CometScanContrib => Option[SparkPlan]): Option[SparkPlan] = {

    def offer(contrib: CometScanContrib): Option[SparkPlan] =
      try hook(contrib)
      catch {
        case NonFatal(e) =>
          logWarning(
            s"Contrib scan handler ${contrib.getClass.getName} threw while examining a scan; " +
              "declining it and continuing with Comet's built-in handling",
            e)
          None
        case e: LinkageError =>
          // A version-skewed contrib jar (compiled against a Comet internal that has since
          // moved, been renamed, or been removed) surfaces as NoSuchMethodError,
          // NoClassDefFoundError, or a sibling LinkageError -- a classpath mismatch, not a
          // query-specific failure, and not the JVM corruption OutOfMemoryError/StackOverflowError
          // signal. Contained the same way a NonFatal decline is: logged and treated as "this
          // contrib does not claim this scan" so a stale contrib jar cannot fail a query Spark
          // could otherwise run.
          logWarning(
            s"Contrib scan handler ${contrib.getClass.getName} failed with " +
              s"${e.getClass.getName}, indicating it was built against a different version of " +
              "Comet's internals than is on the classpath now; declining it and continuing with " +
              "Comet's built-in handling",
            e)
          None
      }

    // Short-circuit before reading the config: a default build registers nothing, and this is on
    // the hot path for every scan in every plan.
    if (candidates.isEmpty) {
      None
    } else if (!detectConflicts) {
      // `view` keeps this lazy, so contribs after the first claim are never consulted.
      candidates.view.flatMap(offer).headOption
    } else {
      // Diagnostic mode: ask everyone so a contract violation is visible rather than silent. This
      // makes every contrib do its full planning work even after one has claimed, which is why it
      // is off by default.
      val claims = candidates.map(contrib => contrib -> offer(contrib)).collect {
        case (contrib, Some(plan)) => contrib -> plan
      }
      if (claims.size > 1) {
        logWarning(
          "More than one contrib claimed the same scan, which violates the ownership contract " +
            "on CometScanContrib: " + claims.map(_._1.getClass.getName).mkString(", ") +
            s". Using the first (${claims.head._1.getClass.getName}); the others are ignored. " +
            "Which one wins depends on ServiceLoader ordering, which is not specified, so this " +
            "is a misconfiguration to fix rather than a preference to rely on.")
      }
      claims.headOption.map(_._2)
    }
  }

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
