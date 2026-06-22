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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{FileSourceScanExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

import org.apache.comet.serde.CometOperatorSerde

/**
 * Reflection-based bridge to the optional `contrib/delta/` integration.
 *
 * On default builds the contrib classes don't exist on the classpath, so the reflective class
 * lookups fail and every method here returns the "not handled" sentinel. On builds compiled with
 * `-Pcontrib-delta` (Maven) + `--features contrib-delta` (Cargo), the contrib classes are present
 * and the lookups resolve, dispatching the call into the contrib helpers.
 *
 * Keeping this bridge as one small file in core lets the Delta detection block in `CometScanRule`
 * and the serde dispatch in `CometExecRule` stay ~10 lines each -- exactly the shape Parth's
 * review on #4339 asked for.
 *
 * No `SPI`, no `ServiceLoader`, no registry: the contrib provides its own static helper objects
 * with stable names; this bridge just calls them.
 */
object DeltaIntegration extends org.apache.spark.internal.Logging {

  // Scala compiles `object Foo` into BOTH `Foo.class` (a static-forwarder class) AND
  // `Foo$.class` (the actual module class). Only the latter has the `MODULE$` singleton
  // field that the reflection bridge dereferences. Looking up the unqualified name
  // returns the forwarder, where `getField("MODULE$")` throws -- and the surrounding
  // try/catch silently turns that into `None`, making every Delta scan fall through to
  // Spark's reader. (This bug shipped silently until the test suite caught it; the Delta
  // regression suite was passing because Delta's own tests don't depend on Comet
  // engaging.) The trailing `$` selects the module class explicitly.
  private val ScanRuleClass = "org.apache.comet.contrib.delta.DeltaScanRule$"
  private val SerdeClass = "org.apache.comet.contrib.delta.CometDeltaNativeScan$"

  // Fully-qualified name of the contrib's planning-time marker (CometDeltaScanMarker), matched by
  // class name so core carries no compile-time dependency on the contrib. CometExecRule routes any
  // node of this type to the Delta serde handler. Type-based dispatch survives node copies / AQE
  // re-planning (a TreeNodeTag would not), and the marker wraps the original, link-bearing scan as
  // the produced exec's originalPlan -- so no logicalLink workaround is needed (mirrors Iceberg's
  // CometBatchScanExec + nativeIcebergScanMetadata field).
  private val MarkerClass = "org.apache.spark.sql.comet.CometDeltaScanMarker"

  /** True when `plan` is the Delta contrib's scan marker. */
  def isDeltaScanMarker(plan: SparkPlan): Boolean = plan.getClass.getName == MarkerClass

  // Lazy class lookups -- single reflection cost per JVM, cached either as the
  // class handle or as the empty option if the contrib wasn't bundled.
  @volatile private var scanRuleLookup: Option[Option[Class[AnyRef]]] = None
  @volatile private var serdeLookup: Option[Option[Class[AnyRef]]] = None

  private def scanRuleCls: Option[Class[AnyRef]] =
    scanRuleLookup.getOrElse {
      val cls =
        try {
          // scalastyle:off classforname
          Some(Class.forName(ScanRuleClass).asInstanceOf[Class[AnyRef]])
          // scalastyle:on classforname
        } catch { case _: ClassNotFoundException => None }
      scanRuleLookup = Some(cls)
      cls
    }

  private def serdeCls: Option[Class[AnyRef]] =
    serdeLookup.getOrElse {
      val cls =
        try {
          // scalastyle:off classforname
          Some(Class.forName(SerdeClass).asInstanceOf[Class[AnyRef]])
          // scalastyle:on classforname
        } catch { case _: ClassNotFoundException => None }
      serdeLookup = Some(cls)
      cls
    }

  /** True when the Delta contrib was bundled into this build. */
  def isAvailable: Boolean = scanRuleCls.isDefined

  /**
   * Delegate the V1 scan transform to the Delta contrib when both (a) the contrib is on the
   * classpath, AND (b) the relation's file format is `DeltaParquetFileFormat`.
   *
   * Returns `Some(plan)` if the contrib handled the scan (either with a transformed
   * `CometScanExec` marker or by explicitly declining via the `withFallbackReason` path); `None`
   * to indicate "not a Delta scan, proceed with the vanilla CometScanRule path".
   */
  // Cached reflective binding: resolved once per JVM. The contrib's
  // `transformV1IfDelta` is invoked for every V1 scan in every plan, even
  // non-Delta ones; resolving the Method on each call would be a per-scan
  // reflection round-trip just to find we don't apply.
  @volatile private var transformV1IfDeltaBindingCache
      : Option[Option[(AnyRef, java.lang.reflect.Method)]] = None

  private def transformV1IfDeltaBinding: Option[(AnyRef, java.lang.reflect.Method)] =
    transformV1IfDeltaBindingCache.getOrElse {
      val binding = scanRuleCls.flatMap { cls =>
        try {
          val module = cls.getField("MODULE$").get(null)
          val m = cls.getMethod(
            "transformV1IfDelta",
            classOf[SparkPlan],
            classOf[SparkSession],
            classOf[FileSourceScanExec],
            classOf[HadoopFsRelation])
          Some((module, m))
        } catch {
          // Only swallow true reflection-binding failures (signature/access drift).
          // Other exceptions (linkage errors, init failures) should surface.
          case _: NoSuchMethodException | _: NoSuchFieldException | _: IllegalAccessException =>
            None
        }
      }
      transformV1IfDeltaBindingCache = Some(binding)
      binding
    }

  def transformV1IfDelta(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] = {
    transformV1IfDeltaBinding.flatMap { case (module, m) =>
      try {
        Option(m.invoke(module, plan, session, scanExec, relation))
          .map(_.asInstanceOf[Option[SparkPlan]])
          .flatten
      } catch {
        // Reflection invocation errors (signature drift / access) -> silent None.
        case _: IllegalAccessException | _: IllegalArgumentException =>
          None
        // The contrib's transform threw a real exception -- DON'T silently swallow;
        // surface it as a log warning + decline. Without this, kernel-rs IO errors,
        // CCE on a Delta version bump, NPE in the CM-id translator etc. would
        // silently fall back to vanilla and the user would never know Comet declined.
        case e: java.lang.reflect.InvocationTargetException =>
          logWarning(
            "CometDeltaNativeScan.transformV1IfDelta threw, declining to vanilla Delta " +
              "for this scan",
            Option(e.getCause).getOrElse(e))
          None
      }
    }
  }

  /**
   * True when `relation` is Delta's Change Data Feed relation (`DeltaCDFRelation`, produced by a
   * `readChangeFeed` read). A pure class-name check -- no contrib dependency and no reflection
   * cost
   * -- so `CometExecRule` can cheaply gate before invoking [[transformCdf]].
   */
  def isCdfRelation(relation: Any): Boolean =
    relation != null && relation.getClass.getName.contains("DeltaCDFRelation")

  // Cached reflective binding to the contrib's `CometDeltaNativeScan.convertCdf`, resolved once per
  // JVM (mirrors `transformV1IfDeltaBinding`).
  @volatile private var convertCdfBindingCache
      : Option[Option[(AnyRef, java.lang.reflect.Method)]] = None

  private def convertCdfBinding: Option[(AnyRef, java.lang.reflect.Method)] =
    convertCdfBindingCache.getOrElse {
      val binding = serdeCls.flatMap { cls =>
        try {
          val module = cls.getField("MODULE$").get(null)
          val m = cls.getMethod("convertCdf", classOf[RowDataSourceScanExec])
          Some((module, m))
        } catch {
          case _: NoSuchMethodException | _: NoSuchFieldException | _: IllegalAccessException =>
            None
        }
      }
      convertCdfBindingCache = Some(binding)
      binding
    }

  /**
   * Delegate a Change Data Feed read (`RowDataSourceScanExec` over `DeltaCDFRelation`) to the
   * contrib, producing a native `CometDeltaCdfScanExec`. Returns `Some(plan)` if the contrib
   * handled it, `None` to leave the vanilla Spark CDF read in place. Called from `CometExecRule`.
   */
  def transformCdf(scan: RowDataSourceScanExec): Option[SparkPlan] = {
    convertCdfBinding.flatMap { case (module, m) =>
      try {
        Option(m.invoke(module, scan))
          .map(_.asInstanceOf[Option[SparkPlan]])
          .flatten
      } catch {
        case _: IllegalAccessException | _: IllegalArgumentException =>
          None
        case e: java.lang.reflect.InvocationTargetException =>
          logWarning(
            "CometDeltaNativeScan.convertCdf threw, leaving the vanilla Spark CDF read in place",
            Option(e.getCause).getOrElse(e))
          None
      }
    }
  }

  /**
   * The Delta scan handler, resolved via reflection from the contrib's `CometDeltaNativeScan`
   * companion object. Returns `None` when the contrib isn't bundled into this build.
   * `CometExecRule` calls this and passes the result through the standard `convertToComet(scan,
   * handler)` path so the Delta scan flows through the same code as `CometNativeScan` etc.
   */
  def scanHandler: Option[CometOperatorSerde[_]] = serdeCls.flatMap { cls =>
    try {
      val module = cls.getField("MODULE$").get(null)
      Some(module.asInstanceOf[CometOperatorSerde[_]])
    } catch {
      // Only swallow true reflection-binding failures. A ClassCastException here
      // would mean the contrib's serde object doesn't implement the expected trait
      // -- a real bug worth surfacing.
      case _: NoSuchFieldException | _: IllegalAccessException => None
    }
  }
}
