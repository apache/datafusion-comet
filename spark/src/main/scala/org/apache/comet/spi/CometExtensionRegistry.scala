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

import java.util.ServiceLoader
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging

/**
 * Process-wide singleton that exposes the contrib extensions bundled into comet-spark.jar.
 *
 * Discovery uses `java.util.ServiceLoader` against `META-INF/services/` entries inside
 * comet-spark.jar. Those entries get there at build time: each contrib (under `contrib/<name>/`)
 * carries its own `META-INF/services/` files, and the `contrib-<name>` Maven profile on
 * spark/pom.xml shades the contrib's classes plus those service entries into the published
 * comet-spark.jar. A vanilla `mvn install` produces a comet-spark.jar with zero contribs; a `mvn
 * install -Pcontrib-example` build bundles the example contrib. The native side mirrors this
 * exactly via `--features contrib-example` on the Rust core crate.
 *
 * Discovery is idempotent: the first `load()` call enumerates the service entries; subsequent
 * calls are no-ops. `load()` is invoked lazily from `CometScanRule._apply` and
 * `CometExecRule._apply` the first time either rule runs against a Comet-enabled session. Spark
 * sessions that never enable Comet pay zero ServiceLoader cost.
 *
 * Failures to instantiate individual extensions are logged at WARN but do NOT fail Comet startup
 * -- a misconfigured contrib shouldn't take down the whole Spark session.
 */
object CometExtensionRegistry extends Logging {

  private val loaded = new AtomicBoolean(false)
  @volatile private var scanExts: Seq[CometScanRuleExtension] = Seq.empty
  @volatile private var serdeExts: Seq[CometOperatorSerdeExtension] = Seq.empty

  /**
   * Discover contrib extensions on the classpath. Idempotent. Safe to call from multiple threads
   * (only the first call performs discovery).
   */
  def load(): Unit = synchronized {
    // `synchronized` (not just compareAndSet) so that concurrent callers wait for the
    // first thread's writes to `scanExts` / `serdeExts` / `mergedSerdesCache` to publish
    // before they return. The previous AtomicBoolean-only gate allowed thread B to
    // observe `loaded=true` and read `Seq.empty` while thread A was still mid-loadOne.
    // CometScanRule._apply and CometExecRule._apply both call this on first invocation,
    // and AQE can run them concurrently across sub-queries, so the race is reachable.
    //
    // Contribs MUST NOT call `load()` from a `#[ctor]`-equivalent (JVM-side: a class's
    // static initializer or trait's `object` init) -- Scala monitors are reentrant so
    // re-entry won't deadlock, but the inner call would observe the partially-built
    // state and re-trigger `loadOne`, shadowing the in-flight publication.
    if (loaded.get()) return
    val newScanExts = loadOne[CometScanRuleExtension]("CometScanRuleExtension")
    val newSerdeExts = loadOne[CometOperatorSerdeExtension]("CometOperatorSerdeExtension")
    val newMerged = newSerdeExts.flatMap(_.serdes).toMap
    val newNativeParquetTags = newSerdeExts.flatMap(_.nativeParquetScanImpls).toSet
    // Publish the @volatile fields BEFORE flipping `loaded` so other threads either see
    // the empty defaults (and may re-enter -- benign, blocked by the monitor) or the
    // fully-populated state (and may skip -- also benign).
    scanExts = newScanExts
    serdeExts = newSerdeExts
    mergedSerdesCache = newMerged
    nativeParquetScanImplsCache = newNativeParquetTags
    loaded.set(true)
    // Call `init()` AFTER publishing the volatile fields and flipping `loaded`. This lets
    // an extension's `init` synchronously call back into the registry (e.g. to read its
    // sibling extensions) without observing a half-built state, and it lets `init` register
    // executor-side callbacks on `CometExecRDD` without racing the first compute call.
    // Failures are isolated per extension so one broken contrib doesn't take down the others.
    newSerdeExts.foreach { ext =>
      try ext.init()
      catch {
        case scala.util.control.NonFatal(e) =>
          logWarning(s"CometOperatorSerdeExtension '${ext.name}' init failed; continuing", e)
      }
    }
    if (newScanExts.nonEmpty || newSerdeExts.nonEmpty) {
      logInfo(
        s"Comet contrib extensions loaded: " +
          s"scan=[${newScanExts.map(_.name).mkString(", ")}], " +
          s"serde=[${newSerdeExts.map(_.name).mkString(", ")}]")
      detectDuplicateSerdeClasses(newSerdeExts)
    } else {
      // Positive signal that discovery ran. Comet-spark.jar's contrib content depends on
      // which `-Pcontrib-<name>` Maven profiles were active at build time; this line is
      // what tells a user whose contrib went missing whether to suspect their Comet build
      // or their classpath.
      logInfo(
        "Comet contrib extensions: none discovered. comet-spark.jar was built " +
          "without any contrib profiles enabled, or the contrib's META-INF/services " +
          "entries were not bundled correctly.")
    }
  }

  /** Registered scan-rule extensions, in classpath discovery order. */
  def scanExtensions: Seq[CometScanRuleExtension] = scanExts

  /** Registered operator-serde extensions, in classpath discovery order. */
  def serdeExtensions: Seq[CometOperatorSerdeExtension] = serdeExts

  /**
   * Pre-merged serde map across all registered extensions, keyed by the `Class[_ <: SparkPlan]`
   * the contrib uses for class-keyed dispatch in `CometExecRule`. Computed once at `load()` time;
   * an empty map until `load()` has run.
   */
  def mergedSerdes: Map[
    Class[_ <: org.apache.spark.sql.execution.SparkPlan],
    org.apache.comet.serde.CometOperatorSerde[_]] = mergedSerdesCache

  @volatile private var mergedSerdesCache: Map[
    Class[_ <: org.apache.spark.sql.execution.SparkPlan],
    org.apache.comet.serde.CometOperatorSerde[_]] = Map.empty

  /**
   * Union of every registered extension's `nativeParquetScanImpls`. Consumed by
   * `CometScanExec.supportedDataFilters` to decide whether the marker scan's filter set should
   * get the same native-parquet exclusions as `SCAN_NATIVE_DATAFUSION`. Computed once at `load()`
   * time; empty until `load()` has run.
   */
  def nativeParquetScanImpls: Set[String] = nativeParquetScanImplsCache

  @volatile private var nativeParquetScanImplsCache: Set[String] = Set.empty

  /**
   * Log a warning when two registered contribs claim the same `Class[_ <: SparkPlan]` for serde
   * dispatch. The convention documented in `contrib-extensions.md` is that each contrib defines
   * its own exec class and registers a serde keyed on that class; a collision usually means a
   * contrib subclassed a core exec by mistake.
   *
   * Detection only -- the last-write-wins toMap behavior stands. We log so the user has a chance
   * to notice; preventing the override would be a harder migration path (silent drop of one
   * contrib's exec).
   */
  private def detectDuplicateSerdeClasses(exts: Seq[CometOperatorSerdeExtension]): Unit = {
    val perClassOwners = scala.collection.mutable.Map
      .empty[
        Class[_ <: org.apache.spark.sql.execution.SparkPlan],
        scala.collection.mutable.ArrayBuffer[String]]
    exts.foreach { ext =>
      ext.serdes.keys.foreach { cls =>
        perClassOwners
          .getOrElseUpdate(cls, scala.collection.mutable.ArrayBuffer.empty)
          .+=(ext.name)
      }
    }
    perClassOwners.foreach { case (cls, owners) =>
      if (owners.size > 1) {
        logWarning(
          s"Multiple Comet contrib extensions claim the same exec class " +
            s"${cls.getName}: [${owners.mkString(", ")}]. Last-write-wins; " +
            s"this usually indicates a contrib has subclassed a core or " +
            s"another contrib's exec instead of defining its own.")
      }
    }
  }

  /**
   * Test-only: reset the registry to the empty state. Lets unit tests re-run discovery with a
   * different classpath / overridden services. Not for production use.
   *
   * Visibility is `public` (rather than `private[comet]`) because contribs are not required to be
   * packaged under `org.apache.comet.*`; a contrib living under e.g. `io.delta.comet.contrib`
   * must still be able to reset between tests. The method's name carries the "test-only" contract
   * by convention.
   */
  def resetForTesting(): Unit = synchronized {
    // synchronized so concurrent `load()` callers don't observe torn state -- e.g.
    // `loaded=false` with `scanExts` still populated, which would let a subsequent
    // `load()` short-circuit on the AtomicBoolean and never re-discover.
    loaded.set(false)
    scanExts = Seq.empty
    serdeExts = Seq.empty
    mergedSerdesCache = Map.empty
    nativeParquetScanImplsCache = Set.empty
    // Also clear any executor-side callbacks registered via the SPI's `init` hook so the
    // next `load()` re-registers from scratch. Without this the test that exercises
    // `resetForTesting` + `load` would accumulate handlers across reset boundaries.
    org.apache.spark.sql.comet.CometExecRDD.clearPartitionMetadataHandlers()
    org.apache.spark.sql.comet.PlanDataInjector.clearContribInjectors()
  }

  private def loadOne[T](label: String)(implicit ct: scala.reflect.ClassTag[T]): Seq[T] = {
    val cls = ct.runtimeClass.asInstanceOf[Class[T]]
    val loader = Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(getClass.getClassLoader)
    try {
      val it = ServiceLoader.load(cls, loader).iterator().asScala
      val out = scala.collection.mutable.ArrayBuffer.empty[T]
      while (it.hasNext) {
        // Pull each extension under its own try so one broken contrib doesn't sink the
        // rest of the registry. ServiceLoader.next() can throw if the extension fails to
        // instantiate (missing class, ctor exception, etc.).
        try out += it.next()
        catch {
          case scala.util.control.NonFatal(e) =>
            logWarning(s"Failed to load a $label entry; skipping: ${e.getMessage}", e)
        }
      }
      out.toSeq
    } catch {
      case scala.util.control.NonFatal(e) =>
        logWarning(s"$label discovery failed; no contrib extensions of this kind loaded", e)
        Seq.empty
    }
  }
}
