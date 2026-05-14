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
 * Process-wide singleton that discovers and exposes contrib extensions found on the classpath via
 * `java.util.ServiceLoader`.
 *
 * Discovery happens once per JVM, idempotent: the first `load()` call enumerates every
 * `META-INF/services/org.apache.comet.spi.CometScanRuleExtension` and
 * `META-INF/services/org.apache.comet.spi.CometOperatorSerdeExtension` resource on the Comet
 * classloader. Subsequent calls are no-ops.
 *
 * `load()` is invoked lazily from `CometScanRule._apply` and `CometExecRule._apply` the first
 * time either rule runs against a Comet-enabled session. Spark sessions that never enable Comet
 * pay zero ServiceLoader cost.
 *
 * Failures to instantiate individual extensions are logged but do NOT fail Comet startup -- a
 * misconfigured contrib JAR shouldn't take down the whole Spark session.
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
    if (loaded.get()) return
    val newScanExts = loadOne[CometScanRuleExtension]("CometScanRuleExtension")
    val newSerdeExts = loadOne[CometOperatorSerdeExtension]("CometOperatorSerdeExtension")
    val newMerged = newSerdeExts.flatMap(_.serdes).toMap
    // Publish the @volatile fields BEFORE flipping `loaded` so other threads either see
    // the empty defaults (and may re-enter -- benign, blocked by the monitor) or the
    // fully-populated state (and may skip -- also benign).
    scanExts = newScanExts
    serdeExts = newSerdeExts
    mergedSerdesCache = newMerged
    loaded.set(true)
    if (newScanExts.nonEmpty || newSerdeExts.nonEmpty) {
      logInfo(
        s"Comet contrib extensions loaded: " +
          s"scan=[${newScanExts.map(_.name).mkString(", ")}], " +
          s"serde=[${newSerdeExts.map(_.name).mkString(", ")}]")
      detectDuplicateSerdeClasses(newSerdeExts)
    } else {
      // Positive signal that discovery ran. Some Spark deploy modes (Ivy `--packages`,
      // isolated UDF classloaders) put Comet on a classloader that the TCCL fallback
      // doesn't see; absent extensions go silent without this line.
      logInfo(
        "Comet contrib extensions: none discovered on classpath " +
          "(no META-INF/services entries for CometScanRuleExtension or " +
          "CometOperatorSerdeExtension)")
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
  def mergedSerdes: Map[Class[_ <: org.apache.spark.sql.execution.SparkPlan],
    org.apache.comet.serde.CometOperatorSerde[_]] = mergedSerdesCache

  @volatile private var mergedSerdesCache
    : Map[Class[_ <: org.apache.spark.sql.execution.SparkPlan],
      org.apache.comet.serde.CometOperatorSerde[_]] = Map.empty

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
      .empty[Class[_ <: org.apache.spark.sql.execution.SparkPlan], scala.collection.mutable.ArrayBuffer[String]]
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
   * Visibility is `public` (rather than `private[comet]`) because contribs are not required to
   * be packaged under `org.apache.comet.*`; a contrib living under e.g. `io.delta.comet.contrib`
   * must still be able to reset between tests. The method's name carries the "test-only"
   * contract by convention.
   */
  def resetForTesting(): Unit = {
    loaded.set(false)
    scanExts = Seq.empty
    serdeExts = Seq.empty
    mergedSerdesCache = Map.empty
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
