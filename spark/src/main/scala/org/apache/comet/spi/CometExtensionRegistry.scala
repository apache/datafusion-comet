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
 * Process-wide singleton that discovers and exposes contrib extensions found on the
 * classpath via `java.util.ServiceLoader`.
 *
 * Discovery happens once per JVM, idempotent: the first `load()` call enumerates every
 * `META-INF/services/org.apache.comet.spi.CometScanRuleExtension` and
 * `META-INF/services/org.apache.comet.spi.CometOperatorSerdeExtension` resource on the
 * Comet classloader. Subsequent calls are no-ops.
 *
 * `CometSparkSessionExtensions.apply` calls `load()` during Comet extension installation
 * (PR1.6) so contrib JARs are picked up automatically when present.
 *
 * Failures to instantiate individual extensions are logged but do NOT fail Comet
 * startup -- a misconfigured contrib JAR shouldn't take down the whole Spark session.
 */
object CometExtensionRegistry extends Logging {

  private val loaded = new AtomicBoolean(false)
  @volatile private var scanExts: Seq[CometScanRuleExtension] = Seq.empty
  @volatile private var serdeExts: Seq[CometOperatorSerdeExtension] = Seq.empty

  /**
   * Discover contrib extensions on the classpath. Idempotent. Safe to call from multiple
   * threads (only the first call performs discovery).
   */
  def load(): Unit = {
    if (loaded.compareAndSet(false, true)) {
      scanExts = loadOne[CometScanRuleExtension]("CometScanRuleExtension")
      serdeExts = loadOne[CometOperatorSerdeExtension]("CometOperatorSerdeExtension")
      if (scanExts.nonEmpty || serdeExts.nonEmpty) {
        logInfo(
          s"Comet contrib extensions loaded: " +
            s"scan=[${scanExts.map(_.name).mkString(", ")}], " +
            s"serde=[${serdeExts.map(_.name).mkString(", ")}]")
      }
    }
  }

  /** Registered scan-rule extensions, in classpath discovery order. */
  def scanExtensions: Seq[CometScanRuleExtension] = scanExts

  /** Registered operator-serde extensions, in classpath discovery order. */
  def serdeExtensions: Seq[CometOperatorSerdeExtension] = serdeExts

  /**
   * Test-only: reset the registry to the empty state. Lets unit tests re-run discovery
   * with a different classpath / overridden services. Not for production use.
   */
  private[comet] def resetForTesting(): Unit = {
    loaded.set(false)
    scanExts = Seq.empty
    serdeExts = Seq.empty
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
