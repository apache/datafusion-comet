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

package org.apache.comet

import java.util.ServiceLoader

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

import org.apache.comet.util.ClassLoaders

/**
 * Shared `ServiceLoader` discovery for Comet's optional, out-of-tree contrib registries
 * (`CometScanContrib`, `CometConfigProvider`, `PlanDataInjector`).
 *
 * Exists because the obvious spellings are both wrong in ways that are invisible until a contrib
 * jar is misbuilt in production:
 *
 *   - `ServiceLoader.load(...).asScala.toSeq` is a LAZY `Stream` on Scala 2.12, so the provider
 *     iterator is forced by whoever first traverses the result -- outside any `try` at the
 *     discovery site. A provider that fails to load then throws `ServiceConfigurationError` from
 *     the middle of query planning.
 *   - Wrapping the whole traversal in one `try` fixes that, but throws away EVERY provider when
 *     any single one fails. With two contribs registered (say Delta and Lance), one misbuilt jar
 *     would silently disable the other, and the only symptom is a warning plus queries quietly
 *     running on vanilla Spark.
 *
 * So iterate defensively instead: catch per step, skip the bad provider, keep the good ones. The
 * JDK consumes a provider's name before reporting its failure, so the iterator advances past a
 * bad entry rather than re-reporting it -- verified for all four realistic failure modes (class
 * missing, class not implementing the service, constructor throwing, no accessible no-arg
 * constructor). `maxSteps` is a belt-and-braces bound on that behaviour: skipping is a JDK
 * implementation detail, and a registry lookup must not be able to hang the planner if it ever
 * changes.
 *
 * Not `private[comet]`: `PlanDataInjector` lives in `org.apache.spark.sql.comet`, which is a
 * different package root, so a comet-scoped qualifier would hide this from one of its three call
 * sites.
 */
object ContribServices extends Logging {

  /** Generous relative to any real deployment; only exists so a stuck iterator cannot hang. */
  private val MaxSteps = 1000

  /**
   * Load every usable implementation of `service` visible to the thread context ClassLoader
   * (falling back to `fallbackLoader`), skipping and logging the ones that fail.
   *
   * The context loader matters: Comet is usually installed on the driver's `extraClassPath` while
   * a contrib arrives via `--jars`, i.e. on a CHILD loader, so discovering from Comet's own
   * defining loader would silently miss it.
   */
  def load[T](service: Class[T], fallbackLoader: ClassLoader): Seq[T] =
    loadFrom(service, ClassLoaders.contextOrDefault(fallbackLoader))

  /** `load` against an explicit ClassLoader. Separate so tests can drive real discovery. */
  def loadFrom[T](service: Class[T], loader: ClassLoader): Seq[T] = {
    val found = ListBuffer.empty[T]
    val iterator =
      try {
        ServiceLoader.load(service, loader).iterator()
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to open ${service.getName} service registry", e)
          return Seq.empty
      }

    var steps = 0
    var done = false
    while (!done && steps < MaxSteps) {
      steps += 1
      // `hasNext` and `next` can each raise ServiceConfigurationError -- the former when a listed
      // class cannot be loaded or does not implement the service, the latter when construction
      // fails. Guard both, and treat a failure as "skip this provider", not "abandon the rest".
      try {
        if (iterator.hasNext) found += iterator.next()
        else done = true
      } catch {
        // NonFatal covers ServiceConfigurationError; LinkageError/OOM still propagate.
        case NonFatal(e) =>
          logWarning(
            s"Skipping an unusable ${service.getSimpleName} provider; the remaining providers " +
              "are unaffected. This usually means a contrib jar is misbuilt (its service file " +
              "names a class that is absent, does not implement the service, or cannot be " +
              "constructed).",
            e)
      }
    }
    if (steps >= MaxSteps) {
      logWarning(
        s"Stopped ${service.getSimpleName} discovery after $MaxSteps steps; the service " +
          "iterator did not terminate. Registry may be incomplete.")
    }
    found.toList
  }
}
