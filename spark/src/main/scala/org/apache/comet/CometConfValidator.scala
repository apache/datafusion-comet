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

import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal

import org.apache.spark.CometDriverPlugin
import org.apache.spark.internal.Logging
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.serde.QueryPlanSerde

/**
 * Warns about `spark.comet.*` keys that Comet does not recognize, which are most likely typos
 * that Spark would otherwise accept silently.
 *
 * A key is recognized when it is a registered config (core or contrib) or one of its
 * alternatives, an `enabled` or `allowIncompatible` key of an expression Comet has a serde for,
 * or `spark.comet.version`. `spark.comet.datafusion.*` keys are passed to DataFusion as they are,
 * so they are not checked. Operator `allowIncompatible` keys are only read for operators whose
 * serde can report `Incompatible`, and those keys are registered, so they need no special case.
 */
object CometConfValidator extends Logging {

  private val cometKeyPrefix = s"${CometConf.COMET_PREFIX}."

  private val passThroughPrefixes = Seq(s"${CometConf.COMET_PREFIX}.datafusion.")

  private val maxSuggestionDistance = 3

  /** Keys already checked, so each one is checked, and warned about, at most once per JVM. */
  private[comet] val checkedKeys = ConcurrentHashMap.newKeySet[String]()

  /** Set when building the known keys fails, so a warning-only check never breaks a query. */
  @volatile private var disabled = false

  private lazy val knownKeys: Set[String] = {
    // Loading the providers constructs the contrib config objects, which registers their entries
    val entries = CometConfigProvider.providers.flatMap(_.configs) ++ CometConf.allConfs.toList
    val exprNames = (QueryPlanSerde.exprSerdeMap.keys ++ QueryPlanSerde.aggrSerdeMap.keys)
      .map(_.getSimpleName)
    val exprKeys = exprNames.flatMap { name =>
      Seq(CometConf.getExprEnabledConfigKey(name), CometConf.getExprAllowIncompatConfigKey(name))
    }
    (entries.flatMap(entry => entry.key +: entry.alternatives) ++ exprKeys ++
      Seq(CometDriverPlugin.COMET_VERSION_CONFIG)).toSet
  }

  /** Known keys in sorted order with their lowercase form, so ties go to the first key. */
  private lazy val suggestionCandidates: Seq[(String, UTF8String)] =
    knownKeys.toSeq.sorted.map(key => key -> UTF8String.fromString(key.toLowerCase(Locale.ROOT)))

  /** Unrecognized keys in sorted order, each with the closest known key if one is close. */
  private[comet] def findUnrecognized(keys: Iterable[String]): Seq[(String, Option[String])] =
    keys.filter(isUnrecognized).toSeq.distinct.sorted.map(key => key -> suggest(key))

  /** Logs a warning for each unrecognized key that has not been checked yet. */
  def warnUnrecognized(keys: Iterable[String]): Unit = {
    if (disabled) return
    val unchecked = keys.filter(key => key.startsWith(cometKeyPrefix) && checkedKeys.add(key))
    if (unchecked.nonEmpty) {
      try {
        findUnrecognized(unchecked).foreach { case (key, suggestion) =>
          val hint =
            suggestion.map(s => s"Did you mean '$s'?").getOrElse("Check the key for typos.")
          logWarning(s"Comet configuration '$key' is not recognized. $hint")
        }
      } catch {
        case e @ (NonFatal(_) | _: LinkageError) =>
          disabled = true
          logDebug("Could not check Comet configuration keys", e)
      }
    }
  }

  private def isUnrecognized(key: String): Boolean =
    key.startsWith(cometKeyPrefix) && !passThroughPrefixes.exists(key.startsWith) &&
      !knownKeys.contains(key)

  private def suggest(key: String): Option[String] = {
    val lowerKey = UTF8String.fromString(key.toLowerCase(Locale.ROOT))
    val (closest, distance) = suggestionCandidates
      .map { case (known, lowerKnown) => known -> lowerKey.levenshteinDistance(lowerKnown) }
      .minBy(_._2)
    if (distance <= maxSuggestionDistance) Some(closest) else None
  }
}
