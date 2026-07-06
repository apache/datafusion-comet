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

import org.apache.spark.sql.internal.SQLConf

/**
 * Curated set of Spark `spark.sql.legacy.*` configs whose behavior is NOT tied to a specific
 * Comet-supported expression (built-in functions with a legacy dependency are handled
 * per-expression via [[org.apache.comet.serde.CodegenDispatchFallback]] or a native passthrough
 * in the serde). The keys in this list are consumed by analyzer/optimizer rules, data-source
 * readers/writers, or type-system utilities, and Comet's native execution does not replicate
 * their legacy semantics.
 *
 * When [[CometConf.COMET_LEGACY_CONF_FALLBACK_ENABLED]] is true (default), Comet disables itself
 * for the session if any of these keys is set to its non-default value, so Spark's own execution
 * path is used instead. Users can set `spark.comet.legacyConfFallback.enabled=false` to override
 * the fallback and keep Comet enabled (Spark compatibility is not guaranteed in that case).
 */
private[comet] object LegacyConfFallback {

  /**
   * Map of legacy config key -> case-insensitive Spark default value. A config triggers the
   * fallback when it is present in the session conf AND its value is not equal (case-insensitive)
   * to the default recorded here.
   */
  val executionAffectingDefaults: Map[String, String] = Map(
    // Decimal type-system / analyzer rules that reshape plans reaching Comet.
    "spark.sql.legacy.allowNegativeScaleOfDecimal" -> "false",
    "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate" -> "false",
    "spark.sql.legacy.literal.pickMinimumPrecision" -> "false",
    // Char/varchar padding + write-side validation inserted by the analyzer.
    "spark.sql.legacy.charVarcharAsString" -> "false",
    // Analyzer rule that lets `count()` be treated as `count(*)`; the native planner has no
    // representation for a Count with zero children.
    "spark.sql.legacy.allowParameterlessCount" -> "false",
    // Type-coercion / upcast rules.
    "spark.sql.legacy.doLooseUpcast" -> "false",
    "spark.sql.legacy.typeCoercion.datetimeToString.enabled" -> "false",
    // Optimizer rules that reshape plans (subqueries, Between, empty-list IN nullability).
    "spark.sql.legacy.duplicateBetweenInput" -> "false",
    "spark.sql.legacy.inSubqueryNullability" -> "false",
    "spark.sql.legacy.scalarSubqueryCountBugBehavior" -> "false",
    // Map-key normalization used by CreateMap and friends inside ArrayBasedMapBuilder.
    "spark.sql.legacy.disableMapKeyNormalization" -> "false",
    // Set-op precedence changes the plan topology handed to Comet operators.
    "spark.sql.legacy.setopsPrecedence.enabled" -> "false",
    // View schema compensation controls whether Cast (Comet-supported) or UpCast (Comet
    // unsupported) is injected during view resolution.
    "spark.sql.legacy.viewSchemaCompensation" -> "true",
    // Datetime parser policy affects CSV/JSON scan options and datetime formatters.
    "spark.sql.legacy.timeParserPolicy" -> "CORRECTED",
    // Datasource readers/writers Comet may accelerate.
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead" -> "CORRECTED",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" -> "CORRECTED",
    "spark.sql.legacy.parquet.int96RebaseModeInRead" -> "CORRECTED",
    "spark.sql.legacy.parquet.int96RebaseModeInWrite" -> "CORRECTED",
    "spark.sql.legacy.parquet.nanosAsLong" -> "false",
    // Cached-plan behavior that leaves stale options on a Comet-accelerated file scan.
    "spark.sql.legacy.readFileSourceTableCacheIgnoreOptions" -> "false")

  /** Keys in [[executionAffectingDefaults]] that are set to a non-default value on `conf`. */
  def triggeredConfigs(conf: SQLConf): Iterable[String] = {
    executionAffectingDefaults.iterator.collect {
      case (key, safeDefault)
          if conf.contains(key) &&
            !conf.getConfString(key).equalsIgnoreCase(safeDefault) =>
        key
    }.toSeq
  }
}
