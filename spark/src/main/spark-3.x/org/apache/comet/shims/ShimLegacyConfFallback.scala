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

package org.apache.comet.shims

/**
 * Spark 3.x variant: several entries in this map (view-schema compensation, decimal truncate,
 * duplicate-between, scalar-subquery count-bug, disable-map-key-normalization, cache-ignore-
 * options) do not exist as [[org.apache.spark.internal.config.ConfigEntry]] instances in Spark
 * 3.5 or earlier, so the defaults are hardcoded. They match the Spark 4 defaults on purpose: the
 * fallback rule is "when a legacy key is set to something other than the Spark 4 default, disable
 * Comet". This keeps 3.x and 4.x behaviourally aligned even though 3.x can't derive the defaults
 * live. See the 4.x shim for the reference-derived variant.
 *
 * Parquet-specific legacy configs (`spark.sql.(legacy.)?parquet.*RebaseMode*`,
 * `spark.sql.legacy.parquet.nanosAsLong`) are intentionally NOT in this session-wide set. They
 * are checked per-scan in [[org.apache.comet.rules.CometScanRule.parquetFallbackReason]] and only
 * fall back the scan they affect, so non-Parquet queries in the same session stay on Comet.
 */
trait ShimLegacyConfFallback {

  protected def legacyConfDefaults: Map[String, String] = Map(
    "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate" -> "false",
    "spark.sql.legacy.literal.pickMinimumPrecision" -> "true",
    "spark.sql.legacy.charVarcharAsString" -> "false",
    "spark.sql.legacy.allowParameterlessCount" -> "false",
    "spark.sql.legacy.doLooseUpcast" -> "false",
    "spark.sql.legacy.typeCoercion.datetimeToString.enabled" -> "false",
    "spark.sql.legacy.duplicateBetweenInput" -> "false",
    "spark.sql.legacy.inSubqueryNullability" -> "false",
    "spark.sql.legacy.scalarSubqueryCountBugBehavior" -> "false",
    "spark.sql.legacy.disableMapKeyNormalization" -> "false",
    "spark.sql.legacy.setopsPrecedence.enabled" -> "false",
    "spark.sql.legacy.viewSchemaCompensation" -> "true",
    "spark.sql.legacy.readFileSourceTableCacheIgnoreOptions" -> "false")
}
