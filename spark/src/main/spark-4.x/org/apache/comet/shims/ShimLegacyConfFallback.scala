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

import org.apache.spark.sql.internal.SQLConf

/**
 * Spark 4.x variant: defaults are read from live `ConfigEntry` references so this file stays in
 * sync with Spark 4 without duplicated string literals. See [[ShimLegacyConfFallback]] on 3.x for
 * the hardcoded counterpart.
 *
 * Every key returned here uses the `spark.sql.legacy.*` name that the check compares against.
 * When a legacy key was removed in Spark 4 (the four parquet rebase aliases), we still ship the
 * legacy key -> Spark 4 non-legacy default; on Spark 4 the check is effectively inert because
 * `SQLConf.set` rejects removed keys, but the entry is kept so 3.x behaviour is consistent.
 *
 * Note: `ConfigEntry` itself is `private[spark]`, so we never spell the type — every value below
 * is a direct method call on a `SQLConf` val, and the compiler resolves `defaultValueString`
 * without exposing the type name to this compilation unit.
 */
trait ShimLegacyConfFallback {

  protected def legacyConfDefaults: Map[String, String] = Map(
    "spark.sql.legacy.allowNegativeScaleOfDecimal" ->
      SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.defaultValueString,
    "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate" ->
      SQLConf.LEGACY_RETAIN_FRACTION_DIGITS_FIRST.defaultValueString,
    "spark.sql.legacy.literal.pickMinimumPrecision" ->
      SQLConf.LITERAL_PICK_MINIMUM_PRECISION.defaultValueString,
    "spark.sql.legacy.charVarcharAsString" ->
      SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.defaultValueString,
    "spark.sql.legacy.allowParameterlessCount" ->
      SQLConf.ALLOW_PARAMETERLESS_COUNT.defaultValueString,
    "spark.sql.legacy.doLooseUpcast" ->
      SQLConf.LEGACY_LOOSE_UPCAST.defaultValueString,
    "spark.sql.legacy.typeCoercion.datetimeToString.enabled" ->
      SQLConf.LEGACY_CAST_DATETIME_TO_STRING.defaultValueString,
    "spark.sql.legacy.duplicateBetweenInput" ->
      SQLConf.LEGACY_DUPLICATE_BETWEEN_INPUT.defaultValueString,
    "spark.sql.legacy.inSubqueryNullability" ->
      SQLConf.LEGACY_IN_SUBQUERY_NULLABILITY.defaultValueString,
    "spark.sql.legacy.scalarSubqueryCountBugBehavior" ->
      SQLConf.LEGACY_SCALAR_SUBQUERY_COUNT_BUG_HANDLING.defaultValueString,
    "spark.sql.legacy.disableMapKeyNormalization" ->
      SQLConf.DISABLE_MAP_KEY_NORMALIZATION.defaultValueString,
    "spark.sql.legacy.setopsPrecedence.enabled" ->
      SQLConf.LEGACY_SETOPS_PRECEDENCE_ENABLED.defaultValueString,
    "spark.sql.legacy.viewSchemaCompensation" ->
      SQLConf.VIEW_SCHEMA_COMPENSATION.defaultValueString,
    "spark.sql.legacy.timeParserPolicy" ->
      SQLConf.LEGACY_TIME_PARSER_POLICY.defaultValueString,
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead" ->
      SQLConf.PARQUET_REBASE_MODE_IN_READ.defaultValueString,
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" ->
      SQLConf.PARQUET_REBASE_MODE_IN_WRITE.defaultValueString,
    "spark.sql.legacy.parquet.int96RebaseModeInRead" ->
      SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.defaultValueString,
    "spark.sql.legacy.parquet.int96RebaseModeInWrite" ->
      SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.defaultValueString,
    "spark.sql.legacy.parquet.nanosAsLong" ->
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.defaultValueString,
    "spark.sql.legacy.readFileSourceTableCacheIgnoreOptions" ->
      SQLConf.READ_FILE_SOURCE_TABLE_CACHE_IGNORE_OPTIONS.defaultValueString)
}
