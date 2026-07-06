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

import org.apache.spark.internal.config.ConfigEntry
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
 */
trait ShimLegacyConfFallback {

  private def entryDefault(entry: ConfigEntry[_]): String = entry.defaultValueString

  protected def legacyConfDefaults: Map[String, String] = Map(
    "spark.sql.legacy.allowNegativeScaleOfDecimal" ->
      entryDefault(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED),
    "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate" ->
      entryDefault(SQLConf.LEGACY_RETAIN_FRACTION_DIGITS_FIRST),
    "spark.sql.legacy.literal.pickMinimumPrecision" ->
      entryDefault(SQLConf.LITERAL_PICK_MINIMUM_PRECISION),
    "spark.sql.legacy.charVarcharAsString" ->
      entryDefault(SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING),
    "spark.sql.legacy.allowParameterlessCount" ->
      entryDefault(SQLConf.ALLOW_PARAMETERLESS_COUNT),
    "spark.sql.legacy.doLooseUpcast" ->
      entryDefault(SQLConf.LEGACY_LOOSE_UPCAST),
    "spark.sql.legacy.typeCoercion.datetimeToString.enabled" ->
      entryDefault(SQLConf.LEGACY_CAST_DATETIME_TO_STRING),
    "spark.sql.legacy.duplicateBetweenInput" ->
      entryDefault(SQLConf.LEGACY_DUPLICATE_BETWEEN_INPUT),
    "spark.sql.legacy.inSubqueryNullability" ->
      entryDefault(SQLConf.LEGACY_IN_SUBQUERY_NULLABILITY),
    "spark.sql.legacy.scalarSubqueryCountBugBehavior" ->
      entryDefault(SQLConf.LEGACY_SCALAR_SUBQUERY_COUNT_BUG_HANDLING),
    "spark.sql.legacy.disableMapKeyNormalization" ->
      entryDefault(SQLConf.DISABLE_MAP_KEY_NORMALIZATION),
    "spark.sql.legacy.setopsPrecedence.enabled" ->
      entryDefault(SQLConf.LEGACY_SETOPS_PRECEDENCE_ENABLED),
    "spark.sql.legacy.viewSchemaCompensation" ->
      entryDefault(SQLConf.VIEW_SCHEMA_COMPENSATION),
    "spark.sql.legacy.timeParserPolicy" ->
      entryDefault(SQLConf.LEGACY_TIME_PARSER_POLICY),
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead" ->
      entryDefault(SQLConf.PARQUET_REBASE_MODE_IN_READ),
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" ->
      entryDefault(SQLConf.PARQUET_REBASE_MODE_IN_WRITE),
    "spark.sql.legacy.parquet.int96RebaseModeInRead" ->
      entryDefault(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ),
    "spark.sql.legacy.parquet.int96RebaseModeInWrite" ->
      entryDefault(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE),
    "spark.sql.legacy.parquet.nanosAsLong" ->
      entryDefault(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG),
    "spark.sql.legacy.readFileSourceTableCacheIgnoreOptions" ->
      entryDefault(SQLConf.READ_FILE_SOURCE_TABLE_CACHE_IGNORE_OPTIONS))
}
