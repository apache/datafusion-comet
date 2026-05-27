<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Supported Spark Expressions

## How to Read This Document

- A function marked with `[x]` has a native implementation in Comet and does not fall back to Spark by default.
- A function marked with `[ ]` has no native Comet implementation and falls back to Spark.

> **Note:** Some functions listed as supported may still be incompatible with Spark in
> certain cases (data types, modes, edge values) and fall back to Spark at runtime. Full
> per-function details are documented in the
> [Compatibility Guide](https://datafusion.apache.org/comet/user-guide/latest/compatibility/expressions/index.html).

### agg_funcs

- [x] any
- [x] any_value
- [ ] approx_count_distinct
- [ ] approx_percentile
- [ ] approx_top_k
- [ ] approx_top_k_accumulate
- [ ] approx_top_k_combine
- [ ] array_agg
- [x] avg
- [x] bit_and
- [x] bit_or
- [x] bit_xor
- [x] bool_and
- [x] bool_or
- [ ] collect_list
- [x] collect_set
- [x] corr
- [x] count
- [x] count_if
- [ ] count_min_sketch
- [x] covar_pop
- [x] covar_samp
- [x] every
- [x] first
- [x] first_value
- [ ] grouping
- [ ] grouping_id
- [ ] histogram_numeric
- [ ] hll_sketch_agg
- [ ] hll_union_agg
- [ ] kll_sketch_agg_bigint
- [ ] kll_sketch_agg_double
- [ ] kll_sketch_agg_float
- [ ] kll_sketch_get_n_bigint
- [ ] kll_sketch_get_n_double
- [ ] kll_sketch_get_n_float
- [ ] kll_sketch_get_quantile_bigint
- [ ] kll_sketch_get_quantile_double
- [ ] kll_sketch_get_quantile_float
- [ ] kll_sketch_get_rank_bigint
- [ ] kll_sketch_get_rank_double
- [ ] kll_sketch_get_rank_float
- [ ] kll_sketch_merge_bigint
- [ ] kll_sketch_merge_double
- [ ] kll_sketch_merge_float
- [ ] kll_sketch_to_string_bigint
- [ ] kll_sketch_to_string_double
- [ ] kll_sketch_to_string_float
- [ ] kurtosis
- [x] last
- [x] last_value
- [ ] listagg
- [x] max
- [ ] max_by
- [x] mean
- [ ] median
- [x] min
- [ ] min_by
- [ ] mode
- [ ] percentile
- [ ] percentile_approx
- [ ] percentile_cont
- [ ] percentile_disc
- [ ] regr_avgx
- [ ] regr_avgy
- [ ] regr_count
- [ ] regr_intercept
- [ ] regr_r2
- [ ] regr_slope
- [ ] regr_sxx
- [ ] regr_sxy
- [ ] regr_syy
- [ ] skewness
- [x] some
- [x] std
- [x] stddev
- [x] stddev_pop
- [x] stddev_samp
- [ ] string_agg
- [x] sum
- [ ] theta_intersection_agg
- [ ] theta_sketch_agg
- [ ] theta_union_agg
- [ ] try_avg
- [ ] try_sum
- [x] var_pop
- [x] var_samp
- [x] variance

### array_funcs

- [x] array
- [x] array_append
- [x] array_compact
- [x] array_contains
- [x] array_distinct
- [x] array_except
- [x] array_insert
  - Spark 3.4.3 audited 2026-04-02
  - Spark 3.5.8 audited 2026-04-02
  - Spark 4.0.1 audited 2026-04-02 (pos=0 error message differs from Spark)
- [x] array_intersect
  - Spark 3.4.3 audited 2026-04-24 (result element order may differ from Spark when the right array is longer than the left; DataFusion probes the longer side)
  - Spark 3.5.8 audited 2026-04-24 (same ordering incompatibility as 3.4.3)
  - Spark 4.0.1 audited 2026-04-24 (ordering incompatibility as above; collated strings now fall back to Spark)
- [x] array_join
- [x] array_max
- [x] array_min
- [x] array_position
- [ ] array_prepend
- [x] array_remove
- [x] array_repeat
- [x] array_union
- [x] arrays_overlap
- [x] arrays_zip
- [x] element_at
- [x] flatten
- [x] get
- [ ] sequence
- [ ] shuffle
- [ ] slice
- [x] sort_array

### bitwise_funcs

- [x] `&`
- [x] `<<`
- [x] `>>`
- [ ] `>>>`
- [x] `^`
- [x] bit_count
- [x] bit_get
- [x] getbit
- [x] shiftright
- [ ] shiftrightunsigned
- [x] `|`
- [x] `~`

### collection_funcs

- [ ] array_size
- [ ] cardinality
- [x] concat
- [x] reverse
- [x] size

### conditional_funcs

- [x] coalesce
- [x] if
- [x] ifnull
- [ ] nanvl
- [x] nullif
- [ ] nullifzero
- [x] nvl
- [x] nvl2
- [x] when
- [ ] zeroifnull

### conversion_funcs

- [ ] bigint
- [ ] binary
- [ ] boolean
- [x] cast
- [ ] date
- [ ] decimal
- [ ] double
- [ ] float
- [ ] int
- [ ] smallint
- [ ] string
- [ ] timestamp
- [ ] tinyint

### csv_funcs

- [ ] from_csv
- [ ] schema_of_csv
- [ ] to_csv

### datetime_funcs

- [x] add_months
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `inputTypes = Seq(DateType, IntegerType)`; returns `DateType`; codegen delegates to `DateTimeUtils.dateAddMonths`.
  - Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true` on `AddMonthsBase`; behaviour and codegen unchanged.
- [x] convert_timezone
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. Ternary `(sourceTz, targetTz, sourceTs)`; `inputTypes = Seq(StringType, StringType, TimestampNTZType)`; delegates to `DateTimeUtils.convertTimestampNtzToAnotherTz`.
  - Spark 4.0.1 (audited 2026-05-27): timezone `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`; behaviour unchanged for ASCII timezone strings.
  - Known divergence: Comet composes `to_utc_timestamp` then `from_utc_timestamp` and its native timezone parser only accepts IANA zone IDs and `+HH:MM` offsets, so legacy forms like `GMT+1`, `UTC+1`, or three-letter abbreviations throw a native parse error at execution (https://github.com/apache/datafusion-comet/issues/2013).
- [ ] curdate
- [ ] current_date
- [ ] current_time
- [ ] current_timestamp
- [x] current_timezone
- [x] date_add
  - Spark 3.4.3 (audited 2026-05-27): baseline. `(DateType, IntegerType|ShortType|ByteType) -> DateType`; `nullSafeEval` returns `startDays + d.intValue()` with Java int wrap-around; no ANSI branch.
  - Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
  - Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true`.
- [x] date_diff
  - Spark 3.4.3 (audited 2026-05-27): baseline. `(DateType, DateType) -> IntegerType`; `nullSafeEval` is `endDays - startDays` with Java int wrap-around.
  - Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
  - Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true`.
  - Known divergence: the native impl uses non-wrapping `i32 -`, which would panic in debug builds on extreme inputs (Spark wraps); practically unreachable for date inputs.
- [x] date_format
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `(TimestampType, StringType) -> StringType`; format string is parsed via `TimestampFormatter` (`DateTimeFormatter` under `CORRECTED` policy, `SimpleDateFormat` under `LEGACY` policy).
  - Spark 4.0.1 (audited 2026-05-27): trait set updated to use `DefaultStringProducingExpression`; `nullIntolerant` becomes a field; format `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`. Behaviour unchanged for ASCII format strings.
  - Known divergence: only a curated allow-list of `SimpleDateFormat` patterns runs natively (via DataFusion `to_char`). Non-UTC session timezones with a whitelisted format require `spark.comet.expr.dateFormat.allowIncompatible=true`. Non-literal formats, non-whitelisted formats, and the default disabled-codegen path route through Spark's `DateFormatClass.doGenCode` only when `spark.comet.exec.scalaUDF.codegen.enabled=true`; otherwise the operator falls back to Spark. `spark.sql.legacy.timeParserPolicy=LEGACY` is honoured only on the codegen-dispatch / Spark-fallback paths; the native allow-list assumes corrected semantics.
- [x] date_from_unix_date
  - Spark 3.4.3 (audited 2026-05-27): baseline. `IntegerType -> DateType`; `nullSafeEval` is the identity on days-since-epoch.
  - Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
  - Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true`.
- [x] date_part
- [x] date_sub
  - Spark 3.4.3 (audited 2026-05-27): baseline. Mirror of `DateAdd` (`startDays - d.intValue()`); same input types and wrap-around behaviour; no ANSI branch.
  - Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
  - Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true`.
- [x] date_trunc
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `inputTypes = Seq(StringType, TimestampType)`; format parsed by `parseTruncLevel` (case-insensitive) and supports `YEAR`/`YYYY`/`YY`, `QUARTER`, `MONTH`/`MM`/`MON`, `WEEK`, `DAY`/`DD`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, `MICROSECOND`. Unknown levels return NULL. Truncation is `TimeZoneAware` and uses `zoneId` for day-and-coarser units.
  - Spark 4.0.1 (audited 2026-05-27): format `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`; truncation semantics unchanged for ASCII format strings.
  - Known divergence: Comet returns incorrect results in non-UTC session timezones for day-and-coarser units (https://github.com/apache/datafusion-comet/issues/2649); marked `Incompatible` when the resolved zone is not `UTC` / `Etc/UTC`. Non-literal and unsupported format strings raise a native execution error instead of returning NULL.
- [x] dateadd
  - Spark 3.4.3 (audited 2026-05-27): SQL alias for `date_add`; see that entry.
  - Spark 3.5.8 (audited 2026-05-27): SQL alias for `date_add`; see that entry.
  - Spark 4.0.1 (audited 2026-05-27): SQL alias for `date_add`; see that entry.
- [x] datediff
  - Spark 3.4.3 (audited 2026-05-27): SQL alias for `date_diff`; see that entry.
  - Spark 3.5.8 (audited 2026-05-27): SQL alias for `date_diff`; see that entry.
  - Spark 4.0.1 (audited 2026-05-27): SQL alias for `date_diff`; see that entry.
- [x] datepart
- [x] day
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. SQL alias for `DayOfMonth`; delegates to `DateTimeUtils.getDayOfMonth` via `LocalDate.getDayOfMonth` (1..31).
  - Spark 4.0.1 (audited 2026-05-27): identical semantics; `GetDateField` drops the `NullIntolerant` mixin in favour of `nullIntolerant: Boolean = true`.
- [ ] dayname
- [x] dayofmonth
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `DayOfMonth extends GetDateField`; delegates to `DateTimeUtils.getDayOfMonth` (1..31).
  - Spark 4.0.1 (audited 2026-05-27): identical semantics; `GetDateField` drops the `NullIntolerant` mixin in favour of `nullIntolerant: Boolean = true`.
- [x] dayofweek
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `DayOfWeek extends GetDateField`; returns 1..7 with Sunday=1 via `LocalDate.getDayOfWeek.plus(1).getValue`.
  - Spark 4.0.1 (audited 2026-05-27): identical semantics; `GetDateField` drops the `NullIntolerant` mixin in favour of `nullIntolerant: Boolean = true`.
- [x] dayofyear
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `DayOfYear extends GetDateField`; returns 1..366 via `LocalDate.getDayOfYear`.
  - Spark 4.0.1 (audited 2026-05-27): identical semantics; `GetDateField` drops the `NullIntolerant` mixin in favour of `nullIntolerant: Boolean = true`.
- [x] extract
- [x] from_unixtime
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `BinaryExpression` with `inputTypes = Seq(LongType, StringType)`; returns `StringType`; uses session `zoneId` to format the resulting timestamp.
  - Spark 4.0.1 (audited 2026-05-27): now `DefaultStringProducingExpression`; format `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`; `nullIntolerant` set via override instead of the `NullIntolerant` trait.
  - Known divergence: Comet only honours the default format pattern `yyyy-MM-dd HH:mm:ss`; any other format falls back to Spark. Implemented by composing DataFusion's `from_unixtime` and `to_char`, so DataFusion's valid timestamp range differs from Spark (https://github.com/apache/datafusion/issues/16594) and Spark datetime patterns are not honoured even when supplied (https://github.com/apache/datafusion/issues/16577).
- [x] from_utc_timestamp
  - Spark 3.4.3 (audited 2026-05-12): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-12): baseline.
  - Spark 4.0.1 (audited 2026-05-12): `inputTypes` widened to `StringTypeWithCollation`; behaviour unchanged for ASCII timezone strings.
  - Known divergence: Comet's native timezone parser does not accept Spark's legacy zone forms (`GMT+1`, `UTC+1`, three-letter abbreviations like `PST`). Such timezones throw a native parse error at execution.
- [x] hour
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `case class Hour` extends `GetTimeField`.
  - Spark 4.0.1 (audited 2026-05-27): `case class Hour` is unchanged; parent `GetTimeField` trait refactored to override `nullIntolerant: Boolean = true` instead of mixing in `NullIntolerant` (no behavioural change).
  - Known divergence: for `TimestampNTZType` inputs Comet's native path applies session-timezone conversion (Spark treats `TIMESTAMP_NTZ` as wall-clock and ignores session timezone), so the returned hour can differ. Marked `Incompatible` and gated by `spark.comet.expr.allowIncompatible` (https://github.com/apache/datafusion-comet/issues/3180).
- [x] last_day
  - Spark 3.4.3 (audited 2026-05-27): baseline. `DateType -> DateType`; computes `DateTimeUtils.getLastDayOfMonth`; no ANSI branch.
  - Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
  - Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true`.
- [x] localtimestamp
- [x] make_date
  - Spark 3.4.3 (audited 2026-05-27): baseline. `(IntegerType, IntegerType, IntegerType) -> DateType`; under `spark.sql.ansi.enabled=true` invalid `(year, month, day)` throws `ansiDateTimeError`, else returns NULL. Documented valid year range is 1 to 9999.
  - Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
  - Spark 4.0.1 (audited 2026-05-27): error helper renamed to `ansiDateTimeArgumentOutOfRange`; behaviour otherwise unchanged.
  - Known divergence: `SparkMakeDate` in `native/spark-expr/src/datetime_funcs/make_date.rs` always returns NULL on invalid input and never raises, so Comet diverges from Spark when `spark.sql.ansi.enabled=true`. It also accepts year 0 and negative years (chrono's proleptic calendar) which Spark rejects.
- [ ] make_dt_interval
- [ ] make_interval
- [ ] make_time
- [x] make_timestamp
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. Septenary `(year, month, day, hour, min, sec[, timezone])` with `sec: DecimalType(16,6)`; honours `spark.sql.ansi.enabled` (throws on invalid input, else NULL); timezone input is `StringType`; result type follows `spark.sql.timestampType`.
  - Spark 4.0.1 (audited 2026-05-27): timezone input widened to `StringTypeWithCollation(supportsTrimCollation = true)`; ANSI error helpers renamed (`ansiDateTimeArgumentOutOfRange`, `invalidFractionOfSecondError(value)`); `NullIntolerant` trait replaced by `nullIntolerant: Boolean = true`; new sibling `TryMakeTimestamp` added but routed through a separate expression.
- [ ] make_timestamp_ltz
- [ ] make_timestamp_ntz
- [ ] make_ym_interval
- [x] minute
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `case class Minute` extends `GetTimeField`.
  - Spark 4.0.1 (audited 2026-05-27): `case class Minute` is unchanged; parent `GetTimeField` trait refactored to override `nullIntolerant: Boolean = true` instead of mixing in `NullIntolerant` (no behavioural change).
  - Known divergence: for `TimestampNTZType` inputs Comet's native path applies session-timezone conversion (Spark treats `TIMESTAMP_NTZ` as wall-clock and ignores session timezone), so the returned minute can differ. Marked `Incompatible` and gated by `spark.comet.expr.allowIncompatible` (https://github.com/apache/datafusion-comet/issues/3180).
- [x] month
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `Month extends GetDateField`; delegates to `DateTimeUtils.getMonth` via `LocalDate.getMonthValue` (1..12).
  - Spark 4.0.1 (audited 2026-05-27): identical semantics; `GetDateField` drops the `NullIntolerant` mixin in favour of `nullIntolerant: Boolean = true`.
- [ ] monthname
- [x] months_between
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. Ternary `(date1: Timestamp, date2: Timestamp, roundOff: Boolean)` returning `DoubleType`; `TimeZoneAwareExpression`; codegen delegates to `DateTimeUtils.monthsBetween`.
  - Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` trait dropped in favour of `nullIntolerant: Boolean = true` override; signature and runtime behaviour unchanged.
- [x] next_day
  - Spark 3.4.3 (audited 2026-05-27): baseline. `(DateType, StringType) -> DateType`; under `spark.sql.ansi.enabled=true` an unrecognised `dayOfWeek` throws `ansiIllegalArgumentError`, else returns NULL. Allowed tokens come from `DateTimeUtils.getDayOfWeekFromString` (`SU/SUN/SUNDAY`, `MO/MON/MONDAY`, ...), case-insensitive via `Locale.ROOT`, no trimming.
  - Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
  - Spark 4.0.1 (audited 2026-05-27): error type changed to `SparkIllegalArgumentException`; `inputTypes` now uses `StringTypeWithCollation(supportsTrimCollation = true)`.
  - Known divergence: `datafusion-spark::SparkNextDay` returns NULL for malformed `dayOfWeek` regardless of `spark.sql.ansi.enabled`, so ANSI mode does not throw. It also `trim()`s the day-of-week argument before matching, so `' MO '` succeeds natively while Spark would treat it as invalid.
- [ ] now
- [x] quarter
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `Quarter extends GetDateField`; returns 1..4 via `IsoFields.QUARTER_OF_YEAR`.
  - Spark 4.0.1 (audited 2026-05-27): identical semantics; `GetDateField` drops the `NullIntolerant` mixin in favour of `nullIntolerant: Boolean = true`.
- [x] second
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `case class Second` extends `GetTimeField`.
  - Spark 4.0.1 (audited 2026-05-27): `case class Second` is unchanged; parent `GetTimeField` trait refactored to override `nullIntolerant: Boolean = true` instead of mixing in `NullIntolerant` (no behavioural change).
  - Known divergence: for `TimestampNTZType` inputs Comet's native path applies session-timezone conversion (Spark treats `TIMESTAMP_NTZ` as wall-clock and ignores session timezone), so the returned second can differ. Marked `Incompatible` and gated by `spark.comet.expr.allowIncompatible` (https://github.com/apache/datafusion-comet/issues/3180).
- [ ] session_window
- [ ] time_diff
- [ ] time_trunc
- [x] timestamp_micros
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `MicrosToTimestamp` extends `IntegralToTimestampBase` with `upScaleFactor = 1`; accepts `IntegralType`, returns `TimestampType`; codegen is identity.
  - Spark 4.0.1 (audited 2026-05-27): `IntegralToTimestampBase` drops the `NullIntolerant` trait in favour of `nullIntolerant: Boolean = true`; behaviour unchanged.
- [x] timestamp_millis
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `MillisToTimestamp` extends `IntegralToTimestampBase` with `upScaleFactor = MICROS_PER_MILLIS (1000)`; multiply overflow throws via `Math.multiplyExact`.
  - Spark 4.0.1 (audited 2026-05-27): same as 3.5.8 modulo the `NullIntolerant` trait/method refactor in `IntegralToTimestampBase`.
- [x] timestamp_seconds
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `inputTypes = Seq(NumericType)` accepting integral, decimal, float, and double; integral values use `Math.multiplyExact` (overflow throws); float and double return NULL on NaN or Infinity.
  - Spark 4.0.1 (audited 2026-05-27): `nullIntolerant` set via override instead of the `NullIntolerant` trait; otherwise identical to 3.5.8.
  - Known divergence: Comet's Rust impl supports only Int32, Int64, Float32, and Float64. `DecimalType`, `ByteType`, and `ShortType` fall back to Spark. Int64 overflow returns a `ComputeError` matching Spark's `ArithmeticException`. NaN and Infinity map to NULL on the float and double paths.
- [ ] to_date
- [ ] to_time
- [ ] to_timestamp
- [ ] to_timestamp_ltz
- [ ] to_timestamp_ntz
- [x] to_unix_timestamp
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `inputTypes = Seq(TypeCollection(StringType, DateType, TimestampType, TimestampNTZType), StringType)`; returns `LongType` seconds; honours `spark.sql.ansi.enabled` (throws on parse error, else NULL); `TimeZoneAwareExpression`.
  - Spark 4.0.1 (audited 2026-05-27): both the value and the format argument become `StringTypeWithCollation(supportsTrimCollation = true)`; a new `suggestedFuncOnFail = "try_to_timestamp"` field is added on `ToTimestamp` (advisory).
  - Known divergence: routed through the JVM codegen dispatcher rather than a native kernel, so behaviour is bit-identical to Spark only when `spark.comet.exec.scalaUDF.codegen.enabled=true`; when the flag is off the operator falls back to Spark.
- [x] to_utc_timestamp
  - Spark 3.4.3 (audited 2026-05-12): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-12): baseline.
  - Spark 4.0.1 (audited 2026-05-12): `inputTypes` widened to `StringTypeWithCollation`; behaviour unchanged for ASCII timezone strings.
  - Known divergence: Comet's native timezone parser does not accept Spark's legacy zone forms (`GMT+1`, `UTC+1`, three-letter abbreviations like `PST`). Such timezones throw a native parse error at execution.
- [x] trunc
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `inputTypes = Seq(DateType, StringType)`; `parseTruncLevel` is case-insensitive and accepts `YEAR`/`YYYY`/`YY`, `QUARTER`, `MONTH`/`MM`/`MON`, `WEEK`. Unknown or sub-week levels (`DAY`, `HOUR`, ...) return NULL because `MIN_LEVEL_OF_DATE_TRUNC` is `TRUNC_TO_WEEK`.
  - Spark 4.0.1 (audited 2026-05-27): format `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`; truncation semantics unchanged for ASCII format strings.
  - Known divergence: Comet's native kernel raises an execution error for unknown format strings instead of returning NULL, so non-literal formats are flagged `Incompatible`. Sub-week formats such as `DAY`/`DD` are rejected with `Unsupported` (Spark would return NULL) and fall back to Spark.
- [ ] try_make_interval
- [ ] try_make_timestamp
- [ ] try_to_date
- [ ] try_to_time
- [ ] try_to_timestamp
- [x] unix_date
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `UnixDate(child)`: `inputTypes = Seq(DateType)`, `dataType = IntegerType`; `nullSafeEval` returns the underlying days-since-epoch int unchanged.
  - Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait is replaced by `nullIntolerant: Boolean = true`.
- [x] unix_micros
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `UnixMicros` extends `TimestampToLongBase` with `scaleFactor = 1`; codegen reduces to identity on the underlying micros.
  - Spark 4.0.1 (audited 2026-05-27): same as 3.5.8 modulo the `NullIntolerant` trait/method refactor.
- [x] unix_millis
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `UnixMillis` extends `TimestampToLongBase` with `scaleFactor = MICROS_PER_MILLIS`; floor-divides timestamp micros by 1000.
  - Spark 4.0.1 (audited 2026-05-27): same as 3.5.8 modulo the `NullIntolerant` trait/method refactor.
- [x] unix_seconds
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `UnixSeconds` extends `TimestampToLongBase` with `scaleFactor = MICROS_PER_SECOND`; accepts `TimestampType` only; returns `LongType`; floor-divides micros by the scale factor.
  - Spark 4.0.1 (audited 2026-05-27): `TimestampToLongBase` swaps the `NullIntolerant` trait for `nullIntolerant: Boolean = true`; numerics unchanged.
- [x] unix_timestamp
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. Inherits from `UnixTime` / `ToTimestamp`; `inputTypes = Seq(TypeCollection(StringType, DateType, TimestampType, TimestampNTZType), StringType)`; result is `LongType`; honours `failOnError` for ANSI parse errors on the string path.
  - Spark 4.0.1 (audited 2026-05-27): `ToTimestamp.inputTypes` widens the string slot to `StringTypeWithCollation(supportsTrimCollation = true)`; behaviour unchanged for non-collated strings.
  - Known divergence: Comet's native path only accepts `TimestampType`, `DateType`, and `TimestampNTZType` (string inputs fall back to Spark). For `TimestampType` and `DateType` the session timezone is applied via `array_with_timezone`; for `TimestampNTZType` the microsecond value is divided directly without timezone adjustment.
- [x] weekday
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `WeekDay extends GetDateField`; returns 0..6 with Monday=0 via `LocalDate.getDayOfWeek.ordinal()`.
  - Spark 4.0.1 (audited 2026-05-27): identical semantics; `GetDateField` drops the `NullIntolerant` mixin in favour of `nullIntolerant: Boolean = true`.
- [x] weekofyear
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `WeekOfYear extends GetDateField`; returns the ISO-8601 week-of-week-based-year via `IsoFields.WEEK_OF_WEEK_BASED_YEAR` (Monday start, week 1 has more than 3 days). Comet maps this to DataFusion's `datepart('week', ...)` which uses Arrow's `iso_week().week()`, matching Spark.
  - Spark 4.0.1 (audited 2026-05-27): identical semantics; `GetDateField` drops the `NullIntolerant` mixin in favour of `nullIntolerant: Boolean = true`.
- [ ] window
- [ ] window_time
- [x] year
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `Year extends GetDateField`; delegates to `DateTimeUtils.getYear` via `LocalDate.getYear`.
  - Spark 4.0.1 (audited 2026-05-27): identical semantics; `GetDateField` drops the `NullIntolerant` mixin in favour of `nullIntolerant: Boolean = true`.

### generator_funcs

- [ ] explode
- [ ] explode_outer
- [ ] inline
- [ ] inline_outer
- [ ] posexplode
- [ ] posexplode_outer
- [ ] stack

### hash_funcs

- [x] crc32
- [x] hash
- [x] md5
- [x] sha
- [x] sha1
- [x] sha2
- [x] xxhash64

### json_funcs

- [ ] from_json
- [x] get_json_object
- [ ] json_array_length
- [ ] json_object_keys
- [ ] json_tuple
- [ ] schema_of_json
- [ ] to_json

### lambda_funcs

- [ ] aggregate
- [ ] array_sort
- [ ] exists
- [ ] filter
- [ ] forall
- [ ] map_filter
- [ ] map_zip_with
- [ ] reduce
- [ ] transform
- [ ] transform_keys
- [ ] transform_values
- [ ] zip_with

### map_funcs

- [x] element_at
- [ ] map
- [ ] map_concat
- [x] map_contains_key
- [x] map_entries
- [x] map_from_arrays
- [x] map_from_entries
- [x] map_keys
- [x] map_values
- [x] str_to_map
- [ ] try_element_at

### math_funcs

- [x] `%`
- [x] `*`
- [x] `+`
- [x] `-`
- [x] `/`
- [x] abs
- [x] acos
- [x] acosh
- [x] asin
- [x] asinh
- [x] atan
- [x] atan2
- [x] atanh
- [x] bin
- [ ] bround
- [x] cbrt
- [x] ceil
- [x] ceiling
- [ ] conv
- [x] cos
- [x] cosh
- [x] cot
- [x] csc
- [x] degrees
- [x] div
- [ ] e
- [x] exp
- [x] expm1
- [x] factorial
  - 3.4.3 (audited 2026-05-15): identical to v3.5.8.
  - 3.5.8 (audited 2026-05-15): canonical reference; `extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant`. Returns NULL for NULL input or values outside `[0, 20]`.
  - 4.0.1 (audited 2026-05-15): `NullIntolerant` trait replaced by `nullIntolerant: Boolean` method override; behavior unchanged.
- [x] floor
- [x] greatest
- [x] hex
- [ ] hypot
- [x] least
- [x] ln
- [x] log
- [x] log10
- [ ] log1p
- [x] log2
- [x] mod
- [x] negative
- [x] pi
- [ ] pmod
- [x] positive
- [x] pow
- [x] power
- [x] radians
- [x] rand
- [x] randn
- [ ] random
- [ ] randstr
- [x] rint
- [x] round
- [x] sec
- [x] shiftleft
- [x] sign
- [x] signum
- [x] sin
- [x] sinh
- [x] sqrt
- [x] tan
- [x] tanh
- [x] try_add
- [x] try_divide
- [ ] try_mod
- [x] try_multiply
- [x] try_subtract
- [x] unhex
- [ ] uniform
- [x] width_bucket

### misc_funcs

- [ ] aes_decrypt
- [ ] aes_encrypt
- [ ] approx_top_k_estimate
- [ ] assert_true
- [ ] bitmap_and_agg
- [ ] bitmap_bit_position
- [ ] bitmap_bucket_number
- [ ] bitmap_construct_agg
- [ ] bitmap_count
- [ ] bitmap_or_agg
- [ ] current_catalog
- [ ] current_database
- [ ] current_schema
- [ ] current_user
- [ ] equal_null
- [ ] from_avro
- [ ] from_protobuf
- [ ] hll_sketch_estimate
- [ ] hll_union
- [ ] input_file_block_length
- [ ] input_file_block_start
- [ ] input_file_name
- [ ] is_variant_null
- [ ] java_method
- [x] monotonically_increasing_id
- [ ] parse_json
- [ ] raise_error
- [x] rand
- [x] randn
- [ ] reflect
- [ ] schema_of_avro
- [ ] schema_of_variant
- [ ] schema_of_variant_agg
- [ ] session_user
- [x] spark_partition_id
- [ ] st_asbinary
- [ ] st_geogfromwkb
- [ ] st_geomfromwkb
- [ ] st_setsrid
- [ ] st_srid
- [ ] theta_difference
- [ ] theta_intersection
- [ ] theta_sketch_estimate
- [ ] theta_union
- [ ] to_avro
- [ ] to_protobuf
- [ ] to_variant_object
- [ ] try_aes_decrypt
- [ ] try_parse_json
- [ ] try_reflect
- [ ] try_variant_get
- [ ] typeof
- [x] user
- [ ] uuid
- [ ] variant_get
- [ ] version

### predicate_funcs

- [x] `!`
- [x] `<`
- [x] `<=`
- [x] `<=>`
- [x] `=`
- [x] `==`
- [x] `>`
- [x] `>=`
- [x] and
- [x] between
- [x] ilike
- [x] in
- [x] isnan
- [x] isnotnull
- [x] isnull
- [x] like
- [x] not
- [x] or
- [ ] regexp
- [ ] regexp_like
- [x] rlike

### string_funcs

- [x] ascii
- [ ] base64
- [x] bit_length
- [x] btrim
- [x] char
- [x] char_length
- [x] character_length
- [x] chr
- [ ] collate
- [ ] collation
- [x] concat_ws
- [x] contains
- [x] decode
- [ ] elt
- [ ] encode
- [x] endswith
- [ ] find_in_set
- [ ] format_number
- [ ] format_string
- [x] initcap
- [x] instr
- [ ] is_valid_utf8
- [x] lcase
- [x] left
- [x] len
- [x] length
- [ ] levenshtein
- [ ] locate
- [x] lower
- [x] lpad
- [x] ltrim
- [ ] luhn_check
- [ ] make_valid_utf8
- [ ] mask
- [x] octet_length
- [ ] overlay
- [ ] position
- [ ] printf
- [ ] quote
- [ ] regexp_count
- [ ] regexp_extract
- [ ] regexp_extract_all
- [ ] regexp_instr
- [x] regexp_replace
- [ ] regexp_substr
- [x] repeat
- [x] replace
- [x] right
- [x] rpad
- [x] rtrim
- [ ] sentences
- [ ] soundex
- [x] space
- [x] split
- [ ] split_part
- [x] startswith
- [x] substr
- [x] substring
- [x] substring_index
- [ ] to_binary
- [ ] to_char
- [ ] to_number
- [ ] to_varchar
- [x] translate
- [x] trim
- [ ] try_to_binary
- [ ] try_to_number
- [ ] try_validate_utf8
- [x] ucase
- [ ] unbase64
- [x] upper
- [ ] validate_utf8

### struct_funcs

- [x] named_struct
- [x] struct

### url_funcs

- [x] parse_url (Incompatible: native diverges from Spark on edge cases)
- [x] try_url_decode
  - 4.0.1, 2026-05-05
- [x] url_decode
  - 3.4.3, 2026-04-29
  - 3.5.8, 2026-04-29
  - 4.0.1, 2026-04-29
- [x] url_encode
  - 3.4.3, 2026-04-29
  - 3.5.8, 2026-04-29
  - 4.0.1, 2026-04-29

### window_funcs

- [ ] cume_dist
- [ ] dense_rank
- [ ] lag
- [ ] lead
- [ ] nth_value
- [ ] ntile
- [ ] percent_rank
- [ ] rank
- [ ] row_number

### xml_funcs

- [ ] from_xml
- [ ] schema_of_xml
- [ ] to_xml
- [ ] xpath
- [ ] xpath_boolean
- [ ] xpath_double
- [ ] xpath_float
- [ ] xpath_int
- [ ] xpath_long
- [ ] xpath_number
- [ ] xpath_short
- [ ] xpath_string
