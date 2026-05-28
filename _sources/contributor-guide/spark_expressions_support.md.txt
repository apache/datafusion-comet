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
  - Spark 3.4.3 (audited 2026-05-26): registered as a SQL alias of `BoolOr`, which extends `RuntimeReplaceableAggregate` with `replacement = Max(child)`. Catalyst rewrites `any(x)` to `max(x)` before Comet sees the plan, so `any` is served by `CometMax` on a `BooleanType` column.
  - Spark 3.5.8 (audited 2026-05-26): identical to 3.4.3.
  - Spark 4.0.1 (audited 2026-05-26): identical to 3.4.3.
- [x] any_value
- [ ] approx_count_distinct
- [ ] approx_percentile
- [ ] approx_top_k
- [ ] approx_top_k_accumulate
- [ ] approx_top_k_combine
- [ ] array_agg
- [x] avg
  - Spark 3.4.3 (2026-05-26)
  - Spark 3.5.8 (2026-05-26): aggregate logic identical to 3.4.3
  - Spark 4.0.1 (2026-05-26): aggregate logic identical to 3.5.8; only `QueryContext` import path differs. `YearMonthIntervalType` and `DayTimeIntervalType` inputs (supported by Spark) fall back to Spark in Comet.
- [x] bit_and
  - Spark 3.4.3 (2026-05-26)
  - Spark 3.5.8 (2026-05-26)
  - Spark 4.0.1 (2026-05-26)
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
- [x] shiftrightunsigned
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
- [x] convert_timezone
- [ ] curdate
- [ ] current_date
- [ ] current_time
- [ ] current_timestamp
- [x] current_timezone
- [x] date_add
- [x] date_diff
- [x] date_format
- [x] date_from_unix_date
- [x] date_part
- [x] date_sub
- [x] date_trunc
- [x] dateadd
- [x] datediff
- [x] datepart
- [x] day
- [ ] dayname
- [x] dayofmonth
- [x] dayofweek
- [x] dayofyear
- [x] extract
- [x] from_unixtime
- [x] from_utc_timestamp
  - Spark 3.4.3 (audited 2026-05-12): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-12): baseline.
  - Spark 4.0.1 (audited 2026-05-12): `inputTypes` widened to `StringTypeWithCollation`; behaviour unchanged for ASCII timezone strings.
  - Known divergence: Comet's native timezone parser does not accept Spark's legacy zone forms (`GMT+1`, `UTC+1`, three-letter abbreviations like `PST`). Such timezones throw a native parse error at execution.
- [x] hour
- [x] last_day
- [x] localtimestamp
- [x] make_date
- [ ] make_dt_interval
- [ ] make_interval
- [ ] make_time
- [x] make_timestamp
- [ ] make_timestamp_ltz
- [ ] make_timestamp_ntz
- [ ] make_ym_interval
- [x] minute
- [x] month
- [ ] monthname
- [x] months_between
- [x] next_day
- [ ] now
- [x] quarter
- [x] second
- [ ] session_window
- [ ] time_diff
- [ ] time_trunc
- [x] timestamp_micros
- [x] timestamp_millis
- [x] timestamp_seconds
- [ ] to_date
- [ ] to_time
- [ ] to_timestamp
- [ ] to_timestamp_ltz
- [ ] to_timestamp_ntz
- [x] to_unix_timestamp
- [x] to_utc_timestamp
  - Spark 3.4.3 (audited 2026-05-12): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-12): baseline.
  - Spark 4.0.1 (audited 2026-05-12): `inputTypes` widened to `StringTypeWithCollation`; behaviour unchanged for ASCII timezone strings.
  - Known divergence: Comet's native timezone parser does not accept Spark's legacy zone forms (`GMT+1`, `UTC+1`, three-letter abbreviations like `PST`). Such timezones throw a native parse error at execution.
- [x] trunc
- [ ] try_make_interval
- [ ] try_make_timestamp
- [ ] try_to_date
- [ ] try_to_time
- [ ] try_to_timestamp
- [x] unix_date
- [x] unix_micros
- [x] unix_millis
- [x] unix_seconds
- [x] unix_timestamp
- [x] weekday
- [x] weekofyear
- [ ] window
- [ ] window_time
- [x] year

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
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Remainder(left, right, evalMode)` signature identical across versions. Native path uses Rust `spark_modulo` UDF; non-ANSI returns NULL on divide-by-zero, ANSI raises `DIVIDE_BY_ZERO` / `REMAINDER_BY_ZERO`. `CometRemainder` rejects `EvalMode.TRY`, so `try_mod` (Spark 4.0+) falls back to Spark (https://github.com/apache/datafusion-comet/issues/4484).
- [x] `*`
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Multiply(left, right, evalMode)` signature identical. Decimal results exceeding `DECIMAL128_MAX_PRECISION` go through `WideDecimalBinaryExpr` (Decimal256 intermediate); smaller decimals and primitives use DataFusion `BinaryExpr`. ANSI integer overflow uses Rust `checked_mul`. Interval multiplication falls back.
- [x] `+`
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Add(left, right, evalMode)` with the same Decimal / ANSI plumbing as `*`. `Date + Int8/16/32` dispatches to the Rust `date_add` UDF to work around DataFusion's Date32 + Interval-only kernel.
- [x] `-`
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Subtract(left, right, evalMode)` mirrors `+`. `Date - Int8/16/32` uses the Rust `date_sub` UDF.
- [x] `/`
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Divide(left, right, evalMode)`. Non-ANSI mode wraps the divisor in `If(EqualTo(right, 0), null, right)` so DataFusion never throws. Decimal output is wrapped in `CheckOverflow(failOnError = ANSI)`; ANSI surfaces `NUMERIC_VALUE_OUT_OF_RANGE`, non-ANSI returns NULL.
- [x] abs
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Abs(child, failOnError)` over `NumericType` plus the two interval types. `failOnError` (ANSI) is propagated to the native `abs` UDF, which throws `ARITHMETIC_OVERFLOW` on `Int.MinValue` / `Long.MinValue` / Decimal MIN. `DayTimeIntervalType` and `YearMonthIntervalType` fall back to Spark. Spark 4.0 / 4.1 do the `NullIntolerant` -> `nullIntolerant: Boolean` refactor; behaviour unchanged.
- [x] acos
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.acos, "ACOS")` unchanged across versions; wired as `CometScalarFunction("acos")` to DataFusion's `acos` UDF. NaN for `|x| > 1`.
- [x] acosh
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): custom `StrictMath.log(x + sqrt(x*x - 1))` unchanged across versions. NaN for `x < 1`. Routes to DataFusion's `acosh`.
- [x] asin
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.asin, "ASIN")` unchanged. NaN for `|x| > 1`.
- [x] asinh
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): special-cases `Double.NegativeInfinity` to avoid `log(NaN)`, otherwise `StrictMath.log(x + sqrt(x*x + 1))`. Identical across versions.
- [x] atan
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.atan, "ATAN")` unchanged.
- [x] atan2
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `BinaryMathExpression(math.atan2, "ATAN2")` with both inputs adjusted by `+0.0` to flip `-0.0` to `+0.0`. `CometAtan2` reproduces this by wrapping each child in `Add(child, Literal.default(child.dataType))` before dispatching to DataFusion's `atan2`.
- [x] atanh
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): custom `0.5 * (log1p(x) - log1p(-x))` (SPARK-28519). NaN for `|x| > 1`, +/-Infinity for `x = +/-1`.
- [x] bin
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Bin(child)` over `LongType -> StringType`. Spark 4.x gains `DefaultStringProducingExpression` and the `nullIntolerant: Boolean` refactor; no behaviour change. Routes to datafusion-spark `SparkBin`.
- [ ] bround
- [x] cbrt
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): passthrough to DataFusion `cbrt`.
- [x] ceil
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): one-arg `ceil(expr)` supported (`LongType` / `DoubleType` / `DecimalType` with scale >= 0). Decimal with negative scale falls back at convert time. The two-arg `ceil(expr, scale)` form (`RoundCeil`) is not wired and falls back to Spark.
- [x] ceiling
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): registry alias for `Ceil`. Same support as `ceil`.
- [ ] conv
- [x] cos
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.cos, "COS")` unchanged across versions.
- [x] cosh
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.cosh, "COSH")` unchanged.
- [x] cot
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): custom `1 / math.tan(x)`. DataFusion's `cot` is also `1.0 / tan(x)`, so the result matches.
- [x] csc
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): custom `1 / math.sin(x)`. Routed to datafusion-spark's `SparkCsc` (registered in `jni_api.rs`).
- [x] degrees
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.toDegrees, "DEGREES")` unchanged across versions.
- [x] div
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `IntegralDivide(left, right, evalMode)`. Non-decimal operands are cast to `DecimalType(19, 0)`; result is recomputed per `IntegralDivide.resultDecimalType`, wrapped in `CheckOverflow`, then cast to `Long`. ANSI overflow for `Long.MinValue div -1` and decimal-overflow ANSI cases are covered by existing tests.
- [ ] e
- [x] exp
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(StrictMath.exp, "EXP")` unchanged. ULP-level differences vs DataFusion `exp` are possible but unflagged.
- [x] expm1
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(StrictMath.expm1, "EXPM1")` unchanged.
- [x] factorial
  - 3.4.3 (audited 2026-05-15): identical to v3.5.8.
  - 3.5.8 (audited 2026-05-15): canonical reference; `extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant`. Returns NULL for NULL input or values outside `[0, 20]`.
  - 4.0.1 (audited 2026-05-15): `NullIntolerant` trait replaced by `nullIntolerant: Boolean` method override; behavior unchanged.
  - 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- [x] floor
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): mirror of `ceil`. Two-arg `floor(expr, scale)` form (`RoundFloor`) falls back to Spark.
- [x] greatest
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): NULL-skipping variadic. Wired as `CometScalarFunction("greatest")` to DataFusion's `GreatestFunc`. Comet does not gate input types, so interval inputs and other Spark-only orderings rely on the native UDF accepting them; no explicit fallback path.
- [x] hex
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): accepts `LongType` / `BinaryType` / `StringType`. Spark 4.x widens `StringType` to `StringTypeWithCollation` and preserves collation in `dataType`; `CometHex` passes `expr.dataType` to native `SparkHex`, which always returns `Utf8` -- collation propagation may diverge on Spark 4.x.
- [ ] hypot
- [x] least
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): mirror of `greatest`; same caveats. Spark 4.1.1 adds `contextIndependentFoldable` (no Comet impact).
- [x] ln
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): registry alias for `Log`. Comet wires through `CometLog` to DataFusion `ln` with a `nullIfNegative` rewrite to match Spark's NULL behaviour for `x <= 0`.
- [x] log
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): one-arg `log(x)` -> `CometLog` (DataFusion `ln`); two-arg `log(base, x)` -> `CometLogarithm` (custom `spark_log` UDF, returns NULL when `base <= 0` or `x <= 0` to match `Logarithm.nullSafeEval`).
- [x] log10
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryLogExpression(StrictMath.log10, "LOG10")`; returns NULL for `x <= 0`. Possible ULP differences from `StrictMath`.
- [ ] log1p
- [x] log2
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryLogExpression(StrictMath.log(x) / StrictMath.log(2), "LOG2")`; returns NULL for `x <= 0`.
- [x] mod
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): registry alias for `Remainder`. Same support as `%`.
- [x] negative
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMinus(child, failOnError)` -> Rust `NegativeExpr`. ANSI overflow is detected for `Int8/Int16/Int32/Int64` and `IntervalYearMonth/IntervalDayTime`. Float / Double / Decimal cannot overflow on negate. Spark 4.0 `NullIntolerant` -> `nullIntolerant: Boolean` refactor; no impact.
- [x] pi
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `LeafMathExpression(math.Pi, "PI")`; foldable, so Spark `ConstantFolding` rewrites it to a `Literal` before Comet sees the plan. The `CometScalarFunction("pi")` registration is exercised only when `ConstantFolding` is excluded.
- [ ] pmod
- [x] positive
  - Spark 3.4.3, 3.5.8 (audited 2026-05-27): `UnaryPositive(child)` is a regular expression. There is no Comet serde for `UnaryPositive`, so projections containing `+col` silently disable Comet for the projection on 3.4/3.5.
  - Spark 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryPositive` is `RuntimeReplaceable` with `replacement = child`; the optimizer removes it before Comet sees the plan, so the gap is transparent on 4.x.
- [x] pow
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Pow(left, right) extends BinaryMathExpression(StrictMath.pow, "POWER")`; routes to DataFusion `pow`. ULP-level differences possible.
- [x] power
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): registry alias for `Pow`. Same support as `pow`.
- [x] radians
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.toRadians, "RADIANS")` unchanged across versions.
- [x] rand
  - See `misc_funcs / rand`.
- [x] randn
  - See `misc_funcs / randn`.
- [ ] random
- [ ] randstr
- [x] rint
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.rint, "ROUND")` with `funcName = "rint"`. Passthrough to DataFusion `rint` (round-half-to-even).
- [x] round
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): HALF_UP rounding for integer / decimal. Float / Double child types always fall back to Spark because `BigDecimal`-via-`toString` rounding cannot be precisely matched (documented inline in `CometRound`). ANSI `failOnError` is propagated for integer overflow. `BRound` (HALF_EVEN) is not wired.
- [x] sec
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): custom `1 / math.cos(x)`. Routed to datafusion-spark's `SparkSec`.
- [x] shiftleft
  - See `bitwise_funcs / <<` (audited in PR #4479). Same support as the operator alias added in 4.0.
- [x] sign
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): registry alias for `Signum`. Same support as `signum`.
- [x] signum
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Signum(child)` over `DoubleType`. Spark also restricts to the two interval types via `inputTypes`; Comet handles only the `Double` case via DataFusion `signum`.
- [x] sin
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.sin, "SIN")` unchanged.
- [x] sinh
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.sinh, "SINH")` unchanged.
- [x] sqrt
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.sqrt, "SQRT")` unchanged.
- [x] tan
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.tan, "TAN")` unchanged.
- [x] tanh
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `UnaryMathExpression(math.tanh, "TANH")` unchanged.
- [x] try_add
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `TryAdd` is `RuntimeReplaceable` and rewrites to `Add(.., EvalMode.TRY)` for numeric inputs (datetime / interval go through `TryEval(Add(.., ANSI))` and fall back). Numeric path uses the Rust `checked_add` UDF, returning NULL on overflow. Decimal goes through `WideDecimalBinaryExpr` with `EvalMode.Try`.
- [x] try_divide
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `TryDivide` rewrites to `Divide(.., EvalMode.TRY)`. The `nullIfWhenPrimitive` wrapper swaps zero divisors to NULL; integer / float divide uses `checked_div`; decimal uses `decimal_div` + `CheckOverflow(failOnError = false)` returning NULL.
- [ ] try_mod
- [x] try_multiply
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): rewrites to `Multiply(.., EvalMode.TRY)`. Integer path uses `checked_mul`; decimal uses `WideDecimalBinaryExpr` with `EvalMode.Try`, returning NULL on overflow.
- [x] try_subtract
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): rewrites to `Subtract(.., EvalMode.TRY)`. Integer path uses `checked_sub`; decimal uses `WideDecimalBinaryExpr` as needed.
- [x] unhex
  - Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1 (audited 2026-05-27): `Unhex(child, failOnError)`. Spark 4.x widens input to `StringTypeWithCollation` and wraps the inner call in try/catch; Comet `CometUnhex` forwards `failOnError` to native `spark_unhex` but does not gate on collation.
- [ ] uniform
- [x] width_bucket
  - Spark 3.5.8 (audited 2026-05-27): introduced; not available in 3.4.3.
  - Spark 4.0.1, 4.1.1 (audited 2026-05-27): same semantics; `NullIntolerant` -> `nullIntolerant: Boolean` refactor.
  - Known limitation: wired via per-version `CometExprShim` rather than a `CometExpressionSerde`, so it bypasses the support-level framework and the auto-generated compatibility doc (https://github.com/apache/datafusion-comet/issues/4485). Native path uses datafusion-spark `SparkWidthBucket`; interval input types are not exercised by Comet tests.

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
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `CreateNamedStruct(children)` with `NoThrow`; `children` is `Seq(name1, val1, name2, val2, ...)`. Names must be foldable `StringType` and distinct; non-null. `dataType` is the `StructType` of the resulting fields. Comet routes via `CometCreateNamedStruct`, builds a `CreateNamedStruct` proto, and the native `CreateNamedStruct` expression (in `native/spark-expr/src/struct_funcs/create_named_struct.rs`) emits a `StructArray`.
  - Spark 4.0.1 (audited 2026-05-27): semantics unchanged; an `override def stateful: Boolean = true` flag is added for the optimizer. Parser-level Alias unwrapping (`case (a: Alias, _) => Seq(Literal(a.name), a)`) is transparent to Comet.
  - Spark 4.1.1 (audited 2026-05-27): semantics unchanged; adds `override def contextIndependentFoldable: Boolean = children.forall(_.contextIndependentFoldable)` for the new constant-folding pass; no impact on Comet conversion.
  - Comet limitation: a `CreateNamedStruct` whose names contain duplicates falls back to Spark. (Spark's analyzer also tolerates duplicates at the column-name level, but the proto layer would lose them, so the fallback is the safe choice.)
- [x] struct
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): the SQL function `struct(a, b, c)` is built by `CreateStruct.create`, which lowers to a `CreateNamedStruct` with synthetic field names `col1`, `col2`, etc. Comet handles both `struct` and `named_struct` via the same `CometCreateNamedStruct` path; the synthetic-name and explicit-name cases behave identically.
  - Spark 4.0.1 (audited 2026-05-27): same lowering; identical to 3.5.8.
  - Spark 4.1.1 (audited 2026-05-27): same lowering; identical to 3.5.8.

### url_funcs

- [x] parse_url
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
