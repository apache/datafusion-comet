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
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringType -> IntegerType`; `nullSafeEval` returns `codePointAt(0)` of the first char, or `0` for the empty string. Wired via `CometScalarFunction("ascii")` and resolved to DataFusion `ascii` (`chars().next() as i32`); first-code-point semantics match for ASCII, BMP, and supplementary code points.
  - Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`; behaviour unchanged for `UTF8_BINARY`. Comet does not propagate collation, so non-default collations may diverge silently.
- [ ] base64
- [x] bit_length
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `(StringType|BinaryType) -> IntegerType`; eval returns `numBytes * 8` for strings and `.length * 8` for binary.
  - Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`; semantics unchanged.
  - Known limitation: wired as a raw `CometScalarFunction("bit_length")` with no `BinaryType` guard. DataFusion's `BitLengthFunc` signature only accepts string types, so `bit_length(<binary>)` execute-fails on the native side instead of falling back cleanly.
- [x] btrim
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringTrimBoth` is `RuntimeReplaceable` and rewritten to `StringTrim(srcStr, trimStr)` before serde runs, so the explicit `CometScalarFunction("btrim")` mapping is unreachable.
  - Spark 4.0.1 (audited 2026-05-27): `StringTrim` (the rewrite target) routes through `CollationSupport.StringTrim.exec` and uses `StringTypeNonCSAICollation(supportsTrimCollation = true)`; semantics unchanged for `UTF8_BINARY`. Non-default collations may diverge in Comet.
- [x] char
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `Chr(LongType) -> StringType`; `lon < 0` returns `""`, else `((lon & 0xFF) as char).toString` (so `chr(256)` and `chr(0)` both return `" "`).
  - Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true`. Resolves natively to `datafusion_spark::function::string::char::CharFunc`, which mirrors Spark's negative-input and `& 0xFF` semantics.
- [x] char_length
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): registry alias of `Length`. Same support as `length`.
  - Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Length`.
- [x] character_length
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): registry alias of `Length`. Same support as `length`.
  - Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Length`.
- [x] chr
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): registry alias of `Chr`. Same support as `char`.
  - Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Chr`.
- [ ] collate
- [ ] collation
- [x] concat_ws
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `Seq[Expression] -> StringType`; NULL separator yields NULL, NULL element values are skipped, children can be `StringType` or `ArrayType(StringType)`. Comet serde rewrites a NULL-literal separator to a NULL of the result type and bails out on all-foldable inputs so Spark's `ConstantFolding` handles them; otherwise delegates to DataFusion `concat_ws`.
  - Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeWithCollation` / `AbstractArrayType`; `dataType` becomes `children.head.dataType` (collation-derived). Semantics unchanged for `UTF8_BINARY`.
- [x] contains
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `UTF8String.contains` on `StringType`; the parser routes `(BinaryType, BinaryType)` to `BinaryPredicate`, so Comet only ever sees the String form.
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.Contains.exec(..., collationId)`; behaviour identical for `UTF8_BINARY`. Non-default collations not honoured by Comet.
- [x] decode
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringDecode(bin, charset)` evaluated directly; invalid sequences silently substitute replacement characters via `new String(bytes, charset)`.
  - Spark 4.0.1 (audited 2026-05-27): refactored to `RuntimeReplaceable` whose `replacement` is a `StaticInvoke(StringDecode.decode, bin, charset, legacyCharsets, legacyErrorAction)`; the 4-arg form raises on malformed input unless legacy flags are set.
  - Known limitations: Comet handles `decode` via `CommonStringExprs.stringDecode` from the version shims (no `CometExpressionSerde[StringDecode]` registration, so the function does not surface in the auto-generated compatibility docs). Only literal `charset = 'utf-8'` (case-insensitive) is supported; everything else falls back. The Spark 4.0 `legacyCharsets` / `legacyErrorAction` flags are ignored: Comet always lowers to `Cast(bin, StringType, TRY)`, so invalid UTF-8 yields NULL where Spark 3.x substitutes replacement characters and Spark 4.0 (non-legacy) raises.
- [ ] elt
- [ ] encode
- [x] endswith
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `UTF8String.endsWith` on `StringType`; binary form routed to `BinaryPredicate` before Comet.
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.EndsWith.exec`; semantics unchanged for `UTF8_BINARY`.
- [ ] find_in_set
- [ ] format_number
- [ ] format_string
- [x] initcap
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `string.toLowerCase.toTitleCase` on `UTF8String`; word boundary is Java `Character.isWhitespace`. Comet routes to DataFusion `initcap`, which splits on `!is_alphanumeric()` (hyphens, apostrophes, and punctuation all split words), so Comet is unconditionally `Incompatible` (https://github.com/apache/datafusion-comet/issues/1052).
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.InitCap.exec` (collation- and ICU-aware) and propagates `child.dataType`. Comet ignores collation; 3.x divergences persist plus collation/ICU mismatches.
- [x] instr
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringInstr(str, substr) -> IntegerType`; returns `string.indexOf(sub, 0) + 1` (1-based, 0 when not found, 1 on empty substring). Resolves to DataFusion `strpos` (alias `instr`) with matching semantics.
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringInstr.exec`; semantics unchanged for `UTF8_BINARY`.
- [ ] is_valid_utf8
- [x] lcase
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): registry alias of `Lower`. Same support as `lower`.
  - Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Lower`.
- [x] left
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `RuntimeReplaceable` with `replacement = Substring(str, Literal(1), len)`; accepts `StringType` or `BinaryType` plus `IntegerType`. Comet serde rewrites to a `Substring` proto with `start=1, len=lenValue`, requires `len` to be a `Literal`, and falls back otherwise.
  - Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened with `StringTypeWithCollation`; behaviour unchanged for `UTF8_BINARY`.
  - Known limitation: the literal-only `len` restriction is enforced inside `convert` via `withInfo` rather than declared in `getSupportLevel`, so EXPLAIN surfaces it only at conversion time.
- [x] len
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): registry alias of `Length`. Same support as `length`.
  - Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Length`.
- [x] length
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `(StringType|BinaryType) -> IntegerType`; eval returns `numChars` for strings and `.length` for binary.
  - Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`; semantics unchanged.
  - Known limitation: `BinaryType` input falls back to Spark via `Unsupported` (DataFusion's `character_length` accepts string types only). Non-default collations not branched.
- [ ] levenshtein
- [ ] locate
- [x] lower
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. JVM default-locale `toLowerCase` on `UTF8String`. Comet routes to DataFusion `lower` (Rust Unicode default case mapping, no locale awareness) and is gated off by default via `spark.comet.caseConversion.enabled=false`.
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.Lower.exec(v, collationId, useICU)` with `SQLConf.ICU_CASE_MAPPINGS_ENABLED`; `inputTypes` widened to `StringTypeWithCollation`. Comet ignores collation and ICU mode, so non-default collations or `ICU_CASE_MAPPINGS_ENABLED=true` diverge even when the case-conversion conf is enabled.
- [x] lpad
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringLPad(str, len, pad) -> StringType`; `len <= 0` returns the empty string, empty `pad` returns `str` unchanged, NULL inputs propagate. Comet serde requires `str` to be a column and `pad` to be a literal; otherwise falls back.
  - Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true`; `inputTypes` widened to `StringTypeWithCollation(supportsTrimCollation = true)`. Semantics unchanged for `UTF8_BINARY`.
  - Known limitation: `lpad(<binary>, ...)` is rewritten by Spark to `BinaryPad / StaticInvoke(ByteArray.lpad)` before serde runs and always falls back to Spark. Non-default collations on Spark 4.0 are not branched in Comet.
- [x] ltrim
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringTrimLeft` extends `String2TrimExpression`; no-arg form strips ASCII space `0x20` only. The two-arg parser form `ltrim(trimStr, srcStr)` is swapped to `(srcStr, Option(trimStr))` by Spark's secondary constructor, so children match DataFusion `ltrim(str, chars)`.
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringTrimLeft.exec`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured in Comet.
- [ ] luhn_check
- [ ] make_valid_utf8
- [ ] mask
- [x] octet_length
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `(StringType|BinaryType) -> IntegerType`; eval returns `numBytes` for strings and `.length` for binary.
  - Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeWithCollation`; semantics unchanged.
  - Known limitation: wired as a raw `CometScalarFunction("octet_length")` with no `BinaryType` guard. DataFusion's `OctetLengthFunc` signature only accepts string types, so `octet_length(<binary>)` execute-fails on the native side instead of falling back cleanly.
- [ ] overlay
- [ ] position
- [ ] printf
- [ ] quote
- [ ] regexp_count
- [ ] regexp_extract
- [ ] regexp_extract_all
- [ ] regexp_instr
- [x] regexp_replace
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `RegExpReplace(subject, regexp, rep, pos)` with foldable `pos > 0`; uses Java `Pattern`. Comet supports only `pos = 1` (other offsets fall back) and injects a `'g'` flag because DataFusion's `regexp_replace` stops at the first match by default.
  - Spark 4.0.1 (audited 2026-05-27): adds raw-string literal support at the parser level and `nullIntolerant: Boolean = true`; runtime semantics unchanged.
  - Known limitation: regex semantics differ (Rust `regex` crate vs Java `Pattern`); `RegExp.isSupportedPattern` currently returns `false` for every pattern, so the path always requires `spark.comet.expression.regexp.allowIncompatible=true`.
- [ ] regexp_substr
- [x] repeat
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringRepeat(str, times)` with `nullSafeEval(s, n) = s.repeat(n)`; `UTF8String.repeat` returns the empty string for `n <= 0`. Comet casts `times` to `LongType` and delegates to DataFusion `repeat`.
  - Spark 4.0.1 (audited 2026-05-27): adds `nullIntolerant: Boolean` field; `dataType` becomes `str.dataType` (collation-tracking). Semantics unchanged for `UTF8_BINARY`.
  - Known divergence: DataFusion `repeat` throws on negative counts instead of returning the empty string Spark produces. Currently surfaced via `getCompatibleNotes` only.
- [x] replace
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringReplace(src, search, replace)`; when `search` is empty, Spark returns `src` unchanged (short-circuit on `search.numBytes == 0`).
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringReplace.exec`; semantics unchanged for `UTF8_BINARY`.
  - Known divergence: DataFusion `replace` inserts `to` between every character (and at both ends) when `search` is empty (https://github.com/apache/datafusion-comet/issues/3344). Currently the support level is `Compatible`.
- [x] right
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `RuntimeReplaceable` with `replacement = If(IsNull(str), null, If(len <= 0, "", Substring(str, -len, len)))`; accepts `StringType` plus `IntegerType`. Comet serde rewrites positive `len` to a `Substring` proto with `start=-len, len=len`; for `len <= 0` it builds an `If(IsNull(str), null, "")` proto chain to preserve NULL propagation.
  - Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened with collation; uses `UnaryMinus(len, failOnError = false)` to avoid integer-overflow exceptions on `len = Int.MinValue`. Semantics unchanged for `UTF8_BINARY`.
  - Known limitation: the literal-only `len` restriction is enforced inside `convert` via `withInfo` rather than declared in `getSupportLevel`.
- [x] rpad
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringRPad(str, len, pad) -> StringType`; same edge-case behaviour as `lpad` (negative len, empty pad, NULL propagation). Comet serde requires column `str` and literal `pad`.
  - Spark 4.0.1 (audited 2026-05-27): same evolution as `lpad`; default-pad literal type tightened; semantics unchanged for `UTF8_BINARY`.
  - Known limitation: same `BinaryPad / StaticInvoke` rewrite as `lpad` causes `rpad(<binary>, ...)` to fall back. Non-default collations on Spark 4.0 are not branched.
- [x] rtrim
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringTrimRight` extends `String2TrimExpression`; semantically symmetric to `ltrim`.
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringTrimRight.exec`; semantics unchanged for `UTF8_BINARY`.
- [ ] sentences
- [ ] soundex
- [x] space
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringSpace(IntegerType) -> StringType`; negative input yields the empty string. Resolves natively to `datafusion_spark::function::string::space::SparkSpace`.
  - Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `nullIntolerant: Boolean` override.
- [x] split
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringSplit(str, regex, limit)`; `limit > 0` permits at most `limit-1` splits, `limit <= 0` is unlimited. Comet registers `split` as a custom UDF (`native/spark-expr/src/string_funcs/split.rs`) using the Rust `regex` crate, and is unconditionally `Incompatible` due to regex-engine differences.
  - Spark 4.0.1 (audited 2026-05-27): wraps the regex via `CollationSupport.collationAwareRegex` and changes `dataType` to `ArrayType(str.dataType, ...)`. Comet does not honour collation flags.
- [ ] split_part
- [x] startswith
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `UTF8String.startsWith` on `StringType`; binary form routed to `BinaryPredicate`.
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StartsWith.exec`; semantics unchanged for `UTF8_BINARY`.
- [x] substr
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): registry alias of `Substring`. Same support as `substring`.
  - Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Substring`.
- [x] substring
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `TernaryExpression`; two-arg form defaults `len = Integer.MAX_VALUE`; supports `StringType` and `BinaryType`. Comet serializes to a dedicated `Substring` proto and requires both `pos` and `len` to be `Literal`.
  - Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened with `StringTypeWithCollation`; semantics unchanged for `UTF8_BINARY`. Native `SubstringExpr` implements Spark's negative-start clamping and is exercised against ASCII, multibyte UTF-8, emoji, decomposed and Telugu inputs.
  - Known limitation: the literal-only `pos`/`len` restriction is enforced inside `convert` rather than `getSupportLevel`.
- [x] substring_index
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `TernaryExpression(StringType, StringType, IntegerType) -> StringType`. Comet casts `count` to `LongType` and delegates to DataFusion's `substr_index` UDF (alias `substring_index`).
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.SubstringIndex.exec` and propagates `strExpr.dataType`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured by Comet.
- [ ] to_binary
- [ ] to_char
- [ ] to_number
- [ ] to_varchar
- [x] translate
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringTranslate(src, from, to)`; `UTF8String.translate(dict)` is code-point based, and any character mapped explicitly to ` ` in `to` is also deleted.
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringTranslate.exec`; semantics unchanged for `UTF8_BINARY`.
  - Known divergence: DataFusion's `translate` is grapheme-based (Spark uses code points), and does not delete characters mapped to ` ` in `to`. Currently the support level is `Compatible`.
- [x] trim
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. `StringTrim` no-arg form strips ASCII space `0x20` only (matches DataFusion `btrim`'s default); two-arg form's children are `(srcStr, trimStr)` after Spark's secondary-constructor swap.
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.StringTrim.exec` and uses `StringTypeNonCSAICollation`; semantics unchanged for `UTF8_BINARY`. Non-default collations not honoured in Comet.
- [ ] try_to_binary
- [ ] try_to_number
- [ ] try_validate_utf8
- [x] ucase
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): registry alias of `Upper`. Same support as `upper`.
  - Spark 4.0.1 (audited 2026-05-27): unchanged alias of `Upper`.
- [ ] unbase64
- [x] upper
  - Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
  - Spark 3.5.8 (audited 2026-05-27): baseline. JVM default-locale `toUpperCase` on `UTF8String`. Comet routes to DataFusion `upper` (Rust Unicode default case mapping, no locale awareness) and is gated off by default via `spark.comet.caseConversion.enabled=false`.
  - Spark 4.0.1 (audited 2026-05-27): routes through `CollationSupport.Upper.exec(v, collationId, useICU)` with `SQLConf.ICU_CASE_MAPPINGS_ENABLED`. Comet does not propagate collation or ICU mode; non-default collations or `ICU_CASE_MAPPINGS_ENABLED=true` diverge even when the case-conversion conf is enabled.
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
