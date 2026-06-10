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

# Spark Expression Support

This page is the complete reference for how Apache Comet handles each Spark built-in
expression. Comet accelerates expressions either with a native (Rust) implementation or by
dispatching to a Spark-compatible codegen path. When an expression is not supported, Comet
transparently falls back to Spark for that part of the plan; results are unaffected.

Expressions marked ✅ Supported are enabled by default and produce Spark-compatible results.
Expressions marked ⚠️ Incorrect by default run natively by default but can return results that
differ from Spark on some inputs; see the linked detail on each affected row.

Some ✅ Supported expressions have specific incompatible cases that fall back to Spark by
default. Those cases must be opted into per expression with
`spark.comet.expression.EXPRNAME.allowIncompatible=true` (where `EXPRNAME` is the Spark
expression class name, for example `Cast`). There is no global opt-in.

Most expressions can also be disabled with `spark.comet.expression.EXPRNAME.enabled=false`, where
`EXPRNAME` is the Spark expression class name (for example `Length` or `StartsWith`). See the
[Comet Configuration Guide](configs.md) for the full list.

## Status legend

| Status | Meaning |
| --- | --- |
| ✅ Supported | Comet produces Spark-compatible results by default. Some inputs or forms may fall back to Spark, and any incompatible behavior is opt-in (off by default). |
| ⚠️ Incorrect by default | Comet runs natively by default but can return results that differ from Spark (a wrong value, or a native error on valid input). See the linked detail on each row. |
| 🔜 Planned | Intended; tracked by an open issue or pull request. |
| 💤 Not currently planned | Not on the current roadmap; falls back to Spark and may be reconsidered later. |

## Not currently planned

Comet focuses acceleration on mainstream relational, string, datetime, math, and collection
expressions. The following function families are **not currently planned** for native acceleration (they are not on the 1.0 roadmap): specialized functionality with narrow real-world analytics use and high implementation cost. They fall back to Spark and may be reconsidered based on demand:

- **Probabilistic sketches and approximate top-k** (`kll_sketch_*`, `hll_*`, `theta_*`, `count_min_sketch`, `bitmap_*`, `approx_top_k*`): specialized data structures with exact-correctness traps.
- **XML / XPath** (`from_xml`, `to_xml`, `schema_of_xml`, `xpath*`): legacy text format, rare in accelerated workloads.
- **Geospatial** (`st_*`): brand-new Spark 4.1 functionality, specialized.
- **Avro / Protobuf codecs** (`from_avro`, `to_avro`, `from_protobuf`, `to_protobuf`, `schema_of_avro`): format conversion belongs at the IO layer, not expression evaluation.
- **JVM reflection** (`java_method`, `reflect`): niche, and they invoke arbitrary JVM methods (a security concern).
- **CSV functions** (`from_csv`, `to_csv`, `schema_of_csv`): row-level CSV parsing and formatting in expressions is niche and better handled at the data source layer.
- **UTF-8 validation** (`is_valid_utf8`, `make_valid_utf8`, `validate_utf8`, `try_validate_utf8`): niche Spark 4.x string-validation helpers.
- **File metadata** (`input_file_name`, `input_file_block_start`, `input_file_block_length`): require scan-internal per-row file information, outside the expression layer.
- **Miscellaneous niche** (`histogram_numeric`, `version`, `sentences`, `quote`): low-value or specialized functions with little benefit from native acceleration.

Note that `approx_count_distinct`, `median`, and `mode` are planned: they are mainstream (`median` and `mode` are exact aggregates). `approx_percentile` / `percentile_approx` are not currently planned because their approximate results cannot be made bit-identical to Spark.

The tables below list every Spark built-in expression with its current status.

## agg_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `any` | ✅ |  |
| `any_value` | ✅ |  |
| `approx_count_distinct` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `array_agg` | 🔜 | Array aggregate (related to `collect_list`, [#2524](https://github.com/apache/datafusion-comet/issues/2524)) |
| `avg` | ✅ | Interval types fall back |
| `bit_and` | ✅ |  |
| `bit_or` | ✅ |  |
| `bit_xor` | ✅ |  |
| `bool_and` | ✅ |  |
| `bool_or` | ✅ |  |
| `collect_list` | 🔜 | [#2524](https://github.com/apache/datafusion-comet/issues/2524) |
| `collect_set` | ✅ |  |
| `corr` | ✅ |  |
| `count` | ✅ |  |
| `count_if` | ✅ |  |
| `covar_pop` | ✅ |  |
| `covar_samp` | ✅ |  |
| `every` | ✅ |  |
| `first` | ✅ |  |
| `first_value` | ✅ |  |
| `grouping` | 🔜 | Grouping indicator for ROLLUP/CUBE/GROUPING SETS |
| `grouping_id` | 🔜 | Grouping indicator for ROLLUP/CUBE/GROUPING SETS |
| `kurtosis` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `last` | ✅ |  |
| `last_value` | ✅ |  |
| `listagg` | 🔜 | String aggregation |
| `max` | ✅ |  |
| `max_by` | 🔜 | [#3841](https://github.com/apache/datafusion-comet/issues/3841) |
| `mean` | ✅ |  |
| `median` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `min` | ✅ |  |
| `min_by` | 🔜 | [#3841](https://github.com/apache/datafusion-comet/issues/3841) |
| `mode` | 🔜 | [#3970](https://github.com/apache/datafusion-comet/issues/3970) |
| `percentile` | 🔜 | [#4542](https://github.com/apache/datafusion-comet/issues/4542) |
| `percentile_cont` | 🔜 | Percentile aggregate |
| `percentile_disc` | 🔜 | Percentile aggregate |
| `regr_avgx` | ✅ | Native: Spark rewrites to `Average` (tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551)) |
| `regr_avgy` | ✅ | Native: Spark rewrites to `Average` (tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551)) |
| `regr_count` | ✅ | Native: Spark rewrites to `Count` (tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551)) |
| `regr_intercept` | 🔜 | Falls back; can reuse `covar_pop`/`var_pop` accumulators ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `regr_r2` | 🔜 | Falls back; can reuse the `corr` accumulator ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `regr_slope` | 🔜 | Falls back; can reuse `covar_pop`/`var_pop` accumulators ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `regr_sxx` | 🔜 | Falls back; can reuse `var_pop` accumulator ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `regr_sxy` | 🔜 | Falls back; can reuse `covar_pop` accumulator ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `regr_syy` | 🔜 | Falls back; can reuse `var_pop` accumulator ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `skewness` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `some` | ✅ |  |
| `std` | ✅ |  |
| `stddev` | ✅ |  |
| `stddev_pop` | ✅ |  |
| `stddev_samp` | ✅ |  |
| `string_agg` | 🔜 | String aggregation (alias of `listagg`) |
| `sum` | ✅ |  |
| `try_avg` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `try_sum` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `var_pop` | ✅ |  |
| `var_samp` | ✅ |  |
| `variance` | ✅ |  |

---

## array_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `array` | ✅ |  |
| `array_append` | ✅ |  |
| `array_compact` | ✅ |  |
| `array_contains` | ✅ | NaN/signed-zero handling may differ ([details](compatibility/floating-point.md)) |
| `array_distinct` | ✅ | NaN/signed-zero handling may differ ([details](compatibility/floating-point.md)) |
| `array_except` | ✅ | Incompatible; falls back by default ([details](compatibility/expressions/array.md)) |
| `array_insert` | ✅ |  |
| `array_intersect` | ✅ | Incompatible; falls back by default ([details](compatibility/expressions/array.md)) |
| `array_join` | ✅ | Incompatible; falls back by default ([details](compatibility/expressions/array.md)) |
| `array_max` | ✅ | NaN ordering may differ ([details](compatibility/floating-point.md)) |
| `array_min` | ✅ | NaN ordering may differ ([details](compatibility/floating-point.md)) |
| `array_position` | ✅ | Binary/struct/map/null elements fall back |
| `array_prepend` | 🔜 | Sibling of `array_append` |
| `array_remove` | ✅ |  |
| `array_repeat` | ✅ |  |
| `array_union` | ✅ | NaN/signed-zero handling may differ ([details](compatibility/floating-point.md)) |
| `arrays_overlap` | ✅ |  |
| `arrays_zip` | ✅ |  |
| `element_at` | ✅ | MapType input falls back |
| `flatten` | ✅ | Binary/struct/map elements fall back |
| `get` | ✅ |  |
| `sequence` | ✅ |  |
| `shuffle` | 🔜 | Random array shuffle |
| `slice` | ✅ | Native ([#4149](https://github.com/apache/datafusion-comet/issues/4149)) |
| `sort_array` | ✅ | Nested struct/null arrays fall back |

---

## bitwise_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `&` | ✅ |  |
| `<<` | ✅ |  |
| `>>` | ✅ |  |
| `>>>` | ✅ | Operator alias for `shiftrightunsigned` (Spark 4.0+) |
| `^` | ✅ |  |
| `bit_count` | ✅ |  |
| `bit_get` | ✅ |  |
| `getbit` | ✅ |  |
| `shiftright` | ✅ |  |
| `shiftrightunsigned` | ✅ |  |
| `\|` | ✅ |  |
| `~` | ✅ |  |

---

## collection_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `array_size` | ✅ |  |
| `cardinality` | ✅ | MapType input falls back |
| `concat` | ✅ | Binary/array children fall back |
| `reverse` | ✅ | Binary-element arrays fall back (Incompatible) ([details](compatibility/expressions/array.md)) |
| `size` | ✅ | MapType input falls back |

---

## conditional_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `coalesce` | ✅ |  |
| `if` | ✅ |  |
| `ifnull` | ✅ |  |
| `nanvl` | ✅ |  |
| `nullif` | ✅ |  |
| `nullifzero` | ✅ | Lowers to `if`/`=` (Spark 4.0+) |
| `nvl` | ✅ |  |
| `nvl2` | ✅ |  |
| `when` | ✅ |  |
| `zeroifnull` | ✅ | Lowers to `coalesce` (Spark 4.0+) |

---

## conversion_funcs

The type-name conversion functions (`bigint`, `binary`, `boolean`, `date`, `decimal`, `double`, `float`, `int`, `smallint`, `string`, `timestamp`, `tinyint`) are SQL aliases for `CAST(... AS <type>)` and share the support and caveats of `cast`.

| Function | Status | Notes |
| --- | --- | --- |
| `cast` | ✅ | Some casts fall back; float-to-decimal is opt-in ([details](compatibility/expressions/cast.md)) |

---

## datetime_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `add_months` | ✅ |  |
| `convert_timezone` | ✅ |  |
| `curdate` | ✅ | Constant-folded to a literal (alias of `current_date`) |
| `current_date` | ✅ | Constant-folded to a literal before Comet sees the plan |
| `current_time` | 🔜 | Blocked on Spark 4.1 TIME type support ([#4288](https://github.com/apache/datafusion-comet/issues/4288)) |
| `current_timestamp` | ✅ | Constant-folded to a literal before Comet sees the plan |
| `current_timezone` | ✅ |  |
| `date_add` | ✅ |  |
| `date_diff` | ✅ |  |
| `date_format` | ✅ |  |
| `date_from_unix_date` | ✅ |  |
| `date_part` | ✅ |  |
| `date_sub` | ✅ |  |
| `date_trunc` | ✅ |  |
| `dateadd` | ✅ |  |
| `datediff` | ✅ |  |
| `datepart` | ✅ |  |
| `day` | ✅ |  |
| `dayname` | 🔜 | [#4544](https://github.com/apache/datafusion-comet/issues/4544) |
| `dayofmonth` | ✅ |  |
| `dayofweek` | ✅ |  |
| `dayofyear` | ✅ |  |
| `extract` | ✅ |  |
| `from_unixtime` | ✅ |  |
| `from_utc_timestamp` | ✅ | Legacy zone forms fall back (Incompatible) ([details](compatibility/expressions/datetime.md)) |
| `hour` | ✅ |  |
| `last_day` | ✅ |  |
| `localtimestamp` | ✅ |  |
| `make_date` | ✅ |  |
| `make_dt_interval` | 🔜 | [#4541](https://github.com/apache/datafusion-comet/issues/4541) |
| `make_interval` | 🔜 | Produces legacy CalendarInterval; tracked by [#4540](https://github.com/apache/datafusion-comet/issues/4540) |
| `make_time` | 🔜 | Spark 4.1 TIME type; tracked by [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `make_timestamp` | ✅ |  |
| `make_timestamp_ltz` | ✅ | 2-arg TIME form falls back |
| `make_timestamp_ntz` | ✅ | 2-arg TIME form falls back |
| `make_ym_interval` | 🔜 | [#4541](https://github.com/apache/datafusion-comet/issues/4541) |
| `minute` | ✅ |  |
| `month` | ✅ |  |
| `monthname` | 🔜 | [#4544](https://github.com/apache/datafusion-comet/issues/4544) |
| `months_between` | ✅ |  |
| `next_day` | ✅ |  |
| `now` | ✅ | Constant-folded to a literal (alias of `current_timestamp`) |
| `quarter` | ✅ |  |
| `second` | ✅ |  |
| `session_window` | 🔜 | Time-window grouping; tracked by [#4553](https://github.com/apache/datafusion-comet/issues/4553) |
| `time_diff` | 🔜 | Spark 4.1 TIME type; tracked by [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `time_trunc` | 🔜 | Spark 4.1 TIME type; tracked by [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `timestamp_micros` | ✅ |  |
| `timestamp_millis` | ✅ |  |
| `timestamp_seconds` | ✅ |  |
| `to_date` | ✅ | Rewrites to `Cast` (or `Cast(GetTimestamp)` with a format) before Comet sees the plan |
| `to_time` | 🔜 | Spark 4.1 TIME type; tracked by [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `to_timestamp` | ✅ | Rewrites to `Cast` (or `GetTimestamp` with a format) before Comet sees the plan |
| `to_timestamp_ltz` | ✅ | Rewrites to `to_timestamp` (`TimestampType`) |
| `to_timestamp_ntz` | ✅ | Rewrites to `to_timestamp` (`TimestampNTZType`) |
| `to_unix_timestamp` | ✅ |  |
| `to_utc_timestamp` | ✅ | Legacy zone forms fall back (Incompatible) ([details](compatibility/expressions/datetime.md)) |
| `trunc` | ✅ |  |
| `try_make_interval` | 🔜 | Produces legacy CalendarInterval; tracked by [#4540](https://github.com/apache/datafusion-comet/issues/4540) |
| `try_make_timestamp` | ✅ |  |
| `try_to_date` | 🔜 | Rewrites to `Cast`/`GetTimestamp` but currently falls back; tracked by [#4556](https://github.com/apache/datafusion-comet/issues/4556) |
| `try_to_time` | 🔜 | Spark 4.1 TIME type; tracked by [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `try_to_timestamp` | 🔜 | Rewrites to `Cast`/`GetTimestamp` but currently falls back; tracked by [#4556](https://github.com/apache/datafusion-comet/issues/4556) |
| `unix_date` | ✅ |  |
| `unix_micros` | ✅ |  |
| `unix_millis` | ✅ |  |
| `unix_seconds` | ✅ |  |
| `unix_timestamp` | ✅ |  |
| `weekday` | ✅ |  |
| `weekofyear` | ✅ |  |
| `window` | 🔜 | Time-window grouping; tracked by [#4553](https://github.com/apache/datafusion-comet/issues/4553) |
| `window_time` | 🔜 | Time-window grouping; tracked by [#4553](https://github.com/apache/datafusion-comet/issues/4553) |
| `year` | ✅ |  |

---

## generator_funcs

`explode` and `posexplode` are supported via `CometExplodeExec` (operator-level, not
expression-level). The `outer` variants are wired but marked `Incompatible`; they require
`spark.comet.exec.explode.enabled=true` and `allowIncompatible`.

| Function | Status | Notes |
| --- | --- | --- |
| `explode` | ✅ | via `CometExplodeExec` |
| `explode_outer` | ✅ | outer=true falls back (Incompatible) ([audit](../../contributor-guide/expression-audits/generator_funcs.md#explode_outer)) |
| `inline` | 🔜 | Operator-level generator (like `explode`) |
| `inline_outer` | 🔜 | Operator-level generator (like `explode`) |
| `posexplode` | ✅ | via `CometExplodeExec` |
| `posexplode_outer` | ✅ | outer=true falls back (Incompatible) ([audit](../../contributor-guide/expression-audits/generator_funcs.md#posexplode_outer)) |
| `stack` | 🔜 | Operator-level generator |

---

## hash_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `crc32` | ✅ |  |
| `hash` | ✅ |  |
| `md5` | ✅ |  |
| `sha` | ✅ |  |
| `sha1` | ✅ |  |
| `sha2` | ✅ |  |
| `xxhash64` | ✅ |  |

---

## json_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `from_json` | ✅ | Falls back by default; opt-in via allowIncompatible ([audit](../../contributor-guide/expression-audits/json_funcs.md#from_json)) |
| `get_json_object` | ✅ | Some inputs need allowIncompatible ([audit](../../contributor-guide/expression-audits/json_funcs.md#get_json_object)) |
| `json_array_length` | ✅ | Single-quoted/trailing JSON needs allowIncompatible ([audit](../../contributor-guide/expression-audits/json_funcs.md#json_array_length)) |
| `json_object_keys` | 🔜 | [#3161](https://github.com/apache/datafusion-comet/issues/3161) |
| `json_tuple` | 🔜 | [#3160](https://github.com/apache/datafusion-comet/issues/3160) |
| `schema_of_json` | 🔜 | [#3163](https://github.com/apache/datafusion-comet/issues/3163) |
| `to_json` | ✅ | Options and map/array inputs fall back ([audit](../../contributor-guide/expression-audits/json_funcs.md#to_json)) |

---

## lambda_funcs

All higher-order functions are planned via [#4224](https://github.com/apache/datafusion-comet/issues/4224).

| Function | Status | Notes |
| --- | --- | --- |
| `aggregate` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `array_sort` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `exists` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `filter` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `forall` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `map_filter` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `map_zip_with` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `reduce` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `transform` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `transform_keys` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `transform_values` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `zip_with` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |

---

## map_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `element_at` | ✅ | MapType input falls back |
| `map` | 🔜 | Constructs a map |
| `map_concat` | ✅ |  |
| `map_contains_key` | ✅ |  |
| `map_entries` | ✅ |  |
| `map_from_arrays` | ✅ |  |
| `map_from_entries` | ✅ | BinaryType key/value falls back (Incompatible) ([details](compatibility/expressions/map.md)) |
| `map_keys` | ✅ |  |
| `map_values` | ✅ |  |
| `str_to_map` | ✅ |  |
| `try_element_at` | ✅ | Lowers to `element_at`; array input (MapType falls back) |

---

## math_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `%` | ✅ | try_mod (TRY mode) falls back |
| `*` | ✅ | Interval multiplication falls back |
| `+` | ✅ |  |
| `-` | ✅ |  |
| `/` | ✅ |  |
| `abs` | ✅ | Interval types fall back |
| `acos` | ✅ |  |
| `acosh` | ✅ |  |
| `asin` | ✅ |  |
| `asinh` | ✅ |  |
| `atan` | ✅ |  |
| `atan2` | ✅ |  |
| `atanh` | ✅ |  |
| `bin` | ✅ |  |
| `bround` | ✅ |  |
| `cbrt` | ✅ |  |
| `ceil` | ✅ | Two-arg form falls back |
| `ceiling` | ✅ |  |
| `conv` | ✅ |  |
| `cos` | ✅ |  |
| `cosh` | ✅ |  |
| `cot` | ✅ |  |
| `csc` | ✅ |  |
| `degrees` | ✅ |  |
| `div` | ✅ |  |
| `e` | ✅ | Folds to a literal (like `pi`) |
| `exp` | ✅ |  |
| `expm1` | ✅ |  |
| `factorial` | ✅ |  |
| `floor` | ✅ | Two-arg form falls back |
| `greatest` | ✅ |  |
| `hex` | ✅ |  |
| `hypot` | ✅ |  |
| `least` | ✅ |  |
| `ln` | ✅ |  |
| `log` | ✅ |  |
| `log10` | ✅ |  |
| `log1p` | ✅ |  |
| `log2` | ✅ |  |
| `mod` | ✅ |  |
| `negative` | ✅ |  |
| `pi` | ✅ |  |
| `pmod` | ✅ |  |
| `positive` | ✅ |  |
| `pow` | ✅ |  |
| `power` | ✅ |  |
| `radians` | ✅ |  |
| `rand` | ✅ |  |
| `randn` | ✅ |  |
| `random` | ✅ | Alias for `rand` (Spark 4.0+); seed must be a literal |
| `randstr` | 🔜 | Random string (Spark 4.0+) |
| `rint` | ✅ |  |
| `round` | ✅ | Float/double inputs fall back |
| `sec` | ✅ |  |
| `shiftleft` | ✅ |  |
| `sign` | ✅ |  |
| `signum` | ✅ |  |
| `sin` | ✅ |  |
| `sinh` | ✅ |  |
| `sqrt` | ✅ |  |
| `tan` | ✅ |  |
| `tanh` | ✅ |  |
| `try_add` | ✅ | Datetime/interval form falls back |
| `try_divide` | ✅ |  |
| `try_mod` | 🔜 | Lowers to `Remainder` with TRY eval mode, which falls back ([#4484](https://github.com/apache/datafusion-comet/issues/4484)) |
| `try_multiply` | ✅ |  |
| `try_subtract` | ✅ |  |
| `unhex` | ✅ |  |
| `uniform` | ✅ | Constant-folded; literal arguments only (Spark 4.0+) |
| `width_bucket` | ✅ |  |

---

## misc_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `aes_decrypt` | ✅ | Routed through the JVM codegen dispatcher |
| `aes_encrypt` | ✅ | Routed through the JVM codegen dispatcher; nondeterministic IV by default |
| `assert_true` | 🔜 | Lowers to `RaiseError`, which falls back |
| `current_catalog` | ✅ | Resolved to a literal by the analyzer (`ReplaceCurrentLike`) |
| `current_database` | ✅ | Resolved to a literal by the analyzer (`ReplaceCurrentLike`) |
| `current_schema` | ✅ | Alias of `current_database`; resolved to a literal by the analyzer |
| `current_user` | ✅ | Resolved to a literal by the analyzer; same as `user` |
| `equal_null` | ✅ | Lowers to `<=>` (`EqualNullSafe`) |
| `is_variant_null` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `monotonically_increasing_id` | ✅ |  |
| `parse_json` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `raise_error` | 🔜 | Raises a runtime error |
| `rand` | ✅ | Seed must be a literal |
| `randn` | ✅ | Seed must be a literal |
| `schema_of_variant` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `schema_of_variant_agg` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `session_user` | ✅ | Alias of `current_user`; resolved to a literal by the analyzer |
| `spark_partition_id` | ✅ |  |
| `to_variant_object` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `try_aes_decrypt` | ✅ | Routed through the JVM codegen dispatcher |
| `try_parse_json` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `try_variant_get` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `typeof` | ✅ | Foldable; resolved to a literal before Comet sees the plan |
| `user` | ✅ | Resolved to a literal by the Spark analyzer before reaching Comet |
| `uuid` | 🔜 | Nondeterministic random UUID |
| `variant_get` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |

---

## predicate_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `!` | ✅ |  |
| `<` | ✅ |  |
| `<=` | ✅ |  |
| `<=>` | ✅ |  |
| `=` | ✅ |  |
| `==` | ✅ |  |
| `>` | ✅ |  |
| `>=` | ✅ |  |
| `and` | ✅ |  |
| `between` | ✅ |  |
| `ilike` | ✅ |  |
| `in` | ✅ |  |
| `isnan` | ✅ |  |
| `isnotnull` | ✅ |  |
| `isnull` | ✅ |  |
| `like` | ✅ |  |
| `not` | ✅ |  |
| `or` | ✅ |  |
| `regexp` | ✅ | Falls back by default; opt-in via allowIncompatible ([details](compatibility/regex.md)) |
| `regexp_like` | ✅ | Falls back by default; opt-in via allowIncompatible ([details](compatibility/regex.md)) |
| `rlike` | ✅ | Falls back by default; opt-in via allowIncompatible ([details](compatibility/regex.md)) |

---

## string_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `ascii` | ✅ |  |
| `base64` | 🔜 | Lowers to `StaticInvoke(encode)` (not allowlisted); falls back |
| `bit_length` | ✅ |  |
| `btrim` | ✅ |  |
| `char` | ✅ |  |
| `char_length` | ✅ |  |
| `character_length` | ✅ |  |
| `chr` | ✅ |  |
| `collate` | 🔜 | Spark collation (umbrella [#2190](https://github.com/apache/datafusion-comet/issues/2190)) |
| `collation` | ✅ | Constant-folded to a literal (Spark 4.0+) |
| `concat_ws` | ✅ |  |
| `contains` | ✅ |  |
| `decode` | ✅ |  |
| `elt` | ✅ |  |
| `encode` | 🔜 | Lowers to `StaticInvoke(encode)` (not allowlisted); falls back |
| `endswith` | ✅ |  |
| `find_in_set` | ✅ |  |
| `format_number` | ✅ |  |
| `format_string` | ✅ |  |
| `initcap` | ✅ |  |
| `instr` | ✅ |  |
| `lcase` | ✅ |  |
| `left` | ✅ |  |
| `len` | ✅ |  |
| `length` | ✅ |  |
| `levenshtein` | ✅ |  |
| `locate` | ✅ |  |
| `lower` | ✅ |  |
| `lpad` | ✅ |  |
| `ltrim` | ✅ |  |
| `luhn_check` | ✅ | Native via `StaticInvoke` (tests: luhn_check.sql) |
| `mask` | 🔜 | Data masking |
| `octet_length` | ✅ |  |
| `overlay` | ✅ |  |
| `position` | ✅ |  |
| `printf` | ✅ |  |
| `regexp_count` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `regexp_extract` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `regexp_extract_all` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `regexp_instr` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `regexp_replace` | ✅ |  |
| `regexp_substr` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `repeat` | ✅ |  |
| `replace` | ✅ |  |
| `right` | ✅ |  |
| `rpad` | ✅ |  |
| `rtrim` | ✅ |  |
| `soundex` | ✅ |  |
| `space` | ✅ |  |
| `split` | ✅ |  |
| `split_part` | 🔜 | Lowers to `element_at(StringSplitSQL(...))`; `StringSplitSQL` falls back ([#4561](https://github.com/apache/datafusion-comet/issues/4561)) |
| `startswith` | ✅ |  |
| `substr` | ✅ |  |
| `substring` | ✅ |  |
| `substring_index` | ✅ |  |
| `to_binary` | ✅ | Hex form accelerated; other formats fall back |
| `to_char` | ✅ |  |
| `to_number` | ✅ |  |
| `to_varchar` | ✅ |  |
| `translate` | ✅ |  |
| `trim` | ✅ |  |
| `try_to_binary` | 🔜 | Lowers to `TryEval(...)`, which falls back |
| `try_to_number` | 🔜 | TRY variant of `to_number` |
| `ucase` | ✅ |  |
| `unbase64` | ✅ |  |
| `upper` | ✅ |  |

---

## struct_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `named_struct` | ✅ | Duplicate field names fall back |
| `struct` | ✅ |  |

---

## url_funcs

| Function | Status | Notes |
| --- | --- | --- |
| `parse_url` | ✅ |  |
| `try_url_decode` | ✅ |  |
| `url_decode` | ✅ |  |
| `url_encode` | ✅ |  |

---

## window_funcs

Window functions run via `CometWindowExec`. Window support is disabled by default due to known
correctness issues (tracking [#2721](https://github.com/apache/datafusion-comet/issues/2721)).
When enabled, `lag` and `lead` are explicitly wired; aggregate window functions (`count`, `min`,
`max`, `sum`) are also supported. Ranking functions (`rank`, `dense_rank`, `row_number`,
`ntile`, `percent_rank`, `cume_dist`, `nth_value`) are not yet wired in the window serde and
fall back to Spark.

| Function | Status | Notes |
| --- | --- | --- |
| `cume_dist` | 🔜 | Window function; tracked by [#2721](https://github.com/apache/datafusion-comet/issues/2721) |
| `dense_rank` | 🔜 | Window function; tracked by [#2721](https://github.com/apache/datafusion-comet/issues/2721) |
| `lag` | ✅ | via `CometWindowExec` |
| `lead` | ✅ | via `CometWindowExec` |
| `nth_value` | 🔜 | Window function; tracked by [#2721](https://github.com/apache/datafusion-comet/issues/2721) |
| `ntile` | 🔜 | Window function; tracked by [#2721](https://github.com/apache/datafusion-comet/issues/2721) |
| `percent_rank` | 🔜 | Window function; tracked by [#2721](https://github.com/apache/datafusion-comet/issues/2721) |
| `rank` | 🔜 | Window function; tracked by [#2721](https://github.com/apache/datafusion-comet/issues/2721) |
| `row_number` | 🔜 | Window function; tracked by [#2721](https://github.com/apache/datafusion-comet/issues/2721) |

---

## Beyond SQL functions

Comet also accelerates a number of Catalyst expressions that have no Spark SQL function name and therefore do not appear in the tables above. These arise from the DataFrame API, from SQL syntax other than function calls, or from the query optimizer. They include:

- **Operator and optimizer-injected expressions:** runtime bloom-filter join probes (`BloomFilterMightContain`, `BloomFilterAggregate`), optimized `IN` sets (`InSet`), scalar subqueries (`ScalarSubquery`), and floating-point normalization (`KnownFloatingPointNormalized`).
- **Accessor expressions (subscript and field access, not functions):** struct field access (`col.field`), array element access (`arr[i]`), and map value access (`map[key]`).
- **Internal decimal arithmetic:** `CheckOverflow`, `MakeDecimal`, and `UnscaledValue`, which the analyzer inserts around decimal operations.
- **User-defined functions:** Scala UDFs registered through the DataFrame or SQL API.
- **Structural expressions:** aliases, attribute references, literals, sort orders, and `CASE WHEN`.

This list is illustrative, not exhaustive: the per-function tables are not the complete set of expressions Comet can accelerate.

## See also

- [Comet Compatibility Guide](compatibility/index.md) - known incompatibilities and edge cases for ⚠️ expressions.
- [Expression Audits (contributor guide)](../../contributor-guide/expression-audits/index.md) - per-version (Spark 3.4 / 3.5 / 4.0 / 4.1) audit notes for audited expressions.
