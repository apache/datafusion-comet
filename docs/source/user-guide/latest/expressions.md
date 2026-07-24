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

Some ✅ Supported expressions have specific incompatible cases that are not run by default.
Those cases must be opted into per expression with
`spark.comet.expression.EXPRNAME.allowIncompatible=true` (where `EXPRNAME` is the Spark
expression class name, for example `Cast`). There is no global opt-in. By default such a case
either falls back to Spark (for example `cast`) or, when the expression has a Spark-compatible
codegen-dispatch implementation, runs through that instead (for example the regex and JSON
families). See [Native and codegen-dispatch implementations](compatibility/index.md#native-and-codegen-dispatch-implementations)
for how Comet chooses.

Most expressions can also be disabled with `spark.comet.expression.EXPRNAME.enabled=false`, where
`EXPRNAME` is the Spark expression class name (for example `Length` or `StartsWith`). See the
[Comet Configuration Guide](configs.md) for the full list.

## Status legend

| Status | Meaning |
| --- | --- |
| ✅ Supported | Comet produces Spark-compatible results by default. Some inputs or forms may fall back to Spark, and any incompatible behavior is opt-in (off by default). |
| 🔜 Planned | Intended; tracked by an open issue or pull request. |

## Implementation legend

The **Implementation** column records how Comet executes each expression when it is not falling back to Spark:

| Implementation | Meaning |
| --- | --- |
| Native | Comet's Rust engine evaluates the expression end to end. |
| Codegen dispatch | Spark's own generated JVM code is evaluated inside the Comet pipeline. Used when a byte-exact match to Spark matters more than the native speedup, or when no native path exists. |
| Hybrid | Both paths exist. Comet picks between them based on input, and the user can override with `spark.comet.expression.EXPRNAME.allowIncompatible=true` — see [Native and codegen-dispatch implementations](compatibility/index.md#native-and-codegen-dispatch-implementations). |
| — | No direct wire-up: the row is either planned, is rewritten to another expression before Comet sees it (for example `to_date` → `Cast`), or is handled at the operator level rather than the expression level (for example `explode`, window functions). |

The Implementation column is auto-generated from the serde definitions in `QueryPlanSerde`; do not edit it by hand.

## Not currently planned

Comet focuses acceleration on mainstream relational, string, datetime, math, and collection
expressions. The following function families are **not currently planned** for native acceleration (they are not on the 1.0 roadmap): specialized functionality with narrow real-world analytics use and high implementation cost. They fall back to Spark and may be reconsidered based on demand:

- **Probabilistic sketches and approximate top-k** (`kll_sketch_*`, `hll_*`, `theta_*`, `count_min_sketch`, `bitmap_*`, `approx_top_k*`): specialized data structures with exact-correctness traps.
- **Geospatial** (`st_*`): brand-new Spark 4.1 functionality, specialized.
- **Avro / Protobuf codecs** (`from_avro`, `to_avro`, `from_protobuf`, `to_protobuf`, `schema_of_avro`): format conversion belongs at the IO layer, not expression evaluation.
- **JVM reflection** (`java_method`, `reflect`): niche, and they invoke arbitrary JVM methods (a security concern).
- **UTF-8 validation** (`is_valid_utf8`, `make_valid_utf8`, `validate_utf8`, `try_validate_utf8`): niche Spark 4.x string-validation helpers.
- **Miscellaneous niche** (`histogram_numeric`, `version`, `sentences`, `quote`): low-value or specialized functions with little benefit from native acceleration.

The file-metadata functions `input_file_name`, `input_file_block_start`, and `input_file_block_length` depend on scan-internal per-row file information rather than the expression layer; their support status is covered in the [scan compatibility guide](compatibility/scans.md).

Note that `median` and `mode` are planned: they are mainstream exact aggregates. `approx_count_distinct` is supported because Comet ports Spark's `HyperLogLogPlusPlus` exactly, so its result is bit-identical to Spark.

The tables below list every Spark built-in expression with its current status.

## agg_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `any` | ✅ | — |  |
| `any_value` | ✅ | — |  |
| `approx_count_distinct` | ✅ | Native |  |
| `approx_percentile` | ✅ | Native | Byte, short, int, long, float, and double input; other input types fall back to Spark |
| `array_agg` | ✅ | Native | Alias for `collect_list` |
| `avg` | ✅ | Native | Interval types fall back |
| `bit_and` | ✅ | Native |  |
| `bit_or` | ✅ | Native |  |
| `bit_xor` | ✅ | Native |  |
| `bool_and` | ✅ | — |  |
| `bool_or` | ✅ | — |  |
| `collect_list` | ✅ | Native |  |
| `collect_set` | ✅ | Native |  |
| `corr` | ✅ | Native |  |
| `count` | ✅ | Native |  |
| `count_if` | ✅ | — |  |
| `covar_pop` | ✅ | Native |  |
| `covar_samp` | ✅ | Native |  |
| `every` | ✅ | — |  |
| `first` | ✅ | Native |  |
| `first_value` | ✅ | Native |  |
| `grouping` | ✅ | — | Grouping indicator for ROLLUP/CUBE/GROUPING SETS |
| `grouping_id` | ✅ | — | Grouping indicator for ROLLUP/CUBE/GROUPING SETS |
| `kurtosis` | 🔜 | — | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `last` | ✅ | Native |  |
| `last_value` | ✅ | Native |  |
| `listagg` | 🔜 | — | String aggregation |
| `max` | ✅ | Native |  |
| `max_by` | 🔜 | — | [#3841](https://github.com/apache/datafusion-comet/issues/3841) |
| `mean` | ✅ | Native |  |
| `median` | ✅ | — | Rewrites to `percentile(col, 0.5)` and runs natively for supported percentile inputs |
| `min` | ✅ | Native |  |
| `min_by` | 🔜 | — | [#3841](https://github.com/apache/datafusion-comet/issues/3841) |
| `mode` | 🔜 | — | [#3970](https://github.com/apache/datafusion-comet/issues/3970) |
| `percentile` | ✅ | Native | Single literal percentage on numeric input runs natively; array of percentages and a frequency argument fall back to Spark |
| `percentile_cont` | ✅ | — | Spark 4.0+ `WITHIN GROUP (ORDER BY ...)`; ascending only runs natively, `DESC` falls back to Spark |
| `percentile_disc` | 🔜 | — | Percentile aggregate |
| `regr_avgx` | ✅ | — | Native: Spark rewrites to `Average` (tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551)) |
| `regr_avgy` | ✅ | — | Native: Spark rewrites to `Average` (tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551)) |
| `regr_count` | ✅ | — | Native: Spark rewrites to `Count` (tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551)) |
| `regr_intercept` | 🔜 | — | Falls back; can reuse `covar_pop`/`var_pop` accumulators ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `regr_r2` | 🔜 | — | Falls back; can reuse the `corr` accumulator ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `regr_slope` | 🔜 | — | Falls back; can reuse `covar_pop`/`var_pop` accumulators ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `regr_sxx` | 🔜 | — | Falls back; can reuse `var_pop` accumulator ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `regr_sxy` | 🔜 | — | Falls back; can reuse `covar_pop` accumulator ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `regr_syy` | 🔜 | — | Falls back; can reuse `var_pop` accumulator ([#4552](https://github.com/apache/datafusion-comet/issues/4552)) |
| `skewness` | 🔜 | — | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `some` | ✅ | — |  |
| `std` | ✅ | Native |  |
| `stddev` | ✅ | Native |  |
| `stddev_pop` | ✅ | Native |  |
| `stddev_samp` | ✅ | Native |  |
| `string_agg` | 🔜 | — | String aggregation (alias of `listagg`) |
| `sum` | ✅ | Native |  |
| `try_avg` | ✅ | — | Interval types fall back |
| `try_sum` | ✅ | — |  |
| `var_pop` | ✅ | Native |  |
| `var_samp` | ✅ | Native |  |
| `variance` | ✅ | Native |  |

---

## array_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `array` | ✅ | Native |  |
| `array_append` | ✅ | Native |  |
| `array_compact` | ✅ | — |  |
| `array_contains` | ✅ | Native | NaN/signed-zero handling may differ ([details](compatibility/floating-point.md)) |
| `array_distinct` | ✅ | Native | NaN/signed-zero handling may differ ([details](compatibility/floating-point.md)) |
| `array_except` | ✅ | Hybrid | Routes through the JVM codegen dispatcher by default; the incompatible native path is opt-in via allowIncompatible ([details](compatibility/expressions/array.md)) |
| `array_insert` | ✅ | Native |  |
| `array_intersect` | ✅ | Hybrid | Routes through the JVM codegen dispatcher by default; the incompatible native path is opt-in via allowIncompatible ([details](compatibility/expressions/array.md)) |
| `array_join` | ✅ | Hybrid | Routes through the JVM codegen dispatcher by default; the incompatible native path is opt-in via allowIncompatible ([details](compatibility/expressions/array.md)) |
| `array_max` | ✅ | Native | NaN ordering may differ ([details](compatibility/floating-point.md)) |
| `array_min` | ✅ | Native | NaN ordering may differ ([details](compatibility/floating-point.md)) |
| `array_position` | ✅ | Native | Binary/struct/map/null elements fall back |
| `array_prepend` | ✅ | — |  |
| `array_remove` | ✅ | Native |  |
| `array_repeat` | ✅ | Native |  |
| `array_union` | ✅ | Native | NaN/signed-zero handling may differ ([details](compatibility/floating-point.md)) |
| `arrays_overlap` | ✅ | Native |  |
| `arrays_zip` | ✅ | Native |  |
| `element_at` | ✅ | Native |  |
| `flatten` | ✅ | Native | Binary/struct/map elements fall back |
| `get` | ✅ | — |  |
| `sequence` | ✅ | Codegen dispatch |  |
| `shuffle` | ✅ | Native | Binary/struct/map elements fall back |
| `slice` | ✅ | Native | Native ([#4149](https://github.com/apache/datafusion-comet/issues/4149)) |
| `sort_array` | ✅ | Hybrid | Nested struct/null arrays fall back |

---

## bitwise_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `&` | ✅ | Native |  |
| `<<` | ✅ | — |  |
| `>>` | ✅ | — |  |
| `>>>` | ✅ | — | Operator alias for `shiftrightunsigned` (Spark 4.0+) |
| `^` | ✅ | Native |  |
| `bit_count` | ✅ | Native |  |
| `bit_get` | ✅ | Native |  |
| `getbit` | ✅ | Native |  |
| `shiftright` | ✅ | Native |  |
| `shiftrightunsigned` | ✅ | Native |  |
| `\|` | ✅ | Native |  |
| `~` | ✅ | Native |  |

---

## collection_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `array_size` | ✅ | — |  |
| `cardinality` | ✅ | Native |  |
| `concat` | ✅ | Hybrid | Binary/array children fall back |
| `reverse` | ✅ | Hybrid | Binary-element arrays fall back (Incompatible) ([details](compatibility/expressions/array.md)) |
| `size` | ✅ | Native |  |

---

## conditional_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `coalesce` | ✅ | Native |  |
| `if` | ✅ | Native |  |
| `ifnull` | ✅ | — |  |
| `nanvl` | ✅ | Codegen dispatch |  |
| `nullif` | ✅ | — |  |
| `nullifzero` | ✅ | — | Lowers to `if`/`=` (Spark 4.0+) |
| `nvl` | ✅ | — |  |
| `nvl2` | ✅ | — |  |
| `when` | ✅ | Native |  |
| `zeroifnull` | ✅ | — | Lowers to `coalesce` (Spark 4.0+) |

---

## conversion_funcs

The type-name conversion functions (`bigint`, `binary`, `boolean`, `date`, `decimal`, `double`, `float`, `int`, `smallint`, `string`, `timestamp`, `tinyint`) are SQL aliases for `CAST(... AS <type>)` and share the support and caveats of `cast`.

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `cast` | ✅ | Native | Some casts fall back; float-to-decimal is opt-in ([details](compatibility/expressions/cast.md)) |

---

## csv_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `from_csv` | ✅ | Codegen dispatch |  |
| `schema_of_csv` | ✅ | Codegen dispatch |  |
| `to_csv` | ✅ | Native |  |

---

## datetime_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `add_months` | ✅ | Codegen dispatch |  |
| `convert_timezone` | ✅ | Hybrid | Routes through the JVM codegen dispatcher by default (handles all timezone forms); the native path is opt-in via allowIncompatible ([details](compatibility/expressions/datetime.md)) |
| `curdate` | ✅ | — | Constant-folded to a literal (alias of `current_date`) |
| `current_date` | ✅ | — | Constant-folded to a literal before Comet sees the plan |
| `current_time` | 🔜 | — | Blocked on Spark 4.1 TIME type support ([#4288](https://github.com/apache/datafusion-comet/issues/4288)) |
| `current_timestamp` | ✅ | — | Constant-folded to a literal before Comet sees the plan |
| `current_timezone` | ✅ | — |  |
| `date_add` | ✅ | Native |  |
| `date_diff` | ✅ | Native |  |
| `date_format` | ✅ | Hybrid |  |
| `date_from_unix_date` | ✅ | Native |  |
| `date_part` | ✅ | — |  |
| `date_sub` | ✅ | Native |  |
| `date_trunc` | ✅ | Hybrid |  |
| `dateadd` | ✅ | Native |  |
| `datediff` | ✅ | Native |  |
| `datepart` | ✅ | — |  |
| `day` | ✅ | Native |  |
| `dayname` | ✅ | — | Abbreviated day name (Spark 4.0+) |
| `dayofmonth` | ✅ | Native |  |
| `dayofweek` | ✅ | Native |  |
| `dayofyear` | ✅ | Native |  |
| `extract` | ✅ | — |  |
| `from_unixtime` | ✅ | Hybrid |  |
| `from_utc_timestamp` | ✅ | Hybrid | Routes through the JVM codegen dispatcher by default (handles all timezone forms); the native path is opt-in via allowIncompatible ([details](compatibility/expressions/datetime.md)) |
| `hour` | ✅ | Native |  |
| `last_day` | ✅ | Native |  |
| `localtimestamp` | ✅ | — |  |
| `make_date` | ✅ | Native |  |
| `make_dt_interval` | ✅ | Codegen dispatch |  |
| `make_interval` | 🔜 | — | Produces legacy CalendarInterval; tracked by [#4540](https://github.com/apache/datafusion-comet/issues/4540) |
| `make_time` | 🔜 | — | Spark 4.1 TIME type; tracked by [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `make_timestamp` | ✅ | Hybrid |  |
| `make_timestamp_ltz` | ✅ | — | 2-arg TIME form falls back |
| `make_timestamp_ntz` | ✅ | — | 2-arg TIME form falls back |
| `make_ym_interval` | ✅ | Codegen dispatch |  |
| `minute` | ✅ | Native |  |
| `month` | ✅ | Native |  |
| `monthname` | ✅ | — | Abbreviated month name (Spark 4.0+) |
| `months_between` | ✅ | Codegen dispatch |  |
| `next_day` | ✅ | Native |  |
| `now` | ✅ | — | Constant-folded to a literal (alias of `current_timestamp`) |
| `quarter` | ✅ | Native |  |
| `second` | ✅ | Native |  |
| `session_window` | 🔜 | — | Batch session-window grouping falls back (`UpdatingSessionsExec` is not yet native); tracked by [#4785](https://github.com/apache/datafusion-comet/issues/4785) |
| `time_diff` | 🔜 | — | Spark 4.1 TIME type; tracked by [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `time_trunc` | 🔜 | — | Spark 4.1 TIME type; tracked by [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `timestamp_micros` | ✅ | Codegen dispatch |  |
| `timestamp_millis` | ✅ | Codegen dispatch |  |
| `timestamp_seconds` | ✅ | Native |  |
| `to_date` | ✅ | — | Rewrites to `Cast` (or `Cast(GetTimestamp)` with a format) before Comet sees the plan |
| `to_time` | 🔜 | — | Spark 4.1 TIME type; tracked by [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `to_timestamp` | ✅ | — | Rewrites to `Cast` (or `GetTimestamp` with a format) before Comet sees the plan |
| `to_timestamp_ltz` | ✅ | — | Rewrites to `to_timestamp` (`TimestampType`) |
| `to_timestamp_ntz` | ✅ | — | Rewrites to `to_timestamp` (`TimestampNTZType`) |
| `to_unix_timestamp` | ✅ | Hybrid |  |
| `to_utc_timestamp` | ✅ | Hybrid | Routes through the JVM codegen dispatcher by default (handles all timezone forms); the native path is opt-in via allowIncompatible ([details](compatibility/expressions/datetime.md)) |
| `trunc` | ✅ | Hybrid |  |
| `try_make_interval` | 🔜 | — | Produces legacy CalendarInterval; tracked by [#4540](https://github.com/apache/datafusion-comet/issues/4540) |
| `try_make_timestamp` | ✅ | — |  |
| `try_to_date` | ✅ | — | Rewrites to `Cast`/`GetTimestamp` before Comet sees the plan; same support as `to_date` |
| `try_to_time` | 🔜 | — | Spark 4.1 TIME type; tracked by [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `try_to_timestamp` | ✅ | — | Rewrites to `Cast`/`GetTimestamp` before Comet sees the plan; same support as `to_timestamp` |
| `unix_date` | ✅ | Native |  |
| `unix_micros` | ✅ | Codegen dispatch |  |
| `unix_millis` | ✅ | Codegen dispatch |  |
| `unix_seconds` | ✅ | Codegen dispatch |  |
| `unix_timestamp` | ✅ | Native |  |
| `weekday` | ✅ | Native |  |
| `weekofyear` | ✅ | Native |  |
| `window` | ✅ | — | Batch tumbling and sliding time-window grouping runs natively |
| `window_time` | ✅ | — | Batch time-window grouping runs natively |
| `year` | ✅ | Native |  |

---

## generator_funcs

`explode` and `posexplode` are supported via `CometExplodeExec` (operator-level, not
expression-level). The `outer` variants are wired but marked `Incompatible`; they require
`spark.comet.exec.explode.enabled=true` and `allowIncompatible`.

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `explode` | ✅ | — | via `CometExplodeExec` |
| `explode_outer` | ✅ | — | outer=true falls back (Incompatible) ([audit](../../contributor-guide/expression-audits/generator_funcs.md#explode_outer)) |
| `inline` | 🔜 | — | Operator-level generator (like `explode`) |
| `inline_outer` | 🔜 | — | Operator-level generator (like `explode`) |
| `posexplode` | ✅ | — | via `CometExplodeExec` |
| `posexplode_outer` | ✅ | — | outer=true falls back (Incompatible) ([audit](../../contributor-guide/expression-audits/generator_funcs.md#posexplode_outer)) |
| `stack` | 🔜 | — | Operator-level generator |

---

## hash_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `crc32` | ✅ | Native |  |
| `hash` | ✅ | Native |  |
| `md5` | ✅ | Native |  |
| `sha` | ✅ | Native |  |
| `sha1` | ✅ | Native |  |
| `sha2` | ✅ | Native |  |
| `xxhash64` | ✅ | Native |  |

---

## json_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `from_json` | ✅ | Hybrid | Falls back by default; opt-in via allowIncompatible ([audit](../../contributor-guide/expression-audits/json_funcs.md#from_json)) |
| `get_json_object` | ✅ | Hybrid | Some inputs need allowIncompatible ([audit](../../contributor-guide/expression-audits/json_funcs.md#get_json_object)) |
| `json_array_length` | ✅ | Hybrid | Single-quoted/trailing JSON needs allowIncompatible ([audit](../../contributor-guide/expression-audits/json_funcs.md#json_array_length)) |
| `json_object_keys` | ✅ | Codegen dispatch |  |
| `json_tuple` | 🔜 | — | [#3160](https://github.com/apache/datafusion-comet/issues/3160) |
| `schema_of_json` | ✅ | Codegen dispatch |  |
| `to_json` | ✅ | Hybrid | Options and map/array inputs fall back ([audit](../../contributor-guide/expression-audits/json_funcs.md#to_json)) |

---

## lambda_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `aggregate` | ✅ | Codegen dispatch |  |
| `array_sort` | ✅ | Codegen dispatch |  |
| `exists` | ✅ | Codegen dispatch |  |
| `filter` | ✅ | Native | General lambda routed through the JVM codegen dispatcher; the `array_compact` form runs natively |
| `forall` | ✅ | Codegen dispatch |  |
| `map_filter` | ✅ | Codegen dispatch |  |
| `map_zip_with` | ✅ | Codegen dispatch |  |
| `reduce` | ✅ | Codegen dispatch |  |
| `transform` | ✅ | Codegen dispatch |  |
| `transform_keys` | ✅ | Codegen dispatch |  |
| `transform_values` | ✅ | Codegen dispatch |  |
| `zip_with` | ✅ | Codegen dispatch |  |

---

## map_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `element_at` | ✅ | Native |  |
| `map` | ✅ | Codegen dispatch | Routed through the JVM codegen dispatcher |
| `map_concat` | ✅ | Codegen dispatch |  |
| `map_contains_key` | ✅ | — |  |
| `map_entries` | ✅ | Native |  |
| `map_from_arrays` | ✅ | Native |  |
| `map_from_entries` | ✅ | Hybrid | BinaryType key/value falls back (Incompatible) ([details](compatibility/expressions/map.md)) |
| `map_keys` | ✅ | Native |  |
| `map_values` | ✅ | Native |  |
| `str_to_map` | ✅ | Hybrid |  |
| `try_element_at` | ✅ | — | Lowers to `element_at` |

---

## math_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `%` | ✅ | Native |  |
| `*` | ✅ | Native | Interval multiplication falls back |
| `+` | ✅ | Native |  |
| `-` | ✅ | Native |  |
| `/` | ✅ | Native |  |
| `abs` | ✅ | Native | Interval types fall back |
| `acos` | ✅ | Native |  |
| `acosh` | ✅ | Native |  |
| `asin` | ✅ | Native |  |
| `asinh` | ✅ | Native |  |
| `atan` | ✅ | Native |  |
| `atan2` | ✅ | Native |  |
| `atanh` | ✅ | Native |  |
| `bin` | ✅ | Native |  |
| `bround` | ✅ | Codegen dispatch |  |
| `cbrt` | ✅ | Native |  |
| `ceil` | ✅ | — | Two-arg form falls back |
| `ceiling` | ✅ | — |  |
| `conv` | ✅ | Codegen dispatch |  |
| `cos` | ✅ | Native |  |
| `cosh` | ✅ | Native |  |
| `cot` | ✅ | Native |  |
| `csc` | ✅ | Native |  |
| `degrees` | ✅ | Native |  |
| `div` | ✅ | Native |  |
| `e` | ✅ | — | Folds to a literal (like `pi`) |
| `exp` | ✅ | Native |  |
| `expm1` | ✅ | Native |  |
| `factorial` | ✅ | Native |  |
| `floor` | ✅ | — | Two-arg form falls back |
| `greatest` | ✅ | Native |  |
| `hex` | ✅ | Native |  |
| `hypot` | ✅ | Codegen dispatch |  |
| `least` | ✅ | Native |  |
| `ln` | ✅ | Native |  |
| `log` | ✅ | Native |  |
| `log10` | ✅ | Native |  |
| `log1p` | ✅ | Codegen dispatch |  |
| `log2` | ✅ | Native |  |
| `mod` | ✅ | Native |  |
| `negative` | ✅ | Native |  |
| `pi` | ✅ | Native |  |
| `pmod` | ✅ | Codegen dispatch |  |
| `positive` | ✅ | Codegen dispatch |  |
| `pow` | ✅ | Native |  |
| `power` | ✅ | Native |  |
| `radians` | ✅ | Native |  |
| `rand` | ✅ | Native |  |
| `randn` | ✅ | Native |  |
| `random` | ✅ | Native | Alias for `rand` (Spark 4.0+); seed must be a literal |
| `randstr` | 🔜 | — | Random string (Spark 4.0+) |
| `rint` | ✅ | Native |  |
| `round` | ✅ | Native | Float/double inputs fall back |
| `sec` | ✅ | Native |  |
| `shiftleft` | ✅ | Native |  |
| `sign` | ✅ | Native |  |
| `signum` | ✅ | Native |  |
| `sin` | ✅ | Native |  |
| `sinh` | ✅ | Native |  |
| `sqrt` | ✅ | Native |  |
| `tan` | ✅ | Native |  |
| `tanh` | ✅ | Native |  |
| `try_add` | ✅ | — | Datetime/interval form falls back |
| `try_divide` | ✅ | — |  |
| `try_mod` | ✅ | — |  |
| `try_multiply` | ✅ | — |  |
| `try_subtract` | ✅ | — |  |
| `unhex` | ✅ | Native |  |
| `uniform` | ✅ | — | Constant-folded; literal arguments only (Spark 4.0+) |
| `width_bucket` | ✅ | Codegen dispatch |  |

---

## misc_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `aes_decrypt` | ✅ | — | Routed through the JVM codegen dispatcher |
| `aes_encrypt` | ✅ | — | Routed through the JVM codegen dispatcher; nondeterministic IV by default |
| `assert_true` | 🔜 | — | Lowers to `RaiseError`, which falls back |
| `current_catalog` | ✅ | — | Resolved to a literal by the analyzer (`ReplaceCurrentLike`) |
| `current_database` | ✅ | — | Resolved to a literal by the analyzer (`ReplaceCurrentLike`) |
| `current_schema` | ✅ | — | Alias of `current_database`; resolved to a literal by the analyzer |
| `current_user` | ✅ | — | Resolved to a literal by the analyzer; same as `user` |
| `equal_null` | ✅ | — | Lowers to `<=>` (`EqualNullSafe`) |
| `is_variant_null` | 🔜 | — | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `monotonically_increasing_id` | ✅ | Native |  |
| `parse_json` | 🔜 | — | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `raise_error` | 🔜 | — | Raises a runtime error |
| `rand` | ✅ | Native | Seed must be a literal |
| `randn` | ✅ | Native | Seed must be a literal |
| `schema_of_variant` | 🔜 | — | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `schema_of_variant_agg` | 🔜 | — | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `session_user` | ✅ | — | Alias of `current_user`; resolved to a literal by the analyzer |
| `spark_partition_id` | ✅ | Native |  |
| `to_variant_object` | 🔜 | — | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `try_aes_decrypt` | ✅ | — | Routed through the JVM codegen dispatcher |
| `try_parse_json` | 🔜 | — | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `try_variant_get` | 🔜 | — | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `typeof` | ✅ | — | Foldable; resolved to a literal before Comet sees the plan |
| `user` | ✅ | — | Resolved to a literal by the Spark analyzer before reaching Comet |
| `uuid` | 🔜 | — | Nondeterministic random UUID |
| `variant_get` | 🔜 | — | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |

---

## predicate_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `!` | ✅ | Native |  |
| `<` | ✅ | Hybrid |  |
| `<=` | ✅ | Hybrid |  |
| `<=>` | ✅ | Hybrid |  |
| `=` | ✅ | Hybrid |  |
| `==` | ✅ | Hybrid |  |
| `>` | ✅ | Hybrid |  |
| `>=` | ✅ | Hybrid |  |
| `and` | ✅ | Native |  |
| `between` | ✅ | — |  |
| `ilike` | ✅ | — |  |
| `in` | ✅ | Hybrid |  |
| `isnan` | ✅ | Native |  |
| `isnotnull` | ✅ | Native |  |
| `isnull` | ✅ | Native |  |
| `like` | ✅ | Hybrid |  |
| `not` | ✅ | Native |  |
| `or` | ✅ | Native |  |
| `regexp` | ✅ | Hybrid | Falls back by default; opt-in via allowIncompatible ([details](compatibility/regex.md)) |
| `regexp_like` | ✅ | Hybrid | Falls back by default; opt-in via allowIncompatible ([details](compatibility/regex.md)) |
| `rlike` | ✅ | Hybrid | Falls back by default; opt-in via allowIncompatible ([details](compatibility/regex.md)) |

---

## string_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `ascii` | ✅ | Native |  |
| `base64` | ✅ | Native |  |
| `bit_length` | ✅ | Native |  |
| `btrim` | ✅ | — |  |
| `char` | ✅ | Native |  |
| `char_length` | ✅ | Native |  |
| `character_length` | ✅ | Native |  |
| `chr` | ✅ | Native |  |
| `collate` | 🔜 | — | Spark collation (umbrella [#2190](https://github.com/apache/datafusion-comet/issues/2190)) |
| `collation` | ✅ | — | Constant-folded to a literal (Spark 4.0+) |
| `concat_ws` | ✅ | Native |  |
| `contains` | ✅ | — |  |
| `decode` | ✅ | — |  |
| `elt` | ✅ | Codegen dispatch |  |
| `encode` | 🔜 | — | Lowers to `StaticInvoke(encode)` (not allowlisted); falls back |
| `endswith` | ✅ | — |  |
| `find_in_set` | ✅ | Codegen dispatch |  |
| `format_number` | ✅ | Codegen dispatch |  |
| `format_string` | ✅ | Codegen dispatch |  |
| `initcap` | ✅ | Hybrid |  |
| `instr` | ✅ | Native |  |
| `lcase` | ✅ | Hybrid |  |
| `left` | ✅ | Native |  |
| `len` | ✅ | Native |  |
| `length` | ✅ | Native |  |
| `levenshtein` | ✅ | Native |  |
| `locate` | ✅ | Codegen dispatch |  |
| `lower` | ✅ | Hybrid |  |
| `lpad` | ✅ | — |  |
| `ltrim` | ✅ | Native |  |
| `luhn_check` | ✅ | — | Native via `StaticInvoke` (tests: luhn_check.sql) |
| `mask` | ✅ | — | Routed through the JVM codegen dispatcher |
| `octet_length` | ✅ | Native |  |
| `overlay` | ✅ | Codegen dispatch |  |
| `position` | ✅ | Codegen dispatch |  |
| `printf` | ✅ | Codegen dispatch |  |
| `regexp_count` | ✅ | — | Runs natively (rewrites to `size(regexp_extract_all(...))`) |
| `regexp_extract` | ✅ | Native |  |
| `regexp_extract_all` | ✅ | Native |  |
| `regexp_instr` | ✅ | Codegen dispatch | Routed through the JVM codegen dispatcher |
| `regexp_replace` | ✅ | Hybrid |  |
| `regexp_substr` | ✅ | — | Runs natively (rewrites to `nullif(regexp_extract(...), '')`) |
| `repeat` | ✅ | Native |  |
| `replace` | ✅ | Hybrid |  |
| `right` | ✅ | Native |  |
| `rpad` | ✅ | — |  |
| `rtrim` | ✅ | Native |  |
| `soundex` | ✅ | Native |  |
| `space` | ✅ | Native |  |
| `split` | ✅ | Hybrid |  |
| `split_part` | ✅ | — | Spark 4.0+ |
| `startswith` | ✅ | — |  |
| `substr` | ✅ | Native |  |
| `substring` | ✅ | Native |  |
| `substring_index` | ✅ | Native |  |
| `to_binary` | ✅ | — | Hex form accelerated; other formats fall back |
| `to_char` | ✅ | Codegen dispatch |  |
| `to_number` | ✅ | Codegen dispatch |  |
| `to_varchar` | ✅ | Codegen dispatch |  |
| `translate` | ✅ | Native | Falls back by default; opt-in via allowIncompatible ([#4463](https://github.com/apache/datafusion-comet/issues/4463)) |
| `trim` | ✅ | Native |  |
| `try_to_binary` | ✅ | — | Runs natively (rewrites to `try_eval(to_binary(...))`) |
| `try_to_number` | ✅ | Codegen dispatch | Routed through the JVM codegen dispatcher |
| `ucase` | ✅ | Hybrid |  |
| `unbase64` | ✅ | Codegen dispatch |  |
| `upper` | ✅ | Hybrid |  |

---

## struct_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `named_struct` | ✅ | Native | Duplicate field names fall back |
| `struct` | ✅ | Native |  |

---

## url_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `parse_url` | ✅ | Native |  |
| `try_url_decode` | ✅ | — |  |
| `url_decode` | ✅ | — |  |
| `url_encode` | ✅ | — |  |

---

## window_funcs

Window functions run via `CometWindowExec`, which is enabled by default.
Aggregate window functions (`count`, `min`, `max`, `sum`, `avg`,
`first_value`, `last_value`), ranking functions (`row_number`, `rank`,
`dense_rank`, `percent_rank`, `cume_dist`, `ntile`), and value-shift
functions (`lag`, `lead`, `nth_value`) are all wired in the window serde
and execute natively. Statistical aggregates such as `stddev`, `var_pop`,
`corr`, and `covar_pop` run natively as plain aggregations but fall back
to Spark when used as window functions. A handful of frame shapes also
fall back. See [window function compatibility](compatibility/operators.md)
for the full list of supported functions, frames, and fallback cases.

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `cume_dist` | ✅ | — | via `CometWindowExec` |
| `dense_rank` | ✅ | — | via `CometWindowExec` |
| `lag` | ✅ | — | via `CometWindowExec`; non-literal default falls back ([#4268](https://github.com/apache/datafusion-comet/issues/4268)) |
| `lead` | ✅ | — | via `CometWindowExec`; non-literal default falls back ([#4268](https://github.com/apache/datafusion-comet/issues/4268)) |
| `nth_value` | ✅ | — | via `CometWindowExec` |
| `ntile` | ✅ | — | via `CometWindowExec` |
| `percent_rank` | ✅ | — | via `CometWindowExec` |
| `rank` | ✅ | — | via `CometWindowExec` |
| `row_number` | ✅ | — | via `CometWindowExec` |

---

## xml_funcs

| Function | Status | Implementation | Notes |
| --- | --- | --- | --- |
| `from_xml` | ✅ | — | Spark 4.0+ |
| `schema_of_xml` | ✅ | — | Spark 4.0+ |
| `to_xml` | ✅ | — | Spark 4.0+ |
| `xpath` | ✅ | Codegen dispatch |  |
| `xpath_boolean` | ✅ | Codegen dispatch |  |
| `xpath_double` | ✅ | Codegen dispatch |  |
| `xpath_float` | ✅ | Codegen dispatch |  |
| `xpath_int` | ✅ | Codegen dispatch |  |
| `xpath_long` | ✅ | Codegen dispatch |  |
| `xpath_number` | ✅ | Codegen dispatch | Alias of `xpath_double` |
| `xpath_short` | ✅ | Codegen dispatch |  |
| `xpath_string` | ✅ | Codegen dispatch |  |

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

- [Comet Compatibility Guide](compatibility/index.md) - known incompatibilities and edge cases for supported expressions.
- [Expression Audits (contributor guide)](../../contributor-guide/expression-audits/index.md) - per-version (Spark 3.4 / 3.5 / 4.0 / 4.1) audit notes for audited expressions.
