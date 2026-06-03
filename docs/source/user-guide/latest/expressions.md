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

<!--BEGIN:EXPR_TABLE[agg_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `any` | ✅ |  |
| `any_value` | ✅ |  |
| `approx_count_distinct` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `approx_percentile` | 🔜 | unclassified; not yet reviewed |
| `approx_top_k` | 🔜 | unclassified; not yet reviewed |
| `approx_top_k_accumulate` | 🔜 | unclassified; not yet reviewed |
| `approx_top_k_combine` | 🔜 | unclassified; not yet reviewed |
| `array_agg` | 🔜 | Array aggregate (related to `collect_list`) [#2524](https://github.com/apache/datafusion-comet/issues/2524) |
| `avg` | ✅ | [details](compatibility/expressions/aggregate.md#average) |
| `bit_and` | ✅ |  |
| `bit_or` | ✅ |  |
| `bit_xor` | ✅ |  |
| `bitmap_and_agg` | 🔜 | unclassified; not yet reviewed |
| `bitmap_construct_agg` | 🔜 | unclassified; not yet reviewed |
| `bitmap_or_agg` | 🔜 | unclassified; not yet reviewed |
| `bool_and` | ✅ |  |
| `bool_or` | ✅ |  |
| `collect_list` | 🔜 | [#2524](https://github.com/apache/datafusion-comet/issues/2524) |
| `collect_set` | ✅ | [details](compatibility/expressions/aggregate.md#collectset) |
| `corr` | ✅ |  |
| `count` | ✅ |  |
| `count_if` | ✅ |  |
| `count_min_sketch` | 🔜 | unclassified; not yet reviewed |
| `covar_pop` | ✅ |  |
| `covar_samp` | ✅ |  |
| `every` | ✅ |  |
| `first` | ✅ | [details](compatibility/expressions/aggregate.md#first) |
| `first_value` | ✅ | [details](compatibility/expressions/aggregate.md#first) |
| `grouping` | 🔜 | Grouping indicator for ROLLUP/CUBE/GROUPING SETS |
| `grouping_id` | 🔜 | Grouping indicator for ROLLUP/CUBE/GROUPING SETS |
| `histogram_numeric` | 🔜 | unclassified; not yet reviewed |
| `hll_sketch_agg` | 🔜 | unclassified; not yet reviewed |
| `hll_union_agg` | 🔜 | unclassified; not yet reviewed |
| `kll_merge_agg_bigint` | 🔜 | unclassified; not yet reviewed |
| `kll_merge_agg_double` | 🔜 | unclassified; not yet reviewed |
| `kll_merge_agg_float` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_agg_bigint` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_agg_double` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_agg_float` | 🔜 | unclassified; not yet reviewed |
| `kurtosis` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `last` | ✅ | [details](compatibility/expressions/aggregate.md#last) |
| `last_value` | ✅ | [details](compatibility/expressions/aggregate.md#last) |
| `listagg` | 🔜 | String aggregation |
| `max` | ✅ |  |
| `max_by` | 🔜 | [#3841](https://github.com/apache/datafusion-comet/issues/3841) |
| `mean` | ✅ | [details](compatibility/expressions/aggregate.md#average) |
| `median` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `min` | ✅ |  |
| `min_by` | 🔜 | [#3841](https://github.com/apache/datafusion-comet/issues/3841) |
| `mode` | 🔜 | [#3970](https://github.com/apache/datafusion-comet/issues/3970) |
| `percentile` | 🔜 | [#4542](https://github.com/apache/datafusion-comet/issues/4542) |
| `percentile_approx` | 🔜 | unclassified; not yet reviewed |
| `percentile_cont` | 🔜 | Percentile aggregate |
| `percentile_disc` | 🔜 | Percentile aggregate |
| `regr_avgx` | ✅ | Native: Spark rewrites to `Average` (tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551)) |
| `regr_avgy` | ✅ | Native: Spark rewrites to `Average` (tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551)) |
| `regr_count` | ✅ | Native: Spark rewrites to `Count` (tests in [#4551](https://github.com/apache/datafusion-comet/issues/4551)) |
| `regr_intercept` | 🔜 | Falls back; can reuse `covar_pop`/`var_pop` accumulators [#4552](https://github.com/apache/datafusion-comet/issues/4552) |
| `regr_r2` | 🔜 | Falls back; can reuse the `corr` accumulator [#4552](https://github.com/apache/datafusion-comet/issues/4552) |
| `regr_slope` | 🔜 | Falls back; can reuse `covar_pop`/`var_pop` accumulators [#4552](https://github.com/apache/datafusion-comet/issues/4552) |
| `regr_sxx` | 🔜 | Falls back; can reuse `var_pop` accumulator [#4552](https://github.com/apache/datafusion-comet/issues/4552) |
| `regr_sxy` | 🔜 | Falls back; can reuse `covar_pop` accumulator [#4552](https://github.com/apache/datafusion-comet/issues/4552) |
| `regr_syy` | 🔜 | Falls back; can reuse `var_pop` accumulator [#4552](https://github.com/apache/datafusion-comet/issues/4552) |
| `skewness` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `some` | ✅ |  |
| `std` | ✅ |  |
| `stddev` | ✅ |  |
| `stddev_pop` | ✅ |  |
| `stddev_samp` | ✅ |  |
| `string_agg` | 🔜 | String aggregation (alias of `listagg`) |
| `sum` | ✅ |  |
| `theta_intersection_agg` | 🔜 | unclassified; not yet reviewed |
| `theta_sketch_agg` | 🔜 | unclassified; not yet reviewed |
| `theta_union_agg` | 🔜 | unclassified; not yet reviewed |
| `try_avg` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `try_sum` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `var_pop` | ✅ |  |
| `var_samp` | ✅ |  |
| `variance` | ✅ |  |
<!--END:EXPR_TABLE[agg_funcs]-->

---

## array_funcs

<!--BEGIN:EXPR_TABLE[array_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `array` | ✅ |  |
| `array_append` | ✅ |  |
| `array_compact` | ✅ |  |
| `array_contains` | ✅ |  |
| `array_distinct` | ✅ |  |
| `array_except` | ✅ | [details](compatibility/expressions/array.md#arrayexcept) |
| `array_insert` | ✅ |  |
| `array_intersect` | ✅ | [details](compatibility/expressions/array.md#arrayintersect) |
| `array_join` | ✅ | [details](compatibility/expressions/array.md#arrayjoin) |
| `array_max` | ✅ |  |
| `array_min` | ✅ |  |
| `array_position` | ✅ |  |
| `array_prepend` | 🔜 | Sibling of `array_append` |
| `array_remove` | ✅ |  |
| `array_repeat` | ✅ |  |
| `array_size` | ✅ |  |
| `array_union` | ✅ |  |
| `arrays_overlap` | ✅ |  |
| `arrays_zip` | ✅ | [details](compatibility/expressions/array.md#arrayszip) |
| `flatten` | ✅ |  |
| `get` | ✅ |  |
| `sequence` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `shuffle` | 🔜 | Random array shuffle |
| `slice` | ✅ |  |
| `sort_array` | ✅ | [details](compatibility/expressions/array.md#sortarray) |
<!--END:EXPR_TABLE[array_funcs]-->

---

## bitwise_funcs

<!--BEGIN:EXPR_TABLE[bitwise_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `&` | ✅ |  |
| `<<` | ✅ |  |
| `>>` | ✅ |  |
| `>>>` | ✅ |  |
| `^` | ✅ |  |
| `bit_count` | ✅ |  |
| `bit_get` | ✅ |  |
| `getbit` | ✅ |  |
| `shiftleft` | ✅ |  |
| `shiftright` | ✅ |  |
| `shiftrightunsigned` | ✅ |  |
| `|` | ✅ |  |
| `~` | ✅ |  |
<!--END:EXPR_TABLE[bitwise_funcs]-->

---

## collection_funcs

<!--BEGIN:EXPR_TABLE[collection_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `cardinality` | ✅ | [details](compatibility/expressions/array.md#size) |
| `concat` | ✅ | [details](compatibility/expressions/string.md#concat) |
| `element_at` | ✅ | [details](compatibility/expressions/array.md#elementat) |
| `reverse` | ✅ | [details](compatibility/expressions/string.md#reverse) |
| `size` | ✅ | [details](compatibility/expressions/array.md#size) |
| `try_element_at` | ✅ | Lowers to `element_at`; array input (MapType falls back) |
<!--END:EXPR_TABLE[collection_funcs]-->

---

## conditional_funcs

<!--BEGIN:EXPR_TABLE[conditional_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `between` | ✅ |  |
| `coalesce` | ✅ |  |
| `if` | ✅ |  |
| `ifnull` | ✅ |  |
| `nanvl` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `nullif` | ✅ |  |
| `nullifzero` | ✅ | Lowers to `if`/`=` (Spark 4.0+) |
| `nvl` | ✅ |  |
| `nvl2` | ✅ |  |
| `when` | ✅ |  |
| `zeroifnull` | ✅ | Lowers to `coalesce` (Spark 4.0+) |
<!--END:EXPR_TABLE[conditional_funcs]-->

---

## conversion_funcs

The type-name conversion functions (`bigint`, `binary`, `boolean`, `date`, `decimal`, `double`, `float`, `int`, `smallint`, `string`, `timestamp`, `tinyint`) are SQL aliases for `CAST(... AS <type>)` and share the support and caveats of `cast`.

<!--BEGIN:EXPR_TABLE[conversion_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `bigint` | ✅ |  |
| `binary` | ✅ |  |
| `boolean` | ✅ |  |
| `cast` | ✅ |  |
| `date` | ✅ |  |
| `decimal` | ✅ |  |
| `double` | ✅ |  |
| `float` | ✅ |  |
| `int` | ✅ |  |
| `smallint` | ✅ |  |
| `string` | ✅ |  |
| `time` | ✅ |  |
| `timestamp` | ✅ |  |
| `tinyint` | ✅ |  |
<!--END:EXPR_TABLE[conversion_funcs]-->

---

## datetime_funcs

<!--BEGIN:EXPR_TABLE[datetime_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `add_months` | ✅ |  |
| `convert_timezone` | ✅ | [details](compatibility/expressions/datetime.md#converttimezone) |
| `curdate` | ✅ | Constant-folded to a literal (alias of `current_date`) |
| `current_date` | ✅ | Constant-folded to a literal before Comet sees the plan |
| `current_time` | 🔜 | Blocked on Spark 4.1 TIME type support [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `current_timestamp` | ✅ | Constant-folded to a literal before Comet sees the plan |
| `current_timezone` | ✅ |  |
| `date_add` | ✅ |  |
| `date_diff` | ✅ |  |
| `date_format` | ✅ | [details](compatibility/expressions/datetime.md#dateformatclass) |
| `date_from_unix_date` | ✅ |  |
| `date_part` | ✅ |  |
| `date_sub` | ✅ |  |
| `date_trunc` | ✅ | [details](compatibility/expressions/datetime.md#trunctimestamp) |
| `dateadd` | ✅ |  |
| `datediff` | ✅ |  |
| `datepart` | ✅ |  |
| `day` | ✅ |  |
| `dayname` | 🔜 | [#4544](https://github.com/apache/datafusion-comet/issues/4544) |
| `dayofmonth` | ✅ |  |
| `dayofweek` | ✅ |  |
| `dayofyear` | ✅ |  |
| `extract` | ✅ |  |
| `from_unixtime` | ✅ | [details](compatibility/expressions/datetime.md#fromunixtime) |
| `from_utc_timestamp` | ✅ | [details](compatibility/expressions/datetime.md#fromutctimestamp) |
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
| `to_utc_timestamp` | ✅ | [details](compatibility/expressions/datetime.md#toutctimestamp) |
| `trunc` | ✅ | [details](compatibility/expressions/datetime.md#truncdate) |
| `try_make_interval` | 🔜 | Produces legacy CalendarInterval; tracked by [#4540](https://github.com/apache/datafusion-comet/issues/4540) |
| `try_make_timestamp` | ✅ | Returns a wrong value instead of NULL for invalid inputs [#4554](https://github.com/apache/datafusion-comet/issues/4554) |
| `try_make_timestamp_ltz` | 🔜 | unclassified; not yet reviewed |
| `try_make_timestamp_ntz` | 🔜 | unclassified; not yet reviewed |
| `try_to_date` | 🔜 | Rewrites to `Cast`/`GetTimestamp` but currently falls back; tracked by [#4556](https://github.com/apache/datafusion-comet/issues/4556) |
| `try_to_time` | 🔜 | Spark 4.1 TIME type; tracked by [#4288](https://github.com/apache/datafusion-comet/issues/4288) |
| `try_to_timestamp` | 🔜 | Rewrites to `Cast`/`GetTimestamp` but currently falls back; tracked by [#4556](https://github.com/apache/datafusion-comet/issues/4556) |
| `unix_date` | ✅ |  |
| `unix_micros` | ✅ |  |
| `unix_millis` | ✅ |  |
| `unix_seconds` | ✅ |  |
| `unix_timestamp` | ✅ | [details](compatibility/expressions/datetime.md#unixtimestamp) |
| `weekday` | ✅ |  |
| `weekofyear` | ✅ |  |
| `window` | 🔜 | Time-window grouping; tracked by [#4553](https://github.com/apache/datafusion-comet/issues/4553) |
| `window_time` | 🔜 | Time-window grouping; tracked by [#4553](https://github.com/apache/datafusion-comet/issues/4553) |
| `year` | ✅ |  |
<!--END:EXPR_TABLE[datetime_funcs]-->

---

## generator_funcs

`explode` and `posexplode` are supported via `CometExplodeExec` (operator-level, not
expression-level). The `outer` variants are wired but marked `Incompatible`; they require
`spark.comet.exec.explode.enabled=true` and `allowIncompatible`.

<!--BEGIN:EXPR_TABLE[generator_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `explode` | ✅ | via `CometExplodeExec` |
| `explode_outer` | ✅ | outer=true falls back (Incompatible) ([audit](../../contributor-guide/expression-audits/generator_funcs.md#explode_outer)) |
| `inline` | 🔜 | Operator-level generator (like `explode`) |
| `inline_outer` | 🔜 | Operator-level generator (like `explode`) |
| `posexplode` | ✅ | via `CometExplodeExec` |
| `posexplode_outer` | ✅ | outer=true falls back (Incompatible) ([audit](../../contributor-guide/expression-audits/generator_funcs.md#posexplode_outer)) |
| `stack` | 🔜 | Operator-level generator |
<!--END:EXPR_TABLE[generator_funcs]-->

---

## hash_funcs

<!--BEGIN:EXPR_TABLE[hash_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `crc32` | ✅ |  |
| `hash` | ✅ |  |
| `md5` | ✅ |  |
| `sha` | ✅ |  |
| `sha1` | ✅ |  |
| `sha2` | ✅ |  |
| `xxhash64` | ✅ |  |
<!--END:EXPR_TABLE[hash_funcs]-->

---

## json_funcs

<!--BEGIN:EXPR_TABLE[json_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `from_json` | ✅ | [details](compatibility/expressions/struct.md#jsontostructs) |
| `get_json_object` | ✅ | [details](compatibility/expressions/string.md#getjsonobject) |
| `json_array_length` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `json_object_keys` | 🔜 | [#3161](https://github.com/apache/datafusion-comet/issues/3161) |
| `json_tuple` | 🔜 | [#3160](https://github.com/apache/datafusion-comet/issues/3160) |
| `schema_of_json` | 🔜 | [#3163](https://github.com/apache/datafusion-comet/issues/3163) |
| `to_json` | ✅ |  |
<!--END:EXPR_TABLE[json_funcs]-->

---

## lambda_funcs

All higher-order functions are planned via [#4224](https://github.com/apache/datafusion-comet/issues/4224).

<!--BEGIN:EXPR_TABLE[lambda_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `aggregate` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `array_sort` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `exists` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `filter` | ✅ | [details](compatibility/expressions/array.md#arrayfilter) |
| `forall` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `map_filter` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `map_zip_with` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `reduce` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `transform` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `transform_keys` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `transform_values` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `zip_with` | 🔜 | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
<!--END:EXPR_TABLE[lambda_funcs]-->

---

## map_funcs

<!--BEGIN:EXPR_TABLE[map_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `map` | 🔜 | Constructs a map |
| `map_concat` | 🔜 | Concatenates maps |
| `map_contains_key` | ✅ |  |
| `map_entries` | ✅ |  |
| `map_from_arrays` | ✅ |  |
| `map_from_entries` | ✅ | [details](compatibility/expressions/map.md#mapfromentries) |
| `map_keys` | ✅ |  |
| `map_values` | ✅ |  |
| `str_to_map` | ✅ |  |
<!--END:EXPR_TABLE[map_funcs]-->

---

## math_funcs

<!--BEGIN:EXPR_TABLE[math_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `%` | ✅ |  |
| `*` | ✅ |  |
| `+` | ✅ |  |
| `-` | ✅ |  |
| `/` | ✅ |  |
| `abs` | ✅ | [details](compatibility/expressions/math.md#abs) |
| `acos` | ✅ |  |
| `acosh` | ✅ |  |
| `asin` | ✅ |  |
| `asinh` | ✅ |  |
| `atan` | ✅ |  |
| `atan2` | ✅ |  |
| `atanh` | ✅ |  |
| `bin` | ✅ |  |
| `bround` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `cbrt` | ✅ |  |
| `ceil` | ✅ | Two-arg form falls back |
| `ceiling` | ✅ |  |
| `conv` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
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
| `hypot` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `least` | ✅ |  |
| `ln` | ✅ |  |
| `log` | ✅ |  |
| `log10` | ✅ |  |
| `log1p` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `log2` | ✅ |  |
| `mod` | ✅ |  |
| `negative` | ✅ |  |
| `pi` | ✅ |  |
| `pmod` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `positive` | ✅ |  |
| `pow` | ✅ |  |
| `power` | ✅ |  |
| `radians` | ✅ |  |
| `rand` | ✅ | [details](compatibility/expressions/math.md#rand) |
| `randn` | ✅ | [details](compatibility/expressions/math.md#randn) |
| `random` | ✅ | [details](compatibility/expressions/math.md#rand) |
| `rint` | ✅ |  |
| `round` | ✅ |  |
| `sec` | ✅ |  |
| `sign` | ✅ |  |
| `signum` | ✅ |  |
| `sin` | ✅ |  |
| `sinh` | ✅ |  |
| `sqrt` | ✅ |  |
| `tan` | ✅ |  |
| `tanh` | ✅ |  |
| `try_add` | ✅ | Datetime/interval form falls back |
| `try_divide` | ✅ |  |
| `try_mod` | 🔜 | Lowers to `Remainder` with TRY eval mode, which falls back [#4484](https://github.com/apache/datafusion-comet/issues/4484) |
| `try_multiply` | ✅ |  |
| `try_subtract` | ✅ |  |
| `unhex` | ✅ |  |
| `uniform` | ✅ | Constant-folded; literal arguments only (Spark 4.0+) |
| `width_bucket` | ✅ |  |
<!--END:EXPR_TABLE[math_funcs]-->

---

## misc_funcs

<!--BEGIN:EXPR_TABLE[misc_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `aes_decrypt` | 🔜 | Falls back; `StaticInvoke` not allowlisted; planned via codegen dispatch [#4558](https://github.com/apache/datafusion-comet/issues/4558) |
| `aes_encrypt` | 🔜 | Falls back; planned via codegen dispatch; nondeterministic IV by default [#4558](https://github.com/apache/datafusion-comet/issues/4558) |
| `approx_top_k_estimate` | 🔜 | unclassified; not yet reviewed |
| `assert_true` | 🔜 | Lowers to `RaiseError`, which falls back |
| `bitmap_bit_position` | 🔜 | unclassified; not yet reviewed |
| `bitmap_bucket_number` | 🔜 | unclassified; not yet reviewed |
| `bitmap_count` | 🔜 | unclassified; not yet reviewed |
| `current_catalog` | ✅ | Resolved to a literal by the analyzer (`ReplaceCurrentLike`) |
| `current_database` | ✅ | Resolved to a literal by the analyzer (`ReplaceCurrentLike`) |
| `current_schema` | ✅ | Alias of `current_database`; resolved to a literal by the analyzer |
| `current_user` | ✅ | Resolved to a literal by the analyzer; same as `user` |
| `from_avro` | 🔜 | unclassified; not yet reviewed |
| `from_protobuf` | 🔜 | unclassified; not yet reviewed |
| `hll_sketch_estimate` | 🔜 | unclassified; not yet reviewed |
| `hll_union` | 🔜 | unclassified; not yet reviewed |
| `input_file_block_length` | 🔜 | unclassified; not yet reviewed |
| `input_file_block_start` | 🔜 | unclassified; not yet reviewed |
| `input_file_name` | 🔜 | unclassified; not yet reviewed |
| `java_method` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_get_n_bigint` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_get_n_double` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_get_n_float` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_get_quantile_bigint` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_get_quantile_double` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_get_quantile_float` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_get_rank_bigint` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_get_rank_double` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_get_rank_float` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_merge_bigint` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_merge_double` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_merge_float` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_to_string_bigint` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_to_string_double` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_to_string_float` | 🔜 | unclassified; not yet reviewed |
| `monotonically_increasing_id` | ✅ |  |
| `raise_error` | 🔜 | Raises a runtime error |
| `reflect` | 🔜 | unclassified; not yet reviewed |
| `schema_of_avro` | 🔜 | unclassified; not yet reviewed |
| `session_user` | ✅ | Alias of `current_user`; resolved to a literal by the analyzer |
| `spark_partition_id` | ✅ |  |
| `theta_difference` | 🔜 | unclassified; not yet reviewed |
| `theta_intersection` | 🔜 | unclassified; not yet reviewed |
| `theta_sketch_estimate` | 🔜 | unclassified; not yet reviewed |
| `theta_union` | 🔜 | unclassified; not yet reviewed |
| `to_avro` | 🔜 | unclassified; not yet reviewed |
| `to_protobuf` | 🔜 | unclassified; not yet reviewed |
| `try_aes_decrypt` | 🔜 | Falls back; planned via codegen dispatch [#4558](https://github.com/apache/datafusion-comet/issues/4558) |
| `try_reflect` | 🔜 | unclassified; not yet reviewed |
| `typeof` | ✅ | Foldable; resolved to a literal before Comet sees the plan |
| `user` | ✅ | Resolved to a literal by the Spark analyzer before reaching Comet |
| `uuid` | 🔜 | Nondeterministic random UUID |
| `version` | 🔜 | unclassified; not yet reviewed |
<!--END:EXPR_TABLE[misc_funcs]-->

---

## predicate_funcs

<!--BEGIN:EXPR_TABLE[predicate_funcs]-->
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
| `equal_null` | ✅ | Lowers to `<=>` (`EqualNullSafe`) |
| `ilike` | ✅ |  |
| `in` | ✅ |  |
| `isnan` | ✅ |  |
| `isnotnull` | ✅ |  |
| `isnull` | ✅ |  |
| `like` | ✅ |  |
| `not` | ✅ |  |
| `or` | ✅ |  |
| `regexp` | ✅ | [details](compatibility/expressions/string.md#rlike) |
| `regexp_like` | ✅ | [details](compatibility/expressions/string.md#rlike) |
| `rlike` | ✅ | [details](compatibility/expressions/string.md#rlike) |
<!--END:EXPR_TABLE[predicate_funcs]-->

---

## string_funcs

<!--BEGIN:EXPR_TABLE[string_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `ascii` | ✅ |  |
| `base64` | 🔜 | Lowers to `StaticInvoke(encode)` (not allowlisted); falls back |
| `bit_length` | ✅ |  |
| `btrim` | ✅ |  |
| `char` | ✅ |  |
| `char_length` | ✅ | [details](compatibility/expressions/string.md#length) |
| `character_length` | ✅ | [details](compatibility/expressions/string.md#length) |
| `chr` | ✅ |  |
| `collate` | 🔜 | Spark collation (umbrella) [#2190](https://github.com/apache/datafusion-comet/issues/2190) |
| `collation` | ✅ | Constant-folded to a literal (Spark 4.0+) |
| `concat_ws` | ✅ |  |
| `contains` | ✅ |  |
| `decode` | ✅ |  |
| `elt` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `encode` | 🔜 | Lowers to `StaticInvoke(encode)` (not allowlisted); falls back |
| `endswith` | ✅ |  |
| `find_in_set` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `format_number` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `format_string` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `initcap` | ✅ |  |
| `instr` | ✅ |  |
| `is_valid_utf8` | 🔜 | unclassified; not yet reviewed |
| `lcase` | ✅ |  |
| `left` | ✅ | [details](compatibility/expressions/string.md#left) |
| `len` | ✅ | [details](compatibility/expressions/string.md#length) |
| `length` | ✅ | [details](compatibility/expressions/string.md#length) |
| `levenshtein` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `locate` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `lower` | ✅ |  |
| `lpad` | ✅ |  |
| `ltrim` | ✅ |  |
| `luhn_check` | ✅ | Native via `StaticInvoke` (tests: luhn_check.sql) |
| `make_valid_utf8` | 🔜 | unclassified; not yet reviewed |
| `mask` | 🔜 | Data masking |
| `octet_length` | ✅ |  |
| `overlay` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `position` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `printf` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `quote` | 🔜 | unclassified; not yet reviewed |
| `randstr` | 🔜 | Random string (Spark 4.0+) |
| `regexp_count` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `regexp_extract` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `regexp_extract_all` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `regexp_instr` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `regexp_replace` | ✅ | [details](compatibility/expressions/string.md#regexpreplace) |
| `regexp_substr` | 🔜 | tracking [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `repeat` | ✅ | [details](compatibility/expressions/string.md#stringrepeat) |
| `replace` | ✅ |  |
| `right` | ✅ | [details](compatibility/expressions/string.md#right) |
| `rpad` | ✅ |  |
| `rtrim` | ✅ |  |
| `sentences` | 🔜 | unclassified; not yet reviewed |
| `soundex` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `space` | ✅ |  |
| `split` | ✅ | [details](compatibility/expressions/string.md#stringsplit) |
| `split_part` | 🔜 | Lowers to `element_at(StringSplitSQL(...))`; `StringSplitSQL` falls back [#4561](https://github.com/apache/datafusion-comet/issues/4561) |
| `startswith` | ✅ |  |
| `substr` | ✅ |  |
| `substring` | ✅ |  |
| `substring_index` | ✅ |  |
| `to_binary` | ✅ | Hex form accelerated; other formats fall back |
| `to_char` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `to_number` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `to_varchar` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `translate` | ✅ |  |
| `trim` | ✅ |  |
| `try_to_binary` | 🔜 | Lowers to `TryEval(...)`, which falls back |
| `try_to_number` | 🔜 | TRY variant of `to_number` |
| `try_validate_utf8` | 🔜 | unclassified; not yet reviewed |
| `ucase` | ✅ |  |
| `unbase64` | 🔜 | [#4538](https://github.com/apache/datafusion-comet/issues/4538) |
| `upper` | ✅ |  |
| `validate_utf8` | 🔜 | unclassified; not yet reviewed |
<!--END:EXPR_TABLE[string_funcs]-->

---

## struct_funcs

<!--BEGIN:EXPR_TABLE[struct_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `named_struct` | ✅ | [details](compatibility/expressions/struct.md#createnamedstruct) |
| `struct` | ✅ | [details](compatibility/expressions/struct.md#createnamedstruct) |
<!--END:EXPR_TABLE[struct_funcs]-->

---

## url_funcs

<!--BEGIN:EXPR_TABLE[url_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `parse_url` | ✅ |  |
| `try_parse_url` | 🔜 | unclassified; not yet reviewed |
| `try_url_decode` | ✅ |  |
| `url_decode` | ✅ |  |
| `url_encode` | ✅ |  |
<!--END:EXPR_TABLE[url_funcs]-->

---

## window_funcs

Window functions run via `CometWindowExec`. Window support is disabled by default due to known
correctness issues (tracking [#2721](https://github.com/apache/datafusion-comet/issues/2721)).
When enabled, `lag` and `lead` are explicitly wired; aggregate window functions (`count`, `min`,
`max`, `sum`) are also supported. Ranking functions (`rank`, `dense_rank`, `row_number`,
`ntile`, `percent_rank`, `cume_dist`, `nth_value`) are not yet wired in the window serde and
fall back to Spark.

<!--BEGIN:EXPR_TABLE[window_funcs]-->
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
<!--END:EXPR_TABLE[window_funcs]-->

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
