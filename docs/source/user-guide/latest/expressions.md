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
| `any` | 🔜 | unclassified; not yet reviewed |
| `any_value` | 🔜 | unclassified; not yet reviewed |
| `approx_count_distinct` | 🔜 | [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `approx_percentile` | 🔜 | unclassified; not yet reviewed |
| `approx_top_k` | 🔜 | unclassified; not yet reviewed |
| `approx_top_k_accumulate` | 🔜 | unclassified; not yet reviewed |
| `approx_top_k_combine` | 🔜 | unclassified; not yet reviewed |
| `array_agg` | 🔜 | unclassified; not yet reviewed |
| `avg` | ✅ | [details](compatibility/expressions/aggregate.md#average) |
| `bit_and` | ✅ |  |
| `bit_or` | ✅ |  |
| `bit_xor` | ✅ |  |
| `bitmap_and_agg` | 🔜 | unclassified; not yet reviewed |
| `bitmap_construct_agg` | 🔜 | unclassified; not yet reviewed |
| `bitmap_or_agg` | 🔜 | unclassified; not yet reviewed |
| `bool_and` | 🔜 | unclassified; not yet reviewed |
| `bool_or` | 🔜 | unclassified; not yet reviewed |
| `collect_list` | 🔜 | unclassified; not yet reviewed |
| `collect_set` | ✅ | [details](compatibility/expressions/aggregate.md#collectset) |
| `corr` | ✅ |  |
| `count` | ✅ |  |
| `count_if` | 🔜 | unclassified; not yet reviewed |
| `count_min_sketch` | 🔜 | unclassified; not yet reviewed |
| `covar_pop` | ✅ |  |
| `covar_samp` | ✅ |  |
| `every` | 🔜 | unclassified; not yet reviewed |
| `first` | ✅ | [details](compatibility/expressions/aggregate.md#first) |
| `first_value` | ✅ | [details](compatibility/expressions/aggregate.md#first) |
| `grouping` | 🔜 | unclassified; not yet reviewed |
| `grouping_id` | 🔜 | unclassified; not yet reviewed |
| `histogram_numeric` | 🔜 | unclassified; not yet reviewed |
| `hll_sketch_agg` | 🔜 | unclassified; not yet reviewed |
| `hll_union_agg` | 🔜 | unclassified; not yet reviewed |
| `kll_merge_agg_bigint` | 🔜 | unclassified; not yet reviewed |
| `kll_merge_agg_double` | 🔜 | unclassified; not yet reviewed |
| `kll_merge_agg_float` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_agg_bigint` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_agg_double` | 🔜 | unclassified; not yet reviewed |
| `kll_sketch_agg_float` | 🔜 | unclassified; not yet reviewed |
| `kurtosis` | 🔜 | [#4098](https://github.com/apache/datafusion-comet/issues/4098) |
| `last` | ✅ | [details](compatibility/expressions/aggregate.md#last) |
| `last_value` | ✅ | [details](compatibility/expressions/aggregate.md#last) |
| `listagg` | 🔜 | unclassified; not yet reviewed |
| `max` | ✅ |  |
| `max_by` | 🔜 | unclassified; not yet reviewed |
| `mean` | ✅ | [details](compatibility/expressions/aggregate.md#average) |
| `median` | 🔜 | unclassified; not yet reviewed |
| `min` | ✅ |  |
| `min_by` | 🔜 | unclassified; not yet reviewed |
| `mode` | 🔜 | unclassified; not yet reviewed |
| `percentile` | 🔜 | unclassified; not yet reviewed |
| `percentile_approx` | 🔜 | unclassified; not yet reviewed |
| `percentile_cont` | 🔜 | unclassified; not yet reviewed |
| `percentile_disc` | 🔜 | unclassified; not yet reviewed |
| `regr_avgx` | 🔜 | unclassified; not yet reviewed |
| `regr_avgy` | 🔜 | unclassified; not yet reviewed |
| `regr_count` | 🔜 | unclassified; not yet reviewed |
| `regr_intercept` | 🔜 | unclassified; not yet reviewed |
| `regr_r2` | 🔜 | unclassified; not yet reviewed |
| `regr_slope` | 🔜 | unclassified; not yet reviewed |
| `regr_sxx` | 🔜 | unclassified; not yet reviewed |
| `regr_sxy` | 🔜 | unclassified; not yet reviewed |
| `regr_syy` | 🔜 | unclassified; not yet reviewed |
| `skewness` | 🔜 | unclassified; not yet reviewed |
| `some` | 🔜 | unclassified; not yet reviewed |
| `std` | ✅ |  |
| `stddev` | ✅ |  |
| `stddev_pop` | ✅ |  |
| `stddev_samp` | ✅ |  |
| `string_agg` | 🔜 | unclassified; not yet reviewed |
| `sum` | ✅ |  |
| `theta_intersection_agg` | 🔜 | unclassified; not yet reviewed |
| `theta_sketch_agg` | 🔜 | unclassified; not yet reviewed |
| `theta_union_agg` | 🔜 | unclassified; not yet reviewed |
| `try_avg` | 🔜 | unclassified; not yet reviewed |
| `try_sum` | 🔜 | unclassified; not yet reviewed |
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
| `array_prepend` | 🔜 | unclassified; not yet reviewed |
| `array_remove` | ✅ |  |
| `array_repeat` | ✅ |  |
| `array_size` | 🔜 | unclassified; not yet reviewed |
| `array_union` | ✅ |  |
| `arrays_overlap` | ✅ |  |
| `arrays_zip` | ✅ | [details](compatibility/expressions/array.md#arrayszip) |
| `flatten` | ✅ |  |
| `get` | 🔜 | unclassified; not yet reviewed |
| `sequence` | 🔜 | unclassified; not yet reviewed |
| `shuffle` | 🔜 | unclassified; not yet reviewed |
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
| `try_element_at` | 🔜 | unclassified; not yet reviewed |
<!--END:EXPR_TABLE[collection_funcs]-->

---

## conditional_funcs

<!--BEGIN:EXPR_TABLE[conditional_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `between` | 🔜 | unclassified; not yet reviewed |
| `coalesce` | ✅ |  |
| `if` | ✅ |  |
| `ifnull` | 🔜 | unclassified; not yet reviewed |
| `nanvl` | 🔜 | unclassified; not yet reviewed |
| `nullif` | 🔜 | unclassified; not yet reviewed |
| `nullifzero` | 🔜 | unclassified; not yet reviewed |
| `nvl` | 🔜 | unclassified; not yet reviewed |
| `nvl2` | 🔜 | unclassified; not yet reviewed |
| `when` | ✅ |  |
| `zeroifnull` | 🔜 | unclassified; not yet reviewed |
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
| `curdate` | 🔜 | unclassified; not yet reviewed |
| `current_date` | 🔜 | unclassified; not yet reviewed |
| `current_time` | 🔜 | unclassified; not yet reviewed |
| `current_timestamp` | 🔜 | unclassified; not yet reviewed |
| `current_timezone` | 🔜 | unclassified; not yet reviewed |
| `date_add` | ✅ |  |
| `date_diff` | ✅ |  |
| `date_format` | ✅ | [details](compatibility/expressions/datetime.md#dateformatclass) |
| `date_from_unix_date` | ✅ |  |
| `date_part` | 🔜 | unclassified; not yet reviewed |
| `date_sub` | ✅ |  |
| `date_trunc` | ✅ | [details](compatibility/expressions/datetime.md#trunctimestamp) |
| `dateadd` | ✅ |  |
| `datediff` | ✅ |  |
| `datepart` | 🔜 | unclassified; not yet reviewed |
| `day` | ✅ |  |
| `dayname` | 🔜 | unclassified; not yet reviewed |
| `dayofmonth` | ✅ |  |
| `dayofweek` | ✅ |  |
| `dayofyear` | ✅ |  |
| `extract` | 🔜 | unclassified; not yet reviewed |
| `from_unixtime` | ✅ | [details](compatibility/expressions/datetime.md#fromunixtime) |
| `from_utc_timestamp` | ✅ | [details](compatibility/expressions/datetime.md#fromutctimestamp) |
| `hour` | 🔜 | unclassified; not yet reviewed |
| `last_day` | ✅ |  |
| `localtimestamp` | 🔜 | unclassified; not yet reviewed |
| `make_date` | ✅ |  |
| `make_dt_interval` | 🔜 | unclassified; not yet reviewed |
| `make_interval` | 🔜 | unclassified; not yet reviewed |
| `make_time` | 🔜 | unclassified; not yet reviewed |
| `make_timestamp` | 🔜 | unclassified; not yet reviewed |
| `make_timestamp_ltz` | 🔜 | unclassified; not yet reviewed |
| `make_timestamp_ntz` | 🔜 | unclassified; not yet reviewed |
| `make_ym_interval` | 🔜 | unclassified; not yet reviewed |
| `minute` | 🔜 | unclassified; not yet reviewed |
| `month` | ✅ |  |
| `monthname` | 🔜 | unclassified; not yet reviewed |
| `months_between` | ✅ |  |
| `next_day` | ✅ |  |
| `now` | 🔜 | unclassified; not yet reviewed |
| `quarter` | ✅ |  |
| `second` | 🔜 | unclassified; not yet reviewed |
| `session_window` | 🔜 | unclassified; not yet reviewed |
| `time_diff` | 🔜 | unclassified; not yet reviewed |
| `time_trunc` | 🔜 | unclassified; not yet reviewed |
| `timestamp_micros` | ✅ |  |
| `timestamp_millis` | ✅ |  |
| `timestamp_seconds` | ✅ |  |
| `to_date` | 🔜 | unclassified; not yet reviewed |
| `to_time` | 🔜 | unclassified; not yet reviewed |
| `to_timestamp` | 🔜 | unclassified; not yet reviewed |
| `to_timestamp_ltz` | 🔜 | unclassified; not yet reviewed |
| `to_timestamp_ntz` | 🔜 | unclassified; not yet reviewed |
| `to_unix_timestamp` | ✅ |  |
| `to_utc_timestamp` | ✅ | [details](compatibility/expressions/datetime.md#toutctimestamp) |
| `trunc` | ✅ | [details](compatibility/expressions/datetime.md#truncdate) |
| `try_make_interval` | 🔜 | unclassified; not yet reviewed |
| `try_make_timestamp` | 🔜 | unclassified; not yet reviewed |
| `try_make_timestamp_ltz` | 🔜 | unclassified; not yet reviewed |
| `try_make_timestamp_ntz` | 🔜 | unclassified; not yet reviewed |
| `try_to_date` | 🔜 | unclassified; not yet reviewed |
| `try_to_time` | 🔜 | unclassified; not yet reviewed |
| `try_to_timestamp` | 🔜 | unclassified; not yet reviewed |
| `unix_date` | ✅ |  |
| `unix_micros` | ✅ |  |
| `unix_millis` | ✅ |  |
| `unix_seconds` | ✅ |  |
| `unix_timestamp` | ✅ | [details](compatibility/expressions/datetime.md#unixtimestamp) |
| `weekday` | ✅ |  |
| `weekofyear` | ✅ |  |
| `window` | 🔜 | unclassified; not yet reviewed |
| `window_time` | 🔜 | unclassified; not yet reviewed |
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
| `explode` | 🔜 | unclassified; not yet reviewed |
| `explode_outer` | 🔜 | unclassified; not yet reviewed |
| `inline` | 🔜 | unclassified; not yet reviewed |
| `inline_outer` | 🔜 | unclassified; not yet reviewed |
| `posexplode` | 🔜 | unclassified; not yet reviewed |
| `posexplode_outer` | 🔜 | unclassified; not yet reviewed |
| `stack` | 🔜 | unclassified; not yet reviewed |
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
| `json_array_length` | 🔜 | unclassified; not yet reviewed |
| `json_object_keys` | 🔜 | unclassified; not yet reviewed |
| `json_tuple` | 🔜 | unclassified; not yet reviewed |
| `schema_of_json` | 🔜 | unclassified; not yet reviewed |
| `to_json` | ✅ |  |
<!--END:EXPR_TABLE[json_funcs]-->

---

## lambda_funcs

All higher-order functions are planned via [#4224](https://github.com/apache/datafusion-comet/issues/4224).

<!--BEGIN:EXPR_TABLE[lambda_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `aggregate` | 🔜 | unclassified; not yet reviewed |
| `array_sort` | 🔜 | unclassified; not yet reviewed |
| `exists` | 🔜 | unclassified; not yet reviewed |
| `filter` | ✅ | [details](compatibility/expressions/array.md#arrayfilter) |
| `forall` | 🔜 | unclassified; not yet reviewed |
| `map_filter` | 🔜 | unclassified; not yet reviewed |
| `map_zip_with` | 🔜 | unclassified; not yet reviewed |
| `reduce` | 🔜 | unclassified; not yet reviewed |
| `transform` | 🔜 | unclassified; not yet reviewed |
| `transform_keys` | 🔜 | unclassified; not yet reviewed |
| `transform_values` | 🔜 | unclassified; not yet reviewed |
| `zip_with` | 🔜 | unclassified; not yet reviewed |
<!--END:EXPR_TABLE[lambda_funcs]-->

---

## map_funcs

<!--BEGIN:EXPR_TABLE[map_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `map` | 🔜 | unclassified; not yet reviewed |
| `map_concat` | 🔜 | unclassified; not yet reviewed |
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
| `bround` | 🔜 | unclassified; not yet reviewed |
| `cbrt` | ✅ |  |
| `ceil` | 🔜 | unclassified; not yet reviewed |
| `ceiling` | 🔜 | unclassified; not yet reviewed |
| `conv` | 🔜 | unclassified; not yet reviewed |
| `cos` | ✅ |  |
| `cosh` | ✅ |  |
| `cot` | ✅ |  |
| `csc` | ✅ |  |
| `degrees` | ✅ |  |
| `div` | ✅ |  |
| `e` | 🔜 | unclassified; not yet reviewed |
| `exp` | ✅ |  |
| `expm1` | ✅ |  |
| `factorial` | ✅ |  |
| `floor` | 🔜 | unclassified; not yet reviewed |
| `greatest` | ✅ |  |
| `hex` | ✅ |  |
| `hypot` | 🔜 | unclassified; not yet reviewed |
| `least` | ✅ |  |
| `ln` | ✅ |  |
| `log` | ✅ |  |
| `log10` | ✅ |  |
| `log1p` | 🔜 | unclassified; not yet reviewed |
| `log2` | ✅ |  |
| `mod` | ✅ |  |
| `negative` | ✅ |  |
| `pi` | ✅ |  |
| `pmod` | 🔜 | unclassified; not yet reviewed |
| `positive` | 🔜 | unclassified; not yet reviewed |
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
| `try_add` | 🔜 | unclassified; not yet reviewed |
| `try_divide` | 🔜 | unclassified; not yet reviewed |
| `try_mod` | 🔜 | unclassified; not yet reviewed |
| `try_multiply` | 🔜 | unclassified; not yet reviewed |
| `try_subtract` | 🔜 | unclassified; not yet reviewed |
| `unhex` | ✅ |  |
| `uniform` | 🔜 | unclassified; not yet reviewed |
| `width_bucket` | 🔜 | unclassified; not yet reviewed |
<!--END:EXPR_TABLE[math_funcs]-->

---

## misc_funcs

<!--BEGIN:EXPR_TABLE[misc_funcs]-->
| Function | Status | Notes |
| --- | --- | --- |
| `aes_decrypt` | 🔜 | unclassified; not yet reviewed |
| `aes_encrypt` | 🔜 | unclassified; not yet reviewed |
| `approx_top_k_estimate` | 🔜 | unclassified; not yet reviewed |
| `assert_true` | 🔜 | unclassified; not yet reviewed |
| `bitmap_bit_position` | 🔜 | unclassified; not yet reviewed |
| `bitmap_bucket_number` | 🔜 | unclassified; not yet reviewed |
| `bitmap_count` | 🔜 | unclassified; not yet reviewed |
| `current_catalog` | 🔜 | unclassified; not yet reviewed |
| `current_database` | 🔜 | unclassified; not yet reviewed |
| `current_schema` | 🔜 | unclassified; not yet reviewed |
| `current_user` | 🔜 | unclassified; not yet reviewed |
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
| `raise_error` | 🔜 | unclassified; not yet reviewed |
| `reflect` | 🔜 | unclassified; not yet reviewed |
| `schema_of_avro` | 🔜 | unclassified; not yet reviewed |
| `session_user` | 🔜 | unclassified; not yet reviewed |
| `spark_partition_id` | ✅ |  |
| `theta_difference` | 🔜 | unclassified; not yet reviewed |
| `theta_intersection` | 🔜 | unclassified; not yet reviewed |
| `theta_sketch_estimate` | 🔜 | unclassified; not yet reviewed |
| `theta_union` | 🔜 | unclassified; not yet reviewed |
| `to_avro` | 🔜 | unclassified; not yet reviewed |
| `to_protobuf` | 🔜 | unclassified; not yet reviewed |
| `try_aes_decrypt` | 🔜 | unclassified; not yet reviewed |
| `try_reflect` | 🔜 | unclassified; not yet reviewed |
| `typeof` | 🔜 | unclassified; not yet reviewed |
| `user` | 🔜 | unclassified; not yet reviewed |
| `uuid` | 🔜 | unclassified; not yet reviewed |
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
| `equal_null` | 🔜 | unclassified; not yet reviewed |
| `ilike` | 🔜 | unclassified; not yet reviewed |
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
| `base64` | 🔜 | unclassified; not yet reviewed |
| `bit_length` | ✅ |  |
| `btrim` | ✅ |  |
| `char` | ✅ |  |
| `char_length` | ✅ | [details](compatibility/expressions/string.md#length) |
| `character_length` | ✅ | [details](compatibility/expressions/string.md#length) |
| `chr` | ✅ |  |
| `collate` | 🔜 | unclassified; not yet reviewed |
| `collation` | 🔜 | unclassified; not yet reviewed |
| `concat_ws` | ✅ |  |
| `contains` | 🔜 | unclassified; not yet reviewed |
| `decode` | 🔜 | unclassified; not yet reviewed |
| `elt` | 🔜 | unclassified; not yet reviewed |
| `encode` | 🔜 | unclassified; not yet reviewed |
| `endswith` | 🔜 | unclassified; not yet reviewed |
| `find_in_set` | 🔜 | unclassified; not yet reviewed |
| `format_number` | 🔜 | unclassified; not yet reviewed |
| `format_string` | 🔜 | unclassified; not yet reviewed |
| `initcap` | ✅ |  |
| `instr` | ✅ |  |
| `is_valid_utf8` | 🔜 | unclassified; not yet reviewed |
| `lcase` | ✅ |  |
| `left` | ✅ | [details](compatibility/expressions/string.md#left) |
| `len` | ✅ | [details](compatibility/expressions/string.md#length) |
| `length` | ✅ | [details](compatibility/expressions/string.md#length) |
| `levenshtein` | 🔜 | unclassified; not yet reviewed |
| `locate` | 🔜 | unclassified; not yet reviewed |
| `lower` | ✅ |  |
| `lpad` | 🔜 | unclassified; not yet reviewed |
| `ltrim` | ✅ |  |
| `luhn_check` | 🔜 | unclassified; not yet reviewed |
| `make_valid_utf8` | 🔜 | unclassified; not yet reviewed |
| `mask` | 🔜 | unclassified; not yet reviewed |
| `octet_length` | ✅ |  |
| `overlay` | 🔜 | unclassified; not yet reviewed |
| `position` | 🔜 | unclassified; not yet reviewed |
| `printf` | 🔜 | unclassified; not yet reviewed |
| `quote` | 🔜 | unclassified; not yet reviewed |
| `randstr` | 🔜 | unclassified; not yet reviewed |
| `regexp_count` | 🔜 | unclassified; not yet reviewed |
| `regexp_extract` | 🔜 | unclassified; not yet reviewed |
| `regexp_extract_all` | 🔜 | unclassified; not yet reviewed |
| `regexp_instr` | 🔜 | unclassified; not yet reviewed |
| `regexp_replace` | ✅ | [details](compatibility/expressions/string.md#regexpreplace) |
| `regexp_substr` | 🔜 | unclassified; not yet reviewed |
| `repeat` | ✅ | [details](compatibility/expressions/string.md#stringrepeat) |
| `replace` | ✅ |  |
| `right` | ✅ | [details](compatibility/expressions/string.md#right) |
| `rpad` | 🔜 | unclassified; not yet reviewed |
| `rtrim` | ✅ |  |
| `sentences` | 🔜 | unclassified; not yet reviewed |
| `soundex` | 🔜 | unclassified; not yet reviewed |
| `space` | ✅ |  |
| `split` | ✅ | [details](compatibility/expressions/string.md#stringsplit) |
| `split_part` | 🔜 | unclassified; not yet reviewed |
| `startswith` | 🔜 | unclassified; not yet reviewed |
| `substr` | ✅ |  |
| `substring` | ✅ |  |
| `substring_index` | ✅ |  |
| `to_binary` | 🔜 | unclassified; not yet reviewed |
| `to_char` | 🔜 | unclassified; not yet reviewed |
| `to_number` | 🔜 | unclassified; not yet reviewed |
| `to_varchar` | 🔜 | unclassified; not yet reviewed |
| `translate` | ✅ |  |
| `trim` | ✅ |  |
| `try_to_binary` | 🔜 | unclassified; not yet reviewed |
| `try_to_number` | 🔜 | unclassified; not yet reviewed |
| `try_validate_utf8` | 🔜 | unclassified; not yet reviewed |
| `ucase` | ✅ |  |
| `unbase64` | 🔜 | unclassified; not yet reviewed |
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
| `try_url_decode` | 🔜 | unclassified; not yet reviewed |
| `url_decode` | 🔜 | unclassified; not yet reviewed |
| `url_encode` | 🔜 | unclassified; not yet reviewed |
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
| `cume_dist` | 🔜 | unclassified; not yet reviewed |
| `dense_rank` | 🔜 | unclassified; not yet reviewed |
| `lag` | 🔜 | unclassified; not yet reviewed |
| `lead` | 🔜 | unclassified; not yet reviewed |
| `nth_value` | 🔜 | unclassified; not yet reviewed |
| `ntile` | 🔜 | unclassified; not yet reviewed |
| `percent_rank` | 🔜 | unclassified; not yet reviewed |
| `rank` | 🔜 | unclassified; not yet reviewed |
| `row_number` | 🔜 | unclassified; not yet reviewed |
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
