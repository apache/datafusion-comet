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

Expressions marked ✅ Supported are enabled by default. Expressions marked ⚠️ Supported
(caveats) include cases that are known to diverge from Spark; those cases fall back to Spark
by default and must be opted into per expression with
`spark.comet.expression.EXPRNAME.allowIncompatible=true` (where `EXPRNAME` is the Spark
expression class name, for example `Cast`). There is no global opt-in.

Most expressions can also be disabled with `spark.comet.expression.EXPRNAME.enabled=false`, where
`EXPRNAME` is the Spark expression class name (for example `Length` or `StartsWith`). See the
[Comet Configuration Guide](configs.md) for the full list.

## Status legend

| Status                 | Meaning                                                                                                                                                                                 |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ✅ Supported           | Native or codegen path; compatible with Spark by default.                                                                                                                               |
| ⚠️ Supported (caveats) | Works, but may diverge from Spark in some cases: incompatible, flag-gated (`allowIncompatible`), or restricted to certain types. See the [Compatibility Guide](compatibility/index.md). |
| 🔜 Planned             | Intended; tracked by an open issue or pull request.                                                                                                                                     |
| 🚫 Out of scope        | Deliberately not planned.                                                                                                                                                               |
| ❓ Not yet supported   | Falls back to Spark today; support is to be determined (not yet evaluated).                                                                                                             |

## Out of scope

Comet focuses acceleration on mainstream relational, string, datetime, math, and collection
expressions. Some Spark function families are **out of scope**: specialized functionality with
narrow real-world analytics use and high implementation cost. These will fall back to Spark and
are not on the roadmap:

- **Probabilistic sketches and approximate top-k** (`kll_sketch_*`, `hll_*`, `theta_*`, `count_min_sketch`, `bitmap_*`, `approx_top_k*`): specialized data structures with exact-correctness traps.
- **XML / XPath** (`from_xml`, `to_xml`, `schema_of_xml`, `xpath*`): legacy text format, rare in accelerated workloads.
- **Geospatial** (`st_*`): brand-new Spark 4.1 functionality, specialized.
- **Avro / Protobuf codecs** (`from_avro`, `to_avro`, `from_protobuf`, `to_protobuf`, `schema_of_avro`): format conversion belongs at the IO layer, not expression evaluation.
- **JVM reflection** (`java_method`, `reflect`): niche, and they invoke arbitrary JVM methods (a security concern).
- **CSV functions** (`from_csv`, `to_csv`, `schema_of_csv`): row-level CSV parsing and formatting in expressions is niche and better handled at the data source layer.

Note that `approx_count_distinct`, `approx_percentile` / `percentile_approx`, `median`, and `mode`
are _not_ out of scope: although approximate, they are mainstream and are planned.

The tables below list every Spark built-in expression with its current status.

## agg_funcs

> 🚫 Out of scope: probabilistic sketch aggregates (`kll_sketch_*`, `hll_sketch_agg`, `hll_union_agg`, `theta_intersection_agg`, `theta_sketch_agg`, `theta_union_agg`, `count_min_sketch`, `approx_top_k`, `approx_top_k_accumulate`, `approx_top_k_combine`). See [Out of scope](#out-of-scope).

| Function                | Status | Notes                                                            |
| ----------------------- | ------ | ---------------------------------------------------------------- |
| `any`                   | ✅     |                                                                  |
| `any_value`             | ✅     |                                                                  |
| `approx_count_distinct` | 🔜     | tracking #4098                                                   |
| `approx_percentile`     | 🔜     | [#3189](https://github.com/apache/datafusion-comet/issues/3189)  |
| `array_agg`             | ❓     |                                                                  |
| `avg`                   | ⚠️     | Interval types (YearMonth, DayTime) fall back                    |
| `bit_and`               | ✅     |                                                                  |
| `bit_or`                | ✅     |                                                                  |
| `bit_xor`               | ✅     |                                                                  |
| `bool_and`              | ✅     |                                                                  |
| `bool_or`               | ✅     |                                                                  |
| `collect_list`          | 🔜     | [#2524](https://github.com/apache/datafusion-comet/issues/2524)  |
| `collect_set`           | ✅     |                                                                  |
| `corr`                  | ✅     |                                                                  |
| `count`                 | ✅     |                                                                  |
| `count_if`              | ✅     |                                                                  |
| `covar_pop`             | ✅     |                                                                  |
| `covar_samp`            | ✅     |                                                                  |
| `every`                 | ✅     |                                                                  |
| `first`                 | ✅     |                                                                  |
| `first_value`           | ✅     |                                                                  |
| `grouping`              | ❓     |                                                                  |
| `grouping_id`           | ❓     |                                                                  |
| `histogram_numeric`     | ❓     |                                                                  |
| `kurtosis`              | 🔜     | tracking #4098                                                   |
| `last`                  | ✅     |                                                                  |
| `last_value`            | ✅     |                                                                  |
| `listagg`               | ❓     |                                                                  |
| `max`                   | ✅     |                                                                  |
| `max_by`                | 🔜     | [#3841](https://github.com/apache/datafusion-comet/issues/3841)  |
| `mean`                  | ✅     |                                                                  |
| `median`                | 🔜     | tracking #4098                                                   |
| `min`                   | ✅     |                                                                  |
| `min_by`                | 🔜     | [#3841](https://github.com/apache/datafusion-comet/issues/3841)  |
| `mode`                  | 🔜     | [#3970](https://github.com/apache/datafusion-comet/issues/3970)  |
| `percentile`            | 🔜     | #4542                                                            |
| `percentile_approx`     | 🔜     | [#3189](https://github.com/apache/datafusion-comet/issues/3189)  |
| `percentile_cont`       | ❓     |                                                                  |
| `percentile_disc`       | ❓     |                                                                  |
| `regr_avgx`             | ✅     | Native: Spark rewrites to `Average` (tests in #4551)             |
| `regr_avgy`             | ✅     | Native: Spark rewrites to `Average` (tests in #4551)             |
| `regr_count`            | ✅     | Native: Spark rewrites to `Count` (tests in #4551)               |
| `regr_intercept`        | 🔜     | Falls back; can reuse `covar_pop`/`var_pop` accumulators (#4552) |
| `regr_r2`               | 🔜     | Falls back; can reuse the `corr` accumulator (#4552)             |
| `regr_slope`            | 🔜     | Falls back; can reuse `covar_pop`/`var_pop` accumulators (#4552) |
| `regr_sxx`              | 🔜     | Falls back; can reuse `var_pop` accumulator (#4552)              |
| `regr_sxy`              | 🔜     | Falls back; can reuse `covar_pop` accumulator (#4552)            |
| `regr_syy`              | 🔜     | Falls back; can reuse `var_pop` accumulator (#4552)              |
| `skewness`              | 🔜     | tracking #4098                                                   |
| `some`                  | ✅     |                                                                  |
| `std`                   | ✅     |                                                                  |
| `stddev`                | ✅     |                                                                  |
| `stddev_pop`            | ✅     |                                                                  |
| `stddev_samp`           | ✅     |                                                                  |
| `string_agg`            | ❓     |                                                                  |
| `sum`                   | ✅     |                                                                  |
| `try_avg`               | 🔜     | tracking #4098                                                   |
| `try_sum`               | 🔜     | tracking #4098                                                   |
| `var_pop`               | ✅     |                                                                  |
| `var_samp`              | ✅     |                                                                  |
| `variance`              | ✅     |                                                                  |

---

## array_funcs

| Function          | Status | Notes                                                                                                                     |
| ----------------- | ------ | ------------------------------------------------------------------------------------------------------------------------- |
| `array`           | ✅     |                                                                                                                           |
| `array_append`    | ⚠️     | On Spark 4.0+ rewrites to `array_insert`; inherits its incompatibilities                                                  |
| `array_compact`   | ✅     |                                                                                                                           |
| `array_contains`  | ⚠️     | NaN-canonicalization may differ for float/double arrays ([#4481](https://github.com/apache/datafusion-comet/issues/4481)) |
| `array_distinct`  | ⚠️     | NaN/signed-zero canonicalization may differ ([#4481](https://github.com/apache/datafusion-comet/issues/4481))             |
| `array_except`    | ⚠️     | Null handling and ordering may differ; `Incompatible`, flag-gated                                                         |
| `array_insert`    | ✅     |                                                                                                                           |
| `array_intersect` | ⚠️     | Result element order may differ when right array is longer than left                                                      |
| `array_join`      | ⚠️     | Null handling may differ ([#3178](https://github.com/apache/datafusion-comet/issues/3178)); `Incompatible`, flag-gated    |
| `array_max`       | ⚠️     | NaN ordering may differ for float/double ([#4482](https://github.com/apache/datafusion-comet/issues/4482))                |
| `array_min`       | ⚠️     | NaN ordering may differ for float/double ([#4482](https://github.com/apache/datafusion-comet/issues/4482))                |
| `array_position`  | ⚠️     | Falls back for binary/struct/map/null element types                                                                       |
| `array_prepend`   | ❓     |                                                                                                                           |
| `array_remove`    | ✅     |                                                                                                                           |
| `array_repeat`    | ✅     |                                                                                                                           |
| `array_union`     | ⚠️     | NaN/signed-zero canonicalization may differ ([#4481](https://github.com/apache/datafusion-comet/issues/4481))             |
| `arrays_overlap`  | ✅     |                                                                                                                           |
| `arrays_zip`      | ✅     |                                                                                                                           |
| `element_at`      | ⚠️     | Only `ArrayType` input; `MapType` input falls back                                                                        |
| `flatten`         | ⚠️     | Falls back for binary/struct/map child element types                                                                      |
| `get`             | ✅     |                                                                                                                           |
| `sequence`        | 🔜     | #4538                                                                                                                     |
| `shuffle`         | ❓     |                                                                                                                           |
| `slice`           | ✅     | Native (#4149)                                                                                                            |
| `sort_array`      | ⚠️     | Incompatible under strict floating-point; falls back for nested struct/null arrays                                        |

---

## bitwise_funcs

| Function             | Status | Notes                                                |
| -------------------- | ------ | ---------------------------------------------------- |
| `&`                  | ✅     |                                                      |
| `<<`                 | ✅     |                                                      |
| `>>`                 | ✅     |                                                      |
| `>>>`                | ✅     | Operator alias for `shiftrightunsigned` (Spark 4.0+) |
| `^`                  | ✅     |                                                      |
| `bit_count`          | ✅     |                                                      |
| `bit_get`            | ✅     |                                                      |
| `getbit`             | ✅     |                                                      |
| `shiftright`         | ✅     |                                                      |
| `shiftrightunsigned` | ✅     |                                                      |
| `\|`                 | ✅     |                                                      |
| `~`                  | ✅     |                                                      |

---

## collection_funcs

| Function      | Status | Notes                                                                                                                            |
| ------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `array_size`  | ❓     |                                                                                                                                  |
| `cardinality` | ⚠️     | Alias for `size`; `MapType` input falls back ([#4472](https://github.com/apache/datafusion-comet/issues/4472))                   |
| `concat`      | ⚠️     | Only `StringType` children; `BinaryType`/`ArrayType` fall back ([#4471](https://github.com/apache/datafusion-comet/issues/4471)) |
| `reverse`     | ⚠️     | Array with `BinaryType` elements is `Incompatible`, flag-gated ([#2763](https://github.com/apache/datafusion-comet/issues/2763)) |
| `size`        | ⚠️     | `MapType` input falls back ([#4472](https://github.com/apache/datafusion-comet/issues/4472))                                     |

---

## conditional_funcs

| Function     | Status | Notes |
| ------------ | ------ | ----- |
| `coalesce`   | ✅     |       |
| `if`         | ✅     |       |
| `ifnull`     | ✅     |       |
| `nanvl`      | 🔜     | #4538 |
| `nullif`     | ✅     |       |
| `nullifzero` | ❓     |       |
| `nvl`        | ✅     |       |
| `nvl2`       | ✅     |       |
| `when`       | ✅     |       |
| `zeroifnull` | ❓     |       |

---

## conversion_funcs

The type-alias keywords (`bigint`, `boolean`, `int`, etc.) are SQL syntax for `CAST`. `cast`
itself is supported with caveats; the keyword aliases are not yet individually wired in Comet's
serde but effectively fall through to the same cast path at runtime.

| Function    | Status | Notes                                                                                                              |
| ----------- | ------ | ------------------------------------------------------------------------------------------------------------------ |
| `bigint`    | ⚠️     | Alias for `CAST(... AS BIGINT)`; see `cast`                                                                        |
| `binary`    | ⚠️     | Alias for `CAST(... AS BINARY)`; see `cast`                                                                        |
| `boolean`   | ⚠️     | Alias for `CAST(... AS BOOLEAN)`; see `cast`                                                                       |
| `cast`      | ⚠️     | Many type pairs supported; float-to-decimal rounding may differ; see [Compatibility Guide](compatibility/index.md) |
| `date`      | ⚠️     | Alias for `CAST(... AS DATE)`; see `cast`                                                                          |
| `decimal`   | ⚠️     | Alias for `CAST(... AS DECIMAL)`; see `cast`                                                                       |
| `double`    | ⚠️     | Alias for `CAST(... AS DOUBLE)`; see `cast`                                                                        |
| `float`     | ⚠️     | Alias for `CAST(... AS FLOAT)`; see `cast`                                                                         |
| `int`       | ⚠️     | Alias for `CAST(... AS INT)`; see `cast`                                                                           |
| `smallint`  | ⚠️     | Alias for `CAST(... AS SMALLINT)`; see `cast`                                                                      |
| `string`    | ⚠️     | Alias for `CAST(... AS STRING)`; see `cast`                                                                        |
| `timestamp` | ⚠️     | Alias for `CAST(... AS TIMESTAMP)`; see `cast`                                                                     |
| `tinyint`   | ⚠️     | Alias for `CAST(... AS TINYINT)`; see `cast`                                                                       |

---

## csv_funcs

🚫 **Out of scope.** `from_csv`, `to_csv`, and `schema_of_csv` fall back to Spark. See [Out of scope](#out-of-scope).

---

## datetime_funcs

| Function              | Status | Notes                                                                                                  |
| --------------------- | ------ | ------------------------------------------------------------------------------------------------------ |
| `add_months`          | ✅     |                                                                                                        |
| `convert_timezone`    | ✅     |                                                                                                        |
| `curdate`             | ✅     | Constant-folded to a literal (alias of `current_date`)                                                 |
| `current_date`        | ✅     | Constant-folded to a literal before Comet sees the plan                                                |
| `current_time`        | 🔜     | Blocked on Spark 4.1 TIME type support (#4288)                                                         |
| `current_timestamp`   | ✅     | Constant-folded to a literal before Comet sees the plan                                                |
| `current_timezone`    | ✅     |                                                                                                        |
| `date_add`            | ✅     |                                                                                                        |
| `date_diff`           | ✅     |                                                                                                        |
| `date_format`         | ✅     |                                                                                                        |
| `date_from_unix_date` | ✅     |                                                                                                        |
| `date_part`           | ✅     |                                                                                                        |
| `date_sub`            | ✅     |                                                                                                        |
| `date_trunc`          | ✅     |                                                                                                        |
| `dateadd`             | ✅     |                                                                                                        |
| `datediff`            | ✅     |                                                                                                        |
| `datepart`            | ✅     |                                                                                                        |
| `day`                 | ✅     |                                                                                                        |
| `dayname`             | 🔜     | #4544                                                                                                  |
| `dayofmonth`          | ✅     |                                                                                                        |
| `dayofweek`           | ✅     |                                                                                                        |
| `dayofyear`           | ✅     |                                                                                                        |
| `extract`             | ✅     |                                                                                                        |
| `from_unixtime`       | ✅     |                                                                                                        |
| `from_utc_timestamp`  | ⚠️     | Legacy zone forms (`GMT+1`, `PST`) throw a native parse error                                          |
| `hour`                | ✅     |                                                                                                        |
| `last_day`            | ✅     |                                                                                                        |
| `localtimestamp`      | ✅     |                                                                                                        |
| `make_date`           | ✅     |                                                                                                        |
| `make_dt_interval`    | 🔜     | #4541                                                                                                  |
| `make_interval`       | 🔜     | Produces legacy CalendarInterval; tracked by #4540                                                     |
| `make_time`           | 🔜     | Spark 4.1 TIME type; tracked by #4288                                                                  |
| `make_timestamp`      | ✅     |                                                                                                        |
| `make_timestamp_ltz`  | ⚠️     | 6-arg form runs via the codegen dispatcher; 2-arg `(date, time)` form (Spark 4.1 TIME type) falls back |
| `make_timestamp_ntz`  | ⚠️     | 6-arg form runs via the codegen dispatcher; 2-arg `(date, time)` form (Spark 4.1 TIME type) falls back |
| `make_ym_interval`    | 🔜     | #4541                                                                                                  |
| `minute`              | ✅     |                                                                                                        |
| `month`               | ✅     |                                                                                                        |
| `monthname`           | 🔜     | #4544                                                                                                  |
| `months_between`      | ✅     |                                                                                                        |
| `next_day`            | ✅     |                                                                                                        |
| `now`                 | ✅     | Constant-folded to a literal (alias of `current_timestamp`)                                            |
| `quarter`             | ✅     |                                                                                                        |
| `second`              | ✅     |                                                                                                        |
| `session_window`      | 🔜     | Time-window grouping; tracked by #4553                                                                 |
| `time_diff`           | 🔜     | Spark 4.1 TIME type; tracked by #4288                                                                  |
| `time_trunc`          | 🔜     | Spark 4.1 TIME type; tracked by #4288                                                                  |
| `timestamp_micros`    | ✅     |                                                                                                        |
| `timestamp_millis`    | ✅     |                                                                                                        |
| `timestamp_seconds`   | ✅     |                                                                                                        |
| `to_date`             | ✅     | Rewrites to `Cast` (or `Cast(GetTimestamp)` with a format) before Comet sees the plan                  |
| `to_time`             | 🔜     | Spark 4.1 TIME type; tracked by #4288                                                                  |
| `to_timestamp`        | ✅     | Rewrites to `Cast` (or `GetTimestamp` with a format) before Comet sees the plan                        |
| `to_timestamp_ltz`    | ✅     | Rewrites to `to_timestamp` (`TimestampType`)                                                           |
| `to_timestamp_ntz`    | ✅     | Rewrites to `to_timestamp` (`TimestampNTZType`)                                                        |
| `to_unix_timestamp`   | ✅     |                                                                                                        |
| `to_utc_timestamp`    | ⚠️     | Legacy zone forms (`GMT+1`, `PST`) throw a native parse error                                          |
| `trunc`               | ✅     |                                                                                                        |
| `try_make_interval`   | 🔜     | Produces legacy CalendarInterval; tracked by #4540                                                     |
| `try_make_timestamp`  | ⚠️     | Runs natively for valid inputs, but returns wrong values for invalid inputs instead of NULL (#4554)    |
| `try_to_date`         | 🔜     | Rewrites to `Cast`/`GetTimestamp` but currently falls back; tracked by #4556                           |
| `try_to_time`         | 🔜     | Spark 4.1 TIME type; tracked by #4288                                                                  |
| `try_to_timestamp`    | 🔜     | Rewrites to `Cast`/`GetTimestamp` but currently falls back; tracked by #4556                           |
| `unix_date`           | ✅     |                                                                                                        |
| `unix_micros`         | ✅     |                                                                                                        |
| `unix_millis`         | ✅     |                                                                                                        |
| `unix_seconds`        | ✅     |                                                                                                        |
| `unix_timestamp`      | ✅     |                                                                                                        |
| `weekday`             | ✅     |                                                                                                        |
| `weekofyear`          | ✅     |                                                                                                        |
| `window`              | 🔜     | Time-window grouping; tracked by #4553                                                                 |
| `window_time`         | 🔜     | Time-window grouping; tracked by #4553                                                                 |
| `year`                | ✅     |                                                                                                        |

---

## generator_funcs

`explode` and `posexplode` are supported via `CometExplodeExec` (operator-level, not
expression-level). The `outer` variants are wired but marked `Incompatible`; they require
`spark.comet.exec.explode.enabled=true` and `allowIncompatible`.

| Function           | Status | Notes                                                |
| ------------------ | ------ | ---------------------------------------------------- |
| `explode`          | ✅     | via `CometExplodeExec`                               |
| `explode_outer`    | ⚠️     | `outer=true` incompatible; needs `allowIncompatible` |
| `inline`           | ❓     |                                                      |
| `inline_outer`     | ❓     |                                                      |
| `posexplode`       | ✅     | via `CometExplodeExec`                               |
| `posexplode_outer` | ⚠️     | `outer=true` incompatible; needs `allowIncompatible` |
| `stack`            | ❓     |                                                      |

---

## hash_funcs

| Function   | Status | Notes |
| ---------- | ------ | ----- |
| `crc32`    | ✅     |       |
| `hash`     | ✅     |       |
| `md5`      | ✅     |       |
| `sha`      | ✅     |       |
| `sha1`     | ✅     |       |
| `sha2`     | ✅     |       |
| `xxhash64` | ✅     |       |

---

## json_funcs

| Function            | Status | Notes                                                                                                                 |
| ------------------- | ------ | --------------------------------------------------------------------------------------------------------------------- |
| `from_json`         | ⚠️     | Partial native support (requires explicit schema, marked `Incompatible`); fuller support via codegen dispatch (#4305) |
| `get_json_object`   | ⚠️     | Single-quoted JSON and unescaped control chars require `allowIncompatible`                                            |
| `json_array_length` | 🔜     | tracking #4098                                                                                                        |
| `json_object_keys`  | 🔜     | [#3161](https://github.com/apache/datafusion-comet/issues/3161)                                                       |
| `json_tuple`        | 🔜     | [#3160](https://github.com/apache/datafusion-comet/issues/3160)                                                       |
| `schema_of_json`    | 🔜     | [#3163](https://github.com/apache/datafusion-comet/issues/3163)                                                       |
| `to_json`           | ⚠️     | Partial native support (options and map/array inputs fall back); fuller support via codegen dispatch (#4305)          |

---

## lambda_funcs

All higher-order functions are planned via [#4224](https://github.com/apache/datafusion-comet/issues/4224).

| Function           | Status | Notes                                                           |
| ------------------ | ------ | --------------------------------------------------------------- |
| `aggregate`        | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `array_sort`       | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `exists`           | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `filter`           | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `forall`           | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `map_filter`       | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `map_zip_with`     | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `reduce`           | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `transform`        | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `transform_keys`   | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `transform_values` | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |
| `zip_with`         | 🔜     | [#4224](https://github.com/apache/datafusion-comet/issues/4224) |

---

## map_funcs

| Function           | Status | Notes                                                        |
| ------------------ | ------ | ------------------------------------------------------------ |
| `element_at`       | ⚠️     | Only `ArrayType` input; `MapType` input falls back           |
| `map`              | ❓     |                                                              |
| `map_concat`       | ❓     |                                                              |
| `map_contains_key` | ✅     |                                                              |
| `map_entries`      | ✅     |                                                              |
| `map_from_arrays`  | ✅     |                                                              |
| `map_from_entries` | ⚠️     | `BinaryType` key/value falls back unless `allowIncompatible` |
| `map_keys`         | ✅     |                                                              |
| `map_values`       | ✅     |                                                              |
| `str_to_map`       | ✅     |                                                              |
| `try_element_at`   | ❓     |                                                              |

---

## math_funcs

| Function       | Status | Notes                                                                                                              |
| -------------- | ------ | ------------------------------------------------------------------------------------------------------------------ |
| `%`            | ⚠️     | `try_mod` form (`EvalMode.TRY`) falls back ([#4484](https://github.com/apache/datafusion-comet/issues/4484))       |
| `*`            | ⚠️     | Interval multiplication falls back                                                                                 |
| `+`            | ✅     |                                                                                                                    |
| `-`            | ✅     |                                                                                                                    |
| `/`            | ✅     |                                                                                                                    |
| `abs`          | ⚠️     | Interval types fall back; ANSI overflow for integer min value                                                      |
| `acos`         | ✅     |                                                                                                                    |
| `acosh`        | ✅     |                                                                                                                    |
| `asin`         | ✅     |                                                                                                                    |
| `asinh`        | ✅     |                                                                                                                    |
| `atan`         | ✅     |                                                                                                                    |
| `atan2`        | ✅     |                                                                                                                    |
| `atanh`        | ✅     |                                                                                                                    |
| `bin`          | ✅     |                                                                                                                    |
| `bround`       | 🔜     | #4538                                                                                                              |
| `cbrt`         | ✅     |                                                                                                                    |
| `ceil`         | ⚠️     | Two-arg `ceil(expr, scale)` form falls back                                                                        |
| `ceiling`      | ✅     |                                                                                                                    |
| `conv`         | 🔜     | #4538                                                                                                              |
| `cos`          | ✅     |                                                                                                                    |
| `cosh`         | ✅     |                                                                                                                    |
| `cot`          | ✅     |                                                                                                                    |
| `csc`          | ✅     |                                                                                                                    |
| `degrees`      | ✅     |                                                                                                                    |
| `div`          | ✅     |                                                                                                                    |
| `e`            | ❓     |                                                                                                                    |
| `exp`          | ✅     |                                                                                                                    |
| `expm1`        | ✅     |                                                                                                                    |
| `factorial`    | ✅     |                                                                                                                    |
| `floor`        | ⚠️     | Two-arg `floor(expr, scale)` form falls back                                                                       |
| `greatest`     | ✅     |                                                                                                                    |
| `hex`          | ✅     |                                                                                                                    |
| `hypot`        | 🔜     | #4538                                                                                                              |
| `least`        | ✅     |                                                                                                                    |
| `ln`           | ✅     |                                                                                                                    |
| `log`          | ✅     |                                                                                                                    |
| `log10`        | ✅     |                                                                                                                    |
| `log1p`        | 🔜     | #4538                                                                                                              |
| `log2`         | ✅     |                                                                                                                    |
| `mod`          | ✅     |                                                                                                                    |
| `negative`     | ✅     |                                                                                                                    |
| `pi`           | ✅     |                                                                                                                    |
| `pmod`         | 🔜     | #4538                                                                                                              |
| `positive`     | ✅     |                                                                                                                    |
| `pow`          | ✅     |                                                                                                                    |
| `power`        | ✅     |                                                                                                                    |
| `radians`      | ✅     |                                                                                                                    |
| `rand`         | ✅     |                                                                                                                    |
| `randn`        | ✅     |                                                                                                                    |
| `random`       | ✅     | Alias for `rand` (Spark 4.0+); seed must be a literal                                                              |
| `randstr`      | ❓     |                                                                                                                    |
| `rint`         | ✅     |                                                                                                                    |
| `round`        | ⚠️     | Float/Double inputs always fall back; integer/decimal HALF_UP supported                                            |
| `sec`          | ✅     |                                                                                                                    |
| `shiftleft`    | ✅     |                                                                                                                    |
| `sign`         | ✅     |                                                                                                                    |
| `signum`       | ✅     |                                                                                                                    |
| `sin`          | ✅     |                                                                                                                    |
| `sinh`         | ✅     |                                                                                                                    |
| `sqrt`         | ✅     |                                                                                                                    |
| `tan`          | ✅     |                                                                                                                    |
| `tanh`         | ✅     |                                                                                                                    |
| `try_add`      | ⚠️     | Datetime/interval form falls back; numeric form supported                                                          |
| `try_divide`   | ✅     |                                                                                                                    |
| `try_mod`      | ❓     |                                                                                                                    |
| `try_multiply` | ✅     |                                                                                                                    |
| `try_subtract` | ✅     |                                                                                                                    |
| `unhex`        | ✅     |                                                                                                                    |
| `uniform`      | ❓     |                                                                                                                    |
| `width_bucket` | ⚠️     | Wired via shim, bypasses support-level framework ([#4485](https://github.com/apache/datafusion-comet/issues/4485)) |

---

## misc_funcs

> 🚫 Out of scope: probabilistic sketch functions (`hll_sketch_estimate`, `hll_union`, `theta_difference`, `theta_intersection`, `theta_sketch_estimate`, `theta_union`, `bitmap_and_agg`, `bitmap_or_agg`, `bitmap_construct_agg`, `bitmap_count`, `bitmap_bucket_number`, `bitmap_bit_position`, `approx_top_k_estimate`). See [Out of scope](#out-of-scope).

> 🚫 Out of scope: geospatial functions (`st_asbinary`, `st_geogfromwkb`, `st_geomfromwkb`, `st_setsrid`, `st_srid`). See [Out of scope](#out-of-scope).

> 🚫 Out of scope: Avro/Protobuf codec functions (`from_avro`, `to_avro`, `schema_of_avro`, `from_protobuf`, `to_protobuf`). See [Out of scope](#out-of-scope).

> 🚫 Out of scope: JVM reflection functions (`java_method`, `reflect`, `try_reflect`). See [Out of scope](#out-of-scope).

| Function                      | Status | Notes                                                             |
| ----------------------------- | ------ | ----------------------------------------------------------------- |
| `aes_decrypt`                 | 🔜     | Codegen-dispatch candidate (RuntimeReplaceable to `StaticInvoke`) |
| `aes_encrypt`                 | 🔜     | Codegen-dispatch candidate; nondeterministic IV by default        |
| `assert_true`                 | ❓     |                                                                   |
| `current_catalog`             | ❓     |                                                                   |
| `current_database`            | ❓     |                                                                   |
| `current_schema`              | ❓     |                                                                   |
| `current_user`                | ❓     |                                                                   |
| `equal_null`                  | ❓     |                                                                   |
| `input_file_block_length`     | ❓     |                                                                   |
| `input_file_block_start`      | ❓     |                                                                   |
| `input_file_name`             | ❓     |                                                                   |
| `is_variant_null`             | 🔜     | tracking #4098                                                    |
| `monotonically_increasing_id` | ✅     |                                                                   |
| `parse_json`                  | 🔜     | tracking #4098                                                    |
| `raise_error`                 | ❓     |                                                                   |
| `rand`                        | ✅     | Seed must be a literal                                            |
| `randn`                       | ✅     | Seed must be a literal                                            |
| `schema_of_variant`           | 🔜     | tracking #4098                                                    |
| `schema_of_variant_agg`       | 🔜     | tracking #4098                                                    |
| `session_user`                | ❓     |                                                                   |
| `spark_partition_id`          | ✅     |                                                                   |
| `to_variant_object`           | 🔜     | tracking #4098                                                    |
| `try_aes_decrypt`             | 🔜     | Codegen-dispatch candidate (RuntimeReplaceable to `StaticInvoke`) |
| `try_parse_json`              | 🔜     | tracking #4098                                                    |
| `try_variant_get`             | 🔜     | tracking #4098                                                    |
| `typeof`                      | ❓     |                                                                   |
| `user`                        | ✅     | Resolved to a literal by the Spark analyzer before reaching Comet |
| `uuid`                        | ❓     |                                                                   |
| `variant_get`                 | 🔜     | tracking #4098                                                    |
| `version`                     | ❓     |                                                                   |

---

## predicate_funcs

| Function      | Status | Notes                                                                                         |
| ------------- | ------ | --------------------------------------------------------------------------------------------- |
| `!`           | ✅     |                                                                                               |
| `<`           | ✅     |                                                                                               |
| `<=`          | ✅     |                                                                                               |
| `<=>`         | ✅     |                                                                                               |
| `=`           | ✅     |                                                                                               |
| `==`          | ✅     |                                                                                               |
| `>`           | ✅     |                                                                                               |
| `>=`          | ✅     |                                                                                               |
| `and`         | ✅     |                                                                                               |
| `between`     | ✅     |                                                                                               |
| `ilike`       | ✅     |                                                                                               |
| `in`          | ✅     |                                                                                               |
| `isnan`       | ✅     |                                                                                               |
| `isnotnull`   | ✅     |                                                                                               |
| `isnull`      | ✅     |                                                                                               |
| `like`        | ✅     |                                                                                               |
| `not`         | ✅     |                                                                                               |
| `or`          | ✅     |                                                                                               |
| `regexp`      | ⚠️     | Alias for `rlike`; uses Rust `regex` crate, requires `allowIncompatible`                      |
| `regexp_like` | ⚠️     | Alias for `rlike`; uses Rust `regex` crate, requires `allowIncompatible`                      |
| `rlike`       | ⚠️     | Uses Rust `regex` crate; requires `allowIncompatible`; results may differ from Java `Pattern` |

---

## string_funcs

| Function             | Status | Notes          |
| -------------------- | ------ | -------------- |
| `ascii`              | ✅     |                |
| `base64`             | ❓     |                |
| `bit_length`         | ✅     |                |
| `btrim`              | ✅     |                |
| `char`               | ✅     |                |
| `char_length`        | ✅     |                |
| `character_length`   | ✅     |                |
| `chr`                | ✅     |                |
| `collate`            | ❓     |                |
| `collation`          | ❓     |                |
| `concat_ws`          | ✅     |                |
| `contains`           | ✅     |                |
| `decode`             | ✅     |                |
| `elt`                | 🔜     | #4538          |
| `encode`             | ❓     |                |
| `endswith`           | ✅     |                |
| `find_in_set`        | 🔜     | #4538          |
| `format_number`      | 🔜     | #4538          |
| `format_string`      | 🔜     | #4538          |
| `initcap`            | ✅     |                |
| `instr`              | ✅     |                |
| `is_valid_utf8`      | ❓     |                |
| `lcase`              | ✅     |                |
| `left`               | ✅     |                |
| `len`                | ✅     |                |
| `length`             | ✅     |                |
| `levenshtein`        | 🔜     | #4538          |
| `locate`             | 🔜     | #4538          |
| `lower`              | ✅     |                |
| `lpad`               | ✅     |                |
| `ltrim`              | ✅     |                |
| `luhn_check`         | ❓     |                |
| `make_valid_utf8`    | ❓     |                |
| `mask`               | ❓     |                |
| `octet_length`       | ✅     |                |
| `overlay`            | 🔜     | #4538          |
| `position`           | 🔜     | #4538          |
| `printf`             | 🔜     | #4538          |
| `quote`              | ❓     |                |
| `regexp_count`       | 🔜     | tracking #4098 |
| `regexp_extract`     | 🔜     | tracking #4098 |
| `regexp_extract_all` | 🔜     | tracking #4098 |
| `regexp_instr`       | 🔜     | tracking #4098 |
| `regexp_replace`     | ✅     |                |
| `regexp_substr`      | 🔜     | tracking #4098 |
| `repeat`             | ✅     |                |
| `replace`            | ✅     |                |
| `right`              | ✅     |                |
| `rpad`               | ✅     |                |
| `rtrim`              | ✅     |                |
| `sentences`          | ❓     |                |
| `soundex`            | 🔜     | #4538          |
| `space`              | ✅     |                |
| `split`              | ✅     |                |
| `split_part`         | ❓     |                |
| `startswith`         | ✅     |                |
| `substr`             | ✅     |                |
| `substring`          | ✅     |                |
| `substring_index`    | ✅     |                |
| `to_binary`          | ❓     |                |
| `to_char`            | 🔜     | #4538          |
| `to_number`          | 🔜     | #4538          |
| `to_varchar`         | 🔜     | #4538          |
| `translate`          | ✅     |                |
| `trim`               | ✅     |                |
| `try_to_binary`      | ❓     |                |
| `try_to_number`      | ❓     |                |
| `try_validate_utf8`  | ❓     |                |
| `ucase`              | ✅     |                |
| `unbase64`           | 🔜     | #4538          |
| `upper`              | ✅     |                |
| `validate_utf8`      | ❓     |                |

---

## struct_funcs

| Function       | Status | Notes                                    |
| -------------- | ------ | ---------------------------------------- |
| `named_struct` | ⚠️     | Duplicate field names fall back to Spark |
| `struct`       | ✅     |                                          |

---

## url_funcs

| Function         | Status | Notes |
| ---------------- | ------ | ----- |
| `parse_url`      | ✅     |       |
| `try_url_decode` | ✅     |       |
| `url_decode`     | ✅     |       |
| `url_encode`     | ✅     |       |

---

## window_funcs

Window functions run via `CometWindowExec`. Window support is disabled by default due to known
correctness issues (tracking [#2721](https://github.com/apache/datafusion-comet/issues/2721)).
When enabled, `lag` and `lead` are explicitly wired; aggregate window functions (`count`, `min`,
`max`, `sum`) are also supported. Ranking functions (`rank`, `dense_rank`, `row_number`,
`ntile`, `percent_rank`, `cume_dist`, `nth_value`) are not yet wired in the window serde and
fall back to Spark.

| Function       | Status | Notes                         |
| -------------- | ------ | ----------------------------- |
| `cume_dist`    | ❓     | Not yet wired in window serde |
| `dense_rank`   | ❓     | Not yet wired in window serde |
| `lag`          | ✅     | via `CometWindowExec`         |
| `lead`         | ✅     | via `CometWindowExec`         |
| `nth_value`    | ❓     | Not yet wired in window serde |
| `ntile`        | ❓     | Not yet wired in window serde |
| `percent_rank` | ❓     | Not yet wired in window serde |
| `rank`         | ❓     | Not yet wired in window serde |
| `row_number`   | ❓     | Not yet wired in window serde |

---

## xml_funcs

🚫 **Out of scope.** `from_xml`, `to_xml`, `schema_of_xml`, and the `xpath*` family fall back to Spark. See [Out of scope](#out-of-scope).

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
- [Spark Expressions Support (contributor guide)](../../contributor-guide/spark_expressions_support.md) - per-version (Spark 3.4 / 3.5 / 4.0 / 4.1) audit notes for each expression.
