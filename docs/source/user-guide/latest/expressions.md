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
<!--END:EXPR_TABLE[agg_funcs]-->

---

## array_funcs

<!--BEGIN:EXPR_TABLE[array_funcs]-->
<!--END:EXPR_TABLE[array_funcs]-->

---

## bitwise_funcs

<!--BEGIN:EXPR_TABLE[bitwise_funcs]-->
<!--END:EXPR_TABLE[bitwise_funcs]-->

---

## collection_funcs

<!--BEGIN:EXPR_TABLE[collection_funcs]-->
<!--END:EXPR_TABLE[collection_funcs]-->

---

## conditional_funcs

<!--BEGIN:EXPR_TABLE[conditional_funcs]-->
<!--END:EXPR_TABLE[conditional_funcs]-->

---

## conversion_funcs

The type-name conversion functions (`bigint`, `binary`, `boolean`, `date`, `decimal`, `double`, `float`, `int`, `smallint`, `string`, `timestamp`, `tinyint`) are SQL aliases for `CAST(... AS <type>)` and share the support and caveats of `cast`.

<!--BEGIN:EXPR_TABLE[conversion_funcs]-->
<!--END:EXPR_TABLE[conversion_funcs]-->

---

## datetime_funcs

<!--BEGIN:EXPR_TABLE[datetime_funcs]-->
<!--END:EXPR_TABLE[datetime_funcs]-->

---

## generator_funcs

`explode` and `posexplode` are supported via `CometExplodeExec` (operator-level, not
expression-level). The `outer` variants are wired but marked `Incompatible`; they require
`spark.comet.exec.explode.enabled=true` and `allowIncompatible`.

<!--BEGIN:EXPR_TABLE[generator_funcs]-->
<!--END:EXPR_TABLE[generator_funcs]-->

---

## hash_funcs

<!--BEGIN:EXPR_TABLE[hash_funcs]-->
<!--END:EXPR_TABLE[hash_funcs]-->

---

## json_funcs

<!--BEGIN:EXPR_TABLE[json_funcs]-->
<!--END:EXPR_TABLE[json_funcs]-->

---

## lambda_funcs

All higher-order functions are planned via [#4224](https://github.com/apache/datafusion-comet/issues/4224).

<!--BEGIN:EXPR_TABLE[lambda_funcs]-->
<!--END:EXPR_TABLE[lambda_funcs]-->

---

## map_funcs

<!--BEGIN:EXPR_TABLE[map_funcs]-->
<!--END:EXPR_TABLE[map_funcs]-->

---

## math_funcs

<!--BEGIN:EXPR_TABLE[math_funcs]-->
<!--END:EXPR_TABLE[math_funcs]-->

---

## misc_funcs

<!--BEGIN:EXPR_TABLE[misc_funcs]-->
<!--END:EXPR_TABLE[misc_funcs]-->

---

## predicate_funcs

<!--BEGIN:EXPR_TABLE[predicate_funcs]-->
<!--END:EXPR_TABLE[predicate_funcs]-->

---

## string_funcs

<!--BEGIN:EXPR_TABLE[string_funcs]-->
<!--END:EXPR_TABLE[string_funcs]-->

---

## struct_funcs

<!--BEGIN:EXPR_TABLE[struct_funcs]-->
<!--END:EXPR_TABLE[struct_funcs]-->

---

## url_funcs

<!--BEGIN:EXPR_TABLE[url_funcs]-->
<!--END:EXPR_TABLE[url_funcs]-->

---

## variant_funcs

<!--BEGIN:EXPR_TABLE[variant_funcs]-->
<!--END:EXPR_TABLE[variant_funcs]-->

---

## window_funcs

Window functions run via `CometWindowExec`. Window support is disabled by default due to known
correctness issues (tracking [#2721](https://github.com/apache/datafusion-comet/issues/2721)).
When enabled, `lag` and `lead` are explicitly wired; aggregate window functions (`count`, `min`,
`max`, `sum`) are also supported. Ranking functions (`rank`, `dense_rank`, `row_number`,
`ntile`, `percent_rank`, `cume_dist`, `nth_value`) are not yet wired in the window serde and
fall back to Spark.

<!--BEGIN:EXPR_TABLE[window_funcs]-->
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

- [Comet Compatibility Guide](compatibility/index.md) - known incompatibilities and edge cases for expressions with opt-in incompatible behavior.
- [Expression Audits (contributor guide)](../../contributor-guide/expression-audits/index.md) - per-version (Spark 3.4 / 3.5 / 4.0 / 4.1) audit notes for audited expressions.
