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

# agg_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## any

- Spark 3.4.3 (audited 2026-05-26): registered as a SQL alias of `BoolOr`, which extends `RuntimeReplaceableAggregate` with `replacement = Max(child)`. Catalyst rewrites `any(x)` to `max(x)` before Comet sees the plan, so `any` is served by `CometMax` on a `BooleanType` column.
- Spark 3.5.8 (audited 2026-05-26): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-26): identical to 3.4.3.

## avg

- Spark 3.4.3 (2026-05-26)
- Spark 3.5.8 (2026-05-26): aggregate logic identical to 3.4.3
- Spark 4.0.1 (2026-05-26): aggregate logic identical to 3.5.8; only `QueryContext` import path differs. `YearMonthIntervalType` and `DayTimeIntervalType` inputs (supported by Spark) fall back to Spark in Comet.

## bit_and

- Spark 3.4.3 (2026-05-26)
- Spark 3.5.8 (2026-05-26)
- Spark 4.0.1 (2026-05-26)

## listagg

- Spark 3.4.3 (audited 2026-07-03): does not exist. `ListAgg` was added in Spark 4.0.
- Spark 3.5.8 (audited 2026-07-03): does not exist.
- Spark 4.0.1 (audited 2026-07-03): `ListAgg(child, delimiter, orderExpressions)` in `aggregate/collect.scala`. Accepts `StringType` or `BinaryType` inputs; result type matches child. Skips nulls; empty or all-null groups return `NULL`. A `NULL` delimiter is treated as an empty string. `CometListAgg` maps only the simple form: `StringType` child with a literal `StringType`/`NullType` delimiter and no `WITHIN GROUP`. `BinaryType` inputs, `WITHIN GROUP (ORDER BY ...)`, non-literal delimiters, and non-default collations fall back to Spark. `DISTINCT` falls back because Comet rejects multi-column distinct aggregates (`ListAgg` has two children).
- Spark 4.1.1 (audited 2026-07-03): byte-identical to 4.0.1.
- Native accumulator (`SparkListAgg`) returns `Utf8` but keeps its intermediate state as `Binary`, matching Spark's `TypedImperativeAggregate` buffer schema so the Comet shuffle layer does not insert a `Utf8` → `Binary` cast the merge side cannot read back.

## median

- Spark 3.4.3 (audited 2026-06-24): `Median(child)` is a `RuntimeReplaceableAggregate` with `replacement = Percentile(child, Literal(0.5))`. Catalyst rewrites `median(x)` to `percentile(x, 0.5)` before Comet sees the plan, so it is served by `CometPercentile`.
- Spark 3.5.8 (audited 2026-06-24): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-06-24): `replacement` becomes `lazy val`; semantics unchanged.
- Spark 4.1.1 (audited 2026-06-24): identical to 4.0.1.

## percentile

- Spark 3.4.3 (audited 2026-06-24): `Percentile(child, percentageExpression, frequencyExpression, ..., reverse)` over `PercentileBase`. Exact percentile using `index = p * (n - 1)` linear interpolation, NULL inputs skipped, empty/all-null group returns NULL. `CometPercentile` maps the single-literal-percentage, default-frequency, numeric-input, ascending form to DataFusion's `percentile_cont` (same interpolation). Array-of-percentages, a non-default frequency argument, descending order, and interval inputs fall back to Spark.
- Spark 3.5.8 (audited 2026-06-24): ordering centralized via `PhysicalDataType.ordering`; behavior identical to 3.4.3.
- Spark 4.0.1 (audited 2026-06-24): adds `PercentileCont`/`PercentileDisc` builders and `SupportsOrderingWithinGroup`, enabling `percentile_cont(p) WITHIN GROUP (ORDER BY col)`, which rewrites to `Percentile(col, p, reverse)`. The ascending form runs natively; the `DESC` form sets `reverse = true` and falls back to Spark because the native `percentile_cont` always interpolates in ascending order.
- Spark 4.1.1 (audited 2026-06-24): identical to 4.0.1.
- `CometPercentile` reports `Incompatible` for the otherwise-supported form because DataFusion's `percentile_cont` quantizes the interpolation weight to 6 decimal places (`INTERPOLATION_PRECISION = 1e6`), so a deeply-interpolated value can differ from Spark by up to roughly `(upper - lower) * 1e-6`. The native path is opt-in via `spark.comet.expression.Percentile.allowIncompatible=true` ([#4719](https://github.com/apache/datafusion-comet/issues/4719)).

[Spark Expression Support]: ../../user-guide/latest/expressions.md
