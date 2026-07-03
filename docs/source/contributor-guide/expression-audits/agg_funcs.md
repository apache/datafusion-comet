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

## max_by

- Spark 3.4.3 (2026-07-03): `MaxBy` is a 2-argument `DeclarativeAggregate` registered as `expression[MaxBy]("max_by")`. Buffer is `(valueWithExtremumOrdering, extremumOrdering)`; null orderings are ignored, the value paired with the maximum ordering is returned (and may itself be null), and an all-null-ordering group yields null. Comet implements a native `max_by` aggregate. Only fixed-length value and ordering types are accelerated: a variable-length or nested type (string, binary, struct) forces Spark's `SortAggregate`, which Comet does not support, so those cases fall back to Spark. `max_by` is non-deterministic when several rows tie on the maximum ordering, matching Spark's documented behavior.
- Spark 3.5.8 (2026-07-03): aggregate logic identical to 3.4.3.
- Spark 4.0.1 (2026-07-03): aggregate logic identical to 3.4.3; only the `@ExpressionDescription` example and note text differ.
- Spark 4.1.1 (2026-07-03): aggregate logic identical to 3.4.3. The 3-argument top-k form `max_by(x, y, k)` (via `MaxByBuilder` / `MaxMinByK`) is only present on Spark master, not in any released 3.4 through 4.1 version, so Comet handles only the 2-argument form.

## median

- Spark 3.4.3 (audited 2026-06-24): `Median(child)` is a `RuntimeReplaceableAggregate` with `replacement = Percentile(child, Literal(0.5))`. Catalyst rewrites `median(x)` to `percentile(x, 0.5)` before Comet sees the plan, so it is served by `CometPercentile`.
- Spark 3.5.8 (audited 2026-06-24): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-06-24): `replacement` becomes `lazy val`; semantics unchanged.
- Spark 4.1.1 (audited 2026-06-24): identical to 4.0.1.

## min_by

- Spark 3.4.3 (2026-07-03): `MinBy` shares the abstract `MaxMinBy` `DeclarativeAggregate` with `MaxBy`, differing only in the comparison direction (`least` / `<` instead of `greatest` / `>`). Registered as `expression[MinBy]("min_by")`. Null orderings are ignored, the value paired with the minimum ordering is returned (and may itself be null), and an all-null-ordering group yields null. Comet serves it through the same native `MaxMinBy` aggregate as `max_by`, with the same fixed-length value and ordering restriction (variable-length or nested types fall back to Spark). Non-deterministic on ties, matching Spark.
- Spark 3.5.8 (2026-07-03): aggregate logic identical to 3.4.3.
- Spark 4.0.1 (2026-07-03): aggregate logic identical to 3.4.3; only the `@ExpressionDescription` example and note text differ.
- Spark 4.1.1 (2026-07-03): aggregate logic identical to 3.4.3. The 3-argument top-k form `min_by(x, y, k)` (via `MinByBuilder` / `MaxMinByK`) is only present on Spark master, so Comet handles only the 2-argument form.

## percentile

- Spark 3.4.3 (audited 2026-06-24): `Percentile(child, percentageExpression, frequencyExpression, ..., reverse)` over `PercentileBase`. Exact percentile using `index = p * (n - 1)` linear interpolation, NULL inputs skipped, empty/all-null group returns NULL. `CometPercentile` maps the single-literal-percentage, default-frequency, numeric-input, ascending form to DataFusion's `percentile_cont` (same interpolation). Array-of-percentages, a non-default frequency argument, descending order, and interval inputs fall back to Spark.
- Spark 3.5.8 (audited 2026-06-24): ordering centralized via `PhysicalDataType.ordering`; behavior identical to 3.4.3.
- Spark 4.0.1 (audited 2026-06-24): adds `PercentileCont`/`PercentileDisc` builders and `SupportsOrderingWithinGroup`, enabling `percentile_cont(p) WITHIN GROUP (ORDER BY col)`, which rewrites to `Percentile(col, p, reverse)`. The ascending form runs natively; the `DESC` form sets `reverse = true` and falls back to Spark because the native `percentile_cont` always interpolates in ascending order.
- Spark 4.1.1 (audited 2026-06-24): identical to 4.0.1.
- `CometPercentile` reports `Incompatible` for the otherwise-supported form because DataFusion's `percentile_cont` quantizes the interpolation weight to 6 decimal places (`INTERPOLATION_PRECISION = 1e6`), so a deeply-interpolated value can differ from Spark by up to roughly `(upper - lower) * 1e-6`. The native path is opt-in via `spark.comet.expression.Percentile.allowIncompatible=true` ([#4719](https://github.com/apache/datafusion-comet/issues/4719)).

[Spark Expression Support]: ../../user-guide/latest/expressions.md
