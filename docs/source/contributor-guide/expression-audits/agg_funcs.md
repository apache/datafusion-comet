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

## pivot_first

- Spark 3.4.3 (audited 2026-07-02): `PivotFirst(pivotColumn, valueColumn, pivotColumnValues)` is an internal `ImperativeAggregate` emitted only by the optimized-pivot fast path in `Analyzer.ResolvePivot`. It buckets `valueColumn` into an array of length `pivotColumnValues.size` indexed by matching `pivotColumn`. Null value columns are ignored, unmatched pivot values are dropped. Value types are gated by `PivotFirst.supportsDataType` (Boolean, Byte, Short, Int, Long, Float, Double, Decimal).
- Spark 3.5.8 (audited 2026-07-02): swaps `StructType.fromAttributes` for `DataTypeUtils.fromAttributes`; no behavior change.
- Spark 4.0.1 (audited 2026-07-02): identical to 3.5.8.
- Spark 4.1.1 (audited 2026-07-02): identical to 4.0.1. (Spark `master` adds a defensive `findPivotIndex` wrapper that returns `-1` for null keys on the non-`AtomicType` `TreeMap` path; not present in any released version Comet builds against, and Comet's HashMap-backed lookup handles `ScalarValue::Null` safely on all types.)
- `CometPivotFirst` (in `spark/src/main/scala/org/apache/comet/serde/aggregates.scala`) forwards the aggregate to the native `SparkPivotFirst` UDAF (`native/spark-expr/src/agg_funcs/pivot_first.rs`) when the value type is in the supported set. State layout matches Spark's `aggBufferAttributes` (one scalar column per pivot slot) so the shuffle schema between Partial and Final stays consistent. `evaluate()` reassembles the slots into a `ListArray` matching `PivotFirst.dataType = ArrayType(valueDataType)`.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
