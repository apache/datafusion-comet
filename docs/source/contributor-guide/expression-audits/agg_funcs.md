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

## collect_list

- Spark 3.4.3 (audited 2026-06-24): `CollectList` extends `Collect[ArrayBuffer[Any]]`, returns `ArrayType(child.dataType, containsNull = false)`, ignores NULL inputs in `update()` (Hive-compatible semantics), and yields an empty array as `defaultResult`. `nullable = false`. No `checkInputDataTypes` override, so any input type accepted by Spark's analyzer is accepted. Registered as both `collect_list` and `array_agg` aliases in `FunctionRegistry`.
- Spark 3.5.8 (audited 2026-06-24): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-06-24): only structural change is adding `with UnaryLike[Expression]` to the case class; no behavior change.
- Spark 4.1.1 (audited 2026-06-24): identical to 4.0.1.
- Comet implementation: native side delegates to `datafusion_spark::function::aggregate::collect::SparkCollectList`, which wraps `ArrayAggAccumulator` with `ignore_nulls = true` and converts a final NULL accumulator state to an empty array. Native intermediate state is `ArrayType(child.dataType, containsNull = true)`, so Comet rewrites intermediate `Partial`/`PartialMerge` object-aggregate buffer attributes from Spark's serialized `BinaryType` to that native state type.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
