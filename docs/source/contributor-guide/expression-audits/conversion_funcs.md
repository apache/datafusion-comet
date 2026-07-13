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

# conversion_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## cast

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8 modulo `Cast.canUpCast` refactored to delegate to `UpCastRule.canUpCast`.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Cast(child, dataType, timeZoneId, evalMode)`; eval modes are `LEGACY`, `ANSI`, `TRY`. The legacy `Cast.canCast` matrix and the `Cast.canAnsiCast` matrix decide acceptance per type pair. Comet routes via `CometCast` (`spark/src/main/scala/org/apache/comet/expressions/CometCast.scala`) using a per-source-type support matrix that returns `Compatible`, `Incompatible(reason)`, or `Unsupported(reason)`; literal children are short-circuited to `Compatible()` so `CometLiteral` validates them. The serialized `Cast` proto carries `datatype`, `evalMode`, `timezone` (default `UTC`), `allowIncompat` (from `spark.comet.expression.Cast.allowIncompatible`), and `isSpark4Plus`. The native side (`native/spark-expr/src/conversion_funcs/cast.rs`) implements explicit per-eval-mode branches for narrowing numeric casts that match Spark's overflow exceptions, and falls through to DataFusion `cast_with_options(safe = !ANSI)` for the rest.
- Spark 4.0.1 (audited 2026-05-27): `VariantType` added; `StringType` literals replaced with `_: StringType` to accommodate collated strings. `(TimestampType, ByteType|ShortType|IntegerType)` added to `canAnsiCast`. `NullIntolerant` -> `nullIntolerant: Boolean` refactor. New `ToPrettyString.BinaryFormatter` semantics for `Binary -> String` are replicated natively via `spark_binary_formatter`. Numeric-to-numeric matrix unchanged.
- Spark 4.1.1 (audited 2026-05-27): `TimeType` added; many `TimeType` arms in `canCast`/`canAnsiCast`. Geospatial `GeographyType` / `GeometryType` types added with their own conversion rules. Numeric-to-numeric matrix unchanged.
- Known divergences and gaps:
  - `CAST(<binary> AS STRING)` uses `unsafe { String::from_utf8_unchecked }` in native code, which is undefined behaviour for non-UTF8 inputs (https://github.com/apache/datafusion-comet/issues/4488).
  - Spark 4.0 collated `StringType` is not explicitly guarded; pattern equality is expected to keep collated-string casts falling back, but there is no test (https://github.com/apache/datafusion-comet/issues/4489; umbrella #2190).
  - Spark 4.1 `TimeType` casts have no explicit `Unsupported` arm; they fall back implicitly but do not appear in the auto-generated compatibility doc (https://github.com/apache/datafusion-comet/issues/4490).
  - `CAST(<map> AS <map>)` falls back to Spark even though native `cast_map_to_map` exists (https://github.com/apache/datafusion-comet/issues/4491).
  - `spark.sql.legacy.castComplexTypesToString.enabled=true` is not honoured by Comet (https://github.com/apache/datafusion-comet/issues/4492).
  - `CAST(<float|double> AS DECIMAL)` rounding may differ from Spark (`Incompatible`, gated by `spark.comet.expression.Cast.allowIncompatible`, tracked at https://github.com/apache/datafusion-comet/issues/1371).
- Spark registers the type-name conversion functions (`bigint`, `binary`, `boolean`, `date`, `decimal`, `double`, `float`, `int`, `smallint`, `string`, `timestamp`, `tinyint`) as cast aliases. Each lowers to the same `Cast` node, so Comet handles it via the `cast` implementation with the same compatibility profile.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
