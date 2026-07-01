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

# misc_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## current_catalog

- Resolved to a literal by the analyzer (`ReplaceCurrentLike`).

## current_database

- Resolved to a literal by the analyzer (`ReplaceCurrentLike`).

## current_schema

- Alias of `current_database`; resolved to a literal by the analyzer.

## current_user

- Resolved to a literal by the analyzer; same as `user`.

## monotonically_increasing_id

- Spark 3.4.3 (audited 2026-05-27): byte-for-byte identical to 4.1.1. `MonotonicallyIncreasingID() extends LeafExpression with Stateful`; produces a Long that encodes the partition id in the upper 31 bits and a per-partition row counter in the lower 33 bits. Comet emits an empty `MonotonicallyIncreasingId` proto and the native side produces the same encoding.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.1.1 (audited 2026-05-27): identical to 3.4.3.

## rand

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Rand(child, hideSeed) extends RDG` (an `UnaryExpression with ExpectsInputTypes with Nondeterministic with ExpressionWithRandomSeed`); `child` is the seed expression, coerced to `IntegerType` or `LongType` via `ImplicitCastInputTypes`. Uses `XORShiftRandom(seed + partitionIndex)` per partition and returns `nextDouble()` in `[0, 1)`. NULL seed evaluates to `0L` (via `null.asInstanceOf[Long]`).
- Spark 4.0.1 (audited 2026-05-27): `RDG` is refactored from an `abstract class` into a trait, and `Rand` now extends a new `NondeterministicUnaryRDG` base. `ExpressionWithRandomSeed.expressionToSeed` is hoisted as a shared helper and throws `QueryCompilationErrors.invalidRandomSeedParameter` for non-literal seeds at analysis time. Runtime semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Comet limitation: the seed argument must be a literal (column-reference seeds are rejected via `getSupportLevel`). Pre-4.0 Spark would otherwise silently fail at runtime; 4.0+ rejects at analysis time before the expression reaches Comet.

## randn

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): same base as `Rand`; differs only in the eval body (`nextGaussian()` instead of `nextDouble()`), producing values from the standard normal distribution.
- Spark 4.0.1 (audited 2026-05-27): same refactor as `Rand`; runtime unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Comet limitation: same as `rand` — the seed argument must be a literal.

## session_user

- Alias of `current_user`; resolved to a literal by the analyzer.

## spark_partition_id

- Spark 3.4.3 (audited 2026-05-27): byte-for-byte identical to 4.1.1. `SparkPartitionID() extends LeafExpression with Nondeterministic`; returns the integer index of the partition being processed. Comet emits an empty `SparkPartitionId` proto.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.1.1 (audited 2026-05-27): identical to 3.4.3.

## typeof

- Foldable; resolved to a literal before Comet sees the plan.

## user

- Spark 3.4.3 (audited 2026-05-27): `CurrentUser() extends LeafExpression with Unevaluable`; the analyzer's `ResolveCurrentLike` rule replaces it with a `StringType` literal of the current user name before Comet sees the plan. No Comet serde needed; the literal flows through `CometLiteral`.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.4.3 except the resulting literal carries the default string collation.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
