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

## randstr

- Spark 3.4.3 (audited 2026-07-24): not present. `RandStr` was added in Spark 4.0, so `randstr` does not exist before then.
- Spark 3.5.8 (audited 2026-07-24): not present.
- Spark 4.0.1 (audited 2026-07-24): `RandStr(length, seedExpression, hideSeed) extends ExpressionWithRandomSeed with BinaryLike with Nondeterministic`. Both `length` (coerced to `IntegerType`) and `seed` (`IntegerType` or `LongType`) must be foldable; a non-foldable argument is rejected at analysis. Per partition it seeds `new XORShiftRandom(seed + partitionIndex)`; per row it calls `ExpressionImplUtils.randStr`, which fills `length` bytes with `abs(rng.nextInt() % 62)` mapped onto `0-9`/`a-z`/`A-Z`. A negative length raises `INVALID_PARAMETER_VALUE.LENGTH` at runtime. Comet emits a `RandStr` proto with the resolved length and seed and reproduces the `XORShiftRandom` and character mapping bit for bit; non-negative literal length and literal seed only (otherwise it falls back to Spark, which also raises the negative-length error).
- Spark 4.1.1 (audited 2026-07-24): `ExpressionImplUtils.randStr` and the `XORShiftRandom(seed + partitionIndex)` seeding are byte-identical to 4.0.1; adds `withShiftedSeed`, no runtime change.

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

## uuid

- Spark 3.4.3 (audited 2026-07-24): `Uuid(randomSeed: Option[Long]) extends LeafExpression with Nondeterministic with ExpressionWithRandomSeed`. The analyzer's `ResolveRandomSeed` fills `randomSeed` with a random `Long`, so it is always defined before Comet sees the plan. Per partition it seeds `RandomUUIDGenerator(randomSeed + partitionIndex)`, a Commons Math3 `MersenneTwister`, and per row draws two `nextLong()`s, masks in the RFC 4122 version 4 and variant bits, and formats via `java.util.UUID.toString`. Only the no-argument `uuid()` form exists (no seed constructor). Comet emits a `Uuid` proto with the resolved seed and reproduces the generator bit for bit via `SparkMersenneTwister`.
- Spark 3.5.8 (audited 2026-07-24): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-07-24): adds `def this(seed: Expression)`, exposing the `uuid(seed)` SQL form (the seed must be an integer or long literal, validated at analysis time). `RandomUUIDGenerator` and the per-row algorithm are unchanged, so results are identical to 3.4.3 for a given seed.
- Spark 4.1.1 (audited 2026-07-24): identical to 4.0.1, plus `withShiftedSeed`. No runtime change.

## variant_get / try_variant_get

- Spark 3.4.3 (audited 2026-10-04): the expressions are absent.
- Spark 3.5.8 (audited 2026-10-04): the expressions are absent.
- Spark 4.0.1 (audited 2026-10-04): selected-value getters use ANSI cast-admission rules with TRY-mode casts and explicit long/decimal-to-timestamp overflow checks. `variant_get` raises `INVALID_VARIANT_CAST` on conversion failure; `try_variant_get` returns NULL. Both retain malformed-value and invalid-path errors.
- Spark 4.1.1 (audited 2026-10-04): the same scalar extraction rules apply. Spark 4.1.3, 4.2.0 and current master were also checked for subsequent changes.
- Comet serializes Spark-parsed foldable paths. Invalid paths stay in Spark because their error timing depends on null inputs and code generation. Native traversal follows Spark's small-object linear lookup and large-object UTF-16 binary lookup, including empty keys and insertion-ordered metadata. It does not validate unrelated values. Current Spark master adds UTF-8 lookup with a legacy fallback; released Spark 4.x still uses UTF-16 ordering.
- Spark uses a 128 MiB Variant size limit outside tests and 16 MiB in testing. Comet transports Spark's current limit with the expression.
- Selected scalar values reuse the native Spark cast kernels in batches grouped by source type. Explicit adaptations cover timestamp-to-numeric, Boolean-to-decimal and checked numeric-to-timestamp conversions. Raw dates and timestamps preserve the full Spark integer range for identity extraction. Strict failures carry the original value and path to the JVM, where Spark constructs the exact version/JDK-dependent error parameters.
- STRING targets remain unsupported: matching Spark requires JSON and scalar formatting beyond the shared native formatter. Decimal targets are opt-in because floating-point decimal conversion can differ on JDK 17. Date/time targets are opt-in because native parsing and timezone conversion have narrower ranges. These remaining scalar compatibility requirements are tracked by [#5424](https://github.com/apache/datafusion-comet/issues/5424).
- TIME targets, dynamic paths, nested targets and Variant outputs fall back. Native admission is restricted to explicit Variant consumers; generic grouping, ordering and hashing remain restricted. Extraction consumes the decoded whole value; selective Parquet reads remain separate work.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
