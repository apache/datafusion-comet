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

# predicate_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## `!`

- Spark 3.4.3 (audited 2026-05-27): registry alias of `Not`. Same support as `not`.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.1.1 (audited 2026-05-27): identical to 3.4.3.

## `<`

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `LessThan(left, right) extends BinaryComparison`. Comet routes via `CometLessThan` to the proto's `lt` binary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## `<=`

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `LessThanOrEqual(left, right) extends BinaryComparison`. Comet routes via `CometLessThanOrEqual` to the proto's `lt_eq` binary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## `<=>`

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `EqualNullSafe(left, right) extends BinaryComparison`; treats two NULLs as equal and a NULL with a non-NULL as not equal. Comet routes via `CometEqualNullSafe` to the proto's `eq_null_safe` binary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## `=`

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `EqualTo(left, right) extends BinaryComparison`. Comet routes via `CometEqualTo` to the proto's `eq` binary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## `==`

- Spark 3.4.3 (audited 2026-05-27): registry alias of `EqualTo`. Same support as `=`.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.1.1 (audited 2026-05-27): identical to 3.4.3.

## `>`

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `GreaterThan(left, right) extends BinaryComparison`. Comet routes via `CometGreaterThan` to the proto's `gt` binary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## `>=`

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `GreaterThanOrEqual(left, right) extends BinaryComparison`. Comet routes via `CometGreaterThanOrEqual` to the proto's `gt_eq` binary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## and

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `And(left, right) extends BinaryOperator with Predicate`; short-circuit left-to-right. Comet routes via `CometAnd` to the proto's `and` binary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## between

- Spark 3.4.3 (audited 2026-05-27): the SQL form `expr BETWEEN low AND high` is rewritten at the parser level to `expr >= low AND expr <= high`. Comet sees only the resulting `And(GreaterThanOrEqual, LessThanOrEqual)` and routes via `CometAnd` + `CometGreaterThanOrEqual` + `CometLessThanOrEqual`.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.1.1 (audited 2026-05-27): identical to 3.4.3.

## ilike

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ILike(left, right, escapeChar) extends RuntimeReplaceable`; the analyzer rewrites to `Like(Lower(left), Lower(right), escapeChar)`. Comet handles via `CometLike` and `CometLower` (case-conversion path, gated by `spark.comet.caseConversion.enabled=false` by default).
- Spark 4.0.1 (audited 2026-05-27): identical to 3.5.8.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## in

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `In(value, list: Seq[Expression]) extends Predicate`; NULL-sensitive set membership (NULL in any element of the list yields NULL when no match found). Comet routes via `CometIn` to the proto's `In` expression with `negated = false`; `InSet` is rewritten to `In` so the native side can perform its own set-lookup optimization.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## isnan

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `IsNaN(child) extends UnaryExpression`; returns `BooleanType`; `false` for NULL or non-float types. Comet routes via `CometIsNaN` to the native `isnan` scalar.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## isnotnull

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `IsNotNull(child) extends UnaryExpression with Predicate`. Comet routes via `CometIsNotNull` to the proto's `is_not_null` unary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## isnull

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `IsNull(child) extends UnaryExpression with Predicate`. Comet routes via `CometIsNull` to the proto's `is_null` unary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## like

- See `string_funcs / like` (audited in PR [#4461](https://github.com/apache/datafusion-comet/pull/4461)). `CometLike` only supports the default `\\` escape character.

## not

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Not(child) extends UnaryExpression with Predicate`. Comet routes via `CometNot`, which optimizes a few special cases: `Not(EqualTo)` -> proto `neq`, `Not(EqualNullSafe)` -> proto `neq_null_safe`, `Not(In)` -> proto `In(negated = true)`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## or

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Or(left, right) extends BinaryOperator with Predicate`; short-circuit left-to-right. Comet routes via `CometOr` to the proto's `or` binary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## rlike

- See `string_funcs / regexp_replace` and the `CometRLike` notes (audited in PR [#4461](https://github.com/apache/datafusion-comet/pull/4461)). By default `CometRLike` routes through the JVM codegen dispatcher so Spark's own `Pattern` engine runs inside the Comet pipeline. The native path uses the Rust `regex` crate, which differs from Java's `Pattern` engine, so it is opt-in via `spark.comet.expression.RLike.allowIncompatible=true` and only applies when the pattern is a string literal.
- Native candidate (assessed 2026-08-13): Recommended for the provably-equivalent pattern subset ([#5351](https://github.com/apache/datafusion-comet/issues/5351)). Compatibility: Medium (the divergence list is already enumerated in `compatibility/regex.md` and every item is detectable by inspecting a literal pattern, but "provably equivalent" is a subtle claim so the analyzer must be conservative; the guard is a plan-time pattern oracle mirroring `CometCast`, with out-of-subset patterns and non-default Spark 4.x collations keeping today's dispatcher default). Upside: High, measured from the emitted kernel via `CometBatchKernelCodegen.generateSource` (7 allocation sites per non-null row, 4 of them inside one `Pattern.matcher()` call: the `Matcher` plus its `groups`, `locals`, and `localsPos` arrays, on top of `UTF8String.toString()`'s `getBytes` copy, `String` header, and internal `byte[]`; the `Pattern` itself is compiled once into mutable state, so it is not a per-row cost). Unlike the other candidates the native kernel already exists, so the work is a JVM-side analyzer plus a differential pattern corpus, and the same analyzer unlocks `regexp_replace`, `split`, `regexp_extract`, and `regexp_extract_all`. Distinct from [#4310](https://github.com/apache/datafusion-comet/issues/4310), which correctly concluded the Rust _engine_ can never be fully Java-compatible; this is a per-pattern decision, not a per-engine one.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
