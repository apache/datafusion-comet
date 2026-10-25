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

## assertnotnull

Internal Catalyst runtime nullability check, with no public SQL function. Native evaluation preserves the child value and checks its outer logical validity, including dictionary values. Structural and lazy conditional parents used by V2 writes are supported when they contain one distinct assertion over a nonthrowing child and no competing errors. Repeated identical assertions are safe; distinct checks or potentially throwing siblings fall back to preserve Spark's first failing row, field and error. Only identity and numeric widening casts are treated as nonthrowing; temporal and nested conversions can overflow even when Spark permits an upcast. Assertions captured entirely in JVM codegen dispatch retain their existing execution path.

Partial consumers keep assertions in Spark's row evaluation. This includes limits, samples, deferred projection/filter fields, join inputs, external row exports such as `DataFrame.rdd`, and separately prepared scalar subqueries that read only two rows. Full-consumption barriers permit native checks only when their own keys or aggregate updates cannot fail before a later child assertion. Late TopK and aggregate result projections retain their external consumption bound. Assertion predicates, including predicates resolved from scalar subqueries after planning, remain outside native Parquet scans. Empty-schema shuffles preserve Spark's key evaluation, and native one-partition hash shuffles decline asserted keys.

Semi/anti join conditions and inner/semi/anti keys fall back when native evaluation could check candidates or keys Spark skips. Hash joins also decline asserted build keys, packed keys, and combinations of keys or residual conditions whose first error can differ. These checks inspect native assertions without traversing opaque JVM-dispatched closures. Typed Dataset conversion remains eligible on the preserved streamed side of a fully consumed outer hash join; an ancestor that can stop consuming rows still prevents conversion.

Aggregate arguments and filters fall back when their serialized aggregate contains a native assertion, because Spark can skip values during lazy, filtered or nullable-argument updates. Grouping keys additionally preserve ordering against aggregate-update errors. Required-field writes can retain a native writer above a Spark row-checked projection converted to Arrow. `RowArrowReader` delivers a completed prefix before a later producer error, while encoding errors and cancellation propagate immediately.

The `CometAssertNotNullBenchmark` compares Spark and Comet on primitive, dictionary-string and struct inputs at several NULL densities, and on required-field Iceberg writes. It consumes output and verifies the assertion and writer implementations before timing.

- Spark 3.4.3 (audited 2026-10-22): full `NullPointerException` message and newline-delimited walked path. Nullable-source V2 writes to required columns are rejected during analysis.
- Spark 3.5.9 (audited 2026-10-22): same expression error contract as 3.4.3. V2 writes insert runtime assertions for required fields.
- Spark 4.0.4 (audited 2026-10-22): `SparkRuntimeException` with `NOT_NULL_ASSERT_VIOLATION` and `walkedTypePath`.
- Spark 4.1.3 (audited 2026-10-22): same expression contract as 4.0.4.
- Spark 4.2.0 (audited 2026-10-22): same expression contract as 4.0.4.

## AtLeastNNonNulls (DataFrame API)

- Spark 3.4.3 (audited 2026-09-24): non-nullable Boolean predicate used by `DataFrame.na.drop`. Counts non-null, non-NaN children and stops evaluating a row once the threshold is reached. Non-positive thresholds return true without evaluating children.
- Spark 3.5.9 (audited 2026-09-24): identical to 3.4.3.
- Spark 4.0.4 (audited 2026-09-24): identical to 3.4.3.
- Spark 4.1.3 (audited 2026-09-24): identical to 3.4.3.

Comet uses a native physical expression to preserve per-row short-circuit evaluation. Non-floating types use outer logical nullability; nested nulls and empty collections remain non-null values. Rust tests cover thresholds, sliced and empty batches, dictionary values, literals, and required and skipped child errors. DataFrame tests cover native routing, disabled-expression fallback, complex types and ANSI casts. This internal predicate has no SQL function name.

Native evaluation, like Spark's, may skip the children after the first `n` on a row. When subexpression elimination shares a subexpression with one of those children, Spark evaluates it for every row, so Comet falls back to Spark rather than skip an error Spark would raise. Plain `DataFrame.na.drop` children are columns and stay native.

- The [Criterion benchmark](../../../../native/spark-expr/benches/at_least_n_non_nulls.rs) covers batch sizes around the 64-row bitmap word boundary and NULL/NaN densities.

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

- See `string_funcs / regexp_replace` and the `CometRLike` notes (audited in PR [#4461](https://github.com/apache/datafusion-comet/pull/4461)). `CometRLike` consults a plan-time whitelist (`CometRegex`) for non-null `UTF8_BINARY` literal patterns. Analyzer-admitted patterns (ASCII literals, simple character classes, greedy quantifiers, groups, alternation) run natively by default. Every other literal stays on the JVM codegen dispatcher unless `spark.comet.expression.RLike.allowIncompatible=true` forces the Rust path. Non-literal and NULL patterns, and Spark 4 non-default collations, always stay on the dispatcher. Anchors (`^`, `$`) and `.` / `\d` / `\w` / `\s` are out of subset because Java and Rust disagree on line terminators and Unicode character classes. Rust class set operators (`&&` / `~~` / `--`), unescaped class delimiters used as atoms or range endpoints, and counted, aggregate, or nested patterns over a conservative compile-size budget are also out of subset so they cannot silently change results, suppress Spark errors, or fail native plan construction.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
