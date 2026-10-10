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

# conditional_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## coalesce

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Coalesce(children) extends Expression with ComplexTypeMergingExpression`; returns the first non-null child, evaluated left-to-right with short-circuit. Result type is the merged child type. Comet routes via `CometCoalesce`, which serialises as nested `CaseWhen(IsNotNull(c1) -> c1, IsNotNull(c2) -> c2, ..., else cN)` so the native engine preserves the short-circuit semantics.
- Spark 4.0.1 (audited 2026-05-27): byte-for-byte identical to 3.5.8.
- Spark 4.1.1 (audited 2026-05-27): byte-for-byte identical to 3.5.8.
- Performance (tuned 2026-09-28, PR [#6350](https://github.com/apache/datafusion-comet/pull/6350)): runs through the `CASE WHEN` evaluation described under `when`. 12-13x faster for two columns. Benchmark: `benches/conditional.rs`.

## if

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `If(predicate, trueValue, falseValue) extends ComplexTypeMergingExpression`; standard ternary semantics with short-circuit, predicate must be `BooleanType`. Comet routes via `CometIf` to the native `IfExpr` proto.
- Spark 4.0.1 (audited 2026-05-27): adds an `override def withNewAlwaysEvaluatedInputs(...)` hook for the new optimizer phase; semantics unchanged. Error messages reformatted (`paramIndex` -> `ordinalNumber`).
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Performance (tuned 2026-09-28, PR [#6350](https://github.com/apache/datafusion-comet/pull/6350)): runs through the `CASE WHEN` evaluation described under `when`, with an `IF` or `CASE WHEN` in the ELSE flattened into one merge. 1.5x faster with literal results, 6.6x with column results, 3.3-4.7x with string columns, 3.1-5.9x nested. Benchmark: `benches/conditional.rs`.

## ifnull

- Spark 3.4.3 (audited 2026-05-27): registry alias of `Nvl` (`expression[Nvl]("ifnull", setAlias = true)`). Same `RuntimeReplaceable` lowering as `nvl`.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.1.1 (audited 2026-05-27): identical to 3.4.3.

## nullif

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): `NullIf(left, right, replacement) extends RuntimeReplaceable with InheritAnalysisRules`; the analyzer rewrites to `If(EqualTo(left, right), Literal(null, left.dataType), left)`. Comet handles via `CometIf` plus `CometEqualTo`.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.5.8.
- Spark 4.1.1 (audited 2026-05-27): identical to 3.5.8.

## nvl

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): `Nvl(left, right, replacement) extends RuntimeReplaceable`; analyzer rewrites to `Coalesce(Seq(left, right))`. Comet handles via `CometCoalesce`.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.5.8.
- Spark 4.1.1 (audited 2026-05-27): identical to 3.5.8.

## nvl2

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): `Nvl2(expr1, expr2, expr3, replacement) extends RuntimeReplaceable`; analyzer rewrites to `If(IsNotNull(expr1), expr2, expr3)`. Comet handles via `CometIf`.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.5.8.
- Spark 4.1.1 (audited 2026-05-27): identical to 3.5.8.

## when

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): the `CASE WHEN ... THEN ...` SQL form lowers to `CaseWhen(branches: Seq[(Expression, Expression)], elseValue: Option[Expression])`. Spark evaluates left-to-right with short-circuit; result type is the merged branch type. Comet routes via `CometCaseWhen` to the native `CaseWhen` proto.
- Spark 4.0.1 (audited 2026-05-27): adds the `withNewAlwaysEvaluatedInputs` optimizer hook; semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Performance (tuned 2026-09-28, PR [#6350](https://github.com/apache/datafusion-comet/pull/6350)): when no branch value and no WHEN after the first can fail, `CaseWhenExpr` evaluates them over the whole batch and takes each row's value with typed merges for primitive, boolean, string and binary results, instead of filtering the batch for each branch and merging the partial results with `MutableArrayData`. A branch is no longer wrapped in a cast to the type it already has, which hid literals and columns from DataFusion's fast paths. 2.8-4x faster with literal results and 2.2-7.8x with column results. When a branch can fail it is about 2% slower, because DataFusion's `CaseExpr` derives the result type of a bare branch for every batch. Benchmark: `benches/conditional.rs`.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
