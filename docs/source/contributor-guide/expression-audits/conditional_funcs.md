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

## if

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `If(predicate, trueValue, falseValue) extends ComplexTypeMergingExpression`; standard ternary semantics with short-circuit, predicate must be `BooleanType`. Comet routes via `CometIf` to the native `IfExpr` proto.
- Spark 4.0.1 (audited 2026-05-27): adds an `override def withNewAlwaysEvaluatedInputs(...)` hook for the new optimizer phase; semantics unchanged. Error messages reformatted (`paramIndex` -> `ordinalNumber`).
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

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

[Spark Expression Support]: ../../user-guide/latest/expressions.md
