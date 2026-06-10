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

# bitwise_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## `&`

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `BitwiseAnd(left, right) extends BinaryArithmetic` over `IntegralType`. Comet routes via `CometBitwiseAnd` to the proto's `bitwise_and` binary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `nullIntolerant: Boolean`.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## `<<`

- Spark 3.4.3 (audited 2026-05-27): only the function form `shiftleft(...)` exists; the `<<` operator alias is added in Spark 4.0.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): `<<` parses to `ShiftLeft` (the same `case class` as `shiftleft`) via a parser alias. `ShiftLeft` now extends a shared `BitShiftOperation` trait; eval semantics unchanged. Comet `CometShiftLeft` casts the `right` operand to `LongType` when `left.dataType == LongType` because DataFusion's bitwise shift requires matching operand types.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## `>>`

- Spark 3.4.3 (audited 2026-05-27): same as `<<` history — only the function form exists pre-4.0.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): `>>` parses to `ShiftRight`. Comet `CometShiftRight` mirrors the same operand-cast logic as `CometShiftLeft`.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## `^`

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `BitwiseXor(left, right) extends BinaryArithmetic` over `IntegralType`. Comet routes via `CometBitwiseXor` to the proto's `bitwise_xor` binary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `nullIntolerant` field refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## bit_count

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `BitwiseCount(child) extends UnaryExpression`; accepts `IntegralType` or `BooleanType` and returns the population count as `IntegerType`. Wired as `CometScalarFunction("bit_count")`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## bit_get

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `BitwiseGet(left, right) extends BinaryExpression`; `inputTypes = (IntegralType, IntegerType) -> ByteType`. The position is bounds-checked at runtime via `BitwiseGetUtil.checkPosition`. Comet routes via `CometBitwiseGet` to the native `bit_get` scalar.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## getbit

- Spark 3.4.3 (audited 2026-05-27): registry alias of `BitwiseGet`. Same support as `bit_get`.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.1.1 (audited 2026-05-27): identical to 3.4.3.

## shiftright

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. Function name for `ShiftRight`. Same support as the `>>` operator alias added in 4.0.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.5.8 (the `>>` operator is the only addition).
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## shiftrightunsigned

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ShiftRightUnsigned(left, right) extends BinaryExpression`; uses Java's `>>>` (zero-fill) semantics. Comet wires as `CometScalarFunction("shiftrightunsigned")` (added in #4375).
- Spark 4.0.1 (audited 2026-05-27): now extends the shared `BitShiftOperation` trait; eval semantics unchanged. The `>>>` operator alias is added at the parser level.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## `|`

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `BitwiseOr(left, right) extends BinaryArithmetic` over `IntegralType`. Comet routes via `CometBitwiseOr` to the proto's `bitwise_or` binary expression.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `nullIntolerant` field refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## `~`

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `BitwiseNot(child) extends UnaryExpression`; accepts `IntegralType` and returns the bitwise complement of the same type. Comet routes via `CometBitwiseNot` to the native `bitwise_not` scalar.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `nullIntolerant` field refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
