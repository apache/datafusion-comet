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

# collection_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## array_size

- Native via `size`. `array_size` lowers to `Size(child, legacySizeOfNull = false)`, so it returns NULL for NULL input. `CometSize` reads the per-expression `legacySizeOfNull` flag.

## concat

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Concat(children) extends ComplexTypeMergingExpression with QueryErrorsBase`; `allowedTypes = Seq(StringType, BinaryType, ArrayType)`; result type is the merged child type. Empty children is allowed and returns the empty string of the result type.
- Spark 4.0.1 (audited 2026-05-27): `allowedTypes` widens `StringType` to `StringTypeWithCollation(supportsTrimCollation = true)`. Error-formatting helper changes from `paramIndex` to `ordinalNumber`. Runtime semantics unchanged for `UTF8_BINARY`.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Known limitation: Comet only supports `StringType` children natively; `BinaryType` and `ArrayType` inputs fall back to Spark (https://github.com/apache/datafusion-comet/issues/4471). Non-default Spark 4.0 string collations are not propagated (https://github.com/apache/datafusion-comet/issues/2190).

## reverse

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Reverse(child) extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant`; `inputTypes = Seq(TypeCollection(StringType, ArrayType))`; `dataType = child.dataType`. For string, calls `UTF8String.reverse()`; for array, reverses element order in-place via `GenericArrayData`.
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true`; `inputTypes` widened to `Seq(TypeCollection(StringTypeWithCollation(supportsTrimCollation = true), ArrayType))`. Semantics unchanged for `UTF8_BINARY`.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Known limitation: `Reverse` on an array containing `BinaryType` elements is reported as `Incompatible` and falls back unless explicitly enabled (https://github.com/apache/datafusion-comet/issues/2763).

## size

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Size(child, legacySizeOfNull) extends UnaryExpression with ExpectsInputTypes`; `inputTypes = Seq(TypeCollection(ArrayType, MapType)) -> IntegerType`. `legacySizeOfNull=true` returns `-1` for NULL input; `false` returns NULL. Comet routes via `CometSize`, which emits a `CaseWhen(isNotNull(child), size_scalar(child), Literal(legacySizeOfNull))`.
- Spark 4.0.1 (audited 2026-05-27): byte-for-byte identical to 3.5.8.
- Spark 4.1.1 (audited 2026-05-27): byte-for-byte identical to 3.5.8.
- Known limitation: `Size` over `MapType` falls back to Spark (https://github.com/apache/datafusion-comet/issues/4472).

[Spark Expression Support]: ../../user-guide/latest/expressions.md
