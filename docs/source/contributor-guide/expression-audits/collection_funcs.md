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
- Known limitation: Comet only supports `StringType` children natively; `BinaryType` and `ArrayType` inputs fall back to Spark ([#4471](https://github.com/apache/datafusion-comet/issues/4471)). Non-default Spark 4.0 string collations are not propagated ([#2190](https://github.com/apache/datafusion-comet/issues/2190)).

## reverse

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Reverse(child) extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant`; `inputTypes = Seq(TypeCollection(StringType, ArrayType))`; `dataType = child.dataType`. For string, calls `UTF8String.reverse()`; for array, reverses element order in-place via `GenericArrayData`.
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` trait replaced by `override def nullIntolerant: Boolean = true`; `inputTypes` widened to `Seq(TypeCollection(StringTypeWithCollation(supportsTrimCollation = true), ArrayType))`. Semantics unchanged for `UTF8_BINARY`.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Known limitation: `Reverse` on an array containing `BinaryType` elements is reported as `Incompatible` and falls back unless explicitly enabled ([#2763](https://github.com/apache/datafusion-comet/issues/2763)).

## size

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Size(child, legacySizeOfNull) extends UnaryExpression with ExpectsInputTypes`; `inputTypes = Seq(TypeCollection(ArrayType, MapType)) -> IntegerType`. `legacySizeOfNull=true` returns `-1` for NULL input; `false` returns NULL. Comet routes via `CometSize`, which emits a `CaseWhen(isNotNull(child), size_scalar(child), Literal(legacySizeOfNull))`.
- Spark 4.0.1 (audited 2026-05-27): byte-for-byte identical to 3.5.8.
- Spark 4.1.1 (audited 2026-05-27): byte-for-byte identical to 3.5.8.
- Both `ArrayType` and `MapType` inputs are `Compatible` and run natively; every other child type is `Unsupported`.
- Performance (tuned 2026-07-10, PR [#4877](https://github.com/apache/datafusion-comet/pull/4877)): compute list row sizes from the offset buffer instead of allocating a sliced `ArrayRef` per row via `list_array.value(i)`, removing one heap allocation per row. ~94% faster. Benchmark: `benches/array_size.rs`.
- Performance (tuned 2026-08-05, PR [#5233](https://github.com/apache/datafusion-comet/pull/5233)): reuse Arrow's `length` kernel for List / LargeList / FixedSizeList and rewrite null slots to `-1` via `into_parts` + inverted-validity `set_indices`, avoiding the per-row builder loop. Up to 12x faster on the no-null production path (`CometSize.convert` wraps in `CASE WHEN isnotnull(child)`). Benchmark: `benches/array_size.rs`.
- Performance (tuned 2026-08-22, PR [#5300](https://github.com/apache/datafusion-comet/pull/5300)): build LargeList Int32 lengths directly from the i64 offset buffer, skipping the `length` kernel's Int64 output and the subsequent `cast_with_options` Int32 allocation. Hot path is `offsets.windows(2).map(|w| (w[1] - w[0]) as i32).collect()` when the total span fits in i32. Falls back to per-row `try_from` (with the same overflow error contract) when it doesn't. 6.6x faster on LargeList shapes. Benchmark: `benches/array_size.rs`.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
