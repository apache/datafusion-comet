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

# array_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## array

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `CreateArray(children, useStringTypeWhenEmpty)`; element type is the common type of children. Comet routes via `CometCreateArray` (native `make_array`) and special-cases the empty-array case to dodge a known DataFusion `coerce_types` issue (#3338).
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): adds `contextIndependentFoldable` override; runtime semantics unchanged.

## array_append

- Spark 3.4.3 (audited 2026-05-27): standalone `BinaryExpression`, evaluated directly. Comet routes via `CometArrayAppend`.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): now `RuntimeReplaceable` and rewritten to `ArrayInsert(arr, Literal(-1), elem)`. `CometArrayAppend` is therefore unreachable; dispatch goes through `CometArrayInsert` (which carries its own `Incompatible` notes documented at the `array_insert` entry).
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## array_compact

- Spark 3.4.3 (audited 2026-05-27): `RuntimeReplaceable` -> `ArrayFilter(arr, IsNotNull(lambda))`. Comet receives the rewritten form, dispatches through `CometArrayFilter`, which delegates back to `CometArrayCompact.convert` for the actual proto emission. The native path uses Comet's `spark_array_compact` UDF rather than DataFusion's `array_remove_all` because DataFusion 53 changed `array_remove_all`'s NULL semantics.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): the replacement is wrapped in `KnownNotContainsNull(...)` (analysis-only hint, no semantic change).
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## array_contains

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ArrayContains(left, right) extends BinaryExpression with NullIntolerant with Predicate`; `inputTypes` uses `findWiderTypeWithoutStringPromotionForTwo`. Wired as `CometScalarFunction("array_contains")`.
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` trait replaced by `nullIntolerant: Boolean`; `checkInputDataTypes` adopts `DataTypeUtils.sameType` (collation-aware in 4.x).
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Known limitation: no NaN-canonicalization guard in `getSupportLevel`. For `Float`/`Double` arrays containing NaN, Spark's `SQLOrderingUtil` may produce different results than DataFusion's IEEE comparison (https://github.com/apache/datafusion-comet/issues/4481).

## array_distinct

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ArrayDistinct(child)` over `ArraySetLike`; uses `SQLOpenHashSet` so NaN and `+0.0`/`-0.0` are canonicalized. Wired as `CometScalarFunction("array_distinct")`.
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` -> `nullIntolerant` field refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Known divergence: DataFusion `array_distinct` uses hash-based equality without NaN/signed-zero canonicalization, so float/double arrays may produce different results (https://github.com/apache/datafusion-comet/issues/4481).

## array_except

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ArrayExcept(left, right) extends ArrayBinaryLike with ComplexTypeMergingExpression`; result preserves left-side first occurrences not present in right. Comet routes via `CometArrayExcept` and unconditionally flags `Incompatible` ("Null handling and ordering may differ from Spark"); also falls back for `BinaryType` / `StructType` element types.
- Spark 4.0.1 (audited 2026-05-27): `nullIntolerant = true` moves into `ArrayBinaryLike`; the overflow path uses `arrayFunctionWithElementsExceedLimitError`.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Known divergence: same NaN/signed-zero canonicalization gap as `array_distinct` for float/double arrays (https://github.com/apache/datafusion-comet/issues/4481).

## array_insert

- Spark 3.4.3 audited 2026-04-02
- Spark 3.5.8 audited 2026-04-02
- Spark 4.0.1 audited 2026-04-02 (pos=0 error message differs from Spark)
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## array_intersect

- Spark 3.4.3 audited 2026-04-24 (result element order may differ from Spark when the right array is longer than the left; DataFusion probes the longer side)
- Spark 3.5.8 audited 2026-04-24 (same ordering incompatibility as 3.4.3)
- Spark 4.0.1 audited 2026-04-24 (ordering incompatibility as above; collated strings now fall back to Spark)
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## array_join

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ArrayJoin(array, delimiter, nullReplacement)`. Comet routes via `CometArrayJoin` to DataFusion's `array_to_string` and is unconditionally flagged `Incompatible` ("Null handling may differ from Spark", #3178).
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `AbstractArrayType(StringTypeWithCollation(supportsTrimCollation = true))`; non-binary collations not propagated (https://github.com/apache/datafusion-comet/issues/2190).
- Spark 4.1.1 (audited 2026-05-27): adds `contextIndependentFoldable` override; runtime unchanged.

## array_max

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ArrayMax(child) extends UnaryExpression with ImplicitCastInputTypes`; skips NULL elements; for float/double Spark's `SQLOrderingUtil` treats NaN as greater than any non-NaN. Wired as `CometScalarFunction("array_max")`.
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` -> `nullIntolerant` field refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Known divergence: DataFusion's `array_max` uses Arrow `partial_cmp`-based ordering, so float/double arrays containing NaN may produce different results (https://github.com/apache/datafusion-comet/issues/4482).

## array_min

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): mirror of `ArrayMax` with `evalInternal` returning the minimum. Same NULL-skip and NaN-ordering semantics. Wired as `CometScalarFunction("array_min")`.
- Spark 4.0.1 (audited 2026-05-27): same trait refactor as `array_max`.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Known divergence: same NaN-handling gap as `array_max` (https://github.com/apache/datafusion-comet/issues/4482).

## array_position

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ArrayPosition(left, right)`; returns 1-based `LongType` position, 0 if not found, NULL if either input is NULL. `CometArrayPosition` falls back for all-foldable args (constant folding handles those) and for unsupported element types (binary/struct/map/null).
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` -> `nullIntolerant` field refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## array_remove

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ArrayRemove(left, right)`; removes all occurrences equal to `right`. Wired as `CometScalarFunction("array_remove")`. Falls back via `ArraysBase.isTypeSupported` for binary/struct/map/null child types.
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` -> `nullIntolerant` field refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## array_repeat

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ArrayRepeat(left, right) extends BinaryExpression with ExpectsInputTypes`; `inputTypes = Seq(AnyDataType, IntegerType)`. NULL count yields NULL; count <= 0 yields empty array; count > `MAX_ROUNDED_ARRAY_LENGTH` throws at runtime. Comet wraps the call in `CaseWhen(IsNotNull(right), array_repeat(...), null)`.
- Spark 4.0.1 (audited 2026-05-27): error message uses `createArrayWithElementsExceedLimitError(prettyName, count)`; semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## array_union

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ArrayUnion(left, right) extends ArrayBinaryLike with ComplexTypeMergingExpression`; result is left-side distinct elements followed by new right-side elements. Wired as `CometScalarFunction("array_union")`.
- Spark 4.0.1 (audited 2026-05-27): `nullIntolerant = true` moves into `ArrayBinaryLike`; overflow path uses `arrayFunctionWithElementsExceedLimitError`.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Known divergence: same NaN/signed-zero canonicalization gap as `array_distinct` (https://github.com/apache/datafusion-comet/issues/4481). Result ordering versus DataFusion is also unverified; compare the `array_intersect` ordering caveat.

## arrays_overlap

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ArraysOverlap(left, right)`; three-valued logic (TRUE if any common non-null element, NULL if a null is present and no overlap is found in non-nulls, FALSE otherwise). Comet routes via `CometArraysOverlap` to the native `spark_arrays_overlap` UDF, which implements the same three-valued logic.
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` -> `nullIntolerant` field refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## arrays_zip

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ArraysZip(children, names)`; returns an array of structs, padding shorter inputs with NULL. Comet routes via `CometArraysZip` and rejects unsupported child element types (anything outside primitives, decimals, dates/timestamps, strings, binary, and nested arrays/structs of those).
- Spark 4.0.1 (audited 2026-05-27): the length-mismatch error switches from `IllegalArgumentException` to `SparkIllegalArgumentException("_LEGACY_ERROR_TEMP_3235")`; runtime unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## element_at

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ElementAt(left, right, defaultValueOutOfBound, failOnError)`; group label `map_funcs`. Comet supports only `ArrayType` input; `MapType` input falls back.
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` -> `nullIntolerant` field refactor; group label changes to `collection_funcs`; ANSI default flips to `true` so out-of-bound throws by default. Comet wires `failOnError` through to native `ListExtract`.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## flatten

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Flatten(child) extends UnaryExpression`; returns NULL if any inner sub-array is NULL. Comet routes via `CometFlatten` and falls back for child types containing `BinaryType` / `StructType` / `MapType` (limitation of `ArraysBase.isTypeSupported`).
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` -> `nullIntolerant` field refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## get

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `GetArrayItem(child, ordinal, failOnError)`; `inputTypes = Seq(AnyDataType, IntegralType)`. Comet routes via `CometGetArrayItem`, wiring `failOnError` through to the proto.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; ANSI default flips to `true`.
- Spark 4.1.1 (audited 2026-05-27): `inputTypes` tightened to `Seq(ArrayType, IntegralType)` (analysis-time only); runtime unchanged.

## sort_array

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `SortArray(base, ascendingOrder) extends BinaryExpression with ArraySortLike`; the second arg must be a `Literal(_: Boolean, BooleanType)`. Comet `CometSortArray` flags `Incompatible` under strict floating-point and falls back for nested arrays whose innermost element is `Struct` or `Null`.
- Spark 4.0.1 (audited 2026-05-27): trait set changes substantively: `ArraySortLike` and `NullIntolerant` are removed, `nullIntolerant = true` becomes an override, and `ascendingOrder` is widened to accept any foldable boolean (not just `Literal`). Comet's `CometSortArray` still requires a `Literal`, so the new foldable form falls back at convert time.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
