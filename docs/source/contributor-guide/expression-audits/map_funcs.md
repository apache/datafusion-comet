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

# map_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## element_at

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `ElementAt(left, right, defaultValueOutOfBound, failOnError) extends GetMapValueUtil`; the parser routes `element_at(<array>, ...)` to one overload and `element_at(<map>, ...)` to another. Comet routes `MapType` input through the same native `map_extract` path used by `GetMapValue`.
- Spark 4.0.1 (audited 2026-05-27): adds `nullIntolerant: Boolean` field; semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## map_contains_key

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapContainsKey(left, right) extends RuntimeReplaceable with InheritAnalysisRules`; the analyzer rewrites to `ArrayContains(MapKeys(left), right)`. Comet routes via `CometMapContainsKey` which emits the equivalent `array_has(map_keys(map), key)`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; minor trait refactors.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## map_entries

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapEntries(child)` returns an array of structs `<key, value>`. Wired to native `map_entries`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## map_from_arrays

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapFromArrays(left, right) extends BinaryExpression with NullIntolerant`; Spark uses `ArrayBasedMapBuilder` to detect duplicate keys (subject to `spark.sql.mapKeyDedupPolicy`) and rejects null keys with `RuntimeException("Cannot use null as map key")`. Comet `CometMapFromArrays` wires the native `map_from_arrays` from `datafusion-spark`, which is null intolerant the same way, so NULL-array inputs return NULL rather than triggering the previously reported native crash ([#3327](https://github.com/apache/datafusion-comet/issues/3327)). When `left` is nullable, the serde still wraps the call in `CASE WHEN left IS NOT NULL THEN map_from_arrays(left, right) END`, for evaluation order rather than the result: `BinaryExpression.eval` never evaluates `right` for a row whose `left` is NULL, and DataFusion evaluates a THEN branch only on the rows its WHEN selected, so under ANSI a failing cast in the values array does not run for such a row. A single `left IS NOT NULL AND right IS NOT NULL` guard would not give that, since DataFusion's `AND` evaluates its right side on the whole batch unless the left side is false on all or most rows. `right` needs no guard: Spark evaluates it whenever `left` is not NULL, and the native function returns a NULL map for a NULL `right`. When `left` is never NULL, the guard selects every row, so the serde emits the call alone.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `nullIntolerant: Boolean`.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- `ArrayBasedMapBuilder` semantics, reproduced natively rather than falling back ([#4680](https://github.com/apache/datafusion-comet/issues/4680)): a `NULL` key raises `NULL_MAP_KEY`, and a duplicate key follows `spark.sql.mapKeyDedupPolicy` (`EXCEPTION` raises `DUPLICATED_MAP_KEY` naming the key, `LAST_WIN` keeps the last value in the slot of the key's first occurrence). Spark checks each row's key and value lengths before inserting any of its entries, then inserts them one at a time, so the builders report whichever of a length mismatch, a `NULL` key and a duplicate key comes first, in the first row that has one. A row whose key and value arrays differ in length raises `SparkError::MapKeyValueDiffSizes`, which `ShimSparkErrorConverter` turns into the `_LEGACY_ERROR_TEMP_2128` Spark raises (`The key array and value array of MapData must have the same length`).
- The duplicate-key policy reaches the native session as `datafusion.spark.map_key_dedup_policy`, which `datafusion-spark`'s map kernels read. `CometExecIterator.serializeCometSQLConfs` reads `spark.sql.mapKeyDedupPolicy` when it builds the native plan for a task, so materializing or explaining a plan does not fix it and a Dataset re-executed after a change to the setting uses the new value. The same applies to `map_from_entries` and `str_to_map`.
- Known limitation ([#6549](https://github.com/apache/datafusion-comet/issues/6549)): the native builder compares a top-level `FLOAT` or `DOUBLE` key by its raw bits, so it keeps `-0.0` and `+0.0` apart, and `NaN`s with different bit patterns apart. On Spark 4.0+, `ArrayBasedMapBuilder` normalizes such a key before comparing it (`keyNormalizer`, added in 4.0 with `spark.sql.legacy.disableMapKeyNormalization`), so `-0.0` and `+0.0` are one key and all `NaN`s are one key. Where Spark raises `DUPLICATED_MAP_KEY`, or keeps one entry under `LAST_WIN`, Comet keeps both entries. When no key repeats, `from` returns the input arrays untouched, so the stored keys match Spark. When a key repeats under `LAST_WIN`, `from` builds the map from the normalized keys, so Spark returns `+0.0` for a `-0.0` key where Comet returns `-0.0`. Spark 3.4 and 3.5 do not normalize a top-level key, so `-0.0` and `+0.0` are two keys in both engines. But the `HashMap` they find duplicates in compares boxed keys with `Double.equals`/`Float.equals`, which treat `NaN`s with different bit patterns as one key, so the `NaN` difference applies to every Spark version. `CometFloatSemanticsSuite` pins each case on each Spark version. `spark.comet.exec.strictFloatingPoint=true` marks the expression `Incompatible` for a floating-point key type, and the projection falls back to Spark (`map_builders_strict_fp.sql`).
- A struct or array key type that contains a `FLOAT` or `DOUBLE` field is `Incompatible` on every Spark version, so the projection falls back to Spark unless `spark.comet.expression.MapFromArrays.allowIncompatible=true`. For such a key type `ArrayBasedMapBuilder` finds duplicates in a `TreeMap` ordered by `TypeUtils.getInterpretedOrdering`, whose `SQLOrderingUtil.compareDoubles` and `compareFloats` treat `-0.0` and `+0.0` as equal and all `NaN`s as equal, while the native builder hashes the nested values by their bits. With `ks = array(named_struct('a', -0.0D), named_struct('a', 0.0D))`, `map_from_arrays(ks, array(1, 2))` raises `DUPLICATED_MAP_KEY` in Spark, or returns one entry under `LAST_WIN`, where the native builder would return two (`map_builders_nested_fp.sql`, `map_builders_nested_fp_last_win.sql`).
- Known limitation: Spark reads `spark.sql.mapKeyDedupPolicy` into `ArrayBasedMapBuilder`, a lazy field of the map expression, so _when_ it reads it depends on how the projection runs. Outside whole-stage codegen (the flag off, or a projection wider than `spark.sql.codegen.maxFields`) the projection is rebuilt in every task and the setting is read again on each action, which is what Comet does. Inside whole-stage codegen Spark creates the builder once on the driver, in the first action, and keeps it, so a Dataset re-executed after a change to the setting still builds its maps under the policy it started with, where Comet uses the new one. Comet cannot tell the two apart: it replaces the operator before `CollapseCodegenStages` runs, so the plan it sees carries no record of which path Spark would have taken. Matching the whole-stage case instead would mean returning a map where Spark raises `DUPLICATED_MAP_KEY` in the other three configurations, so the loud divergence is preferred over the silent one. Only a Dataset that is executed more than once across a change to the setting is affected.
- Known limitation: the NULL guard serializes the keys a second time inside the `map_from_arrays` call, so a nondeterministic keys expression such as `IF(monotonically_increasing_id() % 2 = 0, array(1), NULL)` would advance independently in each copy and the result would drift from Spark ([#5781](https://github.com/apache/datafusion-comet/issues/5781)). `CometMapFromArrays` declines a nondeterministic keys expression as `Unsupported` when it emits the guard, that is when the keys expression is nullable, and the projection falls back to Spark. Keys that are never NULL, such as `array(monotonically_increasing_id())`, get no guard, so they are serialized once and stay native. The values are serialized once and evaluated on the rows Spark evaluates them on, so a nondeterministic values expression stays native.

## map_from_entries

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapFromEntries(child) extends UnaryExpression with NullIntolerant`; expects an array of structs and produces a map. Wired as `CometScalarFunction("map_from_entries")`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; trait refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- The same `ArrayBasedMapBuilder` semantics and policy handling as `map_from_arrays` above, without the length check. A NULL entries array, or one holding a NULL entry, gives a NULL map without inserting any entry, so its keys are not checked.
- Known limitation ([#6549](https://github.com/apache/datafusion-comet/issues/6549)): the native builder compares a top-level `FLOAT` or `DOUBLE` key by its raw bits, as for `map_from_arrays`. On Spark 4.0+, `ArrayBasedMapBuilder` normalizes such a key before comparing it, so `-0.0` and `+0.0` are one key and all `NaN`s are one key. Unlike `map_from_arrays`, this expression always calls `build()`, so Spark also stores the normalized key and returns `+0.0` for a `-0.0` key where Comet returns `-0.0`, even when no key repeats. Spark 3.4 and 3.5 do not normalize a top-level key, so `-0.0` and `+0.0` are two keys in both engines, but `NaN`s with different bit patterns are one key on every Spark version, as for `map_from_arrays`. `spark.comet.exec.strictFloatingPoint=true` marks the expression `Incompatible` for a floating-point key type, and `CodegenDispatchFallback` then runs Spark's own code for it through the JVM codegen dispatcher (`map_builders_strict_fp.sql`).
- A struct or array key type that contains a `FLOAT` or `DOUBLE` field is `Incompatible` on every Spark version, for the reason given under `map_from_arrays`, and `CodegenDispatchFallback` runs Spark's own code for it through the JVM codegen dispatcher unless `spark.comet.expression.MapFromEntries.allowIncompatible=true` (`map_builders_nested_fp.sql`, `map_builders_nested_fp_last_win.sql`).
- Known limitation: input arrays where the struct's key or value type contains `BinaryType` are marked `Incompatible` and fall back unless `spark.comet.expression.MapFromEntries.allowIncompatible=true`.

## map_keys

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapKeys(child)` returns the map's keys as an array. Wired to native `map_keys`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## map_sort

- Performance (tuned locally 2026-09-13; [PR #5901](https://github.com/apache/datafusion-comet/pull/5901), related to [#5900](https://github.com/apache/datafusion-comet/issues/5900)): reuse per-batch prefix-tuple sorting scratch for multi-entry `Utf8`/`Int32` maps and bulk-fill all-empty offsets, preserving the singleton path from [#5887](https://github.com/apache/datafusion-comet/pull/5887). Against upstream including #5887, matched 2–10-entry forward normalization was about 3x faster; 2–50-entry maps improved 28–39% in the full run and 33–39% in independent paired confirmation. Benchmarks: `native/spark-expr/benches/map_sort.rs`, `hash.rs`, and `common/matched_maps.rs`.
- Performance (tuned locally 2026-09-12; [PR #5887](https://github.com/apache/datafusion-comet/pull/5887)): skip Arrow sort dispatch for eligible flat singleton keys, with a batch check and specialized fallback loop for batches without singletons. In the local DataFusion 55.0.0 development cohort, matched singleton normalization measured 19–22x faster in the full run and 18.4x in an independent forward-order confirmation. Benchmarks: `native/spark-expr/benches/map_sort.rs`, `hash.rs`, and `common/matched_maps.rs`; 92 cases cover normalization, hashing, combined execution, nulls, slices, mixed cardinalities, and long Unicode values. Flagged regressions did not remain stable through independent and reversed-order confirmation.

## map_values

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapValues(child)` returns the map's values as an array. Wired to native `map_values`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## str_to_map

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringToMap(text, pairDelim, keyValueDelim) extends TernaryExpression`; splits `text` on `pairDelim`, then each pair on `keyValueDelim` (default `","` and `":"`). Uses `ArrayBasedMapBuilder` for duplicate-key handling. Wired as `CometScalarFunction("str_to_map")`, which follows `spark.sql.mapKeyDedupPolicy` the way `map_from_arrays` does (see there).
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeNonCSAICollation`; uses `CollationAwareUTF8String.splitSQL` with a `collationId`. Runtime unchanged for `UTF8_BINARY`.
- Spark 4.1.1 (audited 2026-05-27): adds the `legacySplitTruncate` flag (driven by `spark.sql.legacy.truncateForEmptyRegexSplit`) to both `splitSQL` calls. The Comet native impl always behaves as if the flag were false, so `CometStrToMap` reads the config by string key and reports `Incompatible` when it is enabled; the `CodegenDispatchFallback` trait then routes the expression through the JVM codegen dispatcher rather than falling the whole projection back to Spark. Non-UTF8_BINARY collations on the input or the delimiters are handled the same way.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
