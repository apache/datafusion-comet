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
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapFromArrays(left, right) extends BinaryExpression with NullIntolerant`; Spark uses `ArrayBasedMapBuilder` to detect duplicate keys (subject to `spark.sql.mapKeyDedupPolicy`) and rejects null keys with `RuntimeException("Cannot use null as map key")`. Comet `CometMapFromArrays` wires the native `map_from_arrays` from `datafusion-spark`, which is null intolerant the same way, so NULL-array inputs return NULL rather than triggering the previously reported native crash ([#3327](https://github.com/apache/datafusion-comet/issues/3327)).
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `nullIntolerant: Boolean`.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- `ArrayBasedMapBuilder` semantics, reproduced natively rather than falling back ([#4680](https://github.com/apache/datafusion-comet/issues/4680)): a `NULL` key element raises `NULL_MAP_KEY`, ahead of any duplicate-key check, matching the order Spark applies them in; a duplicate key follows `spark.sql.mapKeyDedupPolicy`, which `CometExecIterator` forwards to the native session as `datafusion.spark.map_key_dedup_policy` (`EXCEPTION` raises `DUPLICATED_MAP_KEY` naming the key, `LAST_WIN` keeps the last value for the key).
- Known limitation: `ArrayBasedMapBuilder` normalizes a floating-point key before storing it (`-0.0` becomes `+0.0`, every `NaN` collapses to one), while the native builder compares the raw Arrow values, so a map built from both `-0.0` and `+0.0` keeps two entries where Spark reports a duplicate key. Gated only under `spark.comet.exec.strictFloatingPoint`, which marks the expression `Incompatible` for a floating-point key type.
- Spark raises `MAP_KEY_VALUE_DIFF_SIZES` when a row's key and value arrays differ in length; the native path raises the same error.

## map_from_entries

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapFromEntries(child) extends UnaryExpression with NullIntolerant`; expects an array of structs and produces a map. Wired as `CometScalarFunction("map_from_entries")`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; trait refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- `ArrayBasedMapBuilder` semantics, reproduced natively rather than falling back ([#4680](https://github.com/apache/datafusion-comet/issues/4680)): a `NULL` key element raises `NULL_MAP_KEY`, ahead of any duplicate-key check, matching the order Spark applies them in; a duplicate key follows `spark.sql.mapKeyDedupPolicy`, which `CometExecIterator` forwards to the native session as `datafusion.spark.map_key_dedup_policy` (`EXCEPTION` raises `DUPLICATED_MAP_KEY` naming the key, `LAST_WIN` keeps the last value for the key).
- Known limitation: `ArrayBasedMapBuilder` normalizes a floating-point key before storing it (`-0.0` becomes `+0.0`, every `NaN` collapses to one), while the native builder compares the raw Arrow values, so a map built from both `-0.0` and `+0.0` keeps two entries where Spark reports a duplicate key. Gated only under `spark.comet.exec.strictFloatingPoint`, which marks the expression `Incompatible` for a floating-point key type.
- Known limitation: input arrays where the struct's key or value type contains `BinaryType` are marked `Incompatible` and fall back unless `spark.comet.expression.MapFromEntries.allowIncompatible=true`.

## map_keys

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapKeys(child)` returns the map's keys as an array. Wired to native `map_keys`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## map_values

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapValues(child)` returns the map's values as an array. Wired to native `map_values`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## str_to_map

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringToMap(text, pairDelim, keyValueDelim) extends TernaryExpression`; splits `text` on `pairDelim`, then each pair on `keyValueDelim` (default `","` and `":"`). Uses `ArrayBasedMapBuilder` for duplicate-key handling. Wired as `CometScalarFunction("str_to_map")`. The native `str_to_map` reads the duplicate-key policy from `datafusion.spark.map_key_dedup_policy`, which `CometExecIterator` forwards from `spark.sql.mapKeyDedupPolicy`.
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeNonCSAICollation`; uses `CollationAwareUTF8String.splitSQL` with a `collationId`. Runtime unchanged for `UTF8_BINARY`.
- Spark 4.1.1 (audited 2026-05-27): adds the `legacySplitTruncate` flag (driven by `spark.sql.legacy.truncateForEmptyRegexSplit`) to both `splitSQL` calls. The Comet native impl always behaves as if the flag were false, so `CometStrToMap` reads the config by string key and reports `Incompatible` when it is enabled; the `CodegenDispatchFallback` trait then routes the expression through the JVM codegen dispatcher rather than falling the whole projection back to Spark. Non-UTF8_BINARY collations on the input or the delimiters are handled the same way.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
