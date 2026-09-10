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

- Spark 3.4.3 (audited 2026-09-11): baseline. `MapFromArrays(left, right)` accepts two arrays and returns a `MapType` whose key and value types come from the corresponding array element types. Both interpreted evaluation and generated code copy the arrays and call `ArrayBasedMapBuilder.from`. A NULL input array returns NULL. A NULL key element raises `NULL_MAP_KEY`, unequal array lengths raise an error, duplicate keys raise `DUPLICATED_MAP_KEY` under the default `EXCEPTION` policy, and `LAST_WIN` keeps the last value at the key's first insertion position.
- Spark 3.5.8 (audited 2026-09-11): runtime behavior and generated code are unchanged. Adds `stateful = true` because the expression reuses a mutable `ArrayBasedMapBuilder`.
- Spark 4.0.1 (audited 2026-09-11): replaces the `NullIntolerant` trait with `nullIntolerant = true`. `ArrayBasedMapBuilder` adds floating-point key normalization and collation-aware string-key equality. `spark.sql.legacy.disableMapKeyNormalization=true` restores distinct `-0.0` and `+0.0` keys. `VariantType` is also rejected as a map key. The expression's interpreted and generated paths remain unchanged.
- Spark 4.1.1 (audited 2026-09-11): adds the `MAP_FROM_ARRAYS` tree pattern and changes `spark.sql.mapKeyDedupPolicy` from an internal string config to an enum config. Runtime behavior and generated code are unchanged.
- Comet runs the default `EXCEPTION` configuration natively and guards NULL-array inputs before native map construction ([#3327](https://github.com/apache/datafusion-comet/issues/3327)). Under `LAST_WIN`, `CodegenDispatchFallback` routes `MapFromArrays` through Spark's generated code inside the Comet pipeline by default. Setting `spark.comet.expression.MapFromArrays.allowIncompatible=true` opts into the divergent native path; if the dispatcher is disabled instead, the enclosing operator falls back to Spark.
- Known limitation: the native path still does not reject a NULL element inside the keys array ([#4680](https://github.com/apache/datafusion-comet/issues/4680)). The `LAST_WIN` dispatcher path happens to reject it through Spark's builder, but this does not resolve the default native-path divergence.

## map_from_entries

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapFromEntries(child) extends UnaryExpression with NullIntolerant`; expects an array of structs and produces a map. Wired as `CometScalarFunction("map_from_entries")`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; trait refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
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
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringToMap(text, pairDelim, keyValueDelim) extends TernaryExpression`; splits `text` on `pairDelim`, then each pair on `keyValueDelim` (default `","` and `":"`). Uses `ArrayBasedMapBuilder` for duplicate-key handling. Wired as `CometScalarFunction("str_to_map")`.
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeNonCSAICollation`; uses `CollationAwareUTF8String.splitSQL` with a `collationId`. Runtime unchanged for `UTF8_BINARY`.
- Spark 4.1.1 (audited 2026-05-27): adds the `legacySplitTruncate` flag (driven by `spark.sql.legacy.truncateForEmptyRegexSplit`) to both `splitSQL` calls. The Comet native impl always behaves as if the flag were false, so `CometStrToMap` reads the config by string key and reports `Incompatible` when it is enabled; the `CodegenDispatchFallback` trait then routes the expression through the JVM codegen dispatcher rather than falling the whole projection back to Spark. Non-UTF8_BINARY collations on the input or the delimiters are handled the same way.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
