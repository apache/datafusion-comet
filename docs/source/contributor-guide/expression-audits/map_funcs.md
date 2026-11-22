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
- Performance (2026-10-20; [#6525](https://github.com/apache/datafusion-comet/issues/6525), [#6845](https://github.com/apache/datafusion-comet/pull/6845)): values are still gathered with one Arrow `take`, after which nested results pass through the compaction shared with `list_extract` (`native/spark-expr/src/nested_compaction.rs`, from [#6498](https://github.com/apache/datafusion-comet/pull/6498)). `take` reserves nested child buffers from the average candidate size, and `MutableArrayData` zero-fills child validity bitmaps for the full reservation, so a selection of short or mostly NULL values can retain capacity sized from unselected values. Compaction shrinks only buffers holding more than four times their used size plus 64 bytes, and copies an oversized validity bitmap down to its used bits. For 8,192 `map<int,array<int>>` rows with 128-element values and 75% NULL maps that keep their entries, the result shrinks from 4,228,100 to 1,079,300 bytes. In a three-round alternating local release Criterion run against main `3355602e2` (Arrow 59.3.0, DataFusion 55.2.0), comparing the median of each arm's three round medians, unchanged flat-string controls moved within ±1.3%. The 70 nested `array<string>` and `array<boolean>` leaf cases ranged from −2.7% to +5.3% of main, with 64 within ±3%. The shapes from review (one-element values, scalar key, 75% NULL maps with empty entry ranges) measured −0.5% for Boolean and +0.1% for string. When compaction does fire, it costs CPU: 14–23% on the 128-element, 75%-NULL-map shapes that release about 3 MB per batch, and 8–10% on deeply nested child-null shapes. This covers retained output buffers and expression runtime, not peak RSS. Benchmark: `native/spark-expr/benches/map_extract.rs` (`map_extract_nested` and `map_extract_nested_leaf`).

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
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapFromArrays(left, right) extends BinaryExpression with NullIntolerant`; Spark uses `ArrayBasedMapBuilder` to detect duplicate keys (subject to `spark.sql.mapKeyDedupPolicy`) and rejects null keys with `RuntimeException("Cannot use null as map key")`. Comet `CometMapFromArrays` emits nested `CaseWhen(IsNotNull(left), CaseWhen(IsNotNull(right), map(left, right), null), null)` guards so NULL-array inputs return NULL rather than triggering the previously reported native crash ([#3327](https://github.com/apache/datafusion-comet/issues/3327)). The outer key guard preserves Spark's left-before-right evaluation: one `IsNotNull(left) AND IsNotNull(right)` predicate can still evaluate the values child for a NULL-key row in a mixed batch. The native `SparkMapFromArrays` wrapper also validates key/value lengths per non-null row before DataFusion constructs the map, so matching flattened totals cannot pair values across row boundaries.
- Spark 4.0.1 (audited 2026-05-27): `NullIntolerant` trait replaced by `nullIntolerant: Boolean`. `ArrayBasedMapBuilder` gains the `keyNormalizer` described below.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- For `FLOAT` and `DOUBLE` keys, both builders use `SparkFloatMap` and follow Spark's [key equality rules](../floating_point.md#sparks-rules). Under `LAST_WIN` the first occurrence determines the position and the last supplies the value. [`ArrayBasedMapBuilder.from`](https://github.com/apache/spark/blob/v4.1.3/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/ArrayBasedMapBuilder.scala#L139-L150) returns the input arrays when all keys are distinct. Comet therefore preserves the original key bits for these rows, even with normalization enabled. When a duplicate forces a rebuild, normalization applies to every stored key in that row. Keys with a floating-point field inside a struct or array use codegen dispatch ([#6549](https://github.com/apache/datafusion-comet/issues/6549)).
- Both floating-point builders use codegen dispatch for complex input expressions by default. This lets Spark skip the values expression when the keys array is null and preserves the evaluation count of stateful inputs to `arrays_zip`. It also lets Spark discard a duplicate before writing a large value to Arrow. Materialized array columns can carry variable-width values through either Rust builder.
- Spark reads `spark.sql.mapKeyDedupPolicy` when it creates the builder. In a whole-stage projection that happens on the driver as it builds the RDD, so actions that reuse the RDD retain the policy. A projection outside whole-stage codegen creates the builder in the task. Comet carries the driver policy with the RDD and selects the policy per expression. The normalizer is initialized on the first insertion, so Comet reads `spark.sql.legacy.disableMapKeyNormalization` in the task for both cases.
- Spark compares the largest compiled method in each whole stage with `spark.sql.codegen.hugeMethodLimit` when building the RDD. Comet uses Spark's columnar transitions and stage boundaries from the plan before scan conversion. If the limit is below its default at RDD construction, Comet generates and compiles the affected stages on the driver to make the same comparison. This check is per stage, including when Comet fuses several stages into one block. It requires no compilation at the default limit. An unexpected Spark compilation failure at the default limit can still change the policy that Spark observes if the driver and task have different settings.
- Spark forms whole stages in an adaptive plan after Comet's initial conversion. Builders in that pass keep their existing JVM dispatch or Spark fallback, as do stages with already converted operators or materialized adaptive inputs. A projection that Spark plans directly can use Rust with adaptive execution enabled in the session.
- An untracked `Dataset.rdd` action temporarily installs the session's configuration as it constructs the final stage's iterator. A lazy projection inserts its first key after that scope ends. If its factory changes to `NO_CODEGEN` after planning, the interpreted expression also creates its map builder at that point. If a generated sort or hash aggregate falls back, it consumes its input while the session's configuration is active, so Comet uses that configuration to normalize keys below it. Comet handles projections, filters, generated hash aggregate inputs and expands with known consumers. Other contexts, including stages whose code cannot be generated in isolation and interpreted execution, use the existing dispatch path by default.

## map_from_entries

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `MapFromEntries(child) extends UnaryExpression with NullIntolerant`; expects an array of structs and produces a map. Wired as `CometScalarFunction("map_from_entries")`.
- Spark 4.0.1 (audited 2026-05-27): `ArrayBasedMapBuilder` normalizes `FLOAT` and `DOUBLE` keys by default starting in Spark 4.0.0.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Known limitation: input arrays where the struct's key or value type contains `BinaryType` are marked `Incompatible` and fall back unless `spark.comet.expression.MapFromEntries.allowIncompatible=true`.
- Floating-point keys follow the same equality rules and policies as `map_from_arrays`. `map_from_entries` always calls `ArrayBasedMapBuilder.build()`, so it stores normalized keys whenever normalization is enabled, even in a singleton map.

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
- Spark 3.5.8 (audited 2026-05-27): baseline. `StringToMap(text, pairDelim, keyValueDelim) extends TernaryExpression`; splits `text` on `pairDelim`, then each pair on `keyValueDelim` (default `","` and `":"`). Uses `ArrayBasedMapBuilder` for duplicate-key handling. Wired as `CometScalarFunction("str_to_map")`.
- Spark 4.0.1 (audited 2026-05-27): `inputTypes` widened to `StringTypeNonCSAICollation`; uses `CollationAwareUTF8String.splitSQL` with a `collationId`. Runtime unchanged for `UTF8_BINARY`.
- Spark 4.1.1 (audited 2026-05-27): adds the `legacySplitTruncate` flag (driven by `spark.sql.legacy.truncateForEmptyRegexSplit`) to both `splitSQL` calls. The Comet native impl always behaves as if the flag were false, so `CometStrToMap` reads the config by string key and reports `Incompatible` when it is enabled; the `CodegenDispatchFallback` trait then routes the expression through the JVM codegen dispatcher rather than falling the whole projection back to Spark. Non-UTF8_BINARY collations on the input or the delimiters are handled the same way.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
