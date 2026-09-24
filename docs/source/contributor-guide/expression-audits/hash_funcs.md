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

# hash_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## crc32

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Crc32(child) extends UnaryExpression`; `inputTypes = Seq(BinaryType) -> LongType`. Wired as `CometScalarFunction("crc32")`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; `NullIntolerant` trait replaced by `nullIntolerant: Boolean` override.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## hash

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Murmur3Hash(children, seed) extends HashExpression[Int]`; produces a Murmur3 hash with a configurable Int seed and `IntegerType` result. Comet routes via `CometMurmur3Hash` to the native `murmur3_hash` UDF.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; some inner helper refactors only.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Decimals with precision 19–38, including nested decimals, hash natively using the minimal signed big-endian bytes of the unscaled value, matching Spark's `BigInteger.toByteArray()`. Precision ≤18 retains unscaled-long hashing. `TimeType` remains unsupported through the shared `HashUtils`.

## md5

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Md5(child) extends UnaryExpression`; `inputTypes = Seq(BinaryType) -> StringType`. Wired as `CometScalarFunction("md5")`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; trait set gains `DefaultStringProducingExpression` and the `nullIntolerant: Boolean` refactor.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## sha

- Spark 3.4.3 (audited 2026-05-27): registry alias of `Sha1`. Same support as `sha1`.
- Spark 3.5.8 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.0.1 (audited 2026-05-27): identical to 3.4.3.
- Spark 4.1.1 (audited 2026-05-27): identical to 3.4.3.

## sha1

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Sha1(child) extends UnaryExpression with NullIntolerant`; `inputTypes = Seq(BinaryType) -> StringType`. Comet routes via `CometSha1` to the native `sha1` UDF.
- Spark 4.0.1 (audited 2026-05-27): trait set gains `DefaultStringProducingExpression` and `NullIntolerant` is replaced by `nullIntolerant: Boolean`. Runtime unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## sha2

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Sha2(left, right) extends BinaryExpression`; `inputTypes = Seq(BinaryType, IntegerType) -> StringType`. The `numBits` argument selects SHA-224/256/384/512 (0 is treated as 256); other values return NULL. Comet routes via `CometSha2` to the native `sha2` UDF; non-foldable `numBits` falls back to Spark.
- Spark 4.0.1 (audited 2026-05-27): trait set gains `DefaultStringProducingExpression` and the `nullIntolerant: Boolean` refactor. Runtime unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## xxhash64

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `XxHash64(children, seed) extends HashExpression[Long]`; produces an xxHash64 hash with a configurable Long seed and `LongType` result. Comet routes via `CometXxHash64` to the native `xxhash64` UDF.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.
- Native routing: Comet's native `xxhash64` UDF delegates compatible arguments at Spark's default seed (`42`) to `datafusion-spark::SparkXxhash64`. Differential tests in `native/spark-expr/src/hash_funcs/xxhash64_diff.rs` compare Comet's kernel against `SparkXxhash64` for primitives, narrow decimals, dictionaries, lists, maps, and nested combinations. Wide-decimal tests verify that the expression stays on Comet's kernel: the pinned DataFusion implementation uses fixed-width little-endian bytes rather than Spark's minimal signed big-endian bytes. The Comet kernel is retained for wide decimals (also within nested types), a non-default seed (`SparkXxhash64` hardcodes 42), `Struct` (upstream does not push a parent null mask into children; see #5753), a `Dictionary` nested in a list/map (upstream restarts those hashes from 42), and `Time64`. `create_xxhash64_hashes` remains for `approx_count_distinct` and the non-delegated native path.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
