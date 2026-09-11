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
- Known limitation: the native kernel does not hash `DecimalType` children with precision > 18 (Spark hashes them through Java `BigDecimal`), including when nested in array, struct, or map. With the JVM codegen dispatcher enabled (the default), `CodegenDispatchFallback` runs Spark's `HashExpression.doGenCode` inside the Comet pipeline so the enclosing operator stays native. The projection falls back to Spark only when the dispatcher is disabled or refuses the tree. `TimeType` is out of scope for that dispatcher enrollment: `getSupportLevel` reports `Compatible` so the mixin does not intercept it, and `convert` declines the native path so the projection falls back to Spark. The same `HashUtils` type walk applies to `xxhash64`, `sha1`, and `sha2`.

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
- Spark 3.5.8 (audited 2026-05-27): baseline. `Sha1(child) extends UnaryExpression with NullIntolerant`; `inputTypes = Seq(BinaryType) -> StringType`. Comet routes via `CometSha1` to the native `sha1` UDF. Mixes in `CodegenDispatchFallback` for the shared `HashUtils` walk (wide decimal); `TimeType` is excluded from that enrollment. Typical calls coerce to binary before that walk is reached.
- Spark 4.0.1 (audited 2026-05-27): trait set gains `DefaultStringProducingExpression` and `NullIntolerant` is replaced by `nullIntolerant: Boolean`. Runtime unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## sha2

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `Sha2(left, right) extends BinaryExpression`; `inputTypes = Seq(BinaryType, IntegerType) -> StringType`. The `numBits` argument selects SHA-224/256/384/512 (0 is treated as 256); other values return NULL. Comet routes via `CometSha2` to the native `sha2` UDF when `numBits` is foldable. A non-foldable `numBits` has no native path; with the JVM codegen dispatcher enabled (the default), `CodegenDispatchFallback` runs Spark's `Sha2.doGenCode` inside the Comet pipeline. The projection falls back to Spark only when the dispatcher is disabled or refuses the tree.
- Spark 4.0.1 (audited 2026-05-27): trait set gains `DefaultStringProducingExpression` and the `nullIntolerant: Boolean` refactor. Runtime unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

## xxhash64

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `XxHash64(children, seed) extends HashExpression[Long]`; produces an xxHash64 hash with a configurable Long seed and `LongType` result. Comet routes via `CometXxHash64` to the native `xxhash64` UDF. Wide-decimal routing matches `hash` via the shared `HashUtils` and `CodegenDispatchFallback`; `TimeType` is likewise excluded from dispatcher enrollment.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged.
- Spark 4.1.1 (audited 2026-05-27): identical to 4.0.1.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
