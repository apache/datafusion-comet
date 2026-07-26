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

# Compatibility Guide

Comet aims to provide consistent results with the version of Apache Spark that is being used.

This guide documents areas where Comet's behavior is known to differ from Spark. Topics are grouped by subsystem:

- **Parquet**: limitations when reading Parquet files.
- **Floating-point comparison**: NaN and signed-zero handling in comparisons.
- **Regular expressions**: differences between the Rust regexp crate and Java's regex engine.
- **Operators**: operator-level compatibility notes, including window functions and round-robin partitioning.
- **Expressions**: per-expression compatibility notes, including cast.
- **JSON**: choosing between the native and Spark-compatible engines for JSON expressions.
- **Spark versions**: version-specific known issues and limitations.

## Compatible by default, opt in to native

Comet runs a Spark-compatible implementation of every supported expression by default. Some
expressions also have a native implementation that can differ from Spark for certain
inputs. These are not used unless you opt in by setting the relevant
`spark.comet.expression.<Name>.allowIncompatible=true` config (a few use a dedicated config, noted
per expression below), after which you accept the documented differences.

You can discover where a native opt-in is available for a specific query in the verbose extended
explain output. A `[COMET-INFO: ...]` segment points at an available native path and does not mean
the operator falls back to Spark. This is distinct from `[COMET: ...]`, which records a reason an
operator did fall back.

## Native and codegen-dispatch implementations

Some Spark expressions have two implementations in Comet:

- A **codegen-dispatch** implementation that runs Spark's own generated code for the
  expression inside Comet's native pipeline (via the Arrow-direct codegen dispatcher). This
  produces byte-exact Spark results at the cost of one JNI round-trip per batch. It is gated
  globally by `spark.comet.exec.scalaUDF.codegen.enabled` (enabled by default); when the
  dispatcher is disabled, these expressions fall back to Spark.
- A **native** (Rust / DataFusion) implementation that avoids the JNI round-trip but has
  known semantic differences from Spark for some inputs or patterns.

Because the codegen-dispatch path matches Spark exactly, Comet uses it by **default**. The
native path is **opt-in per expression** via that expression's
`spark.comet.expression.<ExprClassName>.allowIncompatible=true` flag, which declares that you
accept its differences from Spark. There is no global opt-in. When the native path is enabled
but a specific input or pattern has no native implementation, Comet routes that case back
through the codegen dispatcher rather than running something incompatible.

This is the model behind the [regular expression](regex.md) and [JSON](json.md) families,
which document their per-expression configs and the specific differences to expect.

This is distinct from expressions that have **no** codegen-dispatch path: there, the
incompatible cases fall back to Spark by default, and `allowIncompatible=true` runs the native
(incompatible) path instead. `cast` is the main example; see the
[expression reference](../expressions.md) for which expressions have incompatible cases.

## Strings with non-UTF-8 bytes

Spark's `StringType` can hold arbitrary bytes, including sequences that are not valid UTF-8 (for
example `CAST(X'FF' AS STRING)`). Arrow's string type requires valid UTF-8, so Comet cannot store
the raw bytes natively. When Comet produces a string from arbitrary bytes (such as
`CAST(binary AS string)` or a columnar shuffle), it decodes them the same way the JVM does
(`new String(bytes, UTF_8)`), replacing each ill-formed sequence with the Unicode replacement
character `U+FFFD`. Spark itself applies the identical replacement whenever such a string is
materialized (collected, printed, or passed to most string functions), so the rendered result
matches Spark.

Decoding is not byte-preserving, so results can differ from Spark for any operation that works on the
underlying bytes rather than on the rendered text:

- **Round-trips.** Spark keeps the original bytes, so `CAST(CAST(X'FF' AS STRING) AS BINARY)` returns
  `X'FF'`, whereas Comet returns the UTF-8 encoding of `U+FFFD` (`X'EFBFBD'`). `octet_length` and
  hashing of such a string differ for the same reason.
- **Value identity.** Decoding maps every ill-formed sequence onto the same `U+FFFD`, so two Spark
  strings that hold different bytes can become equal in Comet. For example, with `b = X'FF'`,
  `CAST(b AS STRING) = CAST(X'EFBFBD' AS STRING)` is `false` in Spark (`UTF8String` compares the raw
  bytes) but `true` in Comet. Equality, joins, grouping, ordering, and byte-based string functions
  such as `contains` can therefore disagree with Spark when non-UTF-8 bytes are involved.

Both differences require string data that is not valid UTF-8, which does not occur for text read from
Parquet or produced by string expressions. Consistent handling of invalid UTF-8 across all native
string paths is tracked by
[#4764](https://github.com/apache/datafusion-comet/issues/4764).

Separately, Comet's native Parquet scan currently rejects string columns whose stored bytes are not
valid UTF-8 rather than reading them like Spark
([#4121](https://github.com/apache/datafusion-comet/issues/4121)).
