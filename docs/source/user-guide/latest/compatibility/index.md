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

- **Parquet and Iceberg**: limitations, differences from Spark, and known issues for reading and
  writing Parquet files and Iceberg tables. These live with the data source, on the
  [Parquet](../parquet.md) and [Iceberg](../iceberg.md) pages.
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
(incompatible) path instead. Aggregate functions such as `mode` are the main example, because the
codegen dispatcher covers only scalar expressions; see the
[expression reference](../expressions.md) for which expressions have incompatible cases.

## Strings with non-UTF-8 bytes

Spark's `StringType` can hold arbitrary bytes, including sequences that are not valid UTF-8 (for
example `CAST(X'FF' AS STRING)`). Arrow's string type requires valid UTF-8, so Comet cannot store
the raw bytes natively. When Comet produces a string from arbitrary bytes (such as
`CAST(binary AS string)`, a columnar shuffle, or a nested Parquet BINARY value read as a string),
it decodes them the same way the JVM does
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

Both differences require string data that is not valid UTF-8. Comet's native Parquet scan never
produces such strings from UTF8-annotated columns (it rejects them, see below), and neither do most
string expressions. They can reach Comet in these ways, which are the ones to check before relying on
a result:

- **Parquet BINARY columns read as strings.** A BINARY column without the UTF8 annotation can hold
  bytes that are not valid UTF-8 when it is read as `string`, through the read schema or
  `spark.sql.parquet.binaryAsString=true`. Comet decodes such a value in a struct field, list element
  or map value. Top-level values and map keys are not checked; see
  [Parquet](../parquet.md#reads-that-differ-from-spark).
- **Text files.** Spark's Text reader keeps the bytes of each line as they are, so a Text file in an
  encoding other than UTF-8, such as Latin-1, holds strings that are not valid UTF-8. Comet decodes
  them when it converts a Text scan to Arrow, which it does only when
  `spark.comet.convert.text.enabled=true` (see [Data sources](../datasources.md#text)), or, as a
  deprecated alternative, when `spark.comet.sparkToColumnar.enabled=true` and
  `spark.comet.sparkToColumnar.supportedOperatorList` names the scan. For example, with a Text file
  holding the bytes `1 caf\xE9`, `hex(value)` returns `3120636166EFBFBD` instead of Spark's
  `3120636166E9`, and `length` of `2 na\xEFve` is 7 instead of 5. Spark decodes CSV files as it reads
  them, so they never hold such strings, and its JSON reader treats a line with such bytes as
  malformed, so only the corrupt record column keeps them. Spark's ORC and Avro readers keep stored
  bytes without checking them, but those formats define strings as UTF-8, so only a malformed file is
  affected.
- **Comet columnar shuffle.** When Comet shuffles rows that come from a Spark operator, such as a
  Text scan or a Spark partial aggregate, it converts them to Arrow and decodes their strings in the
  same way. It does this with the default `spark.comet.shuffle.mode=auto`, so, for example,
  repartitioning a Latin-1 Text table can return `U+FFFD` where Spark keeps the original bytes.
  Setting `spark.comet.shuffle.mode=native` keeps such shuffles on Spark.
- **`CAST(binary AS STRING)`.** Comet decodes the bytes where Spark keeps them. Because `min` and `max`
  over strings run natively, a cast below them is decoded before the comparison, and the two can swap.
  With `b` holding `X'80'` and `X'C3A9'`, Spark returns `min(CAST(b AS STRING))` = the `0x80` string
  and `max` = `é`, whereas Comet returns `min` = `é` and `max` = `U+FFFD`. This is the ordering
  difference above. If the cast input can hold bytes that are not valid UTF-8, disable Comet for the
  query. When the cast is the direct argument of `min` or `max`, a narrower alternative is to set
  `spark.comet.expression.Min.enabled=false`, `spark.comet.expression.Max.enabled=false` and
  `spark.comet.shuffle.mode=native`. All three are needed: with only the first two, the aggregate
  runs in Spark, but Comet's columnar shuffle still decodes its partial results. These settings are
  not enough when the cast is in a projection below the aggregate, for example
  `SELECT min(s) FROM (SELECT CAST(b AS STRING) AS s FROM t)`, because Comet runs that projection
  natively and decodes the bytes before the aggregate sees them.

Consistent handling of invalid UTF-8 across all native string paths is tracked by
[#4764](https://github.com/apache/datafusion-comet/issues/4764).

Separately, Comet's native Parquet scan currently rejects string columns whose stored bytes are not
valid UTF-8 rather than reading them like Spark
([#4121](https://github.com/apache/datafusion-comet/issues/4121)).

## ANSI errors from rows skipped by LIMIT

Native operators evaluate batches of rows. With `spark.sql.ansi.enabled=true`, some `LIMIT`
plan shapes can therefore raise an expression error from a row that Spark stops before evaluating.
For example, with a Parquet batch read in the order `(id, value) = (0, '0'), (1, '1')`:

```sql
SELECT value FROM t
WHERE CAST(IF(id = 1, 'bad', value) AS INT) >= 0
LIMIT 1
```

Spark returns `'0'` after the first row matches. Comet can evaluate the filter predicate on the
second row in the same batch and raise `CAST_INVALID_INPUT`. SQL without `ORDER BY` does not
guarantee this input order; the example describes a physical scan order that exposes the difference.

Comet caps projection input when a collect limit directly consumes the projection. This protects
that projection's expressions, but does not bound evaluation inside its child operators. Throwing
filter predicates and projections beneath non-collect `LocalLimit` / `GlobalLimit` plans, such as
a `LIMIT` inside a subquery, remain affected. These existing limitations and the investigation of
already-ordered `TakeOrderedAndProject` inputs are tracked in
[#7147](https://github.com/apache/datafusion-comet/issues/7147).

An error on a row absent from the final result is not always a mismatch: Spark can also need to
evaluate that row, for example to sort the input or process an `OFFSET` prefix.

## JVM expression detour

`spark.comet.exec.jvmDetour.enabled` is an experimental option and is off by default. It applies
when an item of a projection, or a filter condition, contains an expression that Comet cannot
translate. Instead of falling the operator back to Spark, Comet runs that whole item with codegen
dispatch, so the operator stays in the Comet pipeline. The item runs in Spark's own generated code
for it, including the parts Comet could translate. In verbose extended explain, the operator's
`[COMET-INFO: ...]` segment contains
`Expression evaluated in the JVM by the codegen dispatcher (JVM detour)`.

Where neither raises an error, the item's values for a row match Spark's. What can differ is which
rows, and which parts of an item, get evaluated. That matters for an expression that can raise an
error, for example arithmetic under `spark.sql.ansi.enabled=true`, or `raise_error` and
`assert_true` with ANSI mode on or off. For example, with ANSI mode off this query returns no rows
in Spark, whose whole-stage code never evaluates `d` for the rows the filter drops (all of them),
but it can raise with the detour, for the reason in the first item below:

```sql
SELECT d FROM (
  SELECT CASE WHEN x = 0 THEN raise_error('zero') ELSE abs(x) END AS d, rand(1) AS r FROM t
) WHERE r > 2
```

The other examples use ANSI division:

- Comet evaluates the item for every row of the batch. Spark's whole-stage code evaluates a
  deterministic projection item only when an operator above it in the same stage reads it, so it
  skips the item for rows that those operators drop. Examples are a filter above, a join in the
  same stage such as a broadcast hash join, and a `LIMIT`. So
  `SELECT d FROM (SELECT abs(10 div x) AS d, rand(1) AS r FROM t) WHERE r > 2` returns no rows in
  Spark, and `SELECT t.d FROM (SELECT x, abs(10 div x) AS d FROM t) t JOIN u ON t.x = u.x` returns
  rows in Spark when `u` has no row with `x` = 0. With the detour, both can raise
  `DIVIDE_BY_ZERO`. Comet's Rust-implemented projection also evaluates every item for every row,
  so this is not specific to the detour: turning the detour off avoids it only where the
  projection then falls back to Spark.
- Spark eliminates common subexpressions across all the items of a projection and evaluates them
  up front, including ones that sit on the right of an `AND`. Comet eliminates them only within
  one item. So `SELECT x <> 0 AND abs(10 div x) > 1 AS a, x <> 0 AND abs(10 div x) > 2 AS b FROM t`
  with a row where `x` is 0 raises `DIVIDE_BY_ZERO` in Spark, with or without whole-stage codegen,
  and returns rows with the detour, as Comet's Rust-implemented projection does.
- Spark's whole-stage code for a filter splits out each `IS NOT NULL` conjunct whose argument is a
  column, or a null-intolerant expression of columns such as `x + 1`, and evaluates it only just
  before the first other conjunct that reads one of those columns, or at the end if none does.
  Comet evaluates the condition as one expression, conjunct by conjunct from left to right, as
  Spark does for a filter it runs without whole-stage codegen. The difference goes both ways:
  - For `WHERE y IS NOT NULL AND abs(10 div x) > 0` with a row where `x` is 0 and `y` is NULL,
    Spark raises `DIVIDE_BY_ZERO` and Comet returns the other rows. Comet's Rust-implemented
    filter returns the rows too.
  - For `WHERE abs(10 div coalesce(x, 0)) > 0 AND x IS NOT NULL` with a row where `x` is NULL,
    Spark drops the row before dividing and returns the other rows, and Comet raises
    `DIVIDE_BY_ZERO`.

Spark eliminates common subexpressions in a filter condition only when it runs the filter without
whole-stage codegen. It does that when `spark.sql.codegen.wholeStage` is off, when the condition
contains an expression without code generation of its own, such as a higher-order function, or
when the filter has more fields than `spark.sql.codegen.maxFields`, and then only when
`spark.sql.subexpressionElimination.enabled` is on. For a projection, Spark eliminates them when
that setting is on, except with `spark.sql.codegen.factoryMode=NO_CODEGEN` for a projection it runs
without whole-stage codegen (from Spark 3.5 that is every projection), where it evaluates a common
subexpression only when first needed. Comet compiles each detoured filter condition and projection
item with or without subexpression elimination to match the form Spark plans for that operator. If
Spark's whole-stage code falls back at run time, because the generated method is larger than
`spark.sql.codegen.hugeMethodLimit` or fails to compile, Spark runs the operator without
whole-stage codegen after all, and the error behavior can differ.

The detour does not take a nondeterministic filter condition, because that ordering changes which
rows it keeps. For the same reason it does not take a projection item that Spark sees as
deterministic but that contains a nondeterministic expression, as Spark 4's
`make_valid_utf8(cast(rand(1) AS string))` does: Spark evaluates such an item only for the rows an
operator above reads, so the item's random values would differ. It also does not take an item that
contains an expression disabled with `spark.comet.expression.<Name>.enabled=false`, a subquery, an
expression class from outside Spark's Catalyst packages (such as a Hive UDF), or
`reflect` / `java_method`. Those operators fall back to Spark as they do with the option off.

## ANSI-mode error classes and messages

Under `spark.sql.ansi.enabled=true`, several native error paths raise the error at the correct
input but with a different exception type, error class, SQLSTATE, or message text than Spark.
Code that catches `SparkException` and only asserts on message substrings is unaffected; code that
inspects the exception class, `getCondition()`, or the parameterised error class will observe
divergence:

- Spark 4.2 introduced additional ANSI arithmetic overflow behavior differences that Comet does
  not yet track ([#4967](https://github.com/apache/datafusion-comet/issues/4967)).

## Known result-value divergences

The following native paths can return different values or raise errors that Spark does not raise
for edge-case inputs.
Most also have entries in the per-category expression pages linked above; they are collected here
so users hunting an unexpected value have a single place to check. Parquet and Iceberg reads and
writes have their own lists, under Known Issues on the
[Parquet](../parquet.md#known-issues) and [Iceberg](../iceberg.md#known-issues) pages:

- Native `RANGE` window frames with an explicit `PRECEDING` / `FOLLOWING` offset diverge from
  Spark when the boundary arithmetic overflows for `DATE` or `DECIMAL` `ORDER BY` columns
  ([#5022](https://github.com/apache/datafusion-comet/issues/5022)).
- Grouped ANSI decimal `AVG` can evaluate an overflowing group in a batch even when Spark would
  stop before that group at `LIMIT`. It remains natively enabled with this error-timing difference;
  disabling `spark.comet.exec.aggregate.enabled` restores Spark's aggregate evaluation.
  On final result overflow, Comet's error message prints the average rounded to the result scale;
  Spark 4.0 prints the quotient truncated to 39 fractional digits.
- Ungrouped decimal `SUM` keeps an unbounded intermediate and checks the result precision only
  when a partial is written out or the sum is evaluated, which matches Spark's whole-stage codegen
  path. Without codegen Spark buffers the aggregate in an `UnsafeRow` and latches as soon as a
  running sum leaves the precision. Comet falls back at precision 38 when codegen is disabled by
  `spark.sql.codegen.wholeStage`, by `spark.sql.codegen.factoryMode=NO_CODEGEN` on Spark 3.5+,
  by an imperative sibling aggregate, by an expression Spark cannot compile such as a lambda function,
  or by the `spark.sql.codegen.maxFields` limit on the aggregate's own output and inputs, but not
  when Spark abandons codegen at runtime, after a compile failure under
  `spark.sql.codegen.fallback` or when the generated code exceeds
  `spark.sql.codegen.hugeMethodLimit`, where an intermediate overflow that later cancels out
  returns `NULL` (or raises under ANSI) in Spark but the recovered value in Comet.
