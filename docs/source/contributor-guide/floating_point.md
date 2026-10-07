<!--
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

# Floating-Point Semantics

This page describes how Comet matches Spark's handling of `-0.0` and NaN in `FLOAT` and `DOUBLE`
values. It is aimed at contributors working on an expression or operator that compares, orders,
hashes, deduplicates or groups floating-point values, including values nested in arrays, structs
and maps. For user-facing differences from Spark, see
[Floating-point Number Comparison](../user-guide/latest/compatibility/floating-point.md). The work
to apply these rules systematically is tracked in
[#6385](https://github.com/apache/datafusion-comet/issues/6385).

The short version: Spark has no single rule for floating-point equality. Each function inherits one
from the Java API that its implementation calls. Arrow and DataFusion follow IEEE 754 total order,
which matches none of them. A native implementation must follow the rule of the Spark function it
replaces, using the shared helpers in `native/spark-expr/src/float_semantics/`, and its tests must
include a NaN with the sign bit set.

## Spark's rules

| Rule                                                        | `-0.0` and `0.0`          | NaN                                                        | Used by                                                                                                                                                                                                   |
| ----------------------------------------------------------- | ------------------------- | ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SQL ordering (`SQLOrderingUtil.compareDoubles`, `genEqual`) | Equal                     | All NaNs are equal, and NaN sorts above all values         | Comparisons, `IN`, sorting and ranking, `min`/`max`, `greatest`/`least`, `sort_array`, `array_min`/`array_max`, `array_contains`, `array_position`, `array_remove`, and comparisons of arrays and structs |
| `NormalizeNaNAndZero`, inserted by Spark's optimizer        | `-0.0` becomes `0.0`      | Every NaN becomes the canonical NaN                        | Grouping keys, join keys and window partition keys                                                                                                                                                        |
| `Murmur3Hash` and `XxHash64`                                | Same hash                 | Hashed through `doubleToLongBits`, which canonicalizes NaN | `hash`, `xxhash64` and hash partitioning                                                                                                                                                                  |
| `java.lang.Double.equals`, for boxed values                 | Distinct                  | All NaNs are equal                                         | Java hash sets and maps of boxed values, and Spark's `OpenHashSet` and `OpenHashMap` from Spark 3.5.2 and 4.0.0                                                                                           |
| Scala `==`, for boxed values (`BoxesRunTime.equals`)        | Equal                     | A NaN equals nothing, not even another NaN                 | Scala sets and maps of `Any`, such as `collect_set`'s buffer before Spark 4.2                                                                                                                             |
| `java.lang.Double.compare`                                  | `-0.0` sorts before `0.0` | As in SQL ordering                                         | An ascending `sort_array` of elements that cannot be null, in Spark's generated code                                                                                                                      |

Some functions changed rules in a Spark release, so a native path has to follow the Spark version
it runs against:

- `collect_set` keys its buffer with Scala's `==` before Spark 4.2, so `-0.0` and `0.0` are one
  value and every NaN is a value of its own. Spark 4.2 normalizes NaN and `-0.0` first and keys the
  buffer by the normalized bits, so all NaNs are one value too (SPARK-57298).
- Spark's `OpenHashSet`, and the `OpenHashMap` built on it, follow `Double.equals` only from Spark
  3.5.2 and 4.0.0 (SPARK-45599). In every 3.4 release and in 3.5.0 and 3.5.1 they match a key with
  `==` but hash its bits (`doubleToLongBits`), so two NaNs never match, and `-0.0` and `0.0` match
  only when probing from one reaches the other. `mode` and `percentile` count values in an
  `OpenHashMap`. `array_distinct`, `array_union`, `array_intersect` and `array_except` look up the
  elements of a flat array in an `OpenHashSet`, but treat all NaNs as one value.
- `mode` follows `Double.equals` from Spark 3.5.2 and 4.0.0 until Spark 4.2, which folds `-0.0`
  into `0.0` first (SPARK-57329).
- `array_distinct` and `array_union` keep signed zeros apart in a flat array before Spark 4.2.0
  (before 3.5.2, unless probing merges them). Spark 4.2.0 normalizes their arguments in the plan
  (SPARK-54918). From 4.0.5, 4.1.4 and 4.2.1 they normalize while they evaluate instead
  (SPARK-59602).
- Map construction (`ArrayBasedMapBuilder`) finds duplicate keys by `Double.equals` in Spark 3.4
  and 3.5. From Spark 4.0 it normalizes each key first, unless
  `spark.sql.legacy.disableMapKeyNormalization` is set
  ([#6549](https://github.com/apache/datafusion-comet/issues/6549)).

## How Arrow and DataFusion differ

Arrow compares floats by IEEE 754 total order. `-0.0` sorts below `0.0`, NaNs compare by their bit
patterns, and a NaN with the sign bit set sorts below `-Infinity`. Arrow's sort, row format and
hash kernels all work from the same bits. DataFusion 55 folds `-0.0` into `0.0` in some kernels but
does not canonicalize NaN, and that has changed between DataFusion releases. Don't rely on it:
normalize the values, or compare them with the helpers below.

NaNs with the sign bit set are the normal case on x86-64, not a corner case. Every NaN that
arithmetic produces at run time, such as `sqrt(-1)` or `Infinity - Infinity`, is
`0xfff8000000000000` in both Rust and the JVM on x86-64, while aarch64 produces
`0x7ff8000000000000`. Spark hides the difference through `doubleToLongBits`. A native path that
compares raw values puts those NaNs below every other value, so a query that passes on an Apple
Silicon laptop can fail on a Linux x86 cluster.

## Where Comet applies the rules

### The `float_semantics` module

`native/spark-expr/src/float_semantics/` holds the per-value rules and the kernels built on them.
Its module documentation is the reference. In short:

| Helper                                                           | Rule                         | Use it to                                                                                                   |
| ---------------------------------------------------------------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `compare_floats`, `float_lt`, `float_gt`                         | SQL ordering                 | Compare values in a kernel that returns the original bits, as `array_min` and `greatest` do                 |
| `spark_comparator`, `spark_equality`                             | SQL ordering, at any depth   | Compare arrays and structs in place                                                                         |
| `normalize_float`, `normalize_floats`, `normalize_nested_floats` | `NormalizeNaNAndZero`        | Normalize values before an Arrow kernel sorts, row-encodes, hashes or compares them                         |
| `NormalizeNaNAndZero`, `NormalizeNestedFloats`                   | `NormalizeNaNAndZero`        | Wrap a key or operand expression. `wrap_if_needed` skips other types and keys that Spark already normalized |
| `canonicalize_nan`                                               | `java.lang.Double.equals`    | Key a hash set or map the way boxed Java values do                                                          |
| `compare_floats_java`                                            | `java.lang.Double.compare`   | Sort the way `java.util.Arrays.sort` sorts a primitive array                                                |
| `hash_input`                                                     | `Murmur3Hash` and `XxHash64` | Get the value to hash in place of a float                                                                   |

Once `-0.0` is folded and every NaN is canonical, Arrow's total order agrees with Spark's SQL
ordering. That is why normalizing the inputs of an Arrow kernel works, as long as only the keys are
normalized and the output keeps the original values.

### Keys, sorting and comparisons

- Spark's optimizer wraps grouping, join and window partition keys in `NormalizeNaNAndZero`
  (`NormalizeFloatingNumbers`). Comet serializes it like any other expression.
- `create_normalized_key_expr` in `native/core/src/execution/planner.rs` normalizes sort keys,
  window order and partition keys, and the partition keys of `WindowGroupLimit`, so that Arrow's
  sort orders them the Spark way.
- `spark_comparison` in `native/spark-expr/src/array_funcs/nested_comparison.rs` builds every native
  `=`, `<>`, `<=>`, `<`, `<=`, `>`, `>=` and `IS DISTINCT FROM`, whichever operator evaluates it. It
  normalizes float operands, and folds a literal while the plan is built so that it stays a
  literal. Nested `=` and `<>` compare in place with `spark_equality`.
- A scan's pushed-down data filters leave a float column compared with a literal other than NaN
  unwrapped (`FloatOperands::Raw`) so that Parquet pruning still recognizes it, but only while the
  reader prunes with them without filtering rows. A bloom filter probe hashes the literal's bits, so
  `=` against either zero becomes `= -0.0 OR = 0.0`. With
  `spark.comet.parquet.rowFilterPushdown.enabled=true` they normalize both sides, because the
  reader drops the rows a filter rejects, and a raw column would reject a stored NaN that Spark
  matches ([#6702](https://github.com/apache/datafusion-comet/issues/6702)). Spark's own reader
  keeps the two zeros apart in its dictionary and bloom filters, so it can skip a row group that
  Comet reads. A test of this pruning writes out the expected rows instead of comparing them with
  Spark's.
- `IN` normalizes its operands in the serde (`normalizeInOperand` in `predicates.scala`).
- `hash`, `xxhash64`, the native shuffle's hash partitioner and `approx_count_distinct` hash floats
  through `hash_input`.

### Spark version differences

Rules that depend on the Spark version are decided in Scala. The serde either chooses the native
path or passes a flag to native code, as `Mode`'s `normalize_neg_zero` does. Where a patch release
changed the rule, check the runtime version down to the patch with
`Utils.majorMinorPatchVersion(SPARK_VERSION)`, as `ArraySetSupport` in `arrays.scala` does.

Until an expression has a native implementation that follows its rule, report it `Incompatible`
for floating-point input so that Spark evaluates it.

`spark.comet.exec.strictFloatingPoint=true` makes Comet fall back for floating-point operations
that can still differ from Spark. To gate such an operation, use
`SupportLevel.strictFloatingPointReason(dataType, what)`. It returns a reason only in strict mode,
and only for a type that contains a `FLOAT` or `DOUBLE`.

## Choosing the rule for an expression

1. Read Spark's implementation, both `eval` and `doGenCode`, in every Spark version that Comet
   supports, at the latest patch release of each. What it calls decides the rule. `ordering.compare`,
   `ordering.equiv`, `ctx.genComp` and `ctx.genEqual` mean SQL ordering. A `java.util.HashMap` or
   another Java collection of boxed values means `Double.equals`, and so does an `OpenHashSet` or
   `OpenHashMap` from Spark 3.5.2 and 4.0.0. A Scala collection of `Any`, such as
   `mutable.HashSet[Any]`, means Scala's `==`. `java.util.Arrays.sort` on a primitive array means
   `Double.compare`.
2. Check whether Spark's optimizer normalizes the input in the plan. If it does, Comet receives
   normalized values on that version only.
3. Apply the rule at every depth. A float inside an array, struct or map follows the same rule, and
   some DataFusion kernels fold `-0.0` only in a flat array.
4. Record what you found in the expression's [audit](expression-audits/index.md) and, for a
   difference users can see, in the compatibility guide.

## Guidelines

- Don't compare floating-point values with Arrow's comparison, sort or hash kernels, or with
  `total_cmp` or `partial_cmp`, where Spark uses SQL ordering, unless you normalize them first.
- Normalize keys and operands, not results. Spark returns the original bits: `greatest(-0.0D, 0.0D)`
  returns `-0.0`, `max` returns whichever zero it reads first, and `array_min` returns the first of
  equal elements.
- Don't write a local copy of a rule. If a helper is missing, add it to `float_semantics`.
- In a hot loop, use the `float_lt` and `float_gt` predicates. Comparing `compare_floats(a, b)` with
  an ordering known only at run time was several times slower in a scan for a minimum.
- When equality on nested values can stop early, keep that exit. `spark_equality` rejects lists of
  different lengths without comparing their elements, which an equality built on an ordering
  comparator loses.

## Testing

- Use the edge values `0.0`, `-0.0`, a canonical NaN, a NaN with the sign bit set, `1.0`, `-1.0`,
  `Infinity`, `-Infinity` and `NULL`, for both `FLOAT` and `DOUBLE`.
- Make the sign-bit NaN at query time. Spark's Parquet writer canonicalizes NaN, so a stored NaN
  reads back canonical. Negate it in the query (`-d`), which flips the sign bit on every platform.
  Arithmetic such as `sqrt(-1)` yields a sign-bit NaN only on x86-64, so a test that relies on it
  passes on Apple Silicon without exercising anything. The writer keeps the sign of zero, so `-0.0`
  can be stored directly.
- Read the values from a Parquet table, and also compare them with literals on both sides. The
  [Comet SQL Tests](sql-file-tests.md) turn constant folding off, so `-0.0D` stays a literal with the
  sign bit set, while `double('NaN')` is a cast.
- Cover every place the rule applies. For a comparison, that is `Project`, `Filter`, aggregate
  arguments, `FILTER` clauses, join conditions, sort keys, generators and windows.
- Nest the values in arrays and structs, two levels deep, and make the nested NaN a sign-bit one.
- When arithmetic produces the NaN, run with ANSI mode both on and off
  (`-- ConfigMatrix: spark.sql.ansi.enabled=false,true`), because division and remainder take
  different native paths.
- Where the rule changed in a Spark release, gate the fixtures with `MinSparkVersion` or
  `MaxSparkVersion`, and add the `run-all-spark-profiles` label to the pull request. CI pins one
  patch release per Spark line, so when a patch release changed the rule, also test the routing in
  Scala.
- Add the expression to `CometFloatSemanticsSuite`, which crosses the edge values with operator
  contexts and with every expression and aggregate that accepts floating-point input. New
  expressions go in its `expressions` or `aggregates` list. If one does not match Spark yet, add a
  `KnownGap` that links the issue. Known gaps are strict: a case that starts matching Spark fails,
  so the fix also removes the entry.

Fixtures to start from include `expressions/conditional/float_comparisons.sql`,
`expressions/math/nan_divisor.sql`, `expressions/math/greatest_least_floating_point.sql`,
`expressions/aggregate/min_max_floating_point.sql`, `expressions/array/sort_array_floating_point.sql`
and `windows/nested_float_order_keys.sql`, all under `spark/src/test/resources/sql-tests/`.
