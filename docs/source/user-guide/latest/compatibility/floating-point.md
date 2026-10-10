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

# Floating-point Number Comparison

Spark normalizes NaN and zero for floating point numbers for several cases. See `NormalizeFloatingNumbers` optimization rule in Spark.
However, one exception is comparison. Spark does not normalize NaN and zero when comparing values
because they are handled well in Spark (e.g., `SQLOrderingUtil.compareFloats`). But the comparison
functions of arrow-rs used by DataFusion do not normalize NaN and zero (e.g., [arrow::compute::kernels::cmp::eq](https://docs.rs/arrow/latest/arrow/compute/kernels/cmp/fn.eq.html#)).
For `FLOAT` and `DOUBLE` comparisons (`=`, `<>`, `<=>`, `<`, `<=`, `>` and `>=`), Comet
compares the operands in Spark's order natively, including noncanonical NaNs: `-0.0` equals
`0.0`, all NaNs are equal, and NaN sorts above every other value. This applies wherever a
comparison appears: projections, filters, aggregate arguments and `FILTER` clauses, join
conditions, sort keys, and generator arguments.

A native Parquet scan prunes row groups and pages with its data filters. So that this pruning
still applies, a `FLOAT` or `DOUBLE` column compared with a constant other than NaN in a data
filter is compared without normalizing the column, and the filter above the scan evaluates the
comparison again with Spark's semantics. Every other comparison in a data filter follows Spark's semantics.
Bloom filters store `-0.0` and `0.0` separately, so `=` against either zero checks them for both.
With `spark.comet.parquet.rowFilterPushdown.enabled=true` the scan also drops the rows its data
filters reject, so every comparison in a data filter follows Spark's semantics, and a `FLOAT` or `DOUBLE`
comparison in a data filter does not prune row groups or pages
([#6702](https://github.com/apache/datafusion-comet/issues/6702)).

Spark's own Parquet reader keeps `-0.0` and `0.0` apart when it prunes row groups with dictionaries
and bloom filters, so it can skip a row group holding rows that its filter matches. For example,
`WHERE d = -0.0D` can skip a row group that holds `0.0` but not `-0.0`, and so can `<=>` or `IN`
against `-0.0D`. `WHERE d >= 0.0D` can skip a dictionary-encoded row group whose largest value is
`-0.0`, and `WHERE d <= -0.0D` one whose smallest value is `0.0`. Comet reads those row groups, so
it can return rows that Spark does not. Comet's result is the one that Spark's comparison semantics
call for, and Spark returns the same rows with `spark.sql.parquet.filterPushdown=false`.

Top-level `IN`, `InSet`, and `NOT IN` membership also normalize dynamic candidates and lists
containing NaN. When every candidate is a non-NaN literal, Comet keeps DataFusion's static filter
and pruning path, enumerating both signed-zero forms when a list contains zero.

This scalar membership handling does not yet recurse into floating-point leaves nested in arrays
or structs; see [#6019](https://github.com/apache/datafusion-comet/issues/6019).

## Nested comparisons and membership

For arrays and structs containing `FLOAT` or `DOUBLE`, native `=`, `<>`, `IN`, and `NOT IN`
compare signed zeros as equal and all NaN representations as equal, matching Spark. This also
covers single-candidate membership that Spark rewrites into equality. `<=>`, `<`, `<=`, `>`, and
`>=` follow Spark's order too, in which NaN sorts above every other value.

Comparisons and dynamic membership compare nested elements directly and stop at the first
mismatch.
Constant membership sets use normalized comparison values for static lookup. These operations
preserve SQL null semantics and do not change the values returned by projections.

## Ordering: NaN and signed zero (`-0.0` vs `+0.0`)

Spark's `ORDER BY`, `RANK`, `DENSE_RANK`, and window frame comparisons route through
`SQLOrderingUtil.compareDoubles` / `compareFloats`, which equate all NaN representations and
define `-0.0 == 0.0`. NaN sorts above every non-NaN value.

For `FLOAT` and `DOUBLE` keys, and for keys that nest them in arrays and structs at any depth,
Comet normalizes NaNs and signed zeros before native sorting, window peer comparisons, and
`WindowGroupLimitExec` rank comparisons. Native range partitioning normalizes its keys and
sampled boundaries in the same way; it only accepts scalar keys. Only comparison keys are
normalized; returned values retain their original NaN representations and zero signs.

Because those comparison keys match Spark, `spark.comet.exec.strictFloatingPoint=true` does not
force a fallback for them: sort keys, window and rank order keys, and range partitioning keys all
stay native under strict mode, whether the floats in them are scalar or nested.

That includes a key that nests floats in an array or struct whose type can hold a null element or
field. Spark orders such a null below every other value, whatever the key's `NULLS FIRST` or
`NULLS LAST`, so Comet falls back in every mode, not only in strict mode, for the two shapes where
the native sort or window frame would place it differently: `ASC NULLS LAST` or `DESC NULLS FIRST`
on such a key ([#6476](https://github.com/apache/datafusion-comet/issues/6476)), and a `RANGE`
window frame that has to find a row's peers over it
([#6477](https://github.com/apache/datafusion-comet/issues/6477)). The
[operator compatibility notes](operators.md) describe both.

`array_min` and `array_max` use Spark-compatible native comparisons in both strict and non-strict
floating-point modes. Signed zeros compare equal, and all NaN representations compare equal and
greater than non-NaN values. The original first equal element is retained: for example,
`array_min(array(0.0D, -0.0D))` returns `0.0`, while reversing those elements returns `-0.0`.
The same ordering applies recursively to floating-point fields in arrays and structs. These
expressions do not require Spark's codegen dispatcher for floating-point compatibility.

## `min`, `max`, `greatest`, and `least`

The `min` and `max` aggregates, including over window frames, and `greatest` and `least` compare
`FLOAT` and `DOUBLE` values in the same order: NaN is larger than every other value and `-0.0`
equals `0.0`. Of equal values the first one wins, as in Spark: `greatest(-0.0D, 0.0D)` returns
`-0.0`, and `max` returns whichever zero it reads first. `greatest` and `least` follow the same
order inside arrays and structs. These run natively in strict floating-point mode too.

Spark treats `greatest` and `least` as commutative when it matches expressions, so a projection
that holds both `greatest(a, b)` and `greatest(b, a)` evaluates only one of them and returns the
same value for both. Comet evaluates each one, so when `a` and `b` are zeros of different signs
the two can differ.

## Array distinct and union

`array_distinct` and `array_union` fall back to Spark when their element type contains
`FLOAT` or `DOUBLE`, on every Spark version except 4.2.0. Spark 4.2.0 normalizes signed zeros
and NaNs in the arguments of these functions before they run (SPARK-54918), so native execution
returns the same results. Spark 3.4, 3.5, 4.0.0 to 4.0.4, and 4.1.0 to 4.1.3 keep positive and
negative zero distinct in flat arrays. Spark 4.0.5+, 4.1.4+, and 4.2.1+ normalize while these
functions evaluate instead (SPARK-59602), which native execution does not match for NaNs or for
zeros nested in arrays or structs. Other element types remain native.

The check is based on the element type, not the values. It also applies to NULL or empty
floating-point arrays and columns that never contain negative zero. The entire projection
falls back to Spark, introducing a `CometColumnarToRow` transition and moving unrelated
expressions in the same projection out of Comet. For example,
`SELECT id + 1, array_distinct(a), i[0] + 5` evaluates all three expressions in a Spark `Project`.

This can have a substantial cost. A local Spark 4.1.3 benchmark of
`sum(cardinality(array_distinct(d)))` over two million `array<double>` rows found the default
projection fallback about 15 times slower than native opt-in (best of five runs).
The slowdown depends on the workload.

Setting `spark.comet.expression.ArrayDistinct.allowIncompatible=true` or
`spark.comet.expression.ArrayUnion.allowIncompatible=true` restores native execution on other
versions, but signed-zero and NaN results may differ from Spark. Native execution can keep
NaNs with different signs or payloads distinct. Signed-zero differences also depend on the
element type: native execution merges positive and negative zero in flat floating-point arrays,
but can keep them distinct inside nested arrays or structs. Only opt in if these differences
are acceptable for your data.

The check uses the Spark version number, not the changes a build contains. A build that reports
any version other than 4.2.0 falls back even if it includes SPARK-54918. A vendor build that
reports 4.2.0 but includes SPARK-59602 still runs natively; set
`spark.comet.expression.ArrayDistinct.enabled=false` and
`spark.comet.expression.ArrayUnion.enabled=false` on such a build.

## `array_contains`, `array_remove` and `sort_array`

`array_contains` compares `FLOAT` and `DOUBLE` elements as Spark does: `-0.0` equals `0.0`, and all
NaN representations are equal, inside nested arrays and structs too. The result keeps Spark's
three-valued form: null when nothing matches and the array holds a null element. Flat arrays run a
native kernel; nested float elements go through the codegen dispatcher.

`array_remove` compares `FLOAT` and `DOUBLE` elements as Spark does: `-0.0` equals `0.0`, and all
NaN representations are equal, inside nested arrays too. The elements it keeps retain their
original NaN representations and zero signs.

`sort_array` sorts in Spark's order, in which NaN sorts above every other value and all NaN
representations tie, and keeps equal elements in their original order. In an array whose elements
can be null, such as one built from nullable columns, `-0.0` and `0.0` therefore tie. Sorting an
array whose elements cannot be null in ascending order, Spark's generated code puts `-0.0` before
`0.0`, and so does Comet. All three expressions run natively in strict floating-point mode.
