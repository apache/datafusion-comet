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
For top-level `FLOAT` and `DOUBLE` comparisons, Comet normalizes both operands before native
execution, including noncanonical NaN literals. Top-level `IN`, `InSet`, and `NOT IN` membership
also normalize dynamic candidates and lists containing NaN. When every candidate is a non-NaN
literal, Comet keeps DataFusion's static filter and pruning path, enumerating both signed-zero
forms when a list contains zero.

This scalar membership handling does not yet recurse into floating-point leaves nested in arrays
or structs; see [#6019](https://github.com/apache/datafusion-comet/issues/6019).

## Nested equality and membership

For arrays and structs containing `FLOAT` or `DOUBLE`, native `=`, `<>`, `IN`, and `NOT IN`
compare signed zeros as equal and all NaN representations as equal, matching Spark. This also
covers single-candidate membership that Spark rewrites into equality.

Equality and dynamic membership compare nested elements directly and stop at the first mismatch.
Constant membership sets use normalized comparison values for static lookup. These operations
preserve SQL null semantics and do not change the values returned by projections.

## Ordering: NaN and signed zero (`-0.0` vs `+0.0`)

Spark's `ORDER BY`, `RANK`, `DENSE_RANK`, and window frame comparisons route through
`SQLOrderingUtil.compareDoubles` / `compareFloats`, which equate all NaN representations and
define `-0.0 == 0.0`. NaN sorts above every non-NaN value.

For scalar `FLOAT` and `DOUBLE` keys, Comet normalizes NaNs and signed zeros before native
sorting, window peer comparisons, and `WindowGroupLimitExec` rank comparisons. Native range
partitioning normalizes its keys and sampled boundaries in the same way. Only comparison keys
are normalized; returned values retain their original NaN representations and zero signs.

Native sorting of floating-point values nested in arrays or structs still uses Arrow's raw total
ordering. Nested keys can therefore produce different ordering or rank results from Spark; see
[#5507](https://github.com/apache/datafusion-comet/issues/5507).

Because those scalar comparison keys match Spark, `spark.comet.exec.strictFloatingPoint=true` no
longer forces a fallback for them: scalar `FLOAT` and `DOUBLE` sort keys, window and rank order
keys, and range partitioning keys all stay native under strict mode. Floating-point values nested
in arrays, structs, or maps still fall back under strict mode, because their ordering is the raw
total ordering described above.

## Array distinct and union

`array_distinct` and `array_union` fall back to Spark when their element type contains
`FLOAT` or `DOUBLE` and the running Spark version predates SPARK-54918. Native execution
is enabled for Spark 4.0.5+, 4.1.4+, and 4.2+, which normalize signed zeros and NaNs in these
functions. Spark 3.4 and 3.5 retain the fallback. Other element types remain native.

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
`spark.comet.expression.ArrayUnion.allowIncompatible=true` restores native execution on older
versions, but signed-zero and NaN results may differ from Spark. Native execution can keep
NaNs with different signs or payloads distinct. Signed-zero differences also depend on the
element type: native execution merges positive and negative zero in flat floating-point arrays,
but can keep them distinct inside nested arrays or structs. Only opt in if these differences
are acceptable for your data.

A vendor backport that retains an older Spark version number may still fall back.
