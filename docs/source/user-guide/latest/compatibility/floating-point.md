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
So Comet adds additional normalization expression of NaN and zero for comparisons, and may still have differences
to Spark in some cases, especially when the data contains both positive and negative zero. This is likely an edge
case that is not of concern for many users. If it is a concern, setting `spark.comet.exec.strictFloatingPoint=true`
will make relevant operations fall back to Spark.

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

The existing `spark.comet.exec.strictFloatingPoint=true` fallback policy is unchanged, including
its conservative fallback for scalar floating-point sort keys. Narrowing that scalar-sort
admission policy is tracked in [#5506](https://github.com/apache/datafusion-comet/issues/5506).

## Array distinct and union

`array_distinct` and `array_union` fall back to Spark when their element type contains
`FLOAT` or `DOUBLE` and the running Spark version predates SPARK-54918. Native execution
is enabled for Spark 4.0.5+, 4.1.4+, and 4.2+, which normalize signed zeros in these
functions. Spark 3.4 and 3.5 retain the fallback. Other element types remain native.

The check is based on the element type, not the values. It also applies to NULL or empty
floating-point arrays and columns that never contain negative zero. The entire projection
falls back to Spark, introducing a `CometColumnarToRow` transition and moving unrelated
expressions in the same projection out of Comet. For example,
`SELECT id + 1, array_distinct(a), i[0] + 5` evaluates all three expressions in a Spark `Project`.

This can have a substantial cost. In a [reviewer's local measurement on Spark 4.1.3](https://github.com/apache/datafusion-comet/pull/5750#pullrequestreview-5153510242)
with a release build, `sum(cardinality(array_distinct(d)))` over two million rows of
`array<double>` took 93 ms with native opt-in and 1355 ms with the default projection fallback
(best of five runs, about 15 times slower). These are workload-specific measurements, not a
general performance guarantee. Keeping the expression in Comet through the JVM codegen
dispatcher was slower still in that measurement, so distinct and union use projection fallback.

If signed zeros cannot occur in your data, setting
`spark.comet.expression.ArrayDistinct.allowIncompatible=true` or
`spark.comet.expression.ArrayUnion.allowIncompatible=true` restores native execution on older
versions. These options accept the difference in deduplicating positive and negative zero.

The gate uses the runtime Spark version. A vendor backport that retains an older version
number may still fall back: checking only for a `KnownFloatingPointNormalized` wrapper would
miss Spark's normalization of array constructors and conditional branches, which can lack
that top-level wrapper.
