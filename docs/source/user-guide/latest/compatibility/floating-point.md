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
