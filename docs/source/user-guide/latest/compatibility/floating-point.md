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

## Ordering: signed zero (`-0.0` vs `+0.0`)

Spark's `ORDER BY`, `RANK`, `DENSE_RANK`, and window frame comparisons route through
`SQLOrderingUtil.compareDoubles` / `compareFloats`, which explicitly define `-0.0 == 0.0`. Comet's
native sort and `WindowGroupLimitExec` use the `arrow-row` row-format encoder for `ORDER BY` keys,
which applies Rust's total-ordering transform to the raw IEEE-754 bits. Under that encoding `-0.0`
sorts strictly less than `+0.0`, so a partition that mixes the two zeros can produce a rank
distribution that differs from Spark. For example, `RANK() OVER (ORDER BY v ASC)` over
`[-0.0, 0.0, 1.0]` filtered to `rk <= 1` returns two rows in Spark (both zeros tied at rank 1) but
one row in Comet (`-0.0` at rank 1, `+0.0` at rank 2). If your workload materially mixes `-0.0`
and `+0.0` in a ranked column, prefer Spark for that stage or normalize the column to `+0.0`
upstream.
