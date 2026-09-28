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

# Reducing Row/Columnar Conversion Overhead

When a query stage contains many operators that fall back to Spark row-based execution, Comet may insert
repeated columnar-to-row and row-to-columnar conversions that dominate stage runtime. Set
`spark.comet.exec.transitionRevert.enabled=true` to have Comet revert the entire stage to Spark row execution
when the number of columnar-to-row transitions exceeds
`spark.comet.exec.transitionRevert.maxTransitions` (default `2`). This trades native execution of a small
subset of operators for eliminating conversion overhead across the stage. A stage is not reverted when it holds a
native aggregate whose intermediate buffer Spark cannot exchange with Comet across a stage boundary, because
reverting it would split that aggregate between the two engines.

## Wide or Deeply Nested Schemas

The cost of each conversion also grows sharply with schema shape: for wide or deeply nested schemas,
columnar-to-row conversion is especially expensive because the conversion work scales with the number of
columns and nested fields. If profiling shows these conversions dominating a query over such a schema, set
`spark.comet.exec.transitionRevert.enabled=true` and lower
`spark.comet.exec.transitionRevert.maxTransitions` (default `2`) to `1`. Note that this reverts every stage that
exceeds the threshold to Spark row-based execution — Comet removes the stage's native operators rather than running a
mix of native and fallback operators joined by repeated conversions — which can be cheaper than paying the
expensive conversions again and again.

## Experimental: Direct Columnar-to-Row Conversion

When the JVM columnar-to-row operator is in use (`spark.comet.exec.columnarToRow.native.enabled=false`),
setting `spark.comet.exec.columnarToRow.direct.enabled=true` enables an experimental converter that writes
values straight from Arrow buffers into Spark's row format without allocating an object per value. This is
most beneficial for decimal-heavy schemas, where the default conversion allocates a `Decimal` object per
value (and considerably more for decimals with precision above 18); microbenchmarks show up to 2x faster
conversion and a large reduction in garbage creation for such schemas. Schemas containing data types the
converter does not support fall back to the default conversion automatically.

Batches with fewer rows than `spark.comet.exec.columnarToRow.direct.minBatchSize` (default `128`) also fall
back to the default conversion, since the direct converter's per-batch setup does not pay off on very small
batches. This optimization is experimental: it only affects the operator's non-codegen paths (including
broadcast relation builds), and the default conversion remains enabled unless explicitly opted in.

The converter takes a fixed-width path when every column has a fixed width in Spark's row format, that is,
when the schema has no strings and no decimals with precision above 18. That path converts the whole batch
as soon as it is set, so a consumer that stops early, such as a `LIMIT`, pays for every row of the batch where
the default conversion is lazy per row. It also keeps one buffer of the batch's rows per task for the life of
the task, on heap and outside Comet's memory accounting: for example about 6.7 MB for 100 fixed-width columns
at 8,192 rows per batch.
