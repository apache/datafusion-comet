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

# Operator Tuning

## Optimizing Joins

Spark often chooses `SortMergeJoin` over `ShuffledHashJoin` for stability reasons. If the build-side of a
`ShuffledHashJoin` is very large then it could lead to OOM in Spark.

Vectorized query engines tend to perform better with `ShuffledHashJoin`, so for best performance it is often preferable
to configure Comet to convert `SortMergeJoin` to `ShuffledHashJoin`. Comet does not yet provide spill-to-disk for
`ShuffledHashJoin` so this could result in OOM. Also, `SortMergeJoin` may still be faster in some cases. It is best
to test with both for your specific workloads.

To configure Comet to convert `SortMergeJoin` to `ShuffledHashJoin`, set `spark.comet.exec.forceShuffledHashJoin=true`.

### Join Runtime Filters

Set `spark.comet.exec.join.dynamicFilter.enabled=true` to try experimental native hash join runtime
filtering. It is disabled by default. Eligible joins are inner joins with one direct signed integer
key (`TINYINT`, `SMALLINT`, `INT`, or `BIGINT`) and one native partition per input within each task.
Both broadcast and shuffled hash joins support either Spark build side. Unsupported joins keep
their existing execution path.

Once the build completes, its key domain filters probe batches before the hash probe. Eligible
native Parquet readers also use the domain to prune row groups. Reader attachment can pass through
direct-column `IS NOT NULL` checks, including conjunctions, and remaps columns when the scan itself
projects the file schema. The original null checks and residual runtime filter remain in place.
The original join still verifies matches, including any hash collisions admitted by the filter.
Standalone projections, other filter expressions, and limits prevent reader attachment.

To preserve schema-conversion and timestamp-overflow errors, runtime reader pruning is disabled for
each file whose projected or statically filtered columns require schema adaptations beyond direct
column mappings or literal values. This conservative check also disables reader pruning for allowed
`INT32` to `BIGINT` promotion and for projecting a subset of a struct's fields, even when those
adaptations cannot fail. Nested column pruning still reads only the requested struct fields. Scans
with supplied file statistics also skip reader attachment. These cases still use runtime filtering
on decoded batches.

Filters stay within the task's native plan and do not propagate across Spark exchanges or JVM/Arrow
boundaries. A shuffled hash join can still filter probe batches after shuffle, but it cannot send
its filter back to an earlier scan stage. Compare the [runtime-filter and scan metrics](../metrics.md#hash-joins)
with the setting disabled to distinguish reduced hash-probe work from reader I/O savings.

## Adaptive Partial Aggregation

For high-cardinality grouping, Comet can bypass partial hash aggregation when it is not
reducing the number of rows enough. This currently applies only to fused native shuffle-writer
plans whose partial aggregates are grouping-only or single-argument `COUNT`. Low-cardinality
inputs continue to aggregate normally. The SQL metric `rows bypassing partial aggregation`
shows whether skipping occurred.

Eligibility is conservative for the whole fused native plan: any unsupported partial accumulator,
Spark `PartialMerge`, or mixed-mode aggregate disables skipping in that plan. Multi-argument
`COUNT` and other accumulators are not admitted. Distribution-required grouping-only stages
still fully deduplicate, and non-native-shuffle plans retain ordinary aggregation.
The DataFusion testing configuration override does not bypass these safety checks.

DataFusion 55 defaults to probing after 100,000 input rows per partial aggregation
partition and skipping when the number of groups divided by input rows exceeds `0.8`.
To experiment with these thresholds, enable `spark.comet.exec.respectDataFusionConfigs`,
a development and testing option that defaults to `false`. For example, the following
SQL settings pass through the default threshold values, which you can adjust:

```sql
SET spark.comet.exec.respectDataFusionConfigs=true;
SET spark.comet.datafusion.execution.skip_partial_aggregation_probe_rows_threshold=100000;
SET spark.comet.datafusion.execution.skip_partial_aggregation_probe_ratio_threshold=0.8;
```

A lower row threshold allows an earlier decision; a lower ratio threshold makes
skipping more likely. Skipping can increase the number of partial states emitted
and the amount of shuffle data, so measure the effect on your workload.

To disable skipping, keep `spark.comet.exec.respectDataFusionConfigs=true` and set
the ratio threshold above the maximum possible groups/input-rows ratio:

```sql
SET spark.comet.datafusion.execution.skip_partial_aggregation_probe_ratio_threshold=1.1;
```

These settings only tune eligible plans. Unsupported accumulators and modes remain
disabled even when configuration overrides are enabled.

## Local TopK Fusion

Set `spark.comet.exec.topK.fusion.enabled=true` to run an eligible local TopK in the same native
execution as its Parquet scan. This experimental optimization is disabled by default. It currently
supports a direct native Parquet scan ordered by one signed integer column (`TINYINT`, `SMALLINT`,
`INT`, or `BIGINT`). Both sort directions and null orderings are supported. Other inputs use the
existing TopK execution path.

Each scan partition keeps enough candidates for both `LIMIT` and `OFFSET`. With multiple partitions,
Comet shuffles those candidates and performs the final TopK. With one partition, it reuses the local
ordering without building a second heap. The final stage applies the offset and output projection.

Fusion reduces the work of passing scan batches between native execution blocks. It still reads all
input rows and does not enable TopK reader pruning. It can also reduce overlap between scan decoding
and TopK processing, so some workloads may run slower. Compare enabled and disabled runs with your
data layout, payload width, limit, and partition count before enabling it. The
`CometTopKBenchmark` microbenchmark covers these cases with ascending, descending, and random layouts.

## Optimizing Sorting on Floating-Point Values

Comet normalizes NaN payloads and signed zeros in scalar `FLOAT` and `DOUBLE` ordering keys, so `ORDER BY`, window
ordering and range partitioning on them match Spark and stay native even with
`spark.comet.exec.strictFloatingPoint=true`. Only the comparison key is normalized; returned values keep their original
NaN representation and zero sign.

Floating-point values nested in arrays, structs, or maps are compared with Arrow's raw total ordering instead, which can
differ from Spark when the data contains both zero and negative zero, or more than one NaN representation. This is likely
an edge case that is not of concern for many users. Setting `spark.comet.exec.strictFloatingPoint=true` makes those
nested cases fall back to Spark, and they can be forced back onto the native path with
`spark.comet.expression.SortOrder.allowIncompatible=true`.

`sort_array` is separate. It sorts array elements rather than ordering rows, and its elements are compared with Arrow's
raw total ordering, so `spark.comet.exec.strictFloatingPoint=true` makes it fall back even for a scalar floating-point
element type. Use `spark.comet.expression.SortArray.allowIncompatible=true` to keep it native.

## Reusing Broadcast Hash Builds (Experimental)

`spark.comet.broadcast.reuse.enabled=true` allows tasks in one executor to share a prepared
native hash table for the same Spark broadcast. This avoids repeating Arrow decoding and hash-table
construction on a cache hit. It requires `org.apache.spark.CometPlugin` in `spark.plugins` and Spark
off-heap memory. The feature is disabled by default.

The implementation covers non-spilling inner broadcast joins with direct column keys,
matching key types, and fixed-width or plain UTF8 build columns. Residual join conditions remain
local to each consuming task. Dictionary, view, binary, and nested build columns remain unsupported.
Unsupported joins keep ordinary task-local execution. Tasks can share a build when they use the
same broadcast, build schema, and ordered build keys, even if their probe schemas differ.

`spark.comet.broadcast.reuse.maxMemory` defaults to `1g`. It caps native prepared-build allocations
through Spark off-heap storage memory while tasks are preparing or using a build. When the last
task releases that build, its storage charge is returned. A later task wave may prepare the same
broadcast again; a later query without a broadcast hash join inherits no idle build charge.
The first flat-schema broadcast input marked for reuse fixes the executor's cap, even if its join
later proves ineligible and creates no build. A later input with a different cap uses ordinary
execution. Cache admission never waits for another join to release its memory. When admission
fails, Comet discards the partial preparation and opens a fresh uncached input for that task.
Preparation must also admit the temporary overlap between decoded native batches and DataFusion's
compact build batch; the memory needed to prepare a build can exceed its final retained size.

This cap does not bound the existing driver broadcast representation or JVM decoder allocations.
The first task still decodes the existing broadcast format; bounded broadcast construction and
admission before JVM decoding are separate work.

Spark join metrics expose cache hits, successful preparations, admission fallbacks, preparation
rows/bytes, and preparation time. Probe metrics remain per task. A successful preparation should
normally be followed by cache hits from other tasks using that broadcast. Validate elapsed query
time and executor memory with the feature both enabled and disabled before enabling it broadly;
reuse counts alone do not establish a query speedup.
