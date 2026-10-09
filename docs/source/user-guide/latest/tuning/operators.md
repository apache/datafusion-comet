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
The conversion only happens when the build side is under a size limit, and a join whose build side has no statistics
is left as a `SortMergeJoin`. The size is Spark's planning estimate of the build side, or under AQE the materialized
shuffle size of the build side. By default the limit is Spark's own rule for choosing a `ShuffledHashJoin`:
`spark.sql.autoBroadcastJoinThreshold` times the initial shuffle partition count
(`spark.sql.adaptive.coalescePartitions.initialPartitionNum` when AQE and partition coalescing are both on and it is
set, else `spark.sql.shuffle.partitions`). When broadcasts are disabled with a non-positive threshold, Spark's default
threshold of 10 MB is used instead. Set `spark.comet.exec.forceShuffledHashJoin.maxBuildSize` to a size in bytes to use a fixed
limit, or to a non-positive value to convert every eligible join regardless of size.

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

Set `spark.comet.exec.aggregate.skipPartial.enabled=true` to let Comet bypass partial hash
aggregation for high-cardinality grouping when it is not reducing the number of rows enough. This
experimental optimization is disabled by default. It currently applies only to fused native
shuffle-writer plans whose partial aggregates are grouping-only or single-argument `COUNT`.
Low-cardinality inputs continue to aggregate normally. The SQL metric
`rows bypassing partial aggregation` shows whether skipping occurred.

DataFusion makes the decision separately in each task. It starts checking after the first 100,000
input rows, and as soon as the number of groups divided by the number of input rows exceeds `0.8`,
it stops aggregating and sends the rest of the task's rows to the shuffle as they are. It does not
check again, so a task whose keys repeat after a mostly distinct start, such as several snapshot
files of the same keys packed into one split, can shuffle many times more rows than it would with
skipping disabled. Compare the shuffle write metrics with the setting enabled and disabled before
enabling it for a workload.

Eligibility is conservative for the whole fused native plan: any unsupported partial accumulator,
Spark `PartialMerge`, or mixed-mode aggregate disables skipping in that plan. Multi-argument
`COUNT` and other accumulators are not admitted. Distribution-required grouping-only stages
still fully deduplicate, and non-native-shuffle plans retain ordinary aggregation.

To experiment with the thresholds, also enable `spark.comet.exec.respectDataFusionConfigs`,
a development and testing option that defaults to `false`. For example, the following
SQL settings pass through the default threshold values, which you can adjust:

```sql
SET spark.comet.exec.aggregate.skipPartial.enabled=true;
SET spark.comet.exec.respectDataFusionConfigs=true;
SET spark.comet.datafusion.execution.skip_partial_aggregation_probe_rows_threshold=100000;
SET spark.comet.datafusion.execution.skip_partial_aggregation_probe_ratio_threshold=0.8;
```

A lower row threshold allows an earlier decision; a lower ratio threshold makes
skipping more likely. These settings only tune eligible plans. They cannot enable skipping
while `spark.comet.exec.aggregate.skipPartial.enabled` is `false`, or for unsupported
accumulators and modes.

## Local TopK Fusion

Set `spark.comet.exec.topK.fusion.enabled=true` to run an eligible local TopK in the same native
execution as its Parquet scan. This experimental optimization is disabled by default. It currently
supports a direct native Parquet scan ordered by one signed integer column (`TINYINT`, `SMALLINT`,
`INT`, or `BIGINT`). Both sort directions and null orderings are supported. Other inputs use the
existing TopK execution path.

Each scan partition keeps enough candidates for both `LIMIT` and `OFFSET`. With multiple partitions,
Comet shuffles those candidates and performs the final TopK. With one partition, it reuses the local
ordering without building a second heap. The final stage applies the offset and output projection.

Fusion reduces the work of passing scan batches between native execution blocks. Fusion alone still
reads all input rows. [TopK reader pruning](#topk-reader-pruning) is enabled separately. Fusion can
also reduce overlap between scan decoding
and TopK processing, so some workloads may run slower. Compare enabled and disabled runs with your
data layout, payload width, limit, and partition count before enabling it. The
`CometTopKBenchmark` microbenchmark covers these cases with ascending, descending, and random layouts.

### TopK Reader Pruning

Set both `spark.comet.exec.topK.fusion.enabled=true` and
`spark.comet.exec.topK.dynamicFilter.enabled=true` to pass the local TopK's improving threshold to
its Parquet reader. Both options are experimental and disabled by default. Eligibility is the same
single signed integer key described above. Each task creates a fresh threshold; it is not shared
across Spark partitions or exchanges, or retained for later executions.

Once the heap contains enough candidates for `LIMIT + OFFSET`, the reader can skip later row groups
whose statistics prove that no row can improve those candidates. Existing Parquet page-index and
decoder-filter options can also use the predicate. This option adds no separate filter over decoded
scan batches. TopK continues to select the final candidates.

Reader attachment is conservative. A scan with a fetch limit, supplied file statistics, or a static
predicate other than direct column `IS NOT NULL` checks keeps the existing execution path. For each
file, schema adaptation disables pruning if it could hide a conversion error in a projected or
filtered column. Missing null counts remain unknown, which can prevent pruning even when min/max
statistics are present. These cases can still execute a fused TopK.

Reader pruning is most useful when small K values and the file order establish a strong threshold
early. Descending or random layouts for an ascending query can prune few or no groups, while still
paying the cost of attaching and checking the predicate. Wider rows can increase the benefit when
groups are skipped. Compare `pruning` with `fused` in `CometTopKBenchmark` to measure the reader effect,
and compare both with `unfused` to include the cost of fusion. Check the scan's emitted rows,
`bytes_scanned`, `row_groups_pruned_dynamic_filter`, and `row_groups_pruned_statistics` alongside elapsed
time; attachment alone does not demonstrate a saving. Pruning when later files open uses the TopK
threshold already available and increments `row_groups_pruned_statistics`. With one row group per
file, the dynamic counter can stay zero despite substantial TopK pruning. The statistics counter also
includes other predicates, so compare with filtering disabled to assess TopK savings.
See [TopK metrics](../metrics.md#local-topk).

## Filtering Rows with Missing Values

Spark's `DataFrame.na.drop` uses `AtLeastNNonNulls` to retain rows with enough non-NULL,
non-NaN values. For general counting, Comet uses row counters below
`spark.comet.exec.atLeastNNonNulls.smallBatchThreshold` and bitmap counters at or above it.
The default is 64 rows. Any positive integer is accepted, including values that are not
multiples of 64. The bitmap word size remains 64 bits, and the any/all-valid fast paths
are unaffected.

Set the threshold before planning a query, for example:

```sql
SET spark.comet.exec.atLeastNNonNulls.smallBatchThreshold=128;
```

A higher threshold uses row counters for more batches; a lower threshold uses bitmap
counters for more batches. This does not change the input batch size or Spark's results
and error behavior. Compare query timings on your data before changing the default.

## Optimizing Sorting on Floating-Point Values

Comet normalizes NaN payloads and signed zeros in `FLOAT` and `DOUBLE` ordering keys, including floating-point values
nested in arrays and structs, so `ORDER BY`, window ordering and range partitioning on them match Spark and stay native
even with `spark.comet.exec.strictFloatingPoint=true`. Only the comparison key is normalized; returned values keep their
original NaN representation and zero sign.

The exception is a key that nests floating-point values in an array or struct whose type can hold a null element or
field. Spark orders such a null below every other value, and the native sort and `RANGE` window frames do not
([#6476](https://github.com/apache/datafusion-comet/issues/6476),
[#6477](https://github.com/apache/datafusion-comet/issues/6477)), so
`spark.comet.exec.strictFloatingPoint=true` makes those keys fall back to Spark. They can be forced back onto the
native path with `spark.comet.expression.SortOrder.allowIncompatible=true`.

`sort_array` sorts array elements rather than ordering rows. It follows Spark's floating-point ordering as well, so it
also stays native with `spark.comet.exec.strictFloatingPoint=true`.
