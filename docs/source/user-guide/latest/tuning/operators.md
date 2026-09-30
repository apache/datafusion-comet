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
reducing the number of rows enough. After draining the accumulated groups, each subsequent
row becomes a normal partial state, preserving its grouping key. Final aggregation still
merges these states. Low-cardinality inputs continue to aggregate normally.

Qualification is per aggregate operator within a fused native shuffle-writer plan. An
unsupported operator does not disable its eligible siblings or children. Grouping-only,
multi-argument `COUNT`, `MIN`, `MAX`, bitwise aggregates, exact `percentile`, `collect_set`, and
legacy integer `SUM` are supported.
Filters are applied while producing singleton states; rejected or null inputs still preserve
their group with an empty state. Eligible `PartialMerge` expressions pass their existing states
through, including mixed Partial/PartialMerge producers. Distribution-required deduplication
stages, global aggregates, order-sensitive aggregates, and non-native-shuffle boundaries stay
on ordinary aggregation. Configuration overrides cannot defeat those restrictions.

`spark.comet.exec.aggregate.partialBypass.enabled` controls the optimization and defaults to
`true`. Numeric aggregates with association-sensitive arithmetic additionally require:

```sql
SET spark.comet.exec.aggregate.partialBypass.allowNumericalDifferences=true;
```

This **opt-in changes the numerical contract** for two groups of aggregates:

- Floating-point `SUM`, non-decimal `AVG` (including integer inputs, which use floating-point
  state), variance, standard deviation, covariance and correlation. Regrouping inputs can
  change rounding and finite/infinite/NaN results.
- Decimal `SUM`/`AVG` and ANSI/TRY integer `SUM`. Regrouping can change intermediate overflow,
  so a query may return NULL or throw where ordinary aggregation succeeds, or vice versa.

For example, with `DECIMAL(38,38)` state, partials for `[0.8]` and `[0.4, -0.4]` can merge
successfully, whereas singleton states may overflow at `0.8 + 0.4` before cancellation.
The converters support these aggregates and preserve their state format and arithmetic; the
flag controls whether to accept the effects of changing association. With its default `false`,
an operator containing any of these aggregates stays on ordinary aggregation. Enabling it
never overrides the structural restrictions above.

The generic converter uses each aggregate's ordinary one-row update/state contract, with
temporary groups chunked to 1,024 rows. Decimal `SUM` and `AVG` have direct converters, including
a shared-buffer fast path for non-null, unfiltered input. Output state buffers still grow with
the input batch; bypass is not a guarantee that every hash-table allocation is immediately
released by the underlying DataFusion stream.

Short output batches can be combined before shuffle. The buffer admits memory for both its
retained inputs and concatenation output, and retains at most 8 MiB of input array-size estimates.
It flushes when admission fails; full, oversized, or refused individual batches pass through
without copying. One already-read input can remain pending while the preceding output is emitted,
so this limit describes accumulated fragments rather than the whole pipeline's peak memory.

The SQL metrics `rows bypassing partial aggregation`, `partial aggregate input rows`, and `output rows` show
actual activation and volume. The partial reduction numerator/denominator report grouped
prefix output/input, excluding bypassed rows. Eligible/ineligible partition counters and the
native plan's `CometPartialAggregationExec` reason explain qualification. A qualifying operator
may still never bypass, for example when it continues to reduce its input effectively.

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

To disable skipping without changing development settings:

```sql
SET spark.comet.exec.aggregate.partialBypass.enabled=false;
```

Setting the DataFusion ratio threshold above one also disables skipping. Measure whole-query
time together with final-aggregate work, shuffle bytes, peak memory and spill; a faster partial
operator alone does not establish a net improvement.

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
