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

# Operator Compatibility

## Sampling

Comet runs `SampleExec` natively when sampling is performed without replacement, which covers
`DataFrame.sample`, SQL `TABLESAMPLE`, and `DataFrame.randomSplit`. The native implementation
reproduces Spark's per-row `XORShiftRandom` draw sequence, so for a given seed it selects the same
rows as Spark.

Because the sampler consumes one random value per row, sampling directly above a scan, filter, or
projection reproduces Spark's selection. Above an operator where Comet may emit rows in a different
order than Spark, such as a join or an aggregate, the result is still a valid sample of the same
expected size, but not necessarily the same rows.

Sampling with replacement (`df.sample(withReplacement = true, ...)`) falls back to Spark, because
it draws from a Poisson distribution that Comet does not implement natively
([#5109](https://github.com/apache/datafusion-comet/issues/5109)).

## Window Functions

Comet runs `WindowExec` natively and it is enabled by default (`spark.comet.exec.window.enabled`). A broad set of
window functions is accelerated, and any shape Comet does not support falls back to Spark rather than producing an
incorrect result. When any single window expression in a `WindowExec` falls back, the entire operator runs on Spark.

**Accelerated natively:**

- Ranking functions: `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`.
- Value functions: `lag`, `lead`, `nth_value`, `first_value` (`first`), `last_value` (`last`). `IGNORE NULLS` is
  supported.
- Aggregate window functions: `count`, `min`, `max`, `sum`, `avg`.
- Frame units `ROWS` and `RANGE`, with `UNBOUNDED PRECEDING` / `UNBOUNDED FOLLOWING`, `CURRENT ROW`, and numeric
  `PRECEDING` / `FOLLOWING` offsets.

**Falls back to Spark:**

- Aggregate window functions other than the ones listed above, including the statistical aggregates
  (`stddev`, `stddev_pop`, `stddev_samp`, `var_pop`, `var_samp`, `corr`, `covar_pop`, `covar_samp`). These run
  natively as plain aggregations but not as window functions
  ([#4766](https://github.com/apache/datafusion-comet/issues/4766)).
- `min` / `max` on string, binary, timestamp-without-time-zone, interval, or nested (array / struct) input types,
  and `sum` / `avg` on year-month or day-time interval input types. Windowed aggregates inherit the same input-type
  support as the batch aggregates, so these fall back in both contexts.
- `sum` or `avg` on `DECIMAL` with a sliding (non ever-expanding) frame, because the sliding path would wrap on
  overflow instead of returning Spark's `NULL`.
- `RANGE` frame with an explicit offset when the `ORDER BY` column is `DATE` or `DECIMAL`
  ([#4834](https://github.com/apache/datafusion-comet/issues/4834)).
- `first_value` / `last_value` on a `RANGE` frame with a literal offset
  ([#4835](https://github.com/apache/datafusion-comet/issues/4835)).
- `lag` / `lead` with a non-literal default value ([#4268](https://github.com/apache/datafusion-comet/issues/4268)).
- A `ROWS` offset that is not an integer or long, or a `RANGE` offset that is not numeric.
- `GROUPS` frames ([#4836](https://github.com/apache/datafusion-comet/issues/4836)). `DISTINCT` aggregates over a
  window are not supported by Spark either.
- Any `PARTITION BY` or `ORDER BY` expression that Comet cannot serialize.

`WindowGroupLimitExec` (window-based limit pushdown) is not yet supported and falls back to Spark
([#4837](https://github.com/apache/datafusion-comet/issues/4837)).

## MERGE INTO (MergeRowsExec)

Comet can run `MergeRowsExec` (Spark's row-level `MERGE INTO` dispatch operator, Spark 3.5+)
natively, but it is disabled by default. Enable it with `spark.comet.exec.mergeRows.enabled=true`.

**Missing per-clause row metrics:** Spark 4.x's `MergeRowsExec` exposes eight metrics --
`numTargetRowsCopied`, `numTargetRowsInserted`, `numTargetRowsUpdated`, `numTargetRowsDeleted`,
`numTargetRowsMatchedUpdated`, `numTargetRowsMatchedDeleted`, `numTargetRowsNotMatchedBySourceUpdated`,
and `numTargetRowsNotMatchedBySourceDeleted` -- breaking down how many rows each `MERGE` clause
touched. Comet's native operator does not expose these; it only reports the generic `output_rows`,
`output_batches`, and `elapsed_compute` every native operator reports. EXPLAIN ANALYZE and the
Spark UI will not show a rows-inserted/updated/deleted breakdown for a native `MERGE`. (Spark
3.5.x's own `MergeRowsExec` does not have these metrics either -- they were added alongside a
`Context` field Spark only attaches to `MERGE` clauses starting in 4.x.)

**Cardinality-violation error may differ from Spark's on rare inputs:** Spark validates cardinality
(rejecting an `ON` condition that matches one target row to multiple source rows,
`MERGE_CARDINALITY_VIOLATION`) row-at-a-time, interleaved with applying each `MATCHED` clause, so
whichever failure a given row hits first is the error Spark raises. Comet's native operator is
vectorized: it validates cardinality for an entire input batch before evaluating any clause. If a
single batch contains both a cardinality violation and an unrelated clause-evaluation error (for
example an ANSI divide-by-zero) on different rows, Comet may raise a different error than Spark
would for the same input, depending on which row each engine reaches first. The query fails either
way; only the specific error differs.

## Round-Robin Partitioning

Comet's native shuffle implementation of round-robin partitioning (`df.repartition(n)`) is not compatible with
Spark's implementation and is disabled by default. It can be enabled by setting
`spark.comet.shuffle.native.partitioning.roundrobin.enabled=true`.

**Why the incompatibility exists:**

Spark's round-robin partitioning sorts rows by their binary `UnsafeRow` representation before assigning them to
partitions. This ensures deterministic output for fault tolerance (task retries produce identical results).
Comet uses Arrow format internally, which has a completely different binary layout than `UnsafeRow`, making it
impossible to match Spark's exact partition assignments.

**Comet's approach:**

Instead of true round-robin assignment, Comet implements round-robin as hash partitioning on ALL columns. This
achieves the same semantic goals:

- **Even distribution**: Rows are distributed evenly across partitions (as long as the hash varies sufficiently -
  in some cases there could be skew)
- **Deterministic**: Same input always produces the same partition assignments (important for fault tolerance)
- **No semantic grouping**: Unlike hash partitioning on specific columns, this doesn't group related rows together

The only difference is that Comet's partition assignments will differ from Spark's. When results are sorted,
they will be identical to Spark. Unsorted results may have different row ordering.
