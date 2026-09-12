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

`WindowGroupLimitExec` (window-based limit pushdown for `ROW_NUMBER`, `RANK`, and `DENSE_RANK`)
runs natively; it is controlled by `spark.comet.exec.windowGroupLimit.enabled` (default: true).

**Falls back to Spark:**

- Any `PARTITION BY` or `ORDER BY` key whose type carries a non-default `StringType` collation
  (e.g. `UTF8_LCASE`). The native operator detects partitions and order-key peer groups by
  comparing Arrow row-encoded keys for byte equality, which splits peers that Spark ties.

**Known incompatibilities:**

- Signed-zero ordering (`-0.0` vs `+0.0`) diverges from Spark's `RankLimitIterator`; see
  [floating-point ordering](./floating-point.md#ordering-signed-zero-00-vs-00).

## MERGE INTO (MergeRowsExec)

Comet can run `MergeRowsExec` (Spark's row-level `MERGE INTO` dispatch operator) natively on
Spark 3.5.x and Spark 4.0.x, but it is disabled by default. Enable it with
`spark.comet.exec.mergeRows.enabled=true`.

Spark 4.1+ intentionally falls back to Spark even when that flag is enabled. Starting in Spark
4.1, the V2 existing-table writer locates the concrete Spark `MergeRowsExec`, builds a
`MergeSummary` from its row-level metrics, and passes that summary to the summary-aware
`BatchWrite.commit` overload. Replacing the node with `CometMergeRowsExec` would make summary
discovery fail and silently switch the data source to the legacy summary-less commit overload.
Comet will keep Spark 4.1+ `MERGE` on the JVM until it can preserve that writer contract end-to-end.

**Undeclared physical output order can differ from Spark:** native execution is set-at-a-time. Within
an input batch it emits rows grouped by the MERGE instruction that produced them, and it processes
the MATCHED, NOT MATCHED, then NOT MATCHED BY SOURCE groups. Spark's row-at-a-time implementation
emits rows in input order. This is not a MERGE row-value semantic difference: an unordered table
scan has no row-order guarantee. Downstream V2 write planning still enforces every distribution or
ordering requirement declared by the writer; only a writer that declares no ordering requirement
can persist the same rows in a different physical sequence.

**Failure precedence can differ from Spark on rare inputs:** Spark consumes joined rows one at a
time. For each row it determines the MERGE group, validates cardinality when required, and walks
that row's instruction list until the first clause fires. Comet intentionally vectorizes this work:
it validates cardinality for the input batch, then evaluates each instruction over the remaining
rows of the MATCHED, NOT MATCHED, and NOT MATCHED BY SOURCE groups. Successful deterministic row
results preserve Spark semantics, including first-match-wins within a row, but the two evaluation
orders are not identical when more than one row in the same Arrow batch would fail.

For example, Spark may encounter an ANSI cast failure on an earlier input row before reaching a
later row whose earlier MERGE clause divides by zero, while Comet can evaluate that earlier clause
across the whole group and report `DIVIDE_BY_ZERO` first. The same ordering difference can occur
between different MERGE groups, between the two projections of a `Split`, or between a cardinality
violation and an unrelated clause-evaluation error. In these cases both engines reject the query,
but the surfaced Spark error condition can differ. This limitation only applies to Spark versions
where native `MergeRowsExec` is enabled.

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
