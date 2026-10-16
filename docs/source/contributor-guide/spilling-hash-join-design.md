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

# Spilling Shuffled Hash Join (Design Draft)

Status: implemented as `SpillingHashJoinExec` in `native/operators/src/spilling_hash_join/`, tracked by [#2545].

## Summary

Comet's native shuffled hash join is DataFusion's `HashJoinExec`, and its build side must fit in the task's
share of the memory pool. When it does not, the task fails. That is why `spark.comet.exec.forceShuffledHashJoin`
is off by default and has a build-size guard ([#6384]).

This document proposes a Comet operator, `SpillingHashJoinExec`, that turns the join into a hybrid hash join
when memory runs out. It splits both inputs by a hash of the join keys into buckets. Buckets that fit in memory
are joined right away, and the rest are spilled to disk and joined one bucket at a time afterwards. The operator
never implements join semantics itself. Every bucket is joined by a stock `HashJoinExec`, created from the join
Comet plans today, so all of DataFusion's join types, residual filters and null handling are reused unchanged.
While nothing spills, the operator runs exactly today's `HashJoinExec` on exactly today's inputs.

The new join is enabled by default (`spark.comet.exec.hashJoin.spill.enabled`, default `true`), so every CI suite
that plans a native shuffled hash join runs it. Setting the key to `false` restores today's `HashJoinExec`. The
Spark plan node and its name do not change. The join applies to shuffled hash joins only, because broadcast build
sides are already bounded by the broadcast threshold.

## Why Comet builds its own

DataFusion has an open design for this, [apache/datafusion#24768], but there is no agreed approach yet:

- The epic proposes a hybrid hash join with 16 buckets, recursive re-splitting and Bloom filters, implemented
  inside `HashJoinExec`.
- Review asked for a sort-merge fallback first. That fallback is [apache/datafusion#25217], open since 2026-09-12
  and waiting on reviewers.
- A refactor of the hash join stream ([apache/datafusion#26227]) is in flight. There is also a proposal to build
  spilling as a separate, experimental stream.

Comet's case is much simpler than the general DataFusion problem:

| DataFusion has to handle                                                     | Comet                                                                                                                                                               |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CollectLeft` builds shared by many probe streams                            | Every shuffled hash join is `PartitionMode::Partitioned` (`planner.rs:2792`), and each Spark task runs its own plan instance on one partition, so nothing is shared |
| Joins that promise their probe order to `EnforceSorting`                     | Comet does not run DataFusion's physical optimizer. The order contract exists only on the Spark side, where Comet decides eligibility (see [Ordering](#ordering))   |
| Dynamic filters coupled to the build                                         | Comet's join runtime filter is opt-in (`spark.comet.exec.join.dynamicFilter.enabled`, default `false`) and can be left off for spilling joins                       |
| Null-aware (`NOT IN`) anti joins, which need global knowledge of build nulls | These are broadcast joins in Spark. Comet plans them as `CollectLeft` broadcast joins, which this design does not touch                                             |

So this design covers only Comet's case, and pieces of it can be offered upstream later. One candidate is the
separate experimental stream proposed on the epic.

## Background

### Today's failure

`HashJoinExec::collect_left_input` (DataFusion 55.2, `hash_join/exec.rs:2230`) reserves each build batch as it
arrives (`try_grow`, line 2268). After the last batch it reserves its estimate of the hash table (lines
2321-2338), about 19 bytes per build row with `u32` indices, on top of the batches. It then builds the table and
concatenates all build batches into one (line 2365). That copy is not reserved, so the process briefly
holds twice the build batches. Either `try_grow` can be refused. The error reaches the JVM as a
`CometNativeException` saying `Additional allocation failed for HashJoinInput[0] ...`, and the task fails.

Spark's own `ShuffledHashJoinExec` cannot spill either. `HashedRelation` is built against the task memory manager
and fails when it is short. Spark avoids this by planning a shuffled hash join only when asked to, by a hint or
`spark.sql.join.preferSortMergeJoin=false`, and when the build side is small and much smaller than the other
side (`canBuildLocalHashMapBySize` and `muchSmaller`). Under AQE it also plans one when every partition is under
`spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`. `forceShuffledHashJoin` is what takes Comet past
those limits.

### Comet facts the design depends on

- **Memory pool.** In off-heap mode the pool is `fair_unified` (default) or `greedy_unified`, and both refuse
  with `ResourcesExhausted`. A refusal has to be matched through `find_root()` and not as "any error", because
  a failed Spark acquire surfaces as `External`. On-heap mode does no accounting and is for tests only, so a
  spilling join cannot be triggered by the pool there.
- **Fair-pool shares.** `CometFairMemoryPool::try_grow` (`fair_pool.rs:157`) limits each consumer to
  `pool_size / registered consumers`, whether or not the consumer can spill. The operator must register one
  consumer, as `HashJoinInput` does today, and must not register one per bucket or per inner join.
- **Spill files.** DataFusion's `SpillManager` writes Arrow IPC files under Spark's local directories. Files
  are limited by `spark.comet.maxTempDirectorySize` and compressed with
  `spark.comet.exec.spill.compression.codec`. Temp files are deleted when they are dropped, so a cancelled task
  cleans up.
- **Batch ownership.** JVM batches are imported by reference count with no buffer reuse
  (`operators/scan.rs:187`), so holding input batches across polls is safe.
- **Plan shape.** DataFusion builds on the left. For `BuildRight`, Comet calls `swap_inputs`
  (`planner.rs:2833`), which can add a `ProjectionExec` above the join to restore the column order.

## Design

### The property that makes this simple

Let the join have keys `K`, join type `J` (inner, left, right or full outer, semi, anti, or existence) and an
optional residual filter `F`. Split each side by `bucket(h(K))` for a hash function `h`. Then, for every `J`:

```text
join_J(build, probe)  =  ⊎ over buckets b of  join_J(build_b, probe_b)      (as multisets)
```

Two rows can match only if their keys are equal, and equal keys have equal hashes, so every matching pair is
inside one bucket. Whether a row is "matched" (outer, semi, anti and mark joins) depends only on pairs that
include it, and those are all in its bucket. A row with a null key matches nothing under
`NullEqualsNothing`, which is what Comet uses. It lands in exactly one bucket and is emitted there as unmatched
exactly once. The residual filter only narrows the candidate pairs, all of which are in the same bucket. The
one join this does not hold for is the null-aware anti join, whose result depends on whether any build row
anywhere is null. It is excluded.

As a result, `SpillingHashJoinExec` never needs to know what a join type means. It routes rows to buckets and
schedules stock `HashJoinExec` instances, each created with `with_new_children` from the `HashJoinExec` Comet
builds today, already swapped for `BuildRight`. The probe-side helpers that a hand-written join would need
(`build_batch_from_indices`, `adjust_indices_by_join_type`, `apply_join_filter_to_indices`) are `pub(crate)`
in DataFusion 55.2, so the alternative would be copying them into Comet.

### States

```text
             build input
                 │
                 ▼
          ┌─────────────┐  all of build fits, plus table estimate
          │  Buffering  │─────────────────────────────────────────────┐
          └─────────────┘                                             ▼
                 │ first refused reservation               ┌────────────────────┐
                 ▼                                         │ InMemory: today's  │
          ┌─────────────┐  bucket by h(K) top bits,        │ HashJoinExec over  │
          │  Bucketed   │  destage the largest bucket       │ buffered batches + │
          │  build      │  on every refusal                 │ live probe stream  │
          └─────────────┘                                  └────────────────────┘
                 │ end of build
                 ▼
          ┌─────────────┐  resident rows → HashJoinExec(resident buckets)
          │  Resident   │  spilled-bucket rows → probe spill file of their bucket
          │  probe      │  then drain whatever probe input the inner join did not read
          └─────────────┘
                 │
                 ▼
          ┌─────────────┐  per spilled bucket: fits → HashJoinExec(build.Bi, probe.Bi)
          │  Spilled    │                     too big → re-split both files on the next bits
          │  pairs      │                     cannot shrink → see Skew
          └─────────────┘
```

### Fast path: nothing spills

The operator pulls the build stream and reserves each batch, as `collect_left_input` does. At end of input it
reserves everything the inner join will allocate (see [Memory accounting](#memory-accounting)). If that succeeds,
it creates the inner `HashJoinExec` over the buffered batches and the real probe stream, passed through
unchanged. Output, order and work are the same as today's join.

The inner join runs with a private `TaskContext`. Its memory pool is a fixed-size sub-pool funded by the
operator's own reservation, so:

- the real pool sees one consumer, the operator's;
- the inner join cannot be refused because another task took memory between the check and its `try_grow`;
- if the estimate is too low and the sub-pool refuses, the operator still holds the batches (shared references,
  no extra memory) and can switch to bucketed mode instead of failing.

The real build and probe children are executed with the real `TaskContext`, so operators below the join (scans,
another join, an aggregate) keep using the real pool. Only the inner `HashJoinExec` sees the sub-pool.

### Bucketed build

On the first refused reservation:

1. Hash the key columns of the buffered batches with `create_hashes` and a dedicated seed. The seed is
   independent of the inner join's table seed and of Spark's Murmur3 shuffle partitioning, which already
   grouped this task's keys.
2. Route rows by the top 4 bits into 16 buckets. Each resident bucket is kept by an Arrow `BatchCoalescer`
   (`push_batch_with_filter`), so buckets hold full-size batches instead of 16 slivers of every input batch.
3. Destage the largest buckets to per-bucket `SpillManager` files until the reservation fits again. Choosing
   the largest bucket also means a hot key's bucket goes first.
4. Route every later build batch the same way: resident rows are reserved, and rows of spilled buckets are
   appended to their files. Each further refusal destages the largest resident bucket.

Partitioning a batch needs a batch's worth of memory before the original can be dropped. The operator keeps a
small headroom reservation for this from the first batch onwards, sized from the largest batch seen. This is
the lesson of DataFusion's `sort_spill_reservation_bytes`: memory needed to react to pressure has to be held
before the pressure arrives.

At end of build, the operator reserves the table estimate for the resident buckets, destaging more buckets if
needed, and starts the inner join over the resident buckets.

### Resident probe

Each probe batch is hashed the same way:

- Rows of resident buckets go to the inner join.
- Rows of spilled buckets are appended to `probe.Bi` through a per-bucket coalescer. When memory is short, the
  coalescers are flushed to disk, which always frees memory, so the probe phase cannot fail for lack of memory
  beyond its headroom.

**Trap:** `HashJoinStream` stops reading the probe side when the build side is empty and the join type cannot
produce rows from probe rows alone (`hash_join/stream.rs:526-536`). If every bucket spilled, the resident join
has an empty build and would leave the probe input unread, losing the probe rows of every spilled bucket. So
once the inner join ends, the operator drains the rest of the probe input itself, writing spilled-bucket rows
and dropping resident ones. Dropping them is correct, because the inner join only stops early when those rows
can produce no output.

### Spilled pairs

Spilled buckets are joined one at a time, after the resident join has released its memory. For bucket `i`, the
operator knows the rows of `build.Bi` and the memory they hold once read back. It reserves that memory plus the
table estimate, and on success runs a new inner join over two spill-file streams (`read_spill_as_stream`). If the
reservation is refused, both files are split into 16 children on the next 4 bits of the same hash. The hash is
recomputed from the key columns, which costs less than storing 8 bytes per spilled row. The children are
processed depth first, so a parent's files are deleted before the children's are created. Four levels cover
16^4 times the per-task share.

If a re-split leaves the largest child at about the size of its parent, the bucket is one key's rows, and no
amount of re-splitting will separate them. See [Skew](#skew).

Spilled pairs are a natural recovery point: the inputs are files that can be read again, and a hash join emits
nothing until its build completes. A refused build can therefore always be retried another way.

### Null keys

All null-key rows hash alike, so they land in one bucket that re-splitting cannot shrink. They are still joined
correctly: the [chunked fallback](#skew) handles that bucket like any other that cannot shrink. A null key
matches nothing, so a later optimization can skip the probe replays for these rows:

- Null-key probe rows can be forwarded to the resident join, which emits them as unmatched where the join type
  requires that.
- Null-key build rows matter only for join types that emit unmatched build rows (full outer, and left or right
  outer when the build side is the outer side). They can be joined in fixed-size chunks against an empty probe
  stream, each chunk emitting its own unmatched rows.

This matters because many null keys on the outer side of an outer join is the most common way a real query
fills one bucket with "one key".

### Skew

One non-null key whose build rows exceed the budget cannot be split by hashing either. Such a pair is joined in
chunks. The build file is read in pieces that fit, and each piece is joined with a full replay of the pair's
probe file, again by a stock `HashJoinExec`. Memory stays bounded. The cost is one read of the probe file per
chunk, which is small next to the output that such a key produces.

Chunking is exact for every join type, once probe rows that can match in more than one chunk are tracked. With
DataFusion's join types after Comet's swap, where the build side is the left input:

| Join type                                           | Each chunk runs                                                                         | After the last chunk                                            |
| --------------------------------------------------- | --------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| `Inner`, `Left`, `LeftSemi`, `LeftAnti`, `LeftMark` | the join itself, because each build row is in one chunk and meets every probe row there | nothing                                                         |
| `Right`                                             | `Inner`, recording which probe rows matched                                             | emit the probe rows that never matched, with null build columns |
| `Full`                                              | `Left`, recording which probe rows matched                                              | as for `Right`                                                  |
| `RightSemi`                                         | `RightSemi`, emitting only rows not emitted before                                      | nothing                                                         |
| `RightAnti`                                         | `RightSemi`, recording which probe rows matched                                         | replay the probe file and emit the rows that never matched      |
| `RightMark`                                         | `RightSemi`, recording which probe rows matched                                         | replay the probe file and emit every row with its mark          |

A probe row is identified by its position in the replay, which is the same for every chunk. The replayed
batches carry it as an appended row-number column, which is dropped from the output. The matched set is a
bitmap with one bit per probe row of the pair, reserved like everything else. A residual filter needs nothing
extra, since only pairs that pass it count as matches.

Two other fallbacks were considered. Role reversal builds on the probe file when that is the small side, but
then the replays that chunking costs are cheap anyway. A sort-merge join for the pair would bring in the
sort-merge join costs listed under [Alternatives considered](#alternatives-considered).

### Ordering

Spark's `ShuffledHashJoinExec` reports its streamed side's ordering for most join types (Spark 4.1.3
`ShuffledHashJoinExec.scala:58`, `HashJoin.scala:90`). The exceptions are full outer joins and outer joins whose
outer side is the build side. `CometHashJoinExec` copies that ordering (`operators.scala:3128`), and
`EnsureRequirements` and `RemoveRedundantSorts` may already have relied on it. Rows from spilled buckets are
emitted after the resident pass, so a spilling join does not keep its probe order.

So a join is eligible only when Spark's join reports no output ordering. That is the usual case, because a
shuffled join's streamed child is normally an exchange. It is not the case when the streamed side is already
sorted below the join with no exchange in between, for example the output of another sort-merge join on the
same keys or a sorted bucketed scan. Those joins stay on today's non-spilling join. With [#6785], which plans
the forced hash join before `EnsureRequirements`, this check sees the final plan's orderings. On the DataFusion
side the operator reports `maintains_input_order = [false, false]`.

### Runtime filters

An inner join's dynamic filter would describe only the buckets it holds. Pushing it into the probe scan would
drop the rows of every spilled bucket. Inner joins are therefore always created without dynamic filters, and a
join with `spark.comet.exec.join.dynamicFilter.enabled` set uses either the filter or spilling, not both. A
filter built from every build row, resident or spilled, can come later. Min/max bounds and key domains can be
accumulated during routing.

### Memory accounting

The rule is to reserve, as far as it can be known, every byte the join holds, including bytes that today's join
holds without reserving them:

- One consumer, registered as spillable. Every inner join runs in a sub-pool funded by that consumer's
  reservation, as described above.
- For each inner join, the operator reserves the inner join's own reservations: its build batches, the hash
  table estimate from the same `estimate_memory_size` call, and the visited bitmap for join types that need one.
- It also reserves the copy that `collect_left_input` makes when it concatenates the build batches, which
  DataFusion does not reserve today. The copy is reserved for the duration of the inner build and released
  when the inner join first reads its probe input, by which point `collect_left_input` has dropped the
  originals.
- Also reserved: two batches of read-ahead for each open spill stream, the per-batch hash and routing scratch
  (the headroom, twice the largest input batch), and the chunked fallback's matched bitmap.
- A spilled bucket's build batches hold more memory once read back than when they were written, as
  `HashJoinExec` counts them. A batch read from an uncompressed file shares the buffer its IPC message was read
  into, metadata included, so the file's size bounds it. A batch read from a compressed file holds its
  decompressed buffers and also the buffer of its compressed message, because buffers too small to compress are
  stored as they are and read without a copy. The operator reserves the larger of the in-memory and the file
  size for an uncompressed file, their sum for a compressed one, and 64 bytes a batch for rounding.
- An inner join that reserves more than its budget still gets the memory from the task's pool, under the
  operator's consumer, and the excess shows in the `budget_overflow_bytes` metric. The tests assert that it stays
  zero.

A consequence is that a build side which today's join only just fits in memory now spills. That is intended:
the memory was in use either way, and only the accounting changes.

### Metrics

The operator reports today's join metric names, so `CometMetricNode.joinMetrics` works unchanged. Time
(`build_time`, `join_time`, `elapsed_compute`) and the `probe_hit_rate` and `avg_fanout` ratios add up across the
inner joins and are theirs. Rows, batches and memory are the operator's own, because the inner joins see only
some rows, the chunked fallback replays probe rows, and its final pass emits rows no inner join emits:

- `build_input_rows`, `build_input_batches`, `input_rows` and `input_batches` count what the operator reads. A
  plain `HashJoinExec` also counts its final pass over the build rows as probe input; the operator does not.
- `output_rows` and `output_batches` count what it emits.
- `build_mem_used` is the most memory it has reserved at once.

It adds DataFusion's spill metrics (`spill_count`, `spilled_bytes`, `spilled_rows`), which Comet already feeds
into the task's spill totals, and `spilled_buckets`, `split_buckets`, `chunked_buckets` and
`budget_overflow_bytes`.

### Spill files

DataFusion's writer for an open spill file, `InProgressSpillFile`, cannot be named outside DataFusion, and the
operator keeps one open file per spilled bucket. It writes Arrow IPC streams itself through the public
`SpillFile::open_writer`, with DataFusion's alignment and compression and with view arrays compacted first, and
reads them back with `SpillManager::read_spill_as_stream`. The writer counts toward
`spark.comet.maxTempDirectorySize`.

### Naming

The Spark plan keeps `CometHashJoinExec`, which explain output shows as `CometHashJoin`, so explain output and
the plan-stability golden files do not change. The native operator also displays as the `HashJoinExec` it runs,
so native explain output does not change either. Whether a join spilled shows in its metrics.

### Configuration

| Key                                                   | Default | Purpose                                                                                                                                                                         |
| ----------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.comet.exec.hashJoin.spill.enabled`             | `true`  | Run eligible shuffled hash joins with the spilling join. `false` restores today's `HashJoinExec`                                                                                |
| `spark.comet.exec.hashJoin.spill.maxBuildBufferBytes` | `0`     | Spill once the build side buffers this many bytes, even if the pool would grant more. `0` means spill only when the pool refuses. Tests set it low to force every join to spill |

The second key follows `spark.comet.shuffle.native.maxBufferBytes`. It is also how the test suites run every
join through the spilling path, a lesson from DuckDB's `force_external` mode.

## Alternatives considered

- **Wait for DataFusion.** No agreed design yet, and Comet would also have to wait for a DataFusion release.
- **Sort-merge fallback for the whole join** ([apache/datafusion#25217]'s approach). This is simpler, but a
  build side 5% over budget pays for two full external sorts. DataFusion's sort-merge join also has known costs
  in Comet: high fan-out output ([apache/datafusion#26277]), and mark joins that buffer the whole outer side. It
  remains a candidate fallback for one skewed bucket.
- **Plan-time choice only.** [#6384] already keeps the sort-merge join for build sides over a size limit. It
  cannot see the task's memory share at run time, and an estimate can be wrong, so it complements spilling
  rather than replacing it.
- **Fork `HashJoinStream` into Comet.** This gives full control, but means owning around 3,000 lines of join
  semantics and keeping them in step with DataFusion.

## Testing

- **Rust, differential.** Run `SpillingHashJoinExec` and plain `HashJoinExec` on the same inputs and compare the
  outputs as multisets. Cover every join type, with and without a residual filter, null keys on either side,
  empty sides, duplicate-heavy keys, and forced spilling at levels 0, 1 and 2. Small `maxBuildBufferBytes` values
  make the recursion reachable with small data.
- **Rust, failure paths.** Check that refusals are matched as `ResourcesExhausted`, that a Spark acquire error
  (`External`) is returned and not treated as a refusal, and that temp files are deleted on cancellation and on
  error.
- **Scala.** Run `CometJoinSuite` a second time with spilling forced for every eligible join. Add eligibility
  tests: no spilling when Spark reports an ordering, when the runtime filter is on, or for broadcast joins.
- **Queries.** Run TPC-H and TPC-DS correctness with spilling forced. Benchmark under stepped memory budgets
  against the sort-merge join: the failure today, the cost of spilling, and no change when nothing spills.

## Plan

1. **Baseline.** `CometHashJoinMemoryBenchmark` sweeps native memory budgets over several join shapes. It
   shows where today's hash join fails and the sort-merge join completes, and what each costs on either side of
   that point. Measure hash join against sort-merge join on TPC-H and TPC-DS too, with the existing
   `comet-hashjoin` engine config.
2. **First version** (implemented). One PR, one commit per part:
   - the operator with the fast path, bucketed build, resident probe, spilled pairs, re-splitting and the
     chunked fallback;
   - planner wiring, configs (on by default) and the eligibility checks;
   - metrics, tests and docs.
3. **Hardening.** Null-key routing, Bloom filters for spilled buckets, and prefetching the next pair. A
   Bloom-negative probe row can simply be forwarded to the resident join.
4. **Defaults.** Build a runtime filter over all build rows. Decide whether `forceShuffledHashJoin` can become
   the default, together with [#6785].
5. **Upstream.** Offer the operator, or its bucketing and scheduling, to [apache/datafusion#24768].

## Open questions

1. 16 buckets per level, or a wider fan-out such as 32 or 64? That trades open files and small batches
   against recursion depth.
2. With spilling on, `forceShuffledHashJoin.maxBuildSize` no longer prevents failures, only spills. Should its
   default limit be lifted when the new join is enabled, keeping an explicit setting?

[#2545]: https://github.com/apache/datafusion-comet/issues/2545
[#6384]: https://github.com/apache/datafusion-comet/pull/6384
[#6785]: https://github.com/apache/datafusion-comet/pull/6785
[apache/datafusion#24768]: https://github.com/apache/datafusion/issues/24768
[apache/datafusion#25217]: https://github.com/apache/datafusion/pull/25217
[apache/datafusion#26227]: https://github.com/apache/datafusion/pull/26227
[apache/datafusion#26277]: https://github.com/apache/datafusion/pull/26277
