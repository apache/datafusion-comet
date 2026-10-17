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

| DataFusion has to handle                                                     | Comet                                                                                                                                                                   |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CollectLeft` builds shared by many probe streams                            | Every shuffled hash join is `PartitionMode::Partitioned` (`planner/join.rs:122`), and each Spark task runs its own plan instance on one partition, so nothing is shared |
| Joins that promise their probe order to `EnforceSorting`                     | Comet does not run DataFusion's physical optimizer. The order contract exists only on the Spark side, where Comet decides eligibility (see [Ordering](#ordering))       |
| Dynamic filters coupled to the build                                         | Comet's join runtime filter is opt-in (`spark.comet.exec.join.dynamicFilter.enabled`, default `false`) and can be left off for spilling joins                           |
| Null-aware (`NOT IN`) anti joins, which need global knowledge of build nulls | These are broadcast joins in Spark. Comet plans them as `CollectLeft` broadcast joins, which this design does not touch                                                 |

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
  (`planner/join.rs:160`), which can add a `ProjectionExec` above the join to restore the column order.

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
2. Route rows by the top 4 bits into 16 buckets, with one `take` per bucket. A resident bucket keeps the parts as
   they are, about 512 rows of every 8192-row input batch, because its inner join concatenates its build batches
   anyway.
3. Destage the largest buckets to per-bucket spill files until the reservation fits again. Choosing the largest
   bucket also means a hot key's bucket goes first. A destaged bucket also gets a filter of its build keys, see
   [Probe filters](#probe-filters).
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

- Rows of resident buckets go to the inner join, and so do rows with a null key and rows that the filter of their
  spilled bucket rejects. Each of those matches nothing, and the inner join emits it as the join type requires.
- The other rows of spilled buckets are appended to `probe.Bi`. A spill file's writer holds what it is given until
  it makes a full batch (see [Spill files](#spill-files)), and writes it at once when memory is short, so the probe
  phase cannot fail for lack of memory beyond its headroom.

**Trap:** `HashJoinStream` stops reading the probe side when the build side is empty and the join type cannot
produce rows from probe rows alone (`hash_join/stream.rs:526-536`). If every bucket spilled, the resident join
has an empty build and would leave the probe input unread, losing the probe rows of every spilled bucket. So
once the inner join ends, the operator drains the rest of the probe input itself, writing spilled-bucket rows
and dropping resident ones. Dropping them is correct, because the inner join only stops early when those rows
can produce no output.

### Spilled pairs

Spilled buckets are joined one at a time, after the resident join has released its memory. For bucket `i`, the
operator knows the rows of `build.Bi` and the memory they hold once read back. It reserves that memory plus the
table estimate, and on success runs a new inner join over the bucket's build rows and a stream of its probe file
(`read_spill_as_stream`). If the reservation is refused, both files are split into 16 children on the next 4 bits
of the same hash. The hash is recomputed from the key columns, which costs less than storing 8 bytes per spilled
row. The children are processed depth first, so a parent's files are deleted before the children's are created.
Four levels cover 16^4 times the per-task share.

`HashJoinExec` concatenates its build batches into one copy, and holds the batches and the copy at once while it
builds its table. Given a single batch it keeps the batch as it is, because arrow's `concat` returns a lone array
unchanged. So when every build column is flat, fixed-width, boolean, string or binary, the build file is read back
one batch at a time into buffers allocated once at their final size, from the value bytes its writer recorded for
each column, and the inner join gets that one batch. The bucket then needs about its own size in memory, not twice
that. A build side with nested, dictionary or view columns is read back compacted, batch by batch, and its copy is
reserved.

If a re-split leaves the largest child at about the size of its parent, the bucket is one key's rows, and no
amount of re-splitting will separate them. See [Skew](#skew).

Spilled pairs are a natural recovery point: the inputs are files that can be read again, and a hash join emits
nothing until its build completes. A refused build can therefore always be retried another way.

### Probe filters

A probe row of a spilled bucket is written to disk and read back, only to find no match, whenever no build row of
the bucket has its key. Such a row matches nothing anywhere, so the resident join, where no build row has its key
either, can take it instead and emit it exactly as the bucket's join would, for every join type. In the benchmark,
half of the probe rows are like that.

When a bucket is destaged, the operator hashes its build rows into a Bloom filter, and adds every later build row
of the bucket. During the resident probe, a probe row that its bucket's filter rejects goes to the inner join.

- The lookup has to cost far less than writing and reading a row. A key sets four bits in one 64-bit word, taken
  from bits of the routing hash that routing at the first level leaves unused, so a lookup is one load and one
  comparison. A first version with Parquet's split block Bloom filter cost about 9 ns a lookup, as much as it
  saved.
- A bucket's final size is not known when it is destaged, so its filter grows as a scalable Bloom filter does: a
  filter sized for twice the rows so far, at 10 bits a key, and another of twice the size whenever the newest is
  full. A key is looked up in all of them. The false positive rate is about 2% for each.
- The filters' memory is reserved. A filter the pool refuses to grow is dropped, because a filter missing a key
  would send that key's probe rows to the wrong join. A filter that rejects fewer than one in eight of its first
  64K lookups is dropped too, because then its lookups cost more than they save.
- `bloom_filtered_rows` counts the probe rows the filters kept off disk.

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

### Sort-merge fallback

A hash join pays for every match: a step along the chain of build rows with the probe row's key, and gathers
from wherever those rows are in the build batch. Once the build side outgrows the CPU cache, most of them miss. A
sort-merge join pays once to sort both sides, and then reads each key's matches in order. So where probe rows
match many build rows each, a hash join loses to a sort-merge join whether or not it spills. In TPC-DS q72 at
scale factor 100, the join of `catalog_sales` and `inventory`, on the item alone with a residual filter, matches
each probe row with hundreds of build rows: its tasks take 179 s on average as a sort-merge join, 678 s as a
spilling hash join and over 1,000 s as today's hash join. DataFusion tracks the hash join's cost in
[apache/datafusion#17494], and making each key's rows contiguous ([apache/datafusion#26447]) narrows the gap but
does not close it.

Spark's statistics cannot show this at planning time: without column statistics there are no distinct key
counts, and even those would not show how many probe rows match. The run does, through metrics that the inner
`HashJoinExec` keeps anyway: `avg_fanout`, whose numerator counts matches before the residual filter,
`input_rows`, `join_time` and `build_time`. An inner join switches at the first probe batch boundary where

- its probe rows have matched at least 16 build rows each on average,
- its probe has spent at least 20 ns on each match, which shows that the build side does not fit in the cache: in
  q72, a spilled pair whose build side fits takes about 10 ns a match, and the join of the buckets left in memory
  28 ns or more,
- and its probe has taken at least 4 times as long as its build. Sorting the build side costs about as much as
  building its hash table, so by then the hash join has spent more than switching costs, however much probe
  input is left. This is a rent-or-buy rule, which needs no estimate of the probe side's size.

The inner join reads its probe input through a handoff that checks the rule whenever the join asks for a batch.
When the rule holds, the handoff ends the input, the inner join finishes the batch it has, and a DataFusion
`SortMergeJoinExec` joins the build side with the rest of the probe input, which a `SortExec` sorts, reserving
from the task's pool and spilling. An inner join that may switch gets its build side as one batch, which is the
copy it would otherwise make itself, so that the sort-merge join can read the same batch. A `SortExec` could sort
that batch only by reserving twice its size at once, and could not spill it, so the operator sorts the batch's row
indices by the join keys instead, and the sort-merge join reads the rows in that order. The memory for that sort
is what the inner join's hash table, which it drops, had reserved, and the handoff reserves what the sort may need
beyond it before it switches. Without that memory the join carries on as a hash join, and tries again at the next
batch. After a switch, the spilled pairs that follow are joined by sort-merge joins too, with both sides read from
their files and sorted by `SortExec`s, and are neither split nor joined in chunks.

The sort-merge join takes the inner join's residual filter with its columns reordered. In DataFusion 55,
`SortMergeJoinExec` passes a filter the left side's columns and then the right side's, whatever order the filter
lists them in, and the filter of a `BuildRight` join, swapped along with its inputs, lists the right side's first.
The fallback lists the left side's first until Comet moves to a DataFusion release with [apache/datafusion#25489].

Only inner joins switch. Each probe row is joined by the hash join or by the sort-merge join, and an inner join
emits the same rows either way. The other join types keep a record of which rows matched, which a sort-merge join
could not take over in the middle of the probe input; they could switch at a spilled pair's boundary, before the
pair is probed, but do not yet.

### Ordering

Spark's `ShuffledHashJoinExec` reports its streamed side's ordering for most join types (Spark 4.1.3
`ShuffledHashJoinExec.scala:58`, `HashJoin.scala:90`). The exceptions are full outer joins and outer joins whose
outer side is the build side. `CometHashJoinExec` copies that ordering (`operators.scala:3128`), and
`EnsureRequirements` and `RemoveRedundantSorts` may already have relied on it. Rows from spilled buckets are
emitted after the resident pass, so a spilling join does not keep its probe order.

So a join is eligible only when nothing above it relies on its output order. Spark reports no order for most
shuffled joins, because a shuffled join's streamed child is normally an exchange. When the streamed side is already
sorted below the join with no exchange in between, for example the output of another sort-merge join on the same
keys or a sorted bucketed scan, Spark reports that order, but it matters only if an operator above uses it.
`CometExecRule` walks up from each shuffled hash join, after the forced hash join rewrite:

- An operator that requires a child ordering relies on the order, because `EnsureRequirements` may have left it
  without a sort.
- The root of the plan relies on it, because `RemoveRedundantSorts` may have dropped a sort that the order
  satisfied.
- An operator that reports no order, such as an exchange or a hash aggregate, or that sorts its input itself,
  ends the order, and so nothing above it can rely on it.

A join whose order nothing relies on is tagged, and its serde runs it with spilling. A spilling join reports no
output order. In TPC-H q21, the anti join of `NOT EXISTS` probes the output of the sort-merge join of `EXISTS` and
feeds a shuffle, so Spark reports an order for it that nothing uses, and it can spill. With [#6785], which plans the forced hash join before `EnsureRequirements`, this check sees the final plan's
orderings. On the DataFusion side the operator reports `maintains_input_order = [false, false]`.

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
- Also reserved: the parts that spill files' writers hold, the probe filters, two batches of read-ahead for each
  open spill stream, the per-batch hash and routing scratch, and the chunked fallback's matched bitmap. The
  scratch, the headroom, is three times the largest input batch: one copy while its rows are routed, and when a
  writer writes the parts it holds, one while they are concatenated and one while the result is encoded.
- A batch read back from a spill file can hold up to twice the memory it held when written, as `HashJoinExec`
  counts it. DataFusion 55.2 reads spill files in chunks of 128 KiB, and arrow's `StreamDecoder` assembles a
  message that spans chunks in a buffer that grows by doubling, then slices the batch's buffers from it. A small
  batch inside one chunk keeps the whole chunk.
  [apache/datafusion#24592] fixed both on DataFusion's main branch after 55.2, by reading each message into an
  allocation of its own size, and was not backported to 55. A batch read
  from a compressed file also keeps that buffer when any of its buffers was stored uncompressed. So the build
  side of a spilled bucket, which the inner join holds whole, is read back into one batch (see
  [Spilled pairs](#spilled-pairs)), or for nested, dictionary or view columns with every batch copied into buffers
  of its own size, dictionary values and view data included. Either way it is reserved at what it held when
  written, with 64 bytes a buffer for rounding. Each batch of read-ahead is reserved at twice the written size of
  the file's largest batch.
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
into the task's spill totals, and `spilled_buckets`, `split_buckets`, `chunked_buckets`, `sort_merge_joins` and
`budget_overflow_bytes`. The sorts of a sort-merge fallback report no metrics of their own yet.

### Spill files

DataFusion's writer for an open spill file, `InProgressSpillFile`, cannot be named outside DataFusion, and the
operator keeps one open file per spilled bucket. It writes Arrow IPC streams itself through the public
`SpillFile::open_writer`, with DataFusion's alignment and compression and with view arrays compacted first, and
reads them back with `SpillManager::read_spill_as_stream`. The writer counts toward
`spark.comet.maxTempDirectorySize`.

Routing hands a bucket's writer a part of about 512 rows of every input batch, and a split bucket's parts are
smaller again. Written as they come, every part costs an IPC message, an lz4 frame for each of its buffers, and
on reading back a decode and a probe batch of the inner join. So a writer holds its parts, reserved, until they
make a batch of the session's batch size or 256 KiB, and writes them as one batch. When the pool refuses the
memory for a part, the writer writes what it holds at once. With 16 writers on a side, the parts hold at most
4 MiB.

### Naming

The Spark plan keeps `CometHashJoinExec`, which explain output shows as `CometHashJoin`, so explain output and
the plan-stability golden files do not change. The native operator also displays as the `HashJoinExec` it runs,
so native explain output does not change either. Whether a join spilled shows in its metrics.

### Configuration

| Key                                                         | Default | Purpose                                                                                                                                                                                                      |
| ----------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `spark.comet.exec.hashJoin.spill.enabled`                   | `true`  | Run eligible shuffled hash joins with the spilling join. `false` restores today's `HashJoinExec`                                                                                                             |
| `spark.comet.exec.hashJoin.spill.maxBuildBufferBytes`       | `0`     | Spill once the build side buffers this many bytes, even if the pool would grant more. `0` means spill only when the pool refuses. Tests set it low to force every join to spill                              |
| `spark.comet.exec.hashJoin.spill.sortMergeFallback.enabled` | `true`  | Let an inner join switch to a sort-merge join, see [Sort-merge fallback](#sort-merge-fallback). A testing key, `...sortMergeFallback.always`, switches every inner join after its first matching probe batch |

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
  make the recursion reachable with small data. Cover keys of decimal, date, timestamp, string view, binary and
  dictionary type, payloads of list, struct, large string, string view, dictionary, wide decimal, fixed-size
  binary and boolean type, and rows wide enough that spilled batches span DataFusion's read chunks.
- **Rust, failure paths.** Check that refusals are matched as `ResourcesExhausted`, that a Spark acquire error
  (`External`) is returned and not treated as a refusal, and that temp files are deleted on cancellation and on
  error.
- **Scala.** Run `CometJoinSuite` a second time with spilling forced for every eligible join. Add eligibility
  tests: no spilling when Spark reports an ordering, when the runtime filter is on, or for broadcast joins.
- **Queries.** Run TPC-H and TPC-DS correctness with spilling forced. Benchmark under stepped memory budgets
  against the sort-merge join: the failure today, the cost of spilling, and no change when nothing spills.

## Status

The first version is a research draft, [#7057]. It has the operator with all of its paths, the planner wiring,
the configs and eligibility checks, the metrics, tests, `CometHashJoinMemoryBenchmark` and the user docs. A second
round made spilling cheaper: spill files hold full batches instead of slivers, a spilled bucket's build side is read
back into one batch that its inner join keeps without a copy, and a Bloom filter of each spilled bucket's build keys
keeps probe rows that match nothing off disk.

`CometHashJoinMemoryBenchmark` on an otherwise idle AMD 7950X3D, pinned to its eight V-cache cores, median of three
runs, wall time and process CPU time. Before is the first version, after is the second round:

| Inner join, median ms / CPU s | Comet sort-merge join | Hash join, no spilling | Before    | After     |
| ----------------------------- | --------------------- | ---------------------- | --------- | --------- |
| 1024 MiB                      | 1051 / 4.4            | 641 / 2.7              | 626 / 2.5 | 637 / 2.6 |
| 256 MiB                       | 1198 / 5.3            | 632 / 2.5              | 617 / 2.6 | 621 / 2.5 |
| 192 MiB                       | 1152 / 5.1            | 617 / 2.4              | 730 / 3.2 | 707 / 3.2 |
| 128 MiB                       | 1161 / 5.1            | fails                  | 864 / 3.8 | 825 / 3.4 |
| 96 MiB                        | 1134 / 5.0            | fails                  | 873 / 4.0 | 793 / 3.2 |
| 64 MiB                        | 1152 / 5.2            | fails                  | 889 / 3.9 | 787 / 3.2 |

At 64 MiB, for every join shape:

| 64 MiB, median ms / CPU s                        | Comet sort-merge join | Before     | After     | Join spill MiB, before / after |
| ------------------------------------------------ | --------------------- | ---------- | --------- | ------------------------------ |
| Inner                                            | 1152 / 5.2            | 889 / 3.9  | 787 / 3.2 | 232 / 144                      |
| Left outer, outer side probed                    | 1144 / 5.2            | 918 / 4.1  | 796 / 3.2 | 232 / 144                      |
| Right outer, outer side built                    | 1176 / 5.3            | 883 / 4.0  | 783 / 3.2 | 232 / 144                      |
| Full outer                                       | 1149 / 5.2            | 919 / 4.0  | 797 / 3.3 | 232 / 144                      |
| Left anti                                        | 961 / 4.3             | 539 / 2.2  | 514 / 2.2 | 40 / 20                        |
| Inner, a quarter of the build rows share one key | 1348 / 5.3            | 1103 / 3.9 | 884 / 3.1 | 239 / 135                      |

Spark's own sort-merge join, which the native budget does not limit, takes 2.3 to 2.8 seconds and 9 to 11 CPU
seconds for the same joins.

- The join completes for every join shape at every native memory budget, including where today's hash join
  fails.
- While the build side fits, it costs the same as today's hash join.
- Reserving the concatenated copy moves the spill point. In the 192 MiB case today's join fits and this one
  spills.
- When it spills, it is faster than Comet's sort-merge join in wall time and CPU time, at every budget and for
  every join shape. The first version already was, on an idle machine. The rough numbers in the first draft came
  from a machine that another benchmark was also using.

The native benchmark, `cargo bench --bench spilling_hash_join`, runs one task's join of the JVM benchmark over
in-memory batches: 1 million build rows and 5 million probe rows. Pinned to the V-cache cores, mean of two runs:

| Native benchmark, ms | First version | Second round | Change |
| -------------------- | ------------- | ------------ | ------ |
| 64 MiB, lz4          | 308           | 259          | -16%   |
| 40 MiB, lz4          | 339           | 260          | -23%   |
| 20 MiB, lz4          | 359           | 275          | -24%   |
| 10 MiB, lz4          | 1733          | 277          | -84%   |
| 20 MiB, zstd         | 559           | 368          | -34%   |
| 20 MiB, uncompressed | 283           | 233          | -18%   |

With no memory limit both versions take 77 or 103 ms, depending on which cores the run lands on. At 10 MiB the
first version split every bucket again, and its spill files held slivers of 32 rows. Now no bucket is split,
because a spilled bucket's inner join gets one build batch and needs half the memory. Elsewhere, writing full
batches gives most of the gains, and the probe filters keep 1.4 to 2.3 million of the 5 million probe rows off
disk.

TPC-H at scale factor 100 ran on the same machine, as a Spark 4.1.1 standalone cluster of two executors with 8
cores each, with AQE on and one run of each query. It compared Comet's sort-merge join with the forced shuffled hash
join (`spark.comet.exec.forceShuffledHashJoin`, with `maxBuildSize` set to `-1`), with and without spilling, at
three sizes of the off-heap pool. Total time of the 22 queries:

| Off-heap pool per executor, shuffle partitions | Sort-merge join           | Spilling hash join      | Hash join, no spilling |
| ---------------------------------------------- | ------------------------- | ----------------------- | ---------------------- |
| 16 GiB, 200                                    | 255.5 s                   | 232.8 s                 | 232.5 s                |
| 2 GiB, 32                                      | 268.0 s, 12 tasks retried | 242.2 s, 1 task retried | q21 fails              |
| 1 GiB, 16                                      | 272.1 s, 11 tasks retried | q21 fails               | q9, q21 and q22 fail   |

- Every query that completed returned the same rows as with the sort-merge join, by row count and a hash of its
  sorted rows.
- With 16 GiB nothing spills, and the spilling join costs nothing over the join without spilling.
- With 2 GiB, q9 and q21 spill 29 and 384 buckets, 0.11 and 3.84 GB, and every query completes. q21, which the join
  without spilling fails, takes 33.5 s, against 36.7 s with the sort-merge join.
- With 1 GiB, q9, q10, q17 and q22 spill, and the probe filters keep 17 million of q10's probe rows off disk. Every
  query but q21 completes, the 21 of them in 208.0 s, against 235.6 s with the sort-merge join. q21 fails because
  the join takes memory that a sort below it needs, see [Remaining work](#remaining-work).
- The joins without spilling fail for lack of memory for their build sides. Every other failed task, in every arm,
  was a sort refused memory for its merge (`ExternalSorterMerge`) while another consumer of its task, another sort
  or a spilling join, held most of the task's share. Spark retried them, and the retries succeeded, except in
  q21 with spilling.
- `budget_overflow_bytes` stayed 0 in every query.

TPC-DS at scale factor 100 ran on the same cluster, with 2 GiB and 32 shuffle partitions:

| TPC-DS, 2 GiB, 32 partitions  | Sort-merge join           | Spilling hash join        | Hash join, no spilling |
| ----------------------------- | ------------------------- | ------------------------- | ---------------------- |
| All 99 queries                | 974.8 s, 15 tasks retried | 2174.7 s, 3 tasks retried | q72 runs past 40 min   |
| The 98 queries other than q72 | 596.8 s                   | 564.3 s                   | 564.5 s                |

- Only q72 spills, 162 buckets and 1.45 GB.
- Every query returned the same rows in every arm, except the two statements of q39, whose `stddev_samp` and `avg`
  results differ in their last digits from run to run. Every row agrees within a relative 4e-16, and two runs of the
  sort-merge join differ from each other the same way.
- q72 is slow with either hash join. Its join of `catalog_sales` with `inventory`, on the item alone and with
  `inv_quantity_on_hand < cs_quantity` as its residual filter, produces 16.8 billion rows. The tasks of that stage
  take 179 s on average with the sort-merge join, 678 s with the spilling join, and over 1,000 s without spilling.
  So the cost is the hash join's and not spilling's: the spilling join, whose buckets have hash tables of their
  own, is the faster of the two.
- The retried tasks were again sorts refused memory for their merge.

A third round adds the [sort-merge fallback](#sort-merge-fallback) for joins like q72's: an inner join whose probe
rows match many build rows each switches to a sort-merge join for the rest of its probe input. 34 joins switch in
`CometJoinSortMergeFallbackSuite`. At scale factor 100, on the same cluster with 2 GiB and 32 shuffle partitions,
every TPC-H and TPC-DS query ran with the fallback on and off, alternating between the two per query, and then with
the fallback on and its threshold lowered from 30 to 20 ns a match:

| 2 GiB, 32 partitions                  | Fallback off | Fallback on, 30 ns | Fallback on, 20 ns |
| ------------------------------------- | ------------ | ------------------ | ------------------ |
| TPC-H, 22 queries                     | 244.3 s      | 244.1 s            | 244.0 s            |
| TPC-DS, the 98 queries other than q72 | 564.0 s      | 567.0 s            | 565.8 s            |
| TPC-DS q72                            | 1610 s       | 383.7 to 452 s     | 384.8 s, 380.6 s   |

q72 took 375.2 s with the sort-merge join on the same day. Its time without the fallback is the run of
[the TPC-DS table above](#status), which was not repeated.

- Only q72 switches. With 20 ns, all 32 of its joins of the buckets left in memory switch, and its 162 spilled pairs
  are joined by sort-merge joins, 194 in all. No join of any other query reaches 16 matches per probe row: the most
  among the 2,028 inner joins with at least 16K probe rows is 12.6.
- Every query returned the same rows in every arm, q72 included, except q39, as before.
- With 30 ns, a join of the buckets left in memory in one of q72's 32 tasks measured 28.4 ns a match, at 873 matches
  per probe row. It did not switch, and its task took 254 s, against 174 to 191 s for the tasks that switched, and q72
  took 452 s. Of three more runs, one switched every join and took 383.7 s, one missed a switch the same way, and in
  the other a join switched only after hashing for 156 s, so that its task took 242 s. With 20 ns, both runs switched
  every join after at most 2.4 s of hashing, and the slowest task of the join's stage took 192 and 188 s, against
  187 s with the sort-merge join.
- No other query got slower. None of them switches, and the short TPC-DS queries that differed by more than 6% in
  one run, q41, q43, q90 and q96, at 2 to 3 s, were within run-to-run noise when rerun three times, alternating.

## Remaining work

In priority order.

1. **Extend the sort-merge fallback.** At scale factor 100 it switches q72's joins and no others, and q72 runs
   about as fast as with the sort-merge join, see [Status](#status).
   - The fallback's sorts report no metrics of their own yet.
   - Only inner joins switch. The other join types could switch at a spilled pair's boundary, before the pair is
     probed.
   - The cost of a match depends on the CPU's cache as well as on the build side. The 20 ns threshold sits between
     the 10 ns of a q72 build side that fits in the 7950X3D's cache and the 28 ns or more of one that does not.
     Check it on a machine with a different cache, or derive it from the sizes of the build side and the cache.
2. **Leave memory for the operators that run after the build.** With a 1 GiB pool, TPC-H q21 fails with spilling,
   on every retry. Its anti join takes 124 MB of the task's 130 MB share for its build side. Then a sort below its
   probe side, for the sort-merge join of `EXISTS`, cannot get the 10 MiB it needs to merge. The join takes
   whatever the pool grants and holds its resident buckets until its probe input ends, and a retried task's join
   takes the larger share that the retry finds. In Comet's sort-merge join one sort starves another the same way,
   which is what its retried tasks were, but a sort's need is bounded, and those retries succeed.
   - `fair_unified` limits each consumer to `pool_size / registered consumers`, but `pool_size` is the executor's
     whole off-heap pool, while Spark limits each task to an even share of that pool among the running tasks. So
     the fair limit binds only in a task with more consumers than there are running tasks.
   - Making the pool the task's share is not enough. With `spark.comet.exec.memoryPool.fraction` set to `0.125`,
     one task's share with 8 cores, q21 completes in 34.9 s, but consumers that cannot spill hit their smaller
     limits. q16's broadcast hash join fails in both arms, and so does a sort's merge in q2 with the sort-merge
     join. q17 takes 33 s instead of 26 s in both arms.
   - Limiting only the consumers that can spill, to an even share of what the task's share leaves after the ones
     that cannot, as DataFusion's `FairSpillPool` does, covers both cases. A prototype of it, with the pool set to
     a task's share by the same fraction, completes every TPC-H query in both arms at 1, 2 and 16 GiB, with no
     task retried, in up to 3% more total time. At 1 GiB the sort-merge join takes 277.6 s instead of 272.1 s, and
     the spilling join completes all 22 queries in 248.1 s, q21 in 34.0 s. It changes when every Comet operator
     spills, so it is tracked on its own, in [#7088].
   - The join can also hold back by itself, keeping resident only part of what the pool grants when its probe side
     has operators in the same native plan that need memory. With its build buffer capped at 64 MiB by
     `maxBuildBufferBytes`, q21 completes in 33.9 s.
3. **Make spilling cheaper still.** Profiled in the native benchmark at a 64 MiB pool, the spilling join now spends
   about 12% of its time compressing spill files with lz4, 6% decoding and decompressing them, 10% copying, 15%
   in the inner joins' hash tables, 15% in `take` for routing, the compacted read-back and the joins' output, and
   9% routing probe rows, the filter lookups included.
   - Build a spilled bucket with nested, dictionary or view columns as one batch too. Only flat columns are now.
   - Read the next spilled pair's files ahead while the current pair is joined. `SpillManager` already reads two
     batches ahead on another task.
   - Revisit the fan-out of 16 buckets per level.
   - Spill compression is `spark.comet.exec.spill.compression.codec`, shared with sorts and aggregates. In
     `CometHashJoinMemoryBenchmark`'s inner join at 64 MiB, zstd makes the spilling join 14% slower than lz4 and
     spills 39% fewer bytes, and uncompressed files make it 4% faster and spill twice as many. With the files in
     the page cache, the native benchmark shows the same more strongly: at 20 MiB, uncompressed files are 15%
     faster than lz4 and zstd 34% slower. Which is best depends on the disks, so the join keeps the shared setting.
   - The benchmark's in-memory join looks its keys up in DataFusion's perfect hash table, because each of its four
     tasks holds a quarter of a dense key range. A spilled bucket holds a sixteenth of that, which is too sparse,
     and is looked up in a hash map. With Spark's default of 200 shuffle partitions, keys are rarely that dense,
     so the benchmark flatters the join without spilling a little.
4. **Decide how to account for the concatenated copy.** `collect_left_input` concatenates the build batches into
   one copy without reserving it. Reserving it moves the spill point to about half of where today's join fails,
   for a build side whose memory is mostly its batches.
   - Measure RSS against reserved bytes in a stress run, to see what the copy really costs. The TPC-H runs at scale
     factor 100 logged native memory against reserved memory with `spark.comet.memory.logInterval`, and saw at most
     1 GB per executor that no pool tracked, in every arm, but samples a second apart are too coarse to catch the
     copy.
   - Then keep the reservation, or fix the copy upstream: `collect_left_input` could reserve it, which is the F1
     item of [apache/datafusion#24768], or build it without holding all of its inputs at once. A spilled bucket
     with flat columns already avoids the copy, by handing its inner join one batch.
5. **Get CI verdicts and broaden the tests.**
   - Run the Iceberg suites, and the Spark SQL suites on the other Spark versions, with the `run-*` labels or
     `dev/local-ci.sh`. They run Comet on-heap, where the native pool is unbounded, so they only exercise the path
     that does not spill. A pass shows that the wrapper is transparent. The Spark SQL suites passed on Spark 4.1
     before the sort-merge fallback and the merge with main, so run them again.
   - Run the Spark suites on the other Spark profiles. All of them, 3.4, 3.5 with strict warnings, 4.0, 4.1 and
     4.2, compile with the tests, but the tests have run only on Spark 4.1.
   - TPC-H and TPC-DS at scale factor 1 match the golden answers of `CometTPCHQuerySuite` and
     `CometTPCDSQuerySuite`, all 22 and all 134 queries, when the suites' shuffled hash join configuration also
     sets `spark.comet.exec.hashJoin.spill.maxBuildBufferBytes`, to 1 MiB and 256 KiB, so that every larger build
     side spills. The suites do not take extra configs from outside yet, so those runs edited them. TPC-H and TPC-DS
     at scale factor 100 spill under real memory pressure and return the same rows as the sort-merge join, but
     for floating point noise in TPC-DS q39, see [Status](#status). Run them at more memory sizes, and on a
     cluster of separate machines.
6. **Settle the planner integration and the defaults.**
   - Work out how this fits with [#6785], which plans the forced shuffled hash join at planning time. Once joins
     can spill, `spark.comet.exec.forceShuffledHashJoin.maxBuildSize` decides only when a join spills, so its
     default limit could be lifted.
   - Joins whose output order an operator above relies on are not eligible, and can still fail. In the TPC-DS plans
     at scale factor 1, with `spark.comet.exec.forceShuffledHashJoin` and AQE off, there are none among the 611
     native shuffled hash joins. Before the eligibility check looked at the operators above, 10 joins, in q14a,
     q14b, q16, q23b, q69 and q94, were not eligible because Spark reported an order for them, which nothing used.
     Where some operator does rely on the order, keeping the sort-merge join may be better.
   - Joins that use `spark.comet.exec.join.dynamicFilter.enabled` are not eligible either. Build the runtime
     filter over every build row, resident or spilled: bounds and key domains can be accumulated while routing.
   - Decide whether spilling stays on by default. It is on now so that CI runs it. Then decide whether
     `forceShuffledHashJoin` can be on by default, which was the goal of this work, after measuring TPC-H and
     TPC-DS at realistic memory sizes.
   - A forced hash join can be far slower than the sort-merge join when its output is far larger than its inputs, with
     or without spilling: before the sort-merge fallback, TPC-DS q72 took 1610 s with it and 378 s without it. Forcing
     the hash join by default needs a guard on the join's fan-out as well as on its build side. The fallback is that
     guard, at run time: with it, q72 takes 381 to 385 s with the spilling join. DataFusion tracks the hash join's
     cost in [apache/datafusion#17494]: for each probe row the hash join walks a chain of the build rows with its key,
     and once the build side no longer fits in the cache, most steps miss. A DataFusion reproduction with q72's shape,
     16 partitions of about 4M build rows with 100 rows a key, takes about 60 s with the hash join and 12 s with the
     sort-merge join, on DataFusion 55.2 and on main, and 17.5 s with the hash join of [apache/datafusion#26447],
     which stores each key's build rows together.
7. **Take what was learned upstream.** Search for existing issues before filing any.
   - Report to [apache/datafusion#24768]: joining each bucket with a stock `HashJoinExec`, the chunked fallback
     for each join type, and the Bloom filters that keep unmatched probe rows off disk.
   - `InProgressSpillFile` cannot be named outside DataFusion, because its module is `pub(crate)`. Ask for it to
     be exported.
   - A batch read back from a spill file holding up to twice what it held when written was reported in
     [apache/datafusion#17340] and fixed by [apache/datafusion#24592], which is not in DataFusion 55.2. Once Comet
     is on a DataFusion with it, read-back batches hold close to what they held when written, and the compacted
     read-back can be reconsidered. A batch of a compressed file whose buffers were partly stored uncompressed still
     keeps its compressed message.
   - arrow-rs compresses each IPC buffer with a new lz4 frame encoder, which allocates and zeroes its tables
     every time, while zstd reuses one compressor. Reusing the encoder was tried in [apache/arrow-rs#8423] and
     showed no gain on large buffers, but every buffer of a small batch pays for it. Spill files of full batches
     make it matter less.
   - `HashJoinExec` counts its final pass over the build rows as probe input in `input_rows`.
   - `SortMergeJoinExec` in DataFusion 55 passes a filter the left side's columns first, whatever their order in
     the filter, which [apache/datafusion#25489] fixed on main. Drop `left_columns_first` once Comet is on a
     DataFusion with it.
   - `collect_left_input` allocates the concatenated copy and the hash map's chain of row indices without
     reserving them.
8. **Before leaving draft.** Settle the config names, and trim the research notes from the docs. Coordinate with
   the author of [#6384] and [#6785], who took the Comet side of [#2545].

## Working on it

The work is on the `spilling-hash-join-research` branch of `andygrove/datafusion-comet`, in [#7057]. The native
code is in `native/operators/src/spilling_hash_join/`, and `mod.rs` there gives an overview.

- Native tests, from `native/`: `cargo test -p datafusion-comet-operators --lib spilling_hash_join`. The planner
  test, `cargo test -p datafusion-comet --lib -- hash_join`, links against the JVM, so on macOS it needs
  `DYLD_LIBRARY_PATH` set to the JDK's `lib/server` directory.
- Spark tests, after `make`: `./mvnw test -Dtest=none -Dsuites="org.apache.comet.exec.CometSpillingHashJoinSuite,org.apache.comet.exec.CometSpillingHashJoinGreedyPoolSuite,org.apache.comet.exec.CometJoinSpillingSuite,org.apache.comet.exec.CometJoinSortMergeFallbackSuite,org.apache.comet.exec.CometJoinSuite"`.
  The five suites are listed in the Linux and macOS PR workflows. `CometJoinSortMergeFallbackSuite` runs all of
  `CometJoinSuite` with every inner join that can spill switching to a sort-merge join after its first probe batch.
- The native benchmark, from `native/`: `cargo bench -p datafusion-comet-operators --bench spilling_hash_join`.
  Each case prints what it spilled before it is timed. To profile one case, pass `--profile-time 20 20MiB_lz4_frame`
  after `--`. On a machine with two kinds of cores, such as the 7950X3D, pin it with `taskset`, or the in-memory
  cases alternate between two speeds.
- The JVM benchmark:
  `make benchmark-org.apache.spark.sql.benchmark.CometHashJoinMemoryBenchmark`. `PROFILES` passes system
  properties through to narrow it, for example
  `PROFILES="-Dcomet.bench.joins=inner -Dcomet.bench.budgetsMiB=1024,128"`. The properties are listed in the
  benchmark's scaladoc. A full run takes about 7 minutes on an idle 7950X3D, and it needs a quiet machine.
- The scale factor 100 runs use `benchmarks/tpc/run.py`, one `spark-submit` per query, with an engine config per
  arm. The hash join arms set `spark.comet.exec.forceShuffledHashJoin=true` and
  `spark.comet.exec.forceShuffledHashJoin.maxBuildSize=-1`, every arm sets `spark.memory.offHeap.size` and
  `spark.sql.shuffle.partitions`, and the fallback arms set
  `spark.comet.exec.hashJoin.spill.sortMergeFallback.enabled`. With event logs on and
  `spark.eventLog.logStageExecutorMetrics=true`, the logs give each query's join metrics, `sort_merge_joins`
  among them. Compare the rows of each query by count and a hash of its sorted rows.

[#2545]: https://github.com/apache/datafusion-comet/issues/2545
[#6384]: https://github.com/apache/datafusion-comet/pull/6384
[#6785]: https://github.com/apache/datafusion-comet/pull/6785
[#7057]: https://github.com/apache/datafusion-comet/pull/7057
[#7088]: https://github.com/apache/datafusion-comet/issues/7088
[apache/arrow-rs#8423]: https://github.com/apache/arrow-rs/issues/8423
[apache/datafusion#17340]: https://github.com/apache/datafusion/issues/17340
[apache/datafusion#17494]: https://github.com/apache/datafusion/issues/17494
[apache/datafusion#24592]: https://github.com/apache/datafusion/pull/24592
[apache/datafusion#24768]: https://github.com/apache/datafusion/issues/24768
[apache/datafusion#25217]: https://github.com/apache/datafusion/pull/25217
[apache/datafusion#25489]: https://github.com/apache/datafusion/pull/25489
[apache/datafusion#26447]: https://github.com/apache/datafusion/pull/26447
[apache/datafusion#26227]: https://github.com/apache/datafusion/pull/26227
[apache/datafusion#26277]: https://github.com/apache/datafusion/pull/26277
