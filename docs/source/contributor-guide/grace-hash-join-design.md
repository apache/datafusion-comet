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

# Grace Hash Join Design Document

## Overview

Grace Hash Join (GHJ) is an operator for Apache DataFusion Comet that replaces Spark's `ShuffledHashJoinExec` with a spill-capable hash join. It partitions both build and probe sides into N buckets by hashing join keys, then joins each bucket independently. When memory is tight, partitions spill to disk using Arrow IPC format and are joined later using streaming reads.

GHJ supports all join types (Inner, Left, Right, Full, LeftSemi, LeftAnti, LeftMark, RightSemi, RightAnti, RightMark) and handles skewed data through recursive repartitioning.

## Motivation

Spark's `ShuffledHashJoinExec` loads the entire build side into a hash table in memory. When the build side is large or executor memory is constrained, this causes OOM failures. DataFusion's built-in `HashJoinExec` has the same limitation — its `HashJoinInput` consumer is marked `can_spill: false`.

GHJ solves this by:

1. Partitioning both sides into smaller buckets that fit in memory individually
2. Spilling partitions to disk when memory pressure is detected
3. Joining partitions independently, reading spilled data back via streaming I/O

## Configuration

| Config Key                                     | Type    | Default | Description                         |
| ---------------------------------------------- | ------- | ------- | ----------------------------------- |
| `spark.comet.exec.graceHashJoin.enabled`       | boolean | `false` | Enable Grace Hash Join              |
| `spark.comet.exec.graceHashJoin.numPartitions` | int     | `16`    | Number of hash partitions (buckets) |

## Architecture

### Plan Integration

```
Spark ShuffledHashJoinExec
    → CometExecRule identifies ShuffledHashJoinExec
    → CometHashJoinExec.createExec() checks config
    → If GHJ enabled: CometGraceHashJoinExec (serialized to protobuf)
    → JNI → PhysicalPlanner (Rust) creates GraceHashJoinExec
```

The `RewriteJoin` rule additionally converts `SortMergeJoinExec` to `ShuffledHashJoinExec` so that GHJ can intercept sort-merge joins as well.

### Key Data Structures

```
GraceHashJoinExec          ExecutionPlan implementation
├── left/right             Child input plans
├── on                     Join key pairs [(left_key, right_key)]
├── filter                 Optional post-join filter
├── join_type              Inner/Left/Right/Full/Semi/Anti/Mark
├── num_partitions         Number of hash buckets (default 16)
├── build_left             Whether left input is the build side
└── schema                 Output schema

HashPartition              Per-bucket state during partitioning
├── build_batches          In-memory build-side RecordBatches
├── probe_batches          In-memory probe-side RecordBatches
├── build_spill_writer     Optional SpillWriter for build data
├── probe_spill_writer     Optional SpillWriter for probe data
├── build_mem_size         Tracked memory for build side
└── probe_mem_size         Tracked memory for probe side

FinishedPartition          State after spill writers are closed
├── build_batches          In-memory build batches (if not spilled)
├── probe_batches          In-memory probe batches (if not spilled)
├── build_spill_file       Temp file for spilled build data
└── probe_spill_file       Temp file for spilled probe data
```

## Execution Phases

### Overview

```
execute()
  │
  ├─ Phase 1: Partition build side
  │    Hash-partition all build input into N buckets.
  │    Spill the largest bucket on memory pressure.
  │
  ├─ Phase 2: Partition probe side
  │    Hash-partition probe input into N buckets.
  │    Spill ALL non-spilled buckets on first memory pressure.
  │
  └─ Phase 3: Join each partition (sequential)
       For each bucket, create a per-partition HashJoinExec.
       Spilled probes use streaming SpillReaderExec.
       Oversized builds trigger recursive repartitioning.
       Only one partition's HashJoinInput exists at a time.
```

### Phase 1: Build-Side Partitioning

For each incoming batch from the build input:

1. Evaluate join key expressions and compute hash values
2. Assign each row to a partition: `partition_id = hash % num_partitions`
3. Use the prefix-sum algorithm (from the shuffle operator) to efficiently extract contiguous row groups per partition via `arrow::compute::take()`
4. For each partition's sub-batch:
   - If the partition is already spilled, append to its `SpillWriter`
   - Otherwise, call `reservation.try_grow(batch_size)`
   - On failure: spill the largest non-spilled partition, retry
   - If still fails: spill this partition and write to disk

**Memory tracking**: All in-memory build data is tracked in a shared `MutableReservation` registered as `can_spill: true`. This is critical — it makes GHJ a cooperative citizen in DataFusion's memory pool, allowing other operators to trigger memory reclamation.

### Phase 2: Probe-Side Partitioning

Same hash-partitioning algorithm as Phase 1, with key differences:

1. **Spilled build implies spilled probe**: If a partition's build side was spilled, the probe side must also be spilled for consistency during the join phase. Both sides need to be on disk (or both in memory).

2. **Aggressive spilling strategy**: On first memory pressure event, spill ALL non-spilled partitions (both build and probe sides). This prevents a pattern where spilling one partition frees memory, new probe data accumulates in remaining partitions, pressure returns, another partition is spilled, etc. With multiple concurrent GHJ instances sharing a memory pool, this "whack-a-mole" pattern never converges.

3. **Probe memory tracked in same reservation**: The shared `MutableReservation` from Phase 1 continues to track probe-side memory.

### Phase 3: Per-Partition Joins

Partitions are joined **sequentially** — one at a time — so only one `HashJoinInput` consumer exists at any moment. This keeps peak memory at ~1/N of what a single large hash table would require. DataFusion manages parallelism externally by calling `execute(partition)` from multiple async tasks; GHJ does not spawn its own internal parallelism.

The GHJ reservation is freed before Phase 3 begins, since the partition data has been moved into `FinishedPartition` structs and each per-partition `HashJoinExec` will track its own memory via `HashJoinInput`.

**In-memory probe** → `join_partition_recursive()`:

- Concatenate build and probe sub-batches
- Create `HashJoinExec` with both sides as `MemorySourceConfig`
- If build too large for hash table: recursively repartition (up to `MAX_RECURSION_DEPTH = 3` levels, yielding up to 16^3 = 4096 effective partitions)

**Spilled probe** → `join_with_spilled_probe()`:

- Build side loaded from memory or disk via `spawn_blocking` (to avoid blocking the async executor)
- Probe side streamed via `SpillReaderExec` (never fully loaded into memory)
- If build too large: fall back to eager probe read + recursive repartitioning

## Spill Mechanism

### Writing

`SpillWriter` wraps Arrow IPC `StreamWriter` for incremental appends:

- Uses `BufWriter` with 1 MB buffer (vs 8 KB default) for throughput
- Batches are appended one at a time — no need to rewrite the file
- `finish()` flushes the writer and returns the `RefCountedTempFile`

Temp files are created via DataFusion's `DiskManager`, which handles allocation and cleanup.

### Reading

Two read paths depending on whether the full data is needed:

**Eager read** (`read_spilled_batches`): Opens file, reads all batches into `Vec<RecordBatch>`. Used for small build-side spill files.

**Streaming read** (`SpillReaderExec`): An `ExecutionPlan` that reads batches on-demand:

- Spawns a `tokio::task::spawn_blocking` to read from the file on a blocking thread pool
- Uses an `mpsc` channel (capacity 4) to feed batches to the async executor
- Coalesces small sub-batches into ~8192-row chunks before sending, reducing per-batch overhead in the downstream hash join kernel
- The `RefCountedTempFile` handle is moved into the blocking closure to keep the file alive until reading completes

### Spill I/O Optimization

Spill files contain many tiny sub-batches because each incoming batch is partitioned into N pieces. Without coalescing, a spill file with 1M rows might contain 10,000+ batches of ~100 rows each. The coalescing step in `SpillReaderExec` merges these into ~122 batches of ~8192 rows, dramatically reducing:

- Channel send/recv overhead
- Hash join kernel invocations
- Per-batch `RecordBatch` construction costs

## Memory Management

### Reservation Model

GHJ uses a single `MemoryReservation` registered as a spillable consumer (`with_can_spill(true)`). This reservation:

- Tracks all in-memory build and probe data across all partitions
- Grows via `try_grow()` before each batch is added to memory
- Shrinks via `shrink()` when partitions are spilled to disk
- Acts as a cooperative memory citizen — DataFusion's memory pool can account for GHJ's memory when other operators request allocations

### Why Spillable Registration Matters

DataFusion's memory pool (typically `FairSpillPool`) divides memory between spillable and non-spillable consumers. Non-spillable consumers (`can_spill: false`) like `HashJoinInput` from regular `HashJoinExec` get a guaranteed fraction. When non-spillable consumers exhaust their allocation, the pool returns an error.

GHJ registers as spillable so the pool can account for its memory when computing fair shares. During Phases 1 and 2, the reservation tracks all in-memory partition data and triggers spilling when `try_grow` fails. Before Phase 3, the reservation is freed — the data is now owned by `FinishedPartition` structs and will be tracked by each per-partition `HashJoinExec`'s own `HashJoinInput` reservation.

### Concurrent GHJ Instances

In a typical Spark executor, multiple tasks run concurrently, each potentially executing a GHJ. All instances share the same DataFusion memory pool. This creates contention:

- Instance A spills a partition, freeing memory
- Instance B immediately claims that memory for its probe data
- Instance A needs memory for the next batch, finds none available
- Both instances thrash between spilling and accumulating

The "spill ALL non-spilled partitions" strategy in Phase 2 addresses this by making each instance's spill decision atomic — once triggered, the instance moves all its data to disk in one operation, preventing interleaving with other instances.

## Hash Partitioning Algorithm

### Prefix-Sum Approach

Instead of N separate `take()` kernel calls (one per partition), GHJ uses a prefix-sum algorithm from the shuffle operator:

1. **Hash**: Compute hash values for all rows
2. **Assign**: Map each row to a partition: `partition_id = hash % N`
3. **Count**: Count rows per partition
4. **Prefix-sum**: Accumulate counts into start offsets
5. **Scatter**: Place row indices into contiguous regions per partition
6. **Take**: Single `arrow::compute::take()` per partition using the precomputed indices

This is O(rows) with excellent cache locality, compared to O(rows × partitions) for the naive approach.

### Hash Seed Variation

GHJ hashes on the same join keys that Spark already used for its shuffle exchange, but this is not redundant. Spark's shuffle uses Murmur3 to assign rows to exchange partitions, so all rows arriving at a given Spark partition share the same `murmur3(key) % num_spark_partitions` value — but they have diverse actual key values. GHJ then hashes those same keys with a **different hash function** (ahash via `RandomState` with fixed seeds), producing a completely different distribution:

```
Spark shuffle:   murmur3(key) % 200  →  all rows land in partition 42
GHJ level 0:     ahash(key, seed0) % 16  →  rows spread across buckets 0-15
GHJ level 1:     ahash(key, seed1) % 16  →  further redistribution within each bucket
```

The hash function uses different random seeds at each recursion level:

```rust
fn partition_random_state(recursion_level: usize) -> RandomState {
    RandomState::with_seeds(
        0x517cc1b727220a95 ^ (recursion_level as u64),
        0x3a8b7c9d1e2f4056, 0, 0,
    )
}
```

This ensures that rows which hash to the same partition at level 0 are distributed across different sub-partitions at level 1, breaking up hash collisions. The only case where repartitioning cannot help is true data skew — many rows with the _same_ key value. No amount of rehashing can separate identical keys, which is why there is a `MAX_RECURSION_DEPTH = 3` limit, after which GHJ returns a `ResourcesExhausted` error.

## Recursive Repartitioning

When a partition's build side is too large for a hash table (tested via `try_grow(build_size * 3)`), GHJ recursively repartitions:

1. Sub-partition both build and probe into 16 new buckets using a different hash seed
2. Recursively join each sub-partition
3. Maximum depth: 3 (yielding up to 16^3 = 4096 effective partitions)
4. If still too large at max depth: return `ResourcesExhausted` error

The 3x multiplier accounts for hash table overhead (the `JoinHashMap` typically uses 2-3x the raw data size).

## Build Side Selection

GHJ respects Spark's build side selection (`BuildLeft` or `BuildRight`). The `build_left` flag determines:

- Which input is consumed in Phase 1 (build) vs Phase 2 (probe)
- How join key expressions are mapped (left keys → build keys if `build_left`)
- How `HashJoinExec` is constructed (build side is always left in `CollectLeft` mode)

When `build_left = false`, the `HashJoinExec` is created with swapped inputs and then `swap_inputs()` is called to produce correct output column ordering.

## Metrics

| Metric                | Description                                 |
| --------------------- | ------------------------------------------- |
| `build_time`          | Time spent partitioning the build side      |
| `probe_time`          | Time spent partitioning the probe side      |
| `spill_count`         | Number of partition spill events            |
| `spilled_bytes`       | Total bytes written to spill files          |
| `build_input_rows`    | Total rows from build input                 |
| `build_input_batches` | Total batches from build input              |
| `input_rows`          | Total rows from probe input                 |
| `input_batches`       | Total batches from probe input              |
| `output_rows`         | Total output rows (from `BaselineMetrics`)  |
| `elapsed_compute`     | Total compute time (from `BaselineMetrics`) |

## Lessons Learned

### 1. Memory pool cooperation is non-negotiable

Any optimization that removes the spillable reservation from the memory pool during Phases 1 and 2 breaks other operators. The pool's ability to handle pressure depends on having at least one spillable consumer. The reservation is freed before Phase 3 only because each per-partition `HashJoinExec` tracks its own memory.

### 2. Spill one partition at a time doesn't work with concurrency

With N concurrent GHJ instances sharing a pool, spilling the "largest partition" frees memory that other instances immediately claim. The effective free memory after spilling is near zero. Spilling ALL non-spilled partitions atomically prevents this race.

### 3. Probe-side memory must be tracked

The original implementation only tracked build-side memory in the reservation. Untracked probe-side accumulation (e.g., 170M rows at 6.5GB per executor) caused OOM before any spilling could occur.

### 4. The join phase can be the OOM bottleneck, not the partition phase

Even with proper spilling during partitioning, eagerly loading all spilled probe data in the join phase reintroduces the OOM. `SpillReaderExec` with streaming reads solved this.

### 5. Small batches from spill files kill performance

Hash-partitioning creates N sub-batches per input batch. With N=16 partitions and 1000-row input batches, spill files contain ~62-row sub-batches. Reading and joining millions of tiny batches has massive per-batch overhead. Coalescing to ~8192-row batches on read reduces overhead by 100x+.

### 6. A fast path that skips partitioning creates non-spillable memory pressure

An earlier design included a "fast path" that skipped Phases 2 and 3 when the build side appeared small: it concatenated all build data into a single `HashJoinExec` and streamed the probe directly through it. This was removed because:

- **`HashJoinInput` is non-spillable.** `HashJoinExec` registers its hash table memory as `can_spill: false`. A single large `HashJoinInput` cannot be reclaimed under memory pressure.
- **`build_mem_size` severely underestimates actual memory.** The proportional estimate (`total_batch_size * sub_rows / total_rows`) used during partitioning can undercount by 5-20x because it doesn't account for per-array overhead in sub-batches created by `take()`. A build side estimated at 45 MB could actually be 460 MB, producing a 1.3 GB hash table.
- **The 3x memory check is a point-in-time snapshot.** Even with accurate sizes, the check (`try_grow(build_bytes * 3)`) passes when other operators haven't allocated yet. By the time the hash table is built, concurrent operators (broadcast hash joins, other GHJ instances) have consumed pool space, and the total exceeds the pool limit.
- **The slow path handles small builds efficiently.** With 16 partitions processed sequentially, each hash table is ~1/16 of the total. The overhead of partitioning the probe side is modest compared to the memory safety gained.

In TPC-DS q72 (which has 2 GHJ operators and 8 broadcast hash joins sharing a pool), the fast path created a 1.3 GB non-spillable hash table in a ~954 MB pool, causing OOM. The slow path keeps peak hash table memory at ~86 MB per partition.

### 7. DataFusion's HashJoinExec is not spill-capable

`HashJoinInput` is registered with `can_spill: false`. There is no way to make `HashJoinExec` yield memory under pressure. This is a fundamental DataFusion limitation that GHJ works around by managing memory at the partition level — keeping each per-partition hash table small and processing them one at a time.

### 8. Internal parallelism fights the runtime

An earlier design spawned each partition's join as a separate `tokio::task` for parallel execution. This was removed because DataFusion already manages parallelism by calling `execute(partition)` from multiple async tasks. Internal parallelism creates concurrent `HashJoinInput` reservations that compete for pool space and is redundant with the runtime's own scheduling.

## Future Work

- **Bloom filter pre-filtering**: For inner joins with tiny build sides, a bloom filter could skip probe batches that have no matching keys, reducing both I/O and computation
- **Adaptive partition count**: Dynamically choose the number of partitions based on input size rather than a fixed default
- **Spill file compression**: Compress Arrow IPC data on disk to reduce I/O volume at the cost of CPU
- **Memory-mapped spill files**: Use mmap instead of sequential reads for random access patterns during repartitioning
- **Upstream DataFusion spill support**: Contribute spill capability to DataFusion's `HashJoinExec` to eliminate the need for a separate GHJ operator
