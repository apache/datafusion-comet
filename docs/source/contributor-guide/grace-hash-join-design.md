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

Grace Hash Join (GHJ) is the hash join implementation in Apache DataFusion Comet. When `spark.comet.exec.replaceSortMergeJoin` is enabled, Comet's `RewriteJoin` rule converts `SortMergeJoinExec` to `ShuffledHashJoinExec` (removing the input sorts), and all `ShuffledHashJoinExec` operators are then executed natively as `GraceHashJoinExec`.

GHJ partitions both build and probe sides into N buckets by hashing join keys, then joins each bucket independently. When memory is tight, partitions spill to disk using Arrow IPC format. A fast path skips partitioning entirely when the build side is small enough.

Supports all join types: Inner, Left, Right, Full, LeftSemi, LeftAnti, LeftMark, RightSemi, RightAnti, RightMark.

## Configuration

| Config Key                                           | Type    | Default    | Description                                                |
| ---------------------------------------------------- | ------- | ---------- | ---------------------------------------------------------- |
| `spark.comet.exec.replaceSortMergeJoin`              | boolean | `false`    | Replace SortMergeJoin with ShuffledHashJoin (enables GHJ)  |
| `spark.comet.exec.replaceSortMergeJoin.maxBuildSize` | long    | `-1`       | Max build-side bytes for SMJ replacement. `-1` = no limit  |
| `spark.comet.exec.graceHashJoin.numPartitions`       | int     | `16`       | Number of hash partitions (buckets)                        |
| `spark.comet.exec.graceHashJoin.fastPathThreshold`   | int     | `10485760` | Total fast-path budget in bytes, divided by executor cores |

### SMJ Replacement Guard

The `RewriteJoin` rule checks `maxBuildSize` against Spark's logical plan statistics before replacing a `SortMergeJoinExec`. When both sides are large (e.g., TPC-DS q72's `catalog_sales JOIN inventory`), sort-merge join's streaming merge on pre-sorted data outperforms hash join's per-task hash table construction. Setting `maxBuildSize` (e.g., `104857600` for 100 MB) keeps SMJ for these cases.

### Fast Path Threshold

The configured threshold is the total budget across all concurrent tasks on the executor. The planner divides it by `spark.executor.cores` so each task's fast-path hash table stays within its fair share. For example, with a 32 GB threshold and 8 cores, each task gets a 4 GB per-task limit.

## Architecture

### Plan Integration

```
SortMergeJoinExec
    -> RewriteJoin converts to ShuffledHashJoinExec (removes input sorts)
    -> CometExecRule wraps as CometHashJoinExec
    -> CometHashJoinExec.createExec() creates CometGraceHashJoinExec
    -> Serialized to protobuf via JNI
    -> PhysicalPlanner (Rust) creates GraceHashJoinExec
```

### Key Data Structures

```
GraceHashJoinExec          ExecutionPlan implementation
+-- left/right             Child input plans
+-- on                     Join key pairs [(left_key, right_key)]
+-- filter                 Optional post-join filter
+-- join_type              Inner/Left/Right/Full/Semi/Anti/Mark
+-- num_partitions         Number of hash buckets (default 16)
+-- build_left             Whether left input is the build side
+-- fast_path_threshold    Per-task threshold for fast path (0 = disabled)
+-- schema                 Output schema

HashPartition              Per-bucket state during partitioning
+-- build_batches          In-memory build-side RecordBatches
+-- probe_batches          In-memory probe-side RecordBatches
+-- build_spill_writer     Optional SpillWriter for build data
+-- probe_spill_writer     Optional SpillWriter for probe data
+-- build_mem_size         Tracked memory for build side
+-- probe_mem_size         Tracked memory for probe side

FinishedPartition          State after spill writers are closed
+-- build_batches          In-memory build batches (if not spilled)
+-- probe_batches          In-memory probe batches (if not spilled)
+-- build_spill_file       Temp file for spilled build data
+-- probe_spill_file       Temp file for spilled probe data
```

## Execution Flow

```
execute()
  |
  +- Phase 1: Partition build side
  |    Hash-partition all build input into N buckets.
  |    Spill the largest bucket on memory pressure.
  |
  +- Phase 2: Partition probe side
  |    Hash-partition probe input into N buckets.
  |    Spill ALL non-spilled buckets on first memory pressure.
  |
  +- Decision: fast path or slow path?
  |    If no spilling occurred and total build size <= per-task threshold:
  |      -> Fast path: single HashJoinExec, stream probe directly
  |    Otherwise:
  |      -> Slow path: merge partitions, join sequentially
  |
  +- Phase 3 (slow path): Join each partition sequentially
       Merge adjacent partitions to ~32 MB build-side groups.
       For each group, create a per-partition HashJoinExec.
       Spilled probes use streaming SpillReaderExec.
       Oversized builds trigger recursive repartitioning.
```

### Fast Path

After partitioning both sides, GHJ checks whether the build side is small enough to join in a single `HashJoinExec`:

1. No partitions were spilled during Phases 1 or 2
2. The fast path threshold is non-zero
3. The actual build-side memory (measured via `get_array_memory_size()`) is within the per-task threshold

When all conditions are met, GHJ concatenates all build-side batches, wraps the probe stream in a `StreamSourceExec`, and creates a single `HashJoinExec` with `CollectLeft` mode. The probe side streams directly through without buffering. This avoids the overhead of partition merging and sequential per-partition joins.

The fast path threshold is intentionally conservative because `HashJoinExec` creates non-spillable hash tables (`can_spill: false`). The per-task division ensures that concurrent tasks don't collectively exceed memory.

### Phase 1: Build-Side Partitioning

For each incoming batch from the build input:

1. Evaluate join key expressions and compute hash values
2. Assign each row to a partition: `partition_id = hash % num_partitions`
3. Use the prefix-sum algorithm to efficiently extract contiguous row groups per partition via `arrow::compute::take()`
4. For each partition's sub-batch:
   - If the partition is already spilled, append to its `SpillWriter`
   - Otherwise, call `reservation.try_grow(batch_size)`
   - On failure: spill the largest non-spilled partition, retry
   - If still fails: spill this partition and write to disk

All in-memory build data is tracked in a shared `MemoryReservation` registered as `can_spill: true`, making GHJ a cooperative citizen in DataFusion's memory pool.

### Phase 2: Probe-Side Partitioning

Same hash-partitioning algorithm as Phase 1, with key differences:

1. **Spilled build implies spilled probe**: If a partition's build side was spilled, the probe side is also spilled. Both sides must be on disk (or both in memory) for the join phase.

2. **Aggressive spilling**: On the first memory pressure event, all non-spilled partitions are spilled (both build and probe sides). This prevents thrashing between spilling and accumulating when multiple concurrent GHJ instances share a memory pool.

3. **Shared reservation**: The same `MemoryReservation` from Phase 1 continues to track probe-side memory.

### Phase 3: Per-Partition Joins (Slow Path)

Before joining, adjacent `FinishedPartition`s are merged so each group has roughly `TARGET_PARTITION_BUILD_SIZE` (32 MB) of build data. This reduces the number of `HashJoinExec` invocations while keeping each hash table small.

Merged groups are joined sequentially — one at a time — so only one `HashJoinInput` consumer exists at any moment. The GHJ reservation is freed before Phase 3 begins; each per-partition `HashJoinExec` tracks its own memory.

**In-memory partitions** are joined via `join_partition_recursive()`:

- Concatenate build and probe sub-batches
- Create `HashJoinExec` with both sides as `MemorySourceConfig`
- If the build side is too large for a hash table: recursively repartition (up to `MAX_RECURSION_DEPTH = 3`, yielding up to 16^3 = 4096 effective partitions)

**Spilled partitions** are joined via `join_with_spilled_probe()`:

- Build side loaded from memory or disk via `spawn_blocking`
- Probe side streamed via `SpillReaderExec` (never fully loaded into memory)
- If the build side is too large: fall back to eager probe read + recursive repartitioning

## Spill Mechanism

### Writing

`SpillWriter` wraps Arrow IPC `StreamWriter` for incremental appends:

- Uses `BufWriter` with 1 MB buffer (vs 8 KB default) for sequential throughput
- Batches are appended one at a time — no need to rewrite the file
- `finish()` flushes the writer and returns the `RefCountedTempFile`

Temp files are created via DataFusion's `DiskManager`, which handles allocation and cleanup.

### Reading

Two read paths depending on context:

**Eager read** (`read_spilled_batches`): Opens file, reads all batches into `Vec<RecordBatch>`. Used for build-side spill files bounded by `TARGET_PARTITION_BUILD_SIZE`.

**Streaming read** (`SpillReaderExec`): An `ExecutionPlan` that reads batches on-demand:

- Spawns a `tokio::task::spawn_blocking` to read from the file on a blocking thread pool
- Uses an `mpsc` channel (capacity 4) to feed batches to the async executor
- Coalesces small sub-batches into ~8192-row chunks before sending, reducing per-batch overhead in the downstream hash join kernel
- The `RefCountedTempFile` handle is moved into the blocking closure to keep the file alive until reading completes

### Spill Coalescing

Hash-partitioning creates N sub-batches per input batch. With N=16 partitions and 1000-row input batches, spill files contain ~62-row sub-batches. `SpillReaderExec` coalesces these into ~8192-row batches on read, reducing channel send/recv overhead, hash join kernel invocations, and per-batch `RecordBatch` construction costs.

## Memory Management

### Reservation Model

GHJ uses a single `MemoryReservation` registered as a spillable consumer (`with_can_spill(true)`). This reservation:

- Tracks all in-memory build and probe data across all partitions during Phases 1 and 2
- Grows via `try_grow()` before each batch is added to memory
- Shrinks via `shrink()` when partitions are spilled to disk
- Is freed before Phase 3, where each per-partition `HashJoinExec` tracks its own memory via `HashJoinInput`

### Concurrent Instances

In a typical Spark executor, multiple tasks run concurrently, each potentially executing a GHJ. All instances share the same DataFusion memory pool. The "spill ALL non-spilled partitions" strategy in Phase 2 makes each instance's spill decision atomic — once triggered, the instance moves all its data to disk in one operation, preventing interleaving with other instances that would otherwise claim freed memory immediately.

### DataFusion Memory Pool Integration

DataFusion's memory pool (typically `FairSpillPool`) divides memory between spillable and non-spillable consumers. GHJ registers as spillable so the pool can account for its memory when computing fair shares. The per-partition `HashJoinExec` instances in Phase 3 use non-spillable `HashJoinInput` reservations, but since partitions are joined sequentially, only one hash table exists at a time, keeping peak memory at roughly `build_size / num_partitions`.

## Hash Partitioning Algorithm

### Prefix-Sum Approach

Instead of N separate `take()` kernel calls (one per partition), GHJ uses a prefix-sum algorithm:

1. **Hash**: Compute hash values for all rows
2. **Assign**: Map each row to a partition: `partition_id = hash % N`
3. **Count**: Count rows per partition
4. **Prefix-sum**: Accumulate counts into start offsets
5. **Scatter**: Place row indices into contiguous regions per partition
6. **Take**: Single `arrow::compute::take()` per partition using the precomputed indices

This is O(rows) with good cache locality, compared to O(rows x partitions) for the naive approach.

### Hash Seed Variation

GHJ hashes on the same join keys that Spark already used for its shuffle exchange, but with a different hash function (ahash via `RandomState` with fixed seeds). Spark's shuffle uses Murmur3, so all rows arriving at a given Spark partition share the same `murmur3(key) % num_spark_partitions` value but have diverse actual key values. GHJ's ahash produces a completely different distribution.

At each recursion level, a different random seed is used:

```rust
fn partition_random_state(recursion_level: usize) -> RandomState {
    RandomState::with_seeds(
        0x517cc1b727220a95 ^ (recursion_level as u64),
        0x3a8b7c9d1e2f4056, 0, 0,
    )
}
```

This ensures rows that hash to the same partition at level 0 are distributed across different sub-partitions at level 1. The only case where repartitioning cannot help is true data skew — many rows with the same key value. No amount of rehashing can separate identical keys, which is why there is a `MAX_RECURSION_DEPTH = 3` limit.

## Recursive Repartitioning

When a partition's build side is too large for a hash table (tested via `try_grow(build_size * 3)`, where the 3x accounts for hash table overhead), GHJ recursively repartitions:

1. Sub-partition both build and probe into 16 new buckets using a different hash seed
2. Recursively join each sub-partition
3. Maximum depth: 3 (yielding up to 16^3 = 4096 effective partitions)
4. If still too large at max depth: return `ResourcesExhausted` error

## Partition Merging

After Phase 2, GHJ merges adjacent `FinishedPartition`s to reduce the number of per-partition `HashJoinExec` invocations. The target is `TARGET_PARTITION_BUILD_SIZE` (32 MB) per merged group. For example, with 16 partitions and 200 MB total build data, partitions are merged into ~6 groups of ~32 MB each instead of 16 groups of ~12 MB.

Merging only combines adjacent partitions (preserving hash locality) and never merges spilled with non-spilled partitions. The merge is a metadata-only operation — it combines batch lists and spill file handles without copying data.

## Build Side Selection

GHJ respects Spark's build side selection (`BuildLeft` or `BuildRight`). The `build_left` flag determines:

- Which input is consumed in Phase 1 (build) vs Phase 2 (probe)
- How join key expressions are mapped
- How `HashJoinExec` is constructed (build side is always left in `CollectLeft` mode)

When `build_left = false`, the `HashJoinExec` is created with swapped inputs and then `swap_inputs()` is called to produce correct output column ordering.

## Metrics

| Metric                | Description                            |
| --------------------- | -------------------------------------- |
| `build_time`          | Time spent partitioning the build side |
| `probe_time`          | Time spent partitioning the probe side |
| `spill_count`         | Number of partition spill events       |
| `spilled_bytes`       | Total bytes written to spill files     |
| `build_input_rows`    | Total rows from build input            |
| `build_input_batches` | Total batches from build input         |
| `input_rows`          | Total rows from probe input            |
| `input_batches`       | Total batches from probe input         |
| `output_rows`         | Total output rows                      |
| `elapsed_compute`     | Total compute time                     |

## Future Work

- **Adaptive partition count**: Dynamically choose the number of partitions based on input size rather than a fixed default
- **Spill file compression**: Compress Arrow IPC data on disk to reduce I/O volume at the cost of CPU
- **Upstream DataFusion spill support**: Contribute spill capability to DataFusion's `HashJoinExec` to eliminate the need for a separate GHJ operator
