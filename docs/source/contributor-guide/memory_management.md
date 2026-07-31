<!--
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

# Native Memory Management

Comet's native code allocates from a DataFusion `MemoryPool` that is created per native plan in
`Java_org_apache_comet_Native_createPlan`
(`native/core/src/execution/jni_api.rs`). Which pool it gets, how large it is, how long it lives,
and whether it talks to Spark at all are all decided by
`native/core/src/execution/memory_pools/`. This page covers that accounting layer: how pools are
chosen and sized, how the two JVM-backed pools reach Spark's `TaskMemoryManager`, and what a
native operator has to do so its buffers are visible to the pool.

Everything below is grounded in specific files. Where a claim cites a file, read that file before
relying on the summary here.

## Pool types

`MemoryPoolType` in `native/core/src/execution/memory_pools/config.rs` has nine variants. Two axes
distinguish them.

The first axis is the allocation policy. Greedy pools are first come, first served up to the pool
size. Fair pools divide the pool size by the number of registered consumers, but the two fair
implementations apply that share differently. DataFusion's `FairSpillPool` compares it against the
calling reservation, so one operator cannot starve the others. `CometFairMemoryPool` compares it
against the pool-wide total instead, which caps the pool as a whole rather than any one operator.
`CometFairMemoryPool` is one of only two pools selectable in off-heap mode, so read the exact
arithmetic under "How native memory relates to Spark's" below before reasoning about it. Only two
of the nine are Comet's own implementations (`CometUnifiedMemoryPool` and `CometFairMemoryPool`);
the rest are DataFusion's `GreedyMemoryPool`, `FairSpillPool`, and `UnboundedMemoryPool`.

The second axis is scope, meaning how many native plans share one pool instance. `create_memory_pool`
in `native/core/src/execution/memory_pools/mod.rs` implements three scopes:

- **Per plan.** A fresh pool is constructed on every `createPlan` call.
- **Per task.** The pool is memoized in the `TASK_SHARED_MEMORY_POOLS` map keyed by
  `task_attempt_id` (`memory_pools/task_shared.rs`), with a `num_plans` refcount incremented on
  create and decremented in `handle_task_shared_pool_release`, which `releasePlan` calls
  (`jni_api.rs`). The entry is removed when the count reaches zero. This matters because a single
  Spark task can run more than one native plan at a time, for example a plan feeding a shuffle
  writer plan.
- **Per process.** The pool lives in a `OnceCell` static, so the first plan to run in the executor
  initializes it and every later plan in that JVM reuses it.

| Config value             | Mode     | Implementation                   | Scope       | Sized from              |
| ------------------------ | -------- | -------------------------------- | ----------- | ----------------------- |
| `fair_unified`           | off-heap | `CometFairMemoryPool`            | per task    | `memory_limit`          |
| `greedy_unified`         | off-heap | `CometUnifiedMemoryPool`         | per task    | no size, see below      |
| `greedy`                 | on-heap  | DataFusion `GreedyMemoryPool`    | per plan    | `memory_limit_per_task` |
| `fair_spill`             | on-heap  | DataFusion `FairSpillPool`       | per plan    | `memory_limit_per_task` |
| `greedy_task_shared`     | on-heap  | DataFusion `GreedyMemoryPool`    | per task    | `memory_limit_per_task` |
| `fair_spill_task_shared` | on-heap  | DataFusion `FairSpillPool`       | per task    | `memory_limit_per_task` |
| `greedy_global`          | on-heap  | DataFusion `GreedyMemoryPool`    | per process | `memory_limit`          |
| `fair_spill_global`      | on-heap  | DataFusion `FairSpillPool`       | per process | `memory_limit`          |
| `unbounded`              | on-heap  | DataFusion `UnboundedMemoryPool` | per plan    | unlimited               |

`memory_limit` and `memory_limit_per_task` are computed on the JVM in
`CometExecIterator.getMemoryConfig` and passed to `createPlan` as separate arguments. Which two
values those are depends on the mode: in off-heap mode `memory_limit` is
`spark.memory.offHeap.size` times `spark.comet.exec.memoryPool.fraction`, and in on-heap mode it is
`CometSparkSessionExtensions.getCometMemoryOverhead`. `memory_limit_per_task` is
`memory_limit * spark.task.cpus / cores` in both. In off-heap mode `memory_limit_per_task` crosses
JNI but `parse_memory_pool_config` never reads it: the off-heap branch sizes from `memory_limit`
only.

Every pool is wrapped in DataFusion's `TrackConsumersPool` with `NUM_TRACKED_CONSUMERS = 10`
(`memory_pools/mod.rs`). That wrapper does not change any allocation decision. Its only effect is
on error text: its `try_grow` matches specifically on `DataFusionError::ResourcesExhausted` and
appends a report of the top consumers by reservation size. An inner pool that returns any other
error variant gets no report.

Setting `spark.comet.debug.memory` wraps the result in `LoggingMemoryPool`
(`memory_pools/logging_pool.rs`), which logs every `register`, `grow`, `shrink`, and `try_grow`
against the consumer name and task attempt id. That is the fastest way to see what a new operator
is actually reserving.

## On-heap and off-heap mode

The split is hard. `parse_memory_pool_config` branches on `off_heap_mode` first, and each branch
matches only its own names. In off-heap mode anything other than `fair_unified` and
`greedy_unified` returns `CometError::Config("Unsupported memory pool type for off-heap mode: ...")`.
In on-heap mode the two unified names hit the same fallthrough with the on-heap wording. There is
no fallback to a default pool, and no shared pool name across the two modes.

The mode is not a Comet setting. `CometSparkSessionExtensions.isOffHeapEnabled` reads Spark's own
`spark.memory.offHeap.enabled`. On-heap mode is a testing configuration: `CometDriverPlugin.init`
disables the plugin outright when off-heap mode is off unless
`spark.comet.exec.onHeap.enabled` is also set, and both that flag and
`spark.comet.exec.onHeap.memoryPool` are declared under the testing config category in
`CometConf.scala`. The two config keys are separate per mode:
`spark.comet.exec.memoryPool` for off-heap and `spark.comet.exec.onHeap.memoryPool` for on-heap.
Read the current defaults from `CometConf.scala` or from the generated
[configuration reference](../user-guide/latest/configs.md) rather than assuming them.

The practical consequence for a contributor: adding a pool type is not one edit. The variant goes
in `MemoryPoolType`, the name goes in exactly one branch of `parse_memory_pool_config`, the
construction goes in `create_memory_pool`, and if it is task shared it also has to be listed in
`MemoryPoolType::is_task_shared` or `handle_task_shared_pool_release` will return early and the
entry will never be evicted from `TASK_SHARED_MEMORY_POOLS`.

## How native memory relates to Spark's

Only `CometUnifiedMemoryPool` and `CometFairMemoryPool` talk to Spark. Both hold a JNI global
reference to a `CometTaskMemoryManager` and call two methods on it:

- `acquire_memory(size) -> i64`, which calls
  `TaskMemoryManager.acquireExecutionMemory(size, nativeMemoryConsumer)` and returns the number of
  bytes actually granted (`spark/src/main/java/org/apache/spark/CometTaskMemoryManager.java`).
- `release_memory(size)`, which calls `releaseExecutionMemory`.

Spark is allowed to grant less than requested. Both pools treat a partial grant as a failure with
the same three steps: release the bytes that were granted, return a `ResourcesExhausted` error, and
leave the retry to the caller (`memory_pools/unified_pool.rs`, `memory_pools/fair_pool.rs`). A
partial grant is never usable memory in Comet.

Two consequences that are easy to get wrong:

**Spark cannot make Comet spill.** The `MemoryConsumer` that `CometTaskMemoryManager` registers with
Spark is `NativeMemoryConsumer`, whose `spill` method returns `0` unconditionally. The class comment
states the reason: Comet native does not share Spark's spill API, so when either side acquires
memory, spilling can only be triggered from JVM operators. Pressure flows one way. Native code
learns about pressure only by asking for memory and being told no.

**`greedy_unified` carries pool size 0 on purpose.** `parse_memory_pool_config` constructs it with a
size of zero, and the comment there explains that the pool interacts with Spark's pool to allocate
memory and so does not need a size; the shared size is set by `spark.memory.offHeap.size`.
`CometUnifiedMemoryPool::new` takes no size argument at all, so the zero is inert. The pool imposes
no ceiling of its own: `try_grow` asks Spark and succeeds whenever Spark grants the full amount.

`fair_unified` does impose a ceiling, and its arithmetic is worth reading before you reason about
it. `CometFairMemoryPool::try_grow` computes `limit = pool_size / num`, where `num` is the count of
registered consumers, and rejects the request when `limit < used + additional`. `used` is the
pool's own running total across all consumers, not the size of the calling reservation. The
in-file comments explain the switch away from `reservation.size()`: DataFusion 53 and later call
`try_grow` before incrementing the reservation's atomic size and decrement it before calling
`shrink`, so the reservation's own counter is not usable at those points. The effect is that each
additional registered consumer lowers the ceiling for the whole pool rather than only for that
consumer.

`CometFairMemoryPool` also differs from DataFusion's `FairSpillPool` in what it counts. DataFusion
increments its divisor only for consumers with `can_spill == true` and lets non-spilling consumers
draw against the rest of the pool. Comet's `register` ignores `can_spill` and counts every consumer.

Note that the `CometTaskMemoryManager` a plan passes to `createPlan` is not necessarily the one its
pool uses. `CometExecIterator` constructs a new `CometTaskMemoryManager` per plan, but
`create_memory_pool` only captures the handle inside the `or_insert_with` closure, so for a
task-shared pool the second and later plans in a task hand over a manager that is dropped. Their
allocations are still accounted against Spark correctly, because every `CometTaskMemoryManager` in
a task delegates to the same `TaskContext.taskMemoryManager()`, but the per-instance `used` counter
on the later managers stays at zero.

## Spilling

There is no spill machinery in `memory_pools/`. The pools only grant or deny. Spilling is
implemented by the operators, and there are two kinds in Comet.

Most spilling is DataFusion's. Comet's planner builds DataFusion's `SortExec`, `AggregateExec`,
`SortMergeJoinExec`, `NestedLoopJoinExec`, and `HashJoinExec`
(`native/core/src/execution/planner.rs`), and those operators own their reservations and, where
they have one, their spill logic. `HashJoinExec` is the exception worth knowing about: its build
side calls `state.reservation.try_grow(batch_size)?` and propagates the error, so a hash join that
does not fit fails rather than spills. DataFusion's `ExternalSorter` is the model for the ones that
do spill: `reserve_memory_for_batch_and_maybe_spill` calls `reservation.try_grow(size)`, and on any
`Err` it spills the in-memory batches, if it has any, and retries the same `try_grow` once. Spill
files land in the directories Comet passes down from Spark's `blockManager.getLocalDiskDirs`,
configured through
`DiskManagerBuilder` in `prepare_datafusion_session_context` and capped by
`spark.comet.maxTempDirectorySize`.

The one Comet-owned spilling operator is the native shuffle writer. `MultiPartitionShuffleRepartitioner`
in `native/shuffle/src/partitioners/multi_partition.rs` is the only place in Comet's own native
code that registers a `MemoryConsumer`, and it is the reference for what an operator that buffers
data has to do:

```rust
let reservation = MemoryConsumer::new(format!("ShuffleRepartitioner[{partition}]"))
    .with_can_spill(true)
    .register(&runtime.memory_pool);
```

After buffering a batch it grows the reservation and spills when either the grow fails or a
configured byte limit is reached:

```rust
if self.reservation.try_grow(mem_growth).is_err()
    || self
        .max_buffer_bytes
        .is_some_and(|limit| self.reservation.size() >= limit)
{
    self.spill()?;
}
```

`try_grow` is evaluated first so the reservation is charged either way, which means the writer can
overshoot by at most one batch. `spill` writes every partition out and then calls
`self.reservation.free()`. The `max_buffer_bytes` limit comes from
`spark.comet.shuffle.native.maxBufferBytes`, where the default of `0` disables it and leaves a
denied allocation as the only spill trigger.

The subtle part of that operator is not the spill, it is the measurement. `count_new_buffers` sums
the capacities of the backing buffers a batch newly pins, keyed by buffer start address across all
buffered batches, and the function's doc comment records why the two obvious alternatives are both
wrong. A partial `HashAggregate` emits one group-values buffer sliced into `batch_size` chunks that
all share the allocation, so `RecordBatch::get_array_memory_size` charges that allocation once per
chunk and spills spuriously, while summing `ArrayData::get_slice_memory_size` charges only live rows
and undercounts, because holding a slice pins the whole allocation and the underlying `Vec` rounds
capacity up to a power of two.

## Rules and common mistakes

- **Reserve through the pool or the memory is invisible.** A buffer that is not charged to a
  `MemoryReservation` is invisible to the pool, to `TrackConsumersPool`'s top-consumer report, and
  in off-heap mode to Spark's `TaskMemoryManager`. If you add a native operator that holds batches
  across calls, register a `MemoryConsumer` the way `MultiPartitionShuffleRepartitioner::try_new`
  does.
- **Use `try_grow`, never `grow`, in code that can run under the unified pools.**
  `MemoryReservation::grow` is infallible in DataFusion's API and delegates straight to
  `MemoryPool::grow`. Both of Comet's JVM-backed pools implement `grow` as
  `self.try_grow(reservation, additional).unwrap()` (`unified_pool.rs`, `fair_pool.rs`), so when
  Spark declines the request the native thread panics instead of returning an error that an
  operator could spill on. DataFusion's own pools never fail `grow`, so this hazard is specific to
  Comet's pools and is invisible in a test that uses a `GreedyMemoryPool`.
- **Treat any `Err` from `try_grow` as the signal to spill, not as a fatal error.** That is the
  contract the pools are written against: both Comet pools' comments say the error is returned in
  the hope of triggering spilling from the caller side, and DataFusion's `ExternalSorter` and
  Comet's shuffle writer both implement exactly that. An operator that propagates the error without
  first trying to free memory turns a recoverable condition into a query failure.
- **Return `ResourcesExhausted` from pool code.** The variant is load bearing in two places.
  `TrackConsumersPool::try_grow` matches on it to attach the top-consumer report, and
  `NestedLoopJoinExec::can_fallback_to_spill` requires the error's root to be `ResourcesExhausted`
  before it will switch to its disk-backed mode. A pool that signals exhaustion with any other
  variant loses the diagnostic and disables that join's fallback. Use `resources_err!` or
  `resources_datafusion_err!`, as both Comet pools do.
- **Do not size a reservation with `get_array_memory_size` or with a sum of slice sizes.** See
  `count_new_buffers` above for the two failure modes and the measure that works.
- **Global pools are initialized once per executor process.** `greedy_global` and
  `fair_spill_global` are built inside a `OnceCell`, so the first plan in the JVM fixes the size for
  every later plan. A test that changes the memory limit between queries and expects the new limit
  to take effect will silently keep the old one.
- **Every `createPlan` that takes a task-shared pool needs its matching `releasePlan`.** The
  refcount in `PerTaskMemoryPool::num_plans` is the only thing that evicts the entry from the
  process-wide `TASK_SHARED_MEMORY_POOLS` map. A path that creates a plan without releasing it
  leaks the pool for the life of the executor, and under the two unified pools it also leaks the
  `CometTaskMemoryManager` global reference that pool holds.

## Further reading

- [Arrow FFI](ffi.md) for buffer ownership across the JVM and native boundary. That page covers
  who owns a buffer and when it is freed; this page covers who is charged for it. The two do not
  overlap: importing a batch over FFI does not reserve memory from the pool.
- [Native Shuffle](native_shuffle.md) for the shuffle writer that the spilling example above comes
  from.
- [Tuning Guide](../user-guide/latest/tuning.md) for the operator-facing view of pool selection and
  sizing.
- [Tracing](tracing.md) for the per-thread memory totals that `createPlan` registers when
  `spark.comet.tracing.enabled` is set.
