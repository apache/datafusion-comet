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

# Memory Management

This page describes how memory is budgeted, accounted, and enforced across the JVM/native
boundary. It is aimed at contributors working on memory pools, operators that reserve memory, or
anyone debugging an out-of-memory report. For user-facing tuning advice, see the
[Tuning Guide](../user-guide/latest/tuning.md).

## Overview

A Comet executor has to satisfy three separate memory budgets at once, and they are enforced by
three different parties:

| Budget               | Enforced by       | What happens when it is exceeded                                     |
| -------------------- | ----------------- | -------------------------------------------------------------------- |
| JVM heap             | The JVM           | `OutOfMemoryError` in a task; the executor usually survives          |
| Spark's memory pools | Spark bookkeeping | A consumer is asked to spill, or a `SparkOutOfMemoryError` is thrown |
| Container RSS        | The OS / cgroup   | `SIGKILL` of the whole executor process (exit 137, `OOMKilled`)      |

The first two are _accounting_: a running total of bytes that consumers have voluntarily declared.
The third is _physical_: the kernel measures resident pages and does not care what any accounting
layer believes.

Comet's difficulty is that its allocations are made by Rust code, so they are invisible to the JVM
heap and to Spark's own off-heap accounting, yet they land squarely in container RSS. Comet
therefore maintains its own budget that is meant to shadow the physical one, and the accuracy of
that shadow is the central problem this page is about.

## Who allocates what

Enabling Comet does not add one new memory consumer, it adds several, and they are not all
accounted by the same party. This inventory is worth internalizing before reading the rest of the
page:

| Allocator                               | Lives in    | Bounded by                                                          | Visible to Spark? |
| --------------------------------------- | ----------- | ------------------------------------------------------------------- | ----------------- |
| Spark execution + storage (on-heap)     | JVM heap    | `spark.executor.memory` and the unified memory manager              | Yes               |
| Spark Tungsten (off-heap)               | Off-heap    | `spark.memory.offHeap.size` via `TaskMemoryManager`                 | Yes               |
| Comet native (Rust global allocator)    | Native heap | `memory_limit` (see below), enforced only via the memory pool       | No                |
| Comet JVM Arrow (`CometArrowAllocator`) | Off-heap    | **Nothing** — a `RootAllocator(Long.MaxValue)`                      | No                |
| Comet JVM shuffle pages (off-heap mode) | Off-heap    | `spark.memory.offHeap.size` via `TaskMemoryManager`                 | Yes               |
| Comet JVM shuffle pages (on-heap mode)  | Off-heap    | `spark.comet.shuffle.jvm.memoryFactor * spark.comet.memoryOverhead` | No                |

Three observations follow.

**Comet's JVM-side Arrow allocator is unbounded and accounted by nobody.** `CometArrowAllocator`
(`spark/src/main/scala/org/apache/comet/package.scala`) is a single process-wide
`new RootAllocator(Long.MaxValue)`. Child allocators are cut from it for FFI stream export
(`CometNativeArrowSource`), broadcast coalescing, and `CometSparkToColumnarExec`. These are real
off-heap bytes in container RSS that neither Spark's `TaskMemoryManager` nor Comet's native memory
pool sees. In practice the volume is modest — a batch at a time per stream — but there is no
ceiling and no backpressure.

**The JVM shuffle allocator switches accounting model with the memory mode.**
`CometShuffleMemoryAllocator.getInstance` returns `CometUnifiedShuffleMemoryAllocator` when Tungsten
is off-heap, which is a proper Spark `MemoryConsumer` drawing from `spark.memory.offHeap.size`. In
on-heap mode it returns `CometBoundedShuffleMemoryAllocator`, which calls `UnsafeMemoryAllocator`
directly and bounds itself with its own counter. Only the first is arbitrated against Spark's other
consumers.

**On-heap mode double-counts `spark.comet.memoryOverhead`.** The native pool is sized at
`memory_limit = spark.comet.memoryOverhead`, and the JVM shuffle allocator is _separately_ sized at
`spark.comet.shuffle.jvm.memoryFactor * spark.comet.memoryOverhead`, with the factor defaulting to
`1.0`. They are distinct allocations from the same number, so on-heap Comet can occupy up to roughly
twice `spark.comet.memoryOverhead` in off-heap RSS, before counting `CometArrowAllocator`. Off-heap
mode does not have this problem, which is one more reason it is the recommended configuration.

## Where Comet's budget comes from

`CometExecIterator.getMemoryConfig` computes the budget once per executor and passes it across JNI
to `Java_org_apache_comet_Native_createPlan` as `memory_limit` and `memory_limit_per_task`. There
are two paths.

### Off-heap mode (`spark.memory.offHeap.enabled=true`)

This is the recommended configuration. Comet shares Spark's off-heap pool rather than asking for a
separate allocation:

```text
memory_limit          = spark.memory.offHeap.size * spark.comet.exec.memoryPool.fraction
memory_limit_per_task = memory_limit * spark.task.cpus / executor_cores
```

`spark.comet.exec.memoryPool.fraction` defaults to `1.0`. Lowering it is the current workaround for
Comet's under-accounting (see [The accounting gap](#the-accounting-gap)) — it holds back a slice of
the off-heap pool that Comet is not allowed to reserve, on the assumption that Comet's real usage
overshoots its reservations by roughly that slice.

### On-heap mode

Comet asks for a dedicated overhead allocation outside the heap:

```text
memory_limit          = spark.comet.memoryOverhead      (default 1024 MiB)
memory_limit_per_task = memory_limit * spark.task.cpus / executor_cores
```

On-heap mode is a testing configuration; the pool types it exposes are in the `CATEGORY_TESTING`
group.

### Resolving the pool type

`parse_memory_pool_config` (`native/core/src/execution/memory_pools/config.rs`) turns the mode, the
pool-type string, and the two limits into a `MemoryPoolConfig`. Note which limit each pool type is
sized from — this is a common source of confusion:

| Pool type                             | Mode     | Sized from              | Notes                                       |
| ------------------------------------- | -------- | ----------------------- | ------------------------------------------- |
| `fair_unified` (default)              | off-heap | `memory_limit`          | Delegates to Spark's `TaskMemoryManager`    |
| `greedy_unified`                      | off-heap | n/a (pool size `0`)     | Spark owns the limit entirely               |
| `greedy_task_shared`                  | on-heap  | `memory_limit_per_task` | Default on-heap pool                        |
| `fair_spill_task_shared`              | on-heap  | `memory_limit_per_task` |                                             |
| `greedy` / `fair_spill`               | on-heap  | `memory_limit_per_task` | Per-plan, not shared across plans in a task |
| `greedy_global` / `fair_spill_global` | on-heap  | `memory_limit`          | One pool for the whole executor             |
| `unbounded`                           | on-heap  | n/a                     | No limit; testing only                      |

## The pool stack

`create_memory_pool` builds a base pool and `createPlan` then wraps it in decorators. Reading from
the inside out, a Comet plan in the default off-heap configuration sees:

```text
[LoggingMemoryPool]        <- only when spark.comet.debug.memory=true
  [TaskSharedMemoryPool]   <- RAII handle for the per-task registry
    [TrackConsumersPool]   <- DataFusion; names the top 10 consumers in error messages
      [CometFairMemoryPool]  <- delegates acquire/release to Spark over JNI
```

Each decorator forwards every `MemoryPool` method to its inner pool, so `reserved()` at any level
reports the base pool's number.

### The unified pools

`CometUnifiedMemoryPool` and `CometFairMemoryPool` (`unified_pool.rs`, `fair_pool.rs`) are the
bridge to Spark. Their `try_grow` calls `CometTaskMemoryManager.acquireMemory` over JNI, which goes
through Spark's ordinary `TaskMemoryManager`. That means:

- Comet competes with Spark's own off-heap consumers (Tungsten sorters, `BytesToBytesMap`, and so
  on) for the same `spark.memory.offHeap.size`, and Spark's unified memory manager arbitrates.
- Spark can force _Spark's_ consumers to spill to satisfy Comet's request, and vice versa.
- A partial grant (`acquired < additional`) is released immediately and reported as
  `ResourcesExhausted`, which is the signal DataFusion uses to spill.

`CometFairMemoryPool` additionally caps each registered consumer at `pool_size / num_consumers`
before it even asks Spark, which is why it spills earlier than `greedy_unified` but keeps one
operator from starving the others.

### Task-shared pools and their lifetime

A single Spark task can run more than one native plan concurrently — a shuffle runs the pre-shuffle
operators and the shuffle writer as separate native execution contexts. If each got its own pool,
the per-task limit would be enforced once per plan rather than once per task.

`acquire_task_shared_pool` (`task_shared.rs`) keeps a process-wide
`HashMap<task_attempt_id, Weak<TaskSharedMemoryPool>>`. Plans in the same task upgrade the existing
`Weak` and share one pool; the returned `Arc` is the only lifetime handle, so the registry entry
disappears when the last plan (and its last reservation) drops. There is no explicit release call to
forget, and a `createPlan` that fails partway through cleans up on unwind.

`TaskSharedMemoryPool::drop` has to handle one race: an `acquire` can observe an expired `Weak` and
insert a replacement before the dying pool reaches the registry lock. The drop therefore compares
pointers and only removes an entry that is still its own.

## How DataFusion consumes the pool

Native operators reserve through DataFusion's `MemoryConsumer` / `MemoryReservation` API:

- `try_grow(n)` may fail. Spillable operators (`ExternalSorter`, the grouped hash aggregate,
  sort-merge join) respond to a `ResourcesExhausted` error by spilling to disk and retrying. This is
  the only mechanism that turns memory pressure into progress rather than failure.
- `grow(n)` is infallible and panics if the pool refuses. It is used where the caller cannot spill.
- `shrink(n)` returns bytes to the pool.

An operator that never calls `try_grow` is invisible to the pool no matter how much memory it uses.

## Crossing the FFI boundary

Batches move between the JVM and native over the Arrow C Data and C Stream interfaces, which are
zero-copy. Nothing is copied, so the _allocator_ that produced a batch and the _runtime_ that
decides when it dies can be on opposite sides of the boundary. See [Arrow FFI](ffi.md) for the
mechanics; what matters here is who is charged and who controls the lifetime.

**JVM → native (`ScanExec`).** The JVM allocates the Arrow buffers from a child of
`CometArrowAllocator` and exports the whole per-partition iterator once as an `ArrowArrayStream`.
Native takes ownership by reference through `AlignedArrowStreamReader`. The bytes were allocated by
Java Arrow, so they are absent from Comet's memory pool and from Spark's `TaskMemoryManager` — but
present in container RSS, and pinned for as long as the native side holds the imported batch. A
native operator that buffers many input batches is therefore pinning JVM-allocated off-heap memory
that none of Comet's accounting can observe.

**Native → JVM (`CometExecIterator`).** DataFusion produces the batch in Rust, so those bytes may be
reserved in the pool. The batch is exported as an `ArrowArray`/`ArrowSchema` pair, the JVM wraps the
pointers in `ArrowBuf`s, and the memory is only freed when the JVM calls `close()` and the release
callback runs. The lifetime of native, pool-charged memory is thus controlled by JVM code: a slow or
backed-up JVM consumer keeps native memory resident for batches the native side has logically
finished with.

The asymmetry is the point: **the direction of data flow determines which accounting layer, if any,
charges for a batch.** Neither direction charges both, and the JVM → native direction charges
nothing at all.

## The accounting gap

The pool tracks _declared reservations_. Container RSS counts _pages the process touched_. The two
diverge for several structural reasons:

- **Undeclared allocations.** Arrow array builders, expression kernels producing intermediate
  arrays, decompression buffers, Parquet metadata structures, `object_store` request buffers, and
  tokio's own machinery all allocate without reserving. Only operators that were explicitly written
  to reserve show up in the pool.
- **Rounding and padding.** Arrow buffers are padded to 64-byte boundaries and builders grow by
  doubling, so a reservation of exactly `n` bytes routinely corresponds to more than `n` bytes of
  heap.
- **Allocator behavior.** `malloc`-level fragmentation, size-class rounding, and jemalloc's
  retained/dirty page cache all add resident bytes that no layer above the allocator can see.
  Freeing memory does not necessarily return pages to the OS.
- **Non-Rust allocations.** Memory allocated by C dependencies through libc `malloc`, and anything
  `mmap`ed, never passes through Rust's `GlobalAlloc`.
- **FFI-imported buffers.** Batches arriving from the JVM were allocated by Java Arrow, so they
  belong to no Comet budget at all while native code holds them (see
  [Crossing the FFI boundary](#crossing-the-ffi-boundary)).

The practical consequence is that `reserved()` is a lower bound on Comet's real footprint, and the
gap is workload-dependent. `spark.comet.exec.memoryPool.fraction` exists purely so operators can
hand-tune a haircut that covers the gap for their workload.

To measure the gap on a real query, enable tracing with the `jemalloc` feature and compare
`jemalloc_allocated` against the summed `thread_NNN_comet_memory_reserved` values — see
[Tracing](tracing.md#analyzing-memory-usage).

## What the container sees

On Kubernetes, Spark sizes the executor pod from `ResourceProfile`:

```text
pod memory request = pod memory limit
                   = spark.executor.memory
                   + spark.executor.memoryOverhead   (default max(0.1 * executor.memory, 384 MiB))
                   + spark.memory.offHeap.size       (when off-heap is enabled)
                   + pyspark memory                  (Python applications only)
```

Both the request and the limit are set to this same value, so the pod's cgroup `memory.max` is a
hard ceiling on the sum of everything in the container. That cgroup counts, among other things:

- the JVM heap (`spark.executor.memory`),
- JVM non-heap: metaspace, code cache, thread stacks, GC structures, Netty direct buffers,
- Spark's own off-heap allocations,
- **all of Comet's native allocations**,
- Comet's JVM-side Arrow buffers (`CometArrowAllocator`) and, in on-heap mode, its JVM shuffle pages,
- page cache charged to the cgroup by the container's file I/O, including spill files.

Only the first and a portion of the third are visible to Spark's accounting. When the total crosses
`memory.max`, the kernel OOM killer kills the process. The failure mode is significantly worse than
a task-level OOM: every task running on that executor dies, every cached block it held is lost and
must be recomputed, and the shuffle files it produced become unavailable to downstream fetches.
Spark's driver sees only `ExecutorLostFailure` with exit code 137.

Two facts follow that are easy to get wrong:

1. **`spark.memory.offHeap.size` is part of the pod limit, not extra headroom on top of it.**
   Raising Comet's off-heap budget on Kubernetes raises the pod's memory request by the same
   amount, so the scheduler will place fewer executors per node rather than silently giving Comet
   more room.
2. **`spark.executor.memoryOverhead` is the only slack in the container**, and the JVM's own
   non-heap usage already consumes a large part of it. Comet's overshoot beyond its declared
   reservations eats into the same allowance.

YARN behaves analogously — the container size is the same sum, and the NodeManager kills containers
that exceed it — but the kill is done by the NodeManager's monitor rather than the kernel, so it is
somewhat less abrupt.

## Open problems

Nothing described above prevents an executor from being OOM-killed. Every enforcement point Comet
has bounds _declared reservations_, and the sections above describe several structural reasons why
declared reservations are a lower bound on physical usage. The known gaps, roughly in order of how
much they matter:

- **No signal for real native usage.** The only way to observe the gap today is to enable tracing
  with the `jemalloc` feature and compare `jemalloc_allocated` against summed reservations after
  the fact. There is no runtime value that an operator, a metric, or a policy could read.
- **`spark.comet.exec.memoryPool.fraction` is a manual proxy for the gap.** It asks operators to
  guess a per-workload haircut rather than measuring anything.
- **`CometArrowAllocator` is unbounded** and participates in no budget.
- **The FFI boundary is accounted asymmetrically**, so neither direction of data flow is charged to
  both sides.
- **On-heap mode can occupy roughly twice `spark.comet.memoryOverhead`** across its two independent
  allocators.

[Issue #4576](https://github.com/apache/datafusion-comet/issues/4576) tracks work on the first two.

## Debugging memory issues

| Tool                                             | What it gives you                                                          |
| ------------------------------------------------ | -------------------------------------------------------------------------- |
| `spark.comet.debug.memory=true`                  | `LoggingMemoryPool` logs every register/grow/shrink with the consumer name |
| `spark.comet.explain.native.enabled=true`        | Native plan with per-operator metrics, including spill counts              |
| [Tracing](tracing.md#analyzing-memory-usage)     | `jemalloc_allocated` vs summed pool reservations; the accounting gap       |
| `TrackConsumersPool`                             | Names the top 10 consumers in `ResourcesExhausted` messages (always on)    |
| [`thresher`](https://github.com/cetra3/thresher) | Third-party crate that dumps a jemalloc heap profile at a threshold        |

A checklist for triaging an executor OOM kill:

1. Confirm it is an OOM kill and not a JVM `OutOfMemoryError` — exit code 137 / `OOMKilled` on the
   pod, versus a heap dump and a stack trace.
2. Compare `jemalloc_allocated` against the summed pool reservations from a trace. A large excess
   points at undeclared native allocations; a small excess points at the budget simply being too
   small, or at the JVM side.
3. Check `spark.comet.batchSize` against the schema width. Peak memory scales with
   `batch_size * columns`, and wide or deeply nested schemas amplify it.
4. Check whether the operators involved can spill at all. `ShuffledHashJoin` cannot, so
   `spark.comet.exec.forceShuffledHashJoin=true` converts a spillable sort-merge join into one that
   is not.
