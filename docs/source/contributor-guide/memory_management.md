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
[LoggingMemoryPool]          <- only when spark.comet.debug.memory=true
  [RealUsagePool]            <- only when the oom-guard build + memoryGuard.enabled
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

## The OOM guard (experimental)

The layers described so far cannot prevent an OOM kill, because they bound reservations rather than
allocations. The `oom-guard` feature adds allocator-level tracking and two enforcement points built
on it. It is **a prototype and is not in the default cargo feature set**, so released binaries do
not contain it unless it is explicitly enabled at build time — see
[issue #4576](https://github.com/apache/datafusion-comet/issues/4576) and
[PR #4582](https://github.com/apache/datafusion-comet/pull/4582).

### Building and enabling it

```shell
cd native && cargo build --release --features oom-guard
```

or, through the project Makefile:

```shell
COMET_FEATURES=oom-guard make release
```

```properties
spark.comet.exec.memoryGuard.enabled=true
# optional; defaults to the value of memory_limit above
spark.comet.exec.memoryGuard.size=6g
```

Both configurations are inert in a build without the feature. When the feature is absent there is no
allocator wrapper at all, so the default build carries zero per-allocation overhead.

### Layer 1: the accounting allocator

`AccountingAllocator<A>` (`memory_pools/oom_guard.rs`) wraps whichever allocator the build selected
— jemalloc, mimalloc, or the system allocator — and is installed as `#[global_allocator]`. On every
`alloc` / `alloc_zeroed` / `dealloc` / `realloc` it adds the `Layout` size delta to a thread-local
`LOCAL_DRIFT`. When a thread's drift exceeds 64 KiB in either direction it is flushed into a single
process-wide `AtomicIsize` called `BALANCE`. Batching keeps the common path to a thread-local
add-and-compare, so only roughly one atomic RMW per 64 KiB of churn reaches the shared cacheline.

`BALANCE` is therefore the count of **layout bytes currently handed out by the Rust global
allocator, process-wide**. It is not RSS, and it is not per-task. It does capture the undeclared
allocations that the memory pool misses, which is the whole point.

### Layer 2: the cooperative gate (`RealUsagePool`)

`RealUsagePool` is a `MemoryPool` decorator. Before delegating a `try_grow(additional)` to the inner
pool it checks the projected real usage:

```text
if BALANCE + additional > ceiling:
    reject with ResourcesExhausted
```

where `ceiling` is `memory_limit`. Rejecting _before_ delegating means the inner pool is never
speculatively reserved, so there is nothing to roll back. Because the error is `ResourcesExhausted`,
DataFusion's spilling operators treat it exactly like an ordinary pool rejection: they spill and
retry. This is the layer that is supposed to make Comet react to real usage rather than tracked
reservations, and in principle it removes the need to hand-tune
`spark.comet.exec.memoryPool.fraction`.

Because `BALANCE` is process-wide but the pool is per-task, a naive gate would punish whichever task
happened to call `try_grow` first after the executor crossed the ceiling. The gate therefore applies
a fair-share test: once over the ceiling, a task is only rejected if its own tracked reservation
would exceed `ceiling / active_tasks`. `active_tasks` is the number of live task-shared pools;
`spark.executor.cores` is the fallback divisor for pool types that keep no task registry. Tasks
under their share are allowed through, and the breaker below is the backstop for the runaway case.

The gate adds one relaxed atomic load per `try_grow`.

### Layer 3: the circuit breaker

The breaker is the last resort. `createPlan` calls `oom_guard::arm(limit)`, and tokio worker threads
plus the JNI caller thread are "stamped" as query-worker threads. When a stamped thread flushes a
positive drift that pushes `BALANCE` past the limit, `AccountingAllocator` raises a typed
`OomGuardPanic` via `panic_any` from inside the allocation call.

Several details exist to make that survivable:

- **Only stamped threads panic.** Allocations on unstamped threads (`spawn_blocking`, IO pools,
  anything the JVM owns) are still counted but cannot themselves trip the breaker.
- **`realloc` panics before delegating.** If it panicked after `inner.realloc`, the old block may
  already have been freed or moved while the caller still holds the old pointer, and the unwind
  would free a dangling pointer.
- **Only one thread may fire per arm cycle.** The breaker CASes `ARMED` from `true` to `false`;
  losers bail out before `panic_any`. Several threads dispatching a panic within the same few
  milliseconds can abort the process with "failed to initiate panic" instead of unwinding. The next
  `createPlan` re-arms.
- **Re-entrancy is handled.** `panic_any` boxes its payload, which allocates and re-enters the
  allocator. `ARMED` is already `false` by then, and a thread-local `UNWINDING` flag adds a second
  guard in case a concurrent `createPlan` re-arms mid-unwind.

`executePlan` catches the panic at both execution boundaries (the spawned-task channel path, on both
the producer and the consumer side, and the busy-poll `block_on` path) and maps it to
`DataFusionError::ResourcesExhausted` via `oom_guard::map_panic_to_error`, which also clears the
thread's `UNWINDING` flag — necessary because the JNI caller thread is reused across tasks.

### What the user sees

The error reaches Spark as a `CometNativeException`, an ordinary `RuntimeException`. Spark retries
the task up to `spark.task.maxFailures` and then fails the stage. This is a meaningful improvement
over an OOM kill — the executor, its other tasks, and its cached blocks all survive — but it is a
_failure_ path, not a recovery path. A query whose working set genuinely does not fit will now fail
deterministically instead of taking the executor down.

### Choosing `memoryGuard.size`

The default sets the breaker's limit equal to the cooperative gate's ceiling (`memory_limit`). The
two layers are intended to order correctly even at the same value, because the gate trips on
projected usage (`BALANCE + additional`) while the breaker trips on actual usage (`BALANCE`), so the
gate should fire first. That ordering holds only when a `try_grow` happens between crossing the
ceiling and the next allocation. Usage that grows purely through undeclared allocations — exactly
the case the guard exists for — reaches the breaker with no cooperative spill attempted.

Setting `memoryGuard.size` above `memory_limit` therefore gives the gate a real chance to spill
before the breaker fires. On Kubernetes a reasonable target is the slack between the pod limit and
everything else in the container:

```text
memoryGuard.size  ~  pod memory limit
                     - spark.executor.memory          (JVM heap)
                     - JVM non-heap (metaspace, code cache, thread stacks, direct buffers)
                     - Spark's own off-heap usage
                     - a safety margin for allocator fragmentation and page cache
```

That is necessarily an estimate. Because `BALANCE` undercounts RSS (see below), the guard should be
given a budget comfortably below the true headroom.

### Limitations

These are known and mostly inherent to the prototype:

- **Layout bytes, not RSS.** `BALANCE` counts what the program asked for, not resident pages. It
  misses allocator fragmentation, jemalloc's retained pages, `mmap`ed regions, and allocations made
  by C dependencies through libc `malloc`. There is no periodic resync against real jemalloc stats,
  so the gap between `BALANCE` and RSS is unbounded and one-directional (RSS is always larger).
- **Per-thread drift is lost when a thread exits.** `LOCAL_DRIFT` is a plain `Cell<isize>` with no
  TLS destructor, so up to 64 KiB of un-flushed drift is silently discarded each time a thread dies.
  Tokio worker threads live for the process lifetime, but blocking-pool threads idle out and churn,
  so on a long-lived executor this is a slowly accumulating bias in either direction.
- **Executor-global granularity.** The breaker fires on whichever stamped thread happens to allocate
  when the process crosses the limit, which need not be the task responsible for the usage. The
  fair-share test in the cooperative gate mitigates this for `try_grow`, but not for the breaker.
- **Only tokio workers and the JNI caller thread are stamped**, so allocations elsewhere are tracked
  but cannot trip the breaker.
- **Panicking from inside the global allocator** unwinds through code that was mid-allocation. It is
  memory-safe in the cases exercised so far, but a guard panic raised while another panic is already
  unwinding is a double panic and aborts the process.
- **The budget is not auto-sized.** The gate reacts to real usage but does not yet adjust the pool
  budget or deprecate `spark.comet.exec.memoryPool.fraction`.

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
