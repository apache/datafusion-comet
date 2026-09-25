---
name: review-comet-memory-pr
description: Use when reviewing a DataFusion Comet pull request that touches memory pools, memory reservations, spilling, the Spark TaskMemoryManager bridge, allocators, or memory-related configuration. Load alongside review-comet-pr.
argument-hint: <pr-number>
---

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

Memory-specific review for Comet PR #$ARGUMENTS.

**REQUIRED BACKGROUND:** Use `review-comet-pr` for PR metadata, existing comments, CI, the review
bar, and the output format. This skill only covers memory.

## Read the Contributor Guide First

| Doc                                                  | What you need from it                                                             |
| ---------------------------------------------------- | --------------------------------------------------------------------------------- |
| `docs/source/contributor-guide/memory_management.md` | The three budgets, the allocator inventory, the pool stack, the accounting gap    |
| `docs/source/contributor-guide/ffi.md`               | Where buffers physically live when a batch crosses the boundary                   |
| `docs/source/contributor-guide/debugging.md`         | "Debugging Memory Reservations", for what evidence to ask the author for          |
| `docs/source/contributor-guide/tracing.md`           | `jemalloc_allocated` against summed reservations, the only measurement of the gap |

Read `memory_management.md` in full before the diff. Most bad memory PRs are not wrong about code,
they are wrong about which of the three budgets they are affecting.

## 1. Name the Budget

There are three budgets, enforced by three different parties, and a PR that improves one can make
another worse:

| Budget             | Enforced by       | Failure mode                                                      |
| ------------------ | ----------------- | ----------------------------------------------------------------- |
| JVM heap           | The JVM           | `OutOfMemoryError`, executor exits with code 52                   |
| Spark memory pools | Spark bookkeeping | A consumer spills, or the task fails with `SparkOutOfMemoryError` |
| Container RSS      | OS or cgroup      | `SIGKILL` of the whole executor, exit 137, `OOMKilled`            |

The first two are accounting, a running total of voluntarily declared bytes. The third is physical.
Ask which one the PR claims to fix and check that the mechanism actually reaches it. Lowering a
declared reservation does not reduce resident pages. Reducing resident pages does not by itself
prevent a `SparkOutOfMemoryError`.

Comet's declared reservations are a **lower bound** on real usage, for structural reasons the guide
enumerates: undeclared allocations in builders and kernels, 64-byte padding and doubling growth,
allocator fragmentation and retained pages, non-Rust allocations, and batches in flight across FFI.
A PR that treats `reserved()` as the real footprint is built on a false premise.

## 2. Reservations

Native operators reserve through DataFusion's `MemoryConsumer` and `MemoryReservation` API.

- [ ] **`try_grow` failure turns into spilling, not an error.** For a spillable operator,
      `ResourcesExhausted` is the signal to spill and retry. A new operator that propagates it as a
      query failure has converted a recoverable condition into a lost task.
- [ ] **Treat a new `grow` call site as potential unbacked memory.** `grow` cannot fail. When
      Spark grants less than asked, the Comet pools record the full amount anyway and carry the
      shortfall as overcommit, which Spark does not know about and can hand to another consumer
      or task. Nothing caps how far successive `grow` calls overcommit short of the container
      limit. That is acceptable only for memory that already exists and cannot be spilled, such
      as a spilled batch read back from disk, so a new call site needs that justification.
      Memory the caller is about to allocate belongs behind `try_grow`.
- [ ] **Every `try_grow` has a matching `shrink`, including on the error path.** A reservation
      leaked on an early return is charged for the life of the task.
- [ ] **In `try_grow`, a partial grant is released, not kept.** `acquired < additional` must
      release and report `ResourcesExhausted`. Only `grow` keeps a partial grant, as overcommit.
- [ ] **An operator that buffers without reserving is invisible to the pool.** If the PR adds
      buffering, ask where the reservation is. "It is only a few batches" is how the accounting gap
      grows.
- [ ] **Reservation size matches what is actually held.** Check the PR uses buffer sizes read off
      the `ArrayData`, as `get_reserved_bytes_for_record_batch` and `get_record_batch_memory_size`
      do, rather than a row-count estimate.

## 3. The Pool Stack

`create_memory_pool` builds a base pool and wraps it in decorators. In the default off-heap
configuration, from the inside out:

```text
[LoggingMemoryPool]        <- only when spark.comet.debug.memory=true
  [TaskSharedMemoryPool]   <- RAII handle for the per-task registry
    [TrackConsumersPool]   <- DataFusion, names the top 10 consumers in error messages
      [CometFairMemoryPool]  <- delegates acquire/release to Spark over JNI
```

- [ ] A new decorator forwards **every** `MemoryPool` method to its inner pool. A partial
      implementation makes `reserved()` disagree between levels.
- [ ] **`fair_unified` checks the requesting consumer against its share, and the total against
      the pool.** It divides `pool_size` by the number of registered consumers and rejects if what
      the consumer holds plus the request exceeds that quotient, or if the pool's total reserved
      plus the request exceeds `pool_size`. With two consumers and an 8 GiB pool, once one holds
      3 GiB the other can still reserve up to 4 GiB. The pool keeps a running total for each
      consumer id, so the sibling reservations that `new_empty()`, `split()` and `take()` create
      count against one share. A PR that checks `reservation.size()` instead lets an operator with
      several reservations, such as a sort's streaming merge, take other consumers' shares. It
      also depends on when DataFusion updates the size, which is after it calls `try_grow` but
      before it calls `shrink`. If the PR changes either comparison it changes when every query
      spills, so it needs benchmark evidence, not reasoning.
- [ ] **`num_consumers` counts every consumer in the task**, across every native plan, because the
      pool is task-shared.
- [ ] **Task-shared pool lifetime.** `acquire_task_shared_pool` keeps a process-wide
      `HashMap<task_attempt_id, Weak<TaskSharedMemoryPool>>`. The returned `Arc` is the only
      lifetime handle, so there is no explicit release to forget and a `createPlan` that fails
      partway cleans up on unwind. A PR that adds an explicit release call, or stores the `Arc`
      somewhere longer-lived, breaks that property. `TaskSharedMemoryPool::drop` compares pointers
      so it only removes an entry that is still its own, which handles the race where an `acquire`
      observes an expired `Weak` and inserts a replacement first. Do not let that check be
      simplified away.
- [ ] **`fair_unified` and `greedy_unified` are the only pool types.** On-heap mode ignores the
      pool-type string and always gets `UnboundedMemoryPool`: it exists so the Spark SQL test suite
      can run against Comet, it accounts for nothing, and it must not be used in production. A PR
      re-adding a sized on-heap pool is reintroducing a budget that bounds nothing real (see
      issue #6063).

## 4. The Spark Bridge

`CometUnifiedMemoryPool` and `CometFairMemoryPool` call `CometTaskMemoryManager.acquireMemory` over
JNI, which goes through Spark's ordinary `TaskMemoryManager`. Two asymmetries matter:

- Spark **can** force its own consumers in the same task to spill to satisfy a Comet request.
- Comet **cannot** be forced to spill by Spark. `NativeMemoryConsumer.spill()` returns `0`, so a
  JVM consumer blocked behind native reservations has no way to reclaim them. Native operators
  spill only when their own `try_grow` fails.

If the PR claims Spark-side reclamation works, check `spill()`. If the PR implements native
reclamation, that is a significant change and needs to say what it does to in-flight operators.

## 5. Budget and Configuration

`CometExecIterator.getMemoryConfig` computes the budget once per executor and passes it across JNI
as `memory_limit`:

```text
memory_limit = spark.memory.offHeap.size * spark.comet.exec.memoryPool.fraction
```

- [ ] A new memory config goes through `getMemoryConfig` rather than being read separately in
      native code.
- [ ] Changing `memoryPool.fraction` semantics affects every deployment that tuned it as a haircut
      for the accounting gap.
- [ ] On Kubernetes, `spark.memory.offHeap.size` is **part of** the pod limit, not headroom on top
      of it. A PR whose fix is "raise the off-heap size" is asking for fewer executors per node.
      `spark.executor.memoryOverhead` is the only real slack in the container, and JVM non-heap
      usage already consumes much of it.

## 6. Unaccounted Allocators

Two allocators sit outside Comet's pool entirely, and they are easy to confuse:

- **`CometArrowAllocator`** (`spark/src/main/scala/org/apache/comet/package.scala`) is a
  process-wide `RootAllocator(Long.MaxValue)`. It is unbounded and no budget sees it. Child
  allocators are cut from it for FFI stream export, broadcast coalescing, and
  `CometSparkToColumnarExec`. A PR that adds a child allocator, or makes an existing one hold more,
  is adding uncounted container RSS. Say so even if the volume is small.
- **JVM shuffle pages** go through `CometShuffleMemoryAllocator.getInstance`, which returns
  `CometUnifiedShuffleMemoryAllocator`, an ordinary Spark `MemoryConsumer` drawing from
  `spark.memory.offHeap.size`. These **are** arbitrated by Spark against its other consumers. Do
  not review them as if they were native allocations.

## 7. Evidence

Memory changes are hard to unit test, so the review question is what evidence exists rather than
what test was added. Ask for at least one of:

- `spark.comet.debug.memory=true` output from `LoggingMemoryPool`, showing register, grow, and
  shrink with consumer names
- A trace comparing `jemalloc_allocated` against the summed
  `thread_NNN_comet_memory_reserved` values, which is the only way to see the accounting gap
- Spill counts from `spark.comet.explain.native.enabled=true`, before and after
- For a fix to an OOM report, the exit code that identifies which budget was exceeded. 137 or
  `OOMKilled` is the cgroup. 52 with `java.lang.OutOfMemoryError` is JVM heap. A failed task with
  `SparkOutOfMemoryError` and a surviving executor is Spark's pool, the only one of the three that
  is recoverable at task level.

`spark/src/test/scala/org/apache/spark/CometTaskMemoryManagerSuite.scala` and
`CometUnboundedShuffleMemoryAllocatorSuite.scala` are the existing JVM-side tests. A change to the
bridge or an allocator should extend one of them.

## 8. Does the PR Make `memory_management.md` Stale?

This doc states invariants and enumerates known gaps, so it goes stale in ways a file-path check
will not catch. Check:

- The **Who allocates what** table, if the PR adds an allocator or changes what bounds one
- The **pool stack** diagram, if a decorator is added, removed, or reordered
- The **pool type** table, if a pool type is added, removed, or changes how it is sized
- The `memory_limit` formula, if the budget calculation changes
- The **fair pool** description, which spells out the share and pool-total checks and their
  consequences
- The **Crossing the FFI boundary** section, which names the operators that reserve for imported
  batches
- The **Open problems** list. If the PR closes one of these gaps, the entry must be removed or
  rewritten in the same PR. Leaving a fixed problem listed as open is worse than no doc, because
  the next contributor will re-fix it.
- The **Debugging memory issues** table and checklist, if a tool or signal is added or removed
