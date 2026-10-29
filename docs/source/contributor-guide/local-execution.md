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

# Local Execution

Local execution is an experimental mode for Spark applications that run with a local master.
Instead of running a query as Spark tasks that each create and run a native plan, a `collect`
runs the whole query as one DataFusion graph inside a single Spark task, for every partition at
once. Other actions run the query as ordinary Comet execution; see result delivery.

The internal `spark.comet.exec.local.enabled` option, false by default, turns it on. A query
is admitted only as a whole. Every other query uses ordinary Comet execution.

## What is admitted

Admission runs in `CometRule`, on the plan that Comet's ordinary conversion produces. A query is
admitted when:

- the application's `SparkContext` is local (`local[N]`), so the query runs in this process;
- the plan is a batch query and not a subquery being prepared;
- the converted plan is one native block, a tree of Comet native operators whose root carries
  the block's serialized plan, with no JVM input, broadcast or shuffle inside it;
- the block's only leaf is a `CometRangeExec`, so the range must be converted to Comet
  (`spark.comet.exec.range.enabled`);
- its expressions are deterministic, contain no subquery, and do not call back into the JVM,
  which the serialized plan is also checked for;
- it has between 1 and 1024 partitions.

A root `CollectLimitExec`, which Spark plans for `take`, `head` and `show`, is kept above the
local query, and reads it as described under result delivery. Such plans have no exchange, so AQE does not wrap them and they are admitted
whether or not AQE is enabled.

## Execution

The admitted plan becomes `CometLocalResultExec` over `CometLocalExec`, a leaf whose RDD has a
single partition. That partition's task creates the native graph:

- The memory pool, spill directories and DataFusion session are those `Native.createPlan` creates
  for any Comet plan of the task: `create_execution_context` builds them, with Comet's function
  registrations and session settings, and the pool acquires memory from Spark through the task's
  `CometTaskMemoryManager`.
- The block's operators are created once per partition, by the planner for that partition, as a
  Spark task would create them. Partition `i` of the graph is the output of the operators created
  for partition `i`, so per-partition state such as a range slice is the same as with ordinary
  Comet execution.
- `native/local` runs the partitions on Comet's Tokio runtime and delivers them in partition
  order. At most `spark.executor.cores` partitions (N for `local[N]`) run at once, and the next
  starts when the first is delivered, as Spark runs at most that many tasks. A partition's
  operators register their memory consumers when it starts, so this also bounds how many share
  the task's memory pool. The result therefore has Spark's row order. A partition's error is
  reported when its turn comes.
- The running partitions do not wait for their turn to produce batches, so they all make
  progress, as Spark's tasks do. What they hold ahead of delivery is bounded by
  `spark.driver.maxResultSize`, the most a collect may bring to the driver; past it, each holds
  at most two batches until the earlier partitions are delivered. These batches are not reserved
  in the memory pool: a reservation could not be given back when an operator later needs the
  memory, so it would fail queries that ordinary execution runs. Spark's own collect also holds
  its task results outside the execution pool, on the driver.
- All the partitions' memory consumers share the one task's pool, where ordinary execution gives
  each task its own share. Consumers that can spill each get a share of the task's pool, as they
  would of a task's. Consumers that cannot spill, such as a sort's merge, are bounded together by
  the pool, as the tasks' consumers are together, but no longer each by one task's share: one
  partition can take more than a task could, leaving less for the others.

`CometLocalExec` reports `UnknownPartitioning(1)`: its RDD is one partition, but its rows are
not clustered by anything. Spark's ordering of the block holds within each partition, so
`CometLocalExec` reports it only when it holds across the concatenated partitions: with one
partition, or when the partitions are ranges of a prefix of that ordering, as a range's slices
are. Sorting within partitions, for example, reports no ordering. `CometLocalResultExec` reports
the block's own partitioning and ordering, which hold for the partitions it returns.

Explain shows the native block under `CometLocalExec`. Its native operators do not report SQL
metrics; `CometLocalExec` counts the rows and batches it delivers.

## Result delivery

The native side hands batches to the JVM through a queue that holds at most one batch. Exported
Arrow arrays use `prepare_output` and `NativeUtil`; an imported batch stays valid after its
native query is closed, until the JVM closes it. A pull returns "pending" after 50 ms, so that
the task can check whether it was killed.

`CometLocalResultExec` sits above Spark's columnar-to-row transition. For `collect` it still
runs the single task as a Spark job, which keeps cancellation, job groups and SQL metrics, but
the task copies its rows into a driver-side slot instead of returning an encoded task result. Spark would otherwise encode and compress every row in that one task and decode
them on the driver, all on one thread. While copying, the task counts uncompressed `UnsafeRow`
bytes and stops reading, which closes the native query, once they exceed
`spark.driver.maxResultSize`; the driver then fails the action. Spark counts compressed bytes,
so this check is the stricter one. On a non-local master the node uses Spark's ordinary
collect.

Every other action reads the result as an RDD of the block's partitions: a take requests
partitions in growing batches until it has enough rows, an iterator fetches one partition at a
time so that it holds at most one, a tail scans partitions from the last one, and a cache,
`df.rdd`, `foreachPartition`, `localCheckpoint` or PySpark's collect and `toLocalIterator` keep
the partitions for later reads. So `CometLocalResultExec` executes as the ordinary native block,
one Spark task per partition, for everything but `collect`. The partitions these actions
evaluate, and so the rows they return and the errors they raise, are Spark's, and a cached result
is scanned as many partitions as without local execution.

## Lifecycle and cancellation

The JNI registry holds numeric, never reused IDs for at most 1024 live queries. EOF, errors,
early termination and task completion remove an entry. A concurrent pull holds an `Arc`, so a
close cannot free an entry that is being read, and cancellation does not take the reader lock.
A query records when it has delivered its last batch; if the runtime stops it before that, the
reader reports an error instead of a truncated result.

Closing a query drops its streams on the calling thread, as `releasePlan` does for an ordinary
plan without JVM input: cancelling a Tokio task only takes effect once a worker is free, and
every worker can be waiting, for instance in Spark's `acquireMemory` for memory those streams
hold. Tasks that DataFusion operators spawn themselves still end asynchronously.

## Testing

Build native code before the JVM suite, and never use Maven `-pl`:

```shell
cd native
cargo test -p datafusion-comet-local
cd ..
./mvnw test -Dsuites=org.apache.comet.local.CometLocalExecutionSuite
```

The native tests cover partition-order delivery, the bound on running partitions, read-ahead, errors reported in a partition's turn, cancellation with busy workers,
the one-batch handoff and runtime shutdown. `CometLocalExecutionSuite` compares admitted queries
with Spark's results and order, and covers ranges Spark would overflow, declined expressions,
AQE, reported orderings, takes and limits, iterators, tails, many partitions with sorts in a small memory pool, caching and other RDD consumers, the result size limit, job and task cancellation, and native errors. After
each test it checks that no native query, imported Arrow memory or result slot is left.
