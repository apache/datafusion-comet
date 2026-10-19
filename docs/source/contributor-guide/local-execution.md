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
Instead of running a query as Spark tasks that each create and run a native plan, it runs the
whole query as one DataFusion graph inside a single Spark task, for every partition at once.

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
local query. Such plans have no exchange, so AQE does not wrap them and they are admitted
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
  order. At most `spark.executor.cores` partitions (N for `local[N]`) run at once, each up to two
  batches ahead, and the next starts when the first is delivered, as Spark runs at most that
  many tasks. A partition's operators register their memory consumers when it starts, so this
  also bounds how many share the task's memory pool. The result therefore has Spark's row order, and
  a limit returns the same rows as Spark's `executeTake`. A partition's error is reported when its
  turn comes, so a limit satisfied by earlier partitions does not fail on a later one, as with
  `executeTake`, which never evaluates the partitions it does not need.

`CometLocalExec` reports `UnknownPartitioning(1)`: the result is one partition, but its rows
are not clustered by anything, so a query over a cached local result still gets the exchange
its distribution requires. Spark's ordering of the block holds within each partition, so the
result reports it only when it holds across the concatenated partitions: with one partition, or
when the partitions are ranges of a prefix of that ordering, as a range's slices are. Sorting
within partitions, for example, reports no ordering.

Explain shows the native block under `CometLocalExec`. Its native operators do not report SQL
metrics; `CometLocalExec` counts the rows and batches it delivers.

## Result delivery

The native side hands batches to the JVM through a queue that holds at most one batch. Exported
Arrow arrays use `prepare_output` and `NativeUtil`; an imported batch stays valid after its
native query is closed, until the JVM closes it. A pull returns "pending" after 50 ms, so that
the task can check whether it was killed.

`CometLocalResultExec` sits above Spark's columnar-to-row transition. For `collect` and `take`
it still runs the single task as a Spark job, which keeps cancellation, job groups and SQL
metrics, but the task copies its rows into a driver-side slot instead of returning an encoded
task result. Spark would otherwise encode and compress every row in that one task and decode
them on the driver, all on one thread. While copying, the task counts uncompressed `UnsafeRow`
bytes and stops reading, which closes the native query, once they exceed
`spark.driver.maxResultSize`; the driver then fails the action. Spark counts compressed bytes,
so this check is the stricter one. On a non-local master the node uses Spark's ordinary
collect.

An iterator fetches one result partition at a time so that it holds at most one, but a local
query has a single partition holding the whole result. `toLocalIterator`, which also serves the
Thrift server's incremental collect, therefore runs the native block as ordinary Comet execution,
one Spark task per partition. Consumers that read the result RDD partition by partition, such as
PySpark's `toLocalIterator`, still receive the whole result as one partition.

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

The native tests cover partition-order delivery, the bound on running partitions, errors reported in a partition's turn, cancellation with busy workers,
the one-batch handoff and runtime shutdown. `CometLocalExecutionSuite` compares admitted queries
with Spark's results and order, and covers ranges Spark would overflow, declined expressions,
AQE, reported orderings, limits that stop before a failing partition, iterators, many partitions with sorts in a small memory pool, caching, the result size limit, job and task cancellation, and native errors. After
each test it checks that no native query, imported Arrow memory or result slot is left.
