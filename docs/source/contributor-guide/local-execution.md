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

Local execution is an experimental mode in which a whole admitted query runs as one
DataFusion graph inside the Spark driver process. Spark still parses, analyzes and
optimizes the query, and Comet's Spark-compatible expressions and Parquet scan are
reused, but DataFusion schedules all partitions and exchanges itself. An admitted
query has no Spark shuffle and runs as a single Spark result task.

`spark.comet.exec.local.enabled=true` opts in through the existing Comet extension.
The option is internal and defaults to false. Queries that are not admitted as a
whole use ordinary Comet/Spark planning. See [Local Execution Benchmark](local-execution-benchmark.md)
for the manual benchmark and current results.

## Execution contract

Each query execution owns one fresh physical graph. Every partition uses those same
operator instances, including the channels and state of `RepartitionExec`. Plans
deserialized separately for Spark tasks cannot implement this exchange, so sharing
is a consequence of query ownership, not a cache. Repeating an action on the same
DataFrame, or retrying the result task, creates a new graph; no stateful operator
tree is reused across executions.

The native API accepts a fully planned graph and a query-scoped DataFusion
`TaskContext`. It does not optimize the graph or verify distribution requirements;
the local planner must satisfy them. Execution consumes the query once and returns
one result stream. DataFusion's `execute_stream` drives all root partitions
concurrently, so progress does not depend on Spark scheduling partitions.

The result stream is unordered when the root has several partitions. Global
ordering must therefore be established by the planner with a single ordered root;
coalescing partitions never preserves order. Spark sees one output partition, and
the local node reports its ordering conservatively.

EOF, the first stream error, explicit cancellation and dropping the stream release
the owner's references to the graph and context. DataFusion tasks abort
asynchronously, so dropping is not a cleanup barrier; the process-owned Tokio
runtime must stay alive to finish teardown.

## Module boundaries

- `native/local`: query execution lifecycle and result handoff, independent of JNI
  and Spark tasks. It reuses DataFusion scheduling, exchange and streaming rather
  than implementing another scheduler.
- `native/core/src/local.rs` and `native/core/src/local/planner.rs`: the JNI entry
  points and the local planner. The planner reuses core's `PhysicalPlanner` for
  scans and expressions and owns distribution, ordering and exchange translation
  for the complete graph. Core may depend on `native/local`; not the reverse.
- `spark-local`: JVM mode selection, whole-query admission, query ownership, the
  result bridge, cancellation and SQL metrics. It is compiled into the existing
  Spark artifact, avoiding a cyclic dependency on `CometConf` and `NativeUtil`.
- `native/proto/src/proto/local.proto`: aggregation, join and terminal sort/limit
  descriptions that have no counterpart in Comet's per-task operator protocol.

Task-bound memory managers, JVM input iterators, per-task plan creation and Spark
shuffle readers/writers are not used by local execution.

## Admission

Admission inspects the complete physical plan before execution starts. Fallback is
a planning decision only: there is no fallback after native output has started.

Environment requirements:

- Spark 4.1. Local execution stays disabled on other Spark profiles.
- `SparkContext.isLocal`, as reported by the application's effective SparkContext,
  not a session override of `spark.master`. `local-cluster` and remote masters
  are rejected.
- AQE disabled, Comet and Comet native execution enabled, not in plan-only mode.
- Batch queries only. Streaming plans and subquery preparation are rejected.

Admitted query shapes:

- `RangeExec` with optional direct column or alias projections.
- DataSource V1 scans of Spark's built-in Parquet format on `file:` paths, with
  filter and projection. `CometScanRule` validates a copy of the scan, and Spark's
  static partition pruning and file splitting are reused. Encrypted reads, custom
  or cloud filesystems, object-store options, bucketed or ordered scans and file
  metadata columns are rejected, so credentials and encryption callbacks never
  cross the local boundary.
- Grouped or global `COUNT`, `MIN` and `MAX` over an admitted scan, recognized from
  Spark's partial/final hash aggregate pair and its exchange.
- A single `ShuffledHashJoinExec` over two admitted scans (for example selected
  with a `SHUFFLE_HASH` hint): inner, left/right/full outer, left semi and left
  anti, with equal-type, non-floating-point attribute keys and no residual
  condition. Sort-merge and broadcast joins are not converted.
- A terminal global `SortExec`, `TakeOrderedAndProjectExec` or root `CollectLimitExec`
  over any of the Parquet, aggregate or join shapes. A global sort's range exchange
  is removed only when every sort expression, direction and null placement matches.
  A range query keeps Spark's root `CollectLimitExec` instead.

Expressions are limited to an explicit allowlist: references, literals, aliases,
arithmetic, comparisons, boolean and null predicates, casts, overflow checks and
conditionals. Both the Spark expression and its serialized form must be admitted,
because serde can choose JVM codegen callbacks for familiar expression classes.
Supported types are primitive numeric, boolean, string, binary, decimal, date,
timestamp and timestamp NTZ. Floating-point grouping, join and sort keys, nested
and collated types, UDFs, nondeterministic or partition-sensitive expressions,
subqueries, `DISTINCT`, `SUM`/`AVG`, `HAVING`, nested joins and aggregates around
joins fall back as a whole. Existing Comet operator enablement flags are honored.

Limits are 1,024 file groups or native partitions, 1,024 output columns and batch
size 65,536. These bound execution overhead, not total memory.

## Native plan shapes

- Scans: one native Parquet scan per Spark file partition, combined with a union,
  with shared filter and projection operators above it.
- Grouped aggregation: a partial aggregate per input partition, a DataFusion hash
  `RepartitionExec` into the Spark shuffle partition count, and a
  `FinalPartitioned` aggregate. Global aggregation, or a single shuffle partition,
  coalesces the partial states into a `Final` aggregate instead; with only one input
  partition there is a single aggregate. Partial states and hash buckets never leave
  the graph.
- Joins: two DataFusion hash repartitions with the same partition count feed a
  partitioned `HashJoinExec`. Spark's build side is retained; DataFusion's input
  swap projection restores the logical output order.
- Global sort and Top-K: each input partition is sorted (keeping only the Top-K
  rows, if any), and a `SortPreservingMergeExec` merges the runs into one ordered
  partition. Offset is applied once by a `GlobalLimitExec`. An unordered limit
  gathers the partitions before the limit.
- Range: the native range partitions are merged with a sort-preserving merge, since
  Spark may already have removed a redundant sort based on range ordering.

## Memory and spill

Parquet, aggregate and join queries own a DataFusion `FairSpillPool`. The internal
`spark.comet.exec.local.memoryLimit` (default 256 MiB) bounds DataFusion
reservations per query, not process RSS, scan buffers, driver results or the sum of
concurrent queries. It does not borrow a Spark task memory consumer. Range queries
have no query budget.

`spark.comet.exec.local.spill.enabled` (default true) allows query-owned spill files
in the OS temporary directory; there is no local-mode disk quota. With spill
disabled, queries that exceed the budget fail with a resource error.

The planner adjusts two DataFusion sort settings according to the budget divided by
the number of sorters in the graph (the per-sorter share):

- `sort_spill_reservation_bytes` is reduced to at most a quarter of the share.
  Every sorter reserves this amount up front, and the 10 MiB default can exhaust a
  small budget before any data is sorted.
- `sort_in_place_threshold_bytes` is raised to the share. This works around a
  DataFusion 55.1 `ExternalSorter` bug that is fixed in DataFusion 56.0.0: before
  spilling, the sorter frees its merge reservation and merges buffered batches with
  a new unspillable reservation. Once spillable sorters fill the fair pool, that
  reservation cannot grow and the query fails instead of spilling. Sorting buffered
  batches in place avoids that merge, at the cost of unaccounted transient copies
  and slower multi-column sorts that fit in memory. Remove this override after
  upgrading to DataFusion 56.0.0; the native test
  `multi_column_sorts_spill_under_a_shared_budget` must still pass without it.

DataFusion's hash join build side does not spill, so an oversized build side fails
regardless of the spill setting.

## Result delivery

The native side hands batches to the JVM through a bounded queue holding at most one
batch. Exported Arrow arrays reuse `prepare_output` and `NativeUtil`; imported
buffers stay valid after the native query closes until the JVM closes their batch.
Polling returns "pending" after 50 ms so the Spark task can check for interruption.

Admitted plans are wrapped in `CometLocalResultExec` above Spark's columnar-to-row
transition. For `collect` and `take` (including `head` and `show` through a root
`CollectLimitExec`), it still runs the single result task as a Spark job, keeping
cancellation, job groups and SQL metrics, but the task copies rows into a
driver-side slot instead of returning an encoded task result. Spark would otherwise
encode and compress every row in that one task and decode them on the driver, all on
a single thread. Admission already guarantees an in-process local master; on any
other master the node uses Spark's ordinary collect.

While copying, the task counts uncompressed `UnsafeRow` bytes and stops reading,
which closes the native query, as soon as they exceed `spark.driver.maxResultSize`;
the driver then fails the action. Spark compares the compressed size instead, so
the local check is more conservative and can reject a result Spark would accept.

Other actions keep Spark's behavior. `toLocalIterator` materializes the single
result partition, so it does not bound driver memory. DataFrame `.rdd` requires
object deserialization, which is not admitted.

## Lifecycle and cancellation

The JNI registry holds numeric, never reused IDs for at most 1,024 live queries.
Each entry owns a cancellable producer task. EOF, errors, early termination and task
completion remove it. A concurrent pull holds an `Arc`, so close cannot free an
object that is being read, and cancellation does not take the reader lock. The
producer publishes completion before closing the channel; if the runtime stops a
query before completion, the reader reports an error instead of a truncated EOF.

Every execution owns its DataFusion session configuration, runtime environment and
object-store registry; only the Tokio worker runtime is process-wide. Timezone,
schema matching and scan options travel in scan metadata, ANSI and timezone
behavior in the expression protobufs. Arbitrary `spark.comet.datafusion.*`
overrides are not propagated.

Explain shows a compact `CometLocal` description: query kind, terminal operation,
batch size, one result partition, ordering, and memory and spill settings. SQL
metrics count delivered rows and batches; native peak memory, spill and CPU time are
not reported.

## Testing

Build native code before the JVM suite and never use Maven `-pl`:

```shell
cd native
cargo test -p datafusion-comet-local --locked
cargo test -p datafusion-comet --locked --lib local::
cd ..
./mvnw test -Pspark-4.1 -Dsuites=org.apache.comet.local.CometLocalExecutionSuite
```

The native tests cover exchange routing, single-use graph ownership, cancellation
and teardown, spilling aggregation and sort on one Tokio worker, join reservation
failure and isolation between concurrent queries. The JVM suite compares results
with Spark for every admitted shape, requiring a local node so that fallback cannot
hide a failure. It also covers fallback, cancellation, errors, repeated actions and
the result size limit, and checks that native handles, imported Arrow memory and
result slots return to zero after each test. The suite runs in the Linux and macOS
pull request workflows.

## Spark SQL validation

Changes to the local planner, serde, operators or shims need a Spark SQL verdict.
Prefer the `run-spark-4.1-tests` label on the pull request for broad changes; use
the relevant `dev/local-ci.sh spark <row>` only when no CI verdict is available and
the row covers the change. See [Continuous Integration](ci.md) for the shared Maven
cache and preparation caveats.

## Not supported

AQE, mixed Spark/native plans, independent Spark task retries, partition-preserving
output, arbitrary RDD operations, JVM/Python UDFs, writes, streaming queries and
distributed deployment are outside local execution. Their absence is reflected in
admission and explain output.
