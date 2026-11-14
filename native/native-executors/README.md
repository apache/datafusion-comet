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

# Comet Native Executors

**Experimental.** Comet Native Executors run whole Spark queries without a JVM in the data plane. A
Spark driver plans each query with Catalyst and Comet, then submits it to a cluster of one
`comet-scheduler` and any number of `comet-executor` processes, built on
[Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista). The design is in
[issue #7288](https://github.com/apache/datafusion-comet/issues/7288).

This directory holds the cluster, which is its own Cargo workspace, so the main Comet build never
depends on Ballista. The other half is in Comet's Spark plugin: with
`spark.comet.nativeExecutors.enabled` set, the driver submits each query it can offload to the
scheduler's Flight service (`spark.comet.nativeExecutors.scheduler.url`) and reads the results back.
The two must come from the same Comet build, which the scheduler checks.

**Security.** The scheduler and the executors have no authentication and no TLS, and a query names
the files and object stores the executors read. Run a cluster only on a trusted network.

## The scheduler's Flight service

A client runs a query through the scheduler's Arrow Flight service:

1. `GetFlightInfo`, with a `QueryRequest` (`native/proto/src/proto/native_executors.proto`) as the
   command. The request is a stage graph of Comet native blocks connected by the exchanges between
   them, stamped with the client's Comet version and plan format. The scheduler refuses a request
   from another Comet build and plans each block with Comet's planner to check it. That builds no
   object store for the block's files, so the scheduler uses no storage credentials. It then
   builds a Ballista physical plan (`src/assembly.rs`), runs it on the Ballista scheduler it
   wraps, and answers with one endpoint per result partition. It refuses a request with more
   partitions or a larger batch size than a cluster plausibly needs, a stage whose block reads a
   different number of inputs than the stage declares, and a block that writes files.
2. `DoGet` with each endpoint's ticket, at the endpoint's location: the executor that holds the
   partition. A query's results stay there for `COMET_SCHEDULER_RESULT_RETENTION_SECONDS` after
   the query ends (five minutes by default), whether or not a client has read them, and each
   endpoint's expiration time says until when.

Ballista runs a query in one of two modes, set for every query with
`COMET_SCHEDULER_BALLISTA_CONFIG` (`ballista.execution.mode`, staged by default). In pipelined
mode the executors stream a result once and cannot hold it, so the scheduler reads each query's
result into its own memory and serves it from its own Flight service. Its endpoints have no
location: read them from the scheduler. The scheduler buffers at most
`COMET_SCHEDULER_PIPELINED_RESULT_MAX_BYTES` for a query and
`COMET_SCHEDULER_PIPELINED_STORE_MAX_BYTES` for all queries, and fails a query whose result is
larger than the first. When a new result does not fit in the second, the scheduler drops the oldest
results, which a client then finds expired. With a retention of `0` that is the only thing that
drops them. Run a query with a large result in staged mode.

The scheduler cancels a job whose client disconnects, whose result it cannot read, or that runs
longer than `COMET_SCHEDULER_QUERY_TIMEOUT_SECONDS`.

A stage that several other stages read runs once for each of them, since Ballista's planner
doesn't share a stage between consumers yet.

Spark plans a global sort as a range exchange followed by a sort of each partition, and its
driver samples the exchange's input for the ranges' bounds. A cluster doesn't range-partition:
the stage reads the producer's partitions as they are, sorts each, and merges the sorted
partitions in the order the driver sends with the stage, as DataFusion plans a global sort. A
stage whose order isn't over columns of types Arrow orders as Spark does, such as floats, reads
the exchange as one partition and sorts it in a single task.

Each stage runs on the executors as a `CometFragmentExec`
(`native/core/src/execution/native_executors`), which plans the stage's block with Comet's planner
for each partition. Ballista moves the data between stages:

| Exchange                    | Ballista plan                                                                                |
| --------------------------- | -------------------------------------------------------------------------------------------- |
| Hash                        | `RepartitionExec(Hash)`: Ballista's hash shuffle, with DataFusion's hash                     |
| Single partition, broadcast | `CoalescePartitionsExec`                                                                     |
| Range                       | None: the stage sorts the producer's partitions, and a `SortPreservingMergeExec` merges them |

A stage that fails to plan is refused, not run wrongly or loaded with code it names:

- A stage can't call into the JVM: JVM UDFs and a scalar subquery the driver has not replaced with
  its value fail to plan, and a scan whose object store needs a JVM credential provider fails when
  it runs.
- A stage can't write files: a Parquet writer, an Iceberg write or a shuffle writer in a block is
  refused. The cluster moves data between stages itself.
- A stage can't load a native UDF library. These processes serve every client, and a plan names
  the library path, so a client would choose what the processes load.
- A stage that reads files can't also read a hash exchange. Spark places the rows of a bucketed
  scan with Murmur3, so a join with a side shuffled by DataFusion's hash would return wrong rows.

## Joins whose build side turns out small

Spark plans a join's sides from its estimates, and its estimate of a filtered relation ignores the
filter, so it shuffles both sides of a join whose build side is far smaller than it thinks. The
scheduler moves such a join into the stage that produces its probe side, with the build side
broadcast to it, so the probe side's exchange carries the join's output rather than all of its
input (`src/adaptive.rs`). It picks the joins Spark's estimates say are worth measuring: an inner
hash join built on the right, with column keys and no condition, whose build side Spark estimates
within Ballista's maximum estimate multiple of its broadcast threshold and whose probe side many
times larger.

- In staged mode, a rule in Ballista's adaptive planner holds the probe side's stage back until
  the build side's stage finishes, then moves the join if the build side measured under
  `ballista.optimizer.broadcast_join_threshold_bytes`, or releases the probe side's stage as it
  was.
- In pipelined mode every task is placed before any runs, so a build side cannot be measured first
  and the join stays as Spark planned it.

## Building and running

```shell
CARGO_TARGET_DIR=native/target cargo build --manifest-path native/native-executors/Cargo.toml
native/target/debug/comet-scheduler &
native/target/debug/comet-executor &
```

Sharing `native/target` with the main workspace saves building the common dependencies twice. Both
binaries read their settings from environment variables, listed at the top of
`src/bin/comet-scheduler.rs` and `src/bin/comet-executor.rs`. The scheduler's include:

| Variable                                     | Meaning                                                                        |
| -------------------------------------------- | ------------------------------------------------------------------------------ |
| `COMET_SCHEDULER_BALLISTA_CONFIG`            | Ballista settings for every query, such as `ballista.execution.mode=pipelined` |
| `COMET_SCHEDULER_RESULT_RETENTION_SECONDS`   | How long a query's results stay readable after it ends (default 300)           |
| `COMET_SCHEDULER_FLIGHT_MAX_REQUEST_BYTES`   | The largest query the Flight service accepts (default 512 MiB)                 |
| `COMET_SCHEDULER_QUERY_TIMEOUT_SECONDS`      | The longest a query may take (default 0: no limit)                             |
| `COMET_SCHEDULER_PIPELINED_RESULT_MAX_BYTES` | The most Arrow data buffered for one pipelined result (default 1 GiB)          |
| `COMET_SCHEDULER_PIPELINED_STORE_MAX_BYTES`  | The most Arrow data kept for all pipelined results together (default 4 GiB)    |

## Testing

```shell
CARGO_TARGET_DIR=native/target cargo test --manifest-path native/native-executors/Cargo.toml
```

`tests/assembly.rs` runs stage graphs on DataFusion alone. `tests/cluster.rs` starts the two
binaries and runs queries through the scheduler's Flight service, as a driver would.

## Keeping it building

This workspace builds Comet's native crates by path against a commit of Ballista's main branch,
pinned by `rev` in `Cargo.toml`. That commit has the enhancements Comet Native Executors need,
which haven't been released yet
([#2682](https://github.com/apache/datafusion-ballista/pull/2682),
[#2721](https://github.com/apache/datafusion-ballista/pull/2721),
[#2722](https://github.com/apache/datafusion-ballista/pull/2722),
[#2723](https://github.com/apache/datafusion-ballista/pull/2723) and
[#2743](https://github.com/apache/datafusion-ballista/pull/2743)), on the same DataFusion major
version. To pick up newer Ballista changes, move the `rev` to a newer commit. CI builds and tests the workspace in the
`linux-test-native-executors` job. When Comet moves to a DataFusion major that Ballista hasn't
caught up with, the upgrade can turn that job off until it does. Dependabot updates this
workspace's lockfile, which is separate from `native/Cargo.lock`, so the two can resolve a crate to
different versions between updates.
