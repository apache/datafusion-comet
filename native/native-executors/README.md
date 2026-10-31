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

This directory holds the cluster. The Spark driver side is in progress in
[#7289](https://github.com/apache/datafusion-comet/pull/7289). It is its own Cargo workspace, so the
main Comet build never depends on Ballista.

## The scheduler's Flight service

A client runs a query through the scheduler's Arrow Flight service:

1. `GetFlightInfo`, with a `QueryRequest` (`native/proto/src/proto/native_executors.proto`) as the
   command. The request is a stage graph of Comet native blocks connected by the exchanges between
   them, stamped with the client's Comet version and plan format. The scheduler refuses a request
   from another Comet build and plans each block with Comet's planner to check it. That builds no
   object store for the block's files, so the scheduler uses no storage credentials. It then
   builds a Ballista physical plan (`src/assembly.rs`), runs it on the Ballista scheduler it
   wraps, and answers with one endpoint per result partition.
2. `DoGet` with each endpoint's ticket, at the endpoint's location: the executor that holds the
   partition. A query's results stay there for `COMET_SCHEDULER_RESULT_RETENTION_SECONDS` after
   the query ends (five minutes by default), whether or not a client has read them, and each
   endpoint's expiration time says until when.

A stage that several other stages read runs once for each of them, since Ballista's planner
doesn't share a stage between consumers yet.

Each stage runs on the executors as a `CometFragmentExec`
(`native/core/src/execution/native_executors`), which plans the stage's block with Comet's planner
for each partition. Ballista moves the data between stages:

| Exchange                    | Ballista plan                                                                                                                     |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Hash                        | `RepartitionExec(Hash)`: Ballista's hash shuffle, with DataFusion's hash                                                          |
| Single partition, broadcast | `CoalescePartitionsExec`                                                                                                          |
| Range                       | `CoalescePartitionsExec`, so a sort runs in one task ([ballista#2680](https://github.com/apache/datafusion-ballista/issues/2680)) |

A stage that fails to plan is refused, not run wrongly or loaded with code it names:

- A stage can't call into the JVM: scalar subqueries and JVM UDFs fail to plan, and a scan whose
  object store needs a JVM credential provider fails when it runs.
- A stage can't load a native UDF library. These processes serve every client, and a plan names
  the library path, so a client would choose what the processes load.
- A stage that reads files can't also read a hash exchange. Spark places the rows of a bucketed
  scan with Murmur3, so a join with a side shuffled by DataFusion's hash would return wrong rows.

## Building and running

```shell
CARGO_TARGET_DIR=native/target cargo build --manifest-path native/native-executors/Cargo.toml
native/target/debug/comet-scheduler &
native/target/debug/comet-executor &
```

Sharing `native/target` with the main workspace saves building the common dependencies twice. Both
binaries read their settings from environment variables, listed at the top of
`src/bin/comet-scheduler.rs` and `src/bin/comet-executor.rs`.

## Testing

```shell
CARGO_TARGET_DIR=native/target cargo test --manifest-path native/native-executors/Cargo.toml
```

`tests/assembly.rs` runs stage graphs on DataFusion alone. `tests/cluster.rs` starts the two
binaries and runs queries through the scheduler's Flight service, as a driver would.

## Keeping it building

This workspace builds Comet's native crates by path against the Ballista release on the same
DataFusion major version, and CI builds and tests it in the `linux-test-native-executors` job. When
Comet moves to a DataFusion major that Ballista hasn't released yet, the upgrade can turn that job
off until Ballista catches up. Dependabot updates this workspace's lockfile, which is separate from
`native/Cargo.lock`, so the two can resolve a crate to different versions between updates.
