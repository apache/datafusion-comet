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
Spark driver plans each query with Catalyst and Comet, then submits the plan as a graph of stages
to a cluster of one `comet-scheduler` and any number of `comet-executor` processes, built on
[Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista). Each stage is a
_block_: a run of Comet operators that Comet serializes as one Rust plan, with the exchanges between
blocks as the edges of the graph. The design is in
[issue #7288](https://github.com/apache/datafusion-comet/issues/7288), and
[`native/native-executors/README.md`](https://github.com/apache/datafusion-comet/blob/main/native/native-executors/README.md)
describes the cluster, how to build it and how to test it.

This page is for contributors. It explains how to run a query on a cluster, lists what does not work
yet, and sketches ways to run part of a query in Spark.

![A Spark driver with Comet sends each query as a graph of stages, each a block of Comet operators, to the comet-scheduler, which schedules its tasks on comet-executors with no JVM. The executors read Parquet directly, pass data to each other between stages, and Spark tasks read the results from them as Arrow over Flight.](../_static/images/comet-native-executors.svg)

## Running it

1. Build libcomet with the `native-executors` Cargo feature. It is off by default, so the standard
   build links no gRPC client. A libcomet without it fails the first query with an error that names
   the feature. For example, `make COMET_FEATURES=native-executors` builds libcomet and the Comet
   jar that bundles it. The Spark executors that read the results need this libcomet too.
2. Build and start the cluster. The `comet-scheduler` and `comet-executor` binaries are their own
   Cargo workspace, so the main Comet build does not depend on Ballista. The cluster and the driver
   must come from the same Comet build, which the scheduler checks. See
   [`native/native-executors/README.md`](https://github.com/apache/datafusion-comet/blob/main/native/native-executors/README.md)
   for how to build the binaries and for the environment variables that configure them.
3. Point Spark at the scheduler's Flight service and turn the feature on:

   ```shell
   --conf spark.comet.nativeExecutors.enabled=true \
   --conf spark.comet.nativeExecutors.scheduler.url=http://scheduler-host:50060
   ```

   Both configs are internal while the feature is experimental. Comet itself must be enabled as
   usual.

4. Check that the query ran on the cluster: its physical plan has a `CometNativeExecutorsExec` leaf
   where the Comet plan would otherwise be. A query that cannot run entirely in the Comet pipeline
   and on the cluster fails instead, with a message that names the operator and Comet's reasons
   for it.

`CometNativeExecutorsSuite` runs queries against a cluster it starts from the binaries in
`COMET_NATIVE_EXECUTORS_BIN_DIR`. Its header comment gives the commands.

## Current limitations

Comet Native Executors are a first version. The sub-issues of
[#7288](https://github.com/apache/datafusion-comet/issues/7288) track what is missing:

- Whole queries only. See [below](#whole-queries-only).
- `LIMIT` and `show()`: [#7365](https://github.com/apache/datafusion-comet/issues/7365).
- Global sorts by floats or nested types sort in a single task rather than in parallel:
  [#7366](https://github.com/apache/datafusion-comet/issues/7366).
- Hash partitioning is DataFusion's, not Spark-compatible:
  [#7368](https://github.com/apache/datafusion-comet/issues/7368).
- A small broadcast side runs in every task that reads it:
  [#7369](https://github.com/apache/datafusion-comet/issues/7369).
- Dynamic partition pruning: [#7370](https://github.com/apache/datafusion-comet/issues/7370). A scan
  with a dynamic pruning filter is refused.
- Writes: [#7371](https://github.com/apache/datafusion-comet/issues/7371). Commands and writes stay
  in Spark.
- Cancellation: [#7372](https://github.com/apache/datafusion-comet/issues/7372). A killed Spark job
  leaves its query running on the cluster.
- Spark UI metrics: [#7373](https://github.com/apache/datafusion-comet/issues/7373).
- Security. The scheduler and the executors have no authentication and no TLS, and a query names
  the files and object stores the executors read. Run a cluster only on a trusted network.
- Result expiry. A query's results stay on the cluster for
  `COMET_SCHEDULER_RESULT_RETENTION_SECONDS` after it ends (five minutes by default), whether or
  not Spark has read them. A Spark task that reads a partition after that fails with a message that
  names the setting.

### Whole queries only

The driver offloads a query only if every operator and expression in it runs in the Comet pipeline
(`CometNativeExecutors` in `CometNativeExecutorsExec.scala`). Otherwise it refuses the query, naming
the operator that could not run and Comet's fallback reasons for it. That includes plans that call
back into the JVM, such as JVM scalar UDFs and Arrow Python UDFs, because the cluster has no JVM. It
also includes a scan that stays in Spark, such as one whose rows Comet converts to Arrow batches,
because the cluster cannot read from a JVM.

Data already crosses between Spark and the cluster in two places:

- **Scalar subqueries.** Spark runs each scalar subquery, and the driver writes its result into the
  plan as a literal before submitting the query. Data flows from Spark to the cluster, but only one
  value.
- **Query results.** Each Spark task reads one result partition from the executor that holds it over
  Arrow Flight, one endpoint per partition (`ResultEndpoint`). Data flows from the cluster to Spark,
  but only at the root of the plan.

## Possible future research: running part of a query in Spark

A query could run partly on the cluster and partly in Spark, so that a query with one unsupported
operator or UDF can still run most of its work on the cluster. The approaches below generalize the
two crossings above. They differ mainly in which way data crosses the boundary. Moving data from the
cluster into Spark is much easier than moving data from Spark into the cluster.

### A subtree that runs on the cluster as a leaf of the Spark plan

The driver could offload the largest subtrees of Comet operators under an operator that must run in
Spark, rather than requiring the whole plan to run in the Comet pipeline. Spark would see each
offloaded subtree as a leaf
with one partition per result endpoint, and each Spark task would read its partition from the
cluster over Flight. The batches arrive as Arrow, so Comet's columnar operators above the leaf can
consume them without converting them to rows.

This reuses the result path that already works. What remains:

- Serving partitioned results in pipelined mode. Today the scheduler coalesces a pipelined job's
  results to one partition.
- Reporting the cluster's output statistics to Spark's adaptive query execution.
- Choosing where the offloaded subtree should end.

It suits the common shape where the scans, joins and partial aggregates run in the Comet pipeline,
and an unsupported operator or UDF sits near the top of the plan.

### Small Spark outputs as inputs to stages on the cluster

Spark could compute a whole relation that fits in a broadcast, such as the build side of a
broadcast join, rather than a single value. The driver would encode the relation as Arrow IPC and
send it with the plan or upload it to the scheduler, where a stage reads it. This only
works for small data, but it costs little, because it uses the same round trip through the driver
that scalar subqueries already make.

### A shuffle both engines can read and write

Mixing stages freely needs a shuffle that Spark tasks can write and cluster tasks can read, and the
other way round. Some options:

- **A remote shuffle service.** Comet already integrates with Apache Celeborn. A Celeborn shuffle
  reader and writer for Ballista would let either engine run any stage.
- **An object store or shared filesystem.** Comet's native shuffle already writes Arrow IPC. Spark
  stages would write it to a location the cluster can read, and the scheduler would register those
  files as a finished stage's output.
- **Pushing batches over Flight.** Spark tasks would send batches to executors with `DoPut`. This is
  simpler to deploy, but it ties the lifetimes of Spark tasks and cluster tasks together and makes
  retrying a failed task difficult.

The hard part is scheduling rather than the data format. Spark's DAG scheduler and Ballista's
scheduler would have to agree on stage dependencies, failure handling and adaptive replanning.
Realistically, Spark stays the coordinator, and the cluster runs individual stages on its behalf.

### Running JVM expressions on the executors

Instead of splitting the plan, `comet-executor` could embed a JVM to evaluate the expressions and
UDFs that have no Rust implementation, reusing Comet's existing bridges for calling back into the
JVM. The whole plan would still run on the cluster and scheduling would not change. The costs are that every
executor needs Spark's classpath and enough session state to evaluate the expressions, and that the
plan must carry those expressions in serialized Spark form.

### Speaking Spark's protocols

Another approach is for `comet-executor` to speak Spark's own protocols, so that Spark tasks and
tasks on the cluster exchange data the way Spark executors do. A Spark executor uses two protocols, and
they differ in how practical they are to implement.

The control plane runs over Spark's Netty RPC. Its messages, such as `RegisterExecutor`,
`LaunchTask`, `StatusUpdate` and heartbeats, are Scala case classes in Java serialization. A
`LaunchTask` carries a serialized `ShuffleMapTask` or `ResultTask`, whose RDD chain and closures
are JVM code, and the formats change between Spark versions.

The data plane is Spark's block transfer protocol in `network-common` (`OpenBlocks`,
`FetchShuffleBlocks`, `StreamHandle`, `ChunkFetchRequest` and so on). Its messages have a compact
binary encoding rather than Java serialization, the protocol has been stable for many releases, and
the external shuffle service speaks it too. Shuffle data sits in Spark's sort-based layout of a data
file and an index file per map task. Comet's native shuffle already writes that layout: the data
comes from Rust and `CometNativeShuffleWriter` commits the index through Spark's
`IndexShuffleBlockResolver`.

That suggests three options:

- **A Spark executor without a JVM.** Not practical. Running a Spark task means deserializing Java
  objects and running JVM closures. Even a task that only wraps a Comet plan would need its plan
  extracted from a Java-serialized object graph, separately for each Spark version.
- **A data plane in Rust, with Spark coordinating.** `comet-executor` writes shuffle output in
  Spark's layout, serves those blocks over the block transfer protocol, and fetches blocks from
  Spark executors and the external shuffle service. When a stage on the cluster finishes, the
  driver registers its map outputs with Spark's `MapOutputTrackerMaster`, and downstream Spark stages read
  them as they would any other shuffle. In the other direction, the driver gives a stage on
  the cluster the block locations of the Spark shuffle it reads. This provides a shuffle both engines can read and
  write without a separate shuffle service. The work includes Spark's authentication and encryption
  (SASL, or `spark.network.crypto`), which a first version could require to be off, mapping fetch
  failures onto Spark's `FetchFailed` handling and stage retries, and possibly push-based shuffle
  later. A block fetch client in Rust would also benefit Comet on Spark, by moving shuffle reads out
  of the JVM.
- **An embedded JVM for the control plane.** `comet-executor` hosts a small JVM that runs a cut-down
  `CoarseGrainedExecutorBackend`. It registers with the driver, deserializes tasks, passes each
  task's Comet plan to the Rust side, and reports map statuses and accumulators. This works, but the
  result is a Spark executor with a small heap whose tasks run in Rust: Comet on Spark with a data
  plane that lives more in Rust. Spark schedules every task one stage at a time, so the cluster's pipelined
  execution and lower per-task overhead are lost. An embedded JVM is more useful as an evaluator for
  expressions that have no Rust implementation, as described above, where Spark's protocols are
  not involved.

The data-plane option combines with either way of placing the work. The cluster's executors could
join a Spark application as executors, which needs the embedded JVM for their control plane, with
stage-level scheduling sending the stages that run entirely in the Comet pipeline to them through a
`ResourceProfile` that asks for a resource only they offer. Or the driver could keep offloading
whole subgraphs of stages to the cluster, which keeps its own scheduling and pipelining, and use a
Spark-compatible shuffle as the boundary between Spark stages and stages on the cluster. The second
keeps more of what the cluster provides.

A first experiment ([#7384](https://github.com/apache/datafusion-comet/issues/7384)) is a Rust client and server for `OpenBlocks` and `FetchShuffleBlocks`, tested
against a Spark executor and the external shuffle service by reading a shuffle that Comet's native
shuffle wrote. That shows whether the data plane works independently of any scheduling decision.

### When splitting pays off

Whichever approach is used, the driver should decide whether to split a query based on cost. If
most of a query's work has to run in Spark, running all of it with Comet on Spark is likely to beat
sending part of it to the cluster and reading the results back over the network.
