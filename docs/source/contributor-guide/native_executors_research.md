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

# Hybrid Execution and Spark Protocol Compatibility for Comet Native Executors

This page is for contributors. It records research on how a query could run partly on a Comet
Native Executors cluster and partly in Spark, and on whether the cluster could speak Spark's own
protocols. It describes ideas and a proposed experiment. None of it is implemented.

Comet Native Executors are an experimental way to run whole Spark queries without a JVM in the data
plane. A Spark driver plans each query with Catalyst and Comet, then submits the plan as a graph of
stages to a cluster of one `comet-scheduler` and any number of `comet-executor` processes, built on
[Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista). Each stage is a
_block_: a run of Comet operators that Comet serializes as one Rust plan, with the exchanges between
blocks as the edges of the graph. The design and its sub-issues are tracked in
[#7288](https://github.com/apache/datafusion-comet/issues/7288). The cluster binaries are in
[`native/native-executors`](https://github.com/apache/datafusion-comet/tree/main/native/native-executors),
and its
[README](https://github.com/apache/datafusion-comet/blob/main/native/native-executors/README.md)
describes how to build and test them. This page does not cover the Spark driver side.

![The design of Comet Native Executors: a Spark driver with Comet sends each query as a graph of stages, each a block of Comet operators, to the comet-scheduler, which schedules its tasks on comet-executors with no JVM. The executors read Parquet directly, pass data to each other between stages, and the results return to the driver as Arrow over Flight.](../_static/images/comet-native-executors.svg)

## Where the Design Stops

In the design, a query either runs entirely in the Comet pipeline on the cluster or fails with an
error that names the operator that could not run and Comet's reasons for it. There are no Spark
executors to fall back to. That includes plans that call back into the JVM, such as JVM scalar UDFs
and Arrow Python UDFs, because the cluster has no JVM. It also includes a scan that stays in Spark,
such as one whose rows Comet converts to Arrow batches, because the cluster cannot read from a JVM.

Data crosses between Spark and the cluster in two places:

- **Scalar subqueries.** Spark runs each scalar subquery, and the driver writes its result into the
  plan as a literal before submitting the query. Data flows from Spark to the cluster, but only one
  value.
- **Query results.** Each Spark task reads one result partition from the executor that holds it
  over Arrow Flight, one endpoint per partition. Data flows from the cluster to Spark, but only at
  the root of the plan.

## Possible Future Research: Running Part of a Query in Spark

A query could run partly on the cluster and partly in Spark, so that a query with one unsupported
operator or UDF can still run most of its work on the cluster. The approaches below generalize the
two crossings above. They differ mainly in which way data crosses the boundary. Moving data from
the cluster into Spark is much easier than moving data from Spark into the cluster.

### Offloading the Lower Part of the Plan as a Leaf of the Spark Plan

The driver could offload the largest parts of the plan that consist only of operators in the Comet
pipeline, below an operator that must stay in Spark, rather than requiring the whole plan to run on
the cluster. Spark would see each offloaded part as a leaf with one partition per result endpoint,
and each Spark task would read its partition from the cluster over Flight. The batches arrive as
Arrow, so Comet's columnar operators above the leaf can consume them without converting them to
rows.

This reuses the result path that the design already needs. What remains:

- Serving partitioned results from a job whose stages stream their output. The result path
  described above has one endpoint per partition, so the scheduler would have to keep a streaming
  job's results partitioned rather than coalescing them.
- Reporting the cluster's output statistics to Spark's adaptive query execution.
- Choosing where the offloaded part of the plan should end.

It suits the common shape where the scans, joins and partial aggregates run in the Comet pipeline,
and an unsupported operator or UDF sits near the top of the plan.

### Small Spark Outputs as Inputs to Stages on the Cluster

Spark could compute a whole relation that fits in a broadcast, such as the build side of a
broadcast join, rather than a single value. The driver would encode the relation as Arrow IPC and
send it with the plan or upload it to the scheduler, where a stage reads it. This only works for
small data, but it costs little, because it uses the same round trip through the driver that scalar
subqueries already make.

### A Shuffle Both Engines Can Read and Write

Mixing stages freely needs a shuffle that Spark tasks can write and cluster tasks can read, and the
other way round. Some options:

- **A remote shuffle service.** Comet already integrates with Apache Celeborn. A Celeborn shuffle
  reader and writer for Ballista would let either engine run any stage.
- **An object store or shared filesystem.** Comet's native shuffle already writes Arrow IPC. Spark
  stages would write it to a location the cluster can read, and the scheduler would register those
  files as a finished stage's output.
- **Pushing batches over Flight.** Spark tasks would send batches to executors with `DoPut`. This
  is simpler to deploy, but it ties the lifetimes of Spark tasks and cluster tasks together and
  makes retrying a failed task difficult.

The hard part is scheduling rather than the data format. Spark's DAG scheduler and Ballista's
scheduler would have to agree on stage dependencies, failure handling and adaptive replanning.
Realistically, Spark stays the coordinator, and the cluster runs individual stages on its behalf.

### Running JVM Expressions on the Executors

Instead of splitting the plan, `comet-executor` could embed a JVM to evaluate the expressions and
UDFs that have no Rust implementation, reusing Comet's existing bridges for calling back into the
JVM. The whole plan would still run on the cluster and scheduling would not change. The costs are
that every executor needs Spark's classpath and enough session state to evaluate the expressions,
and that the plan must carry those expressions in serialized Spark form.

### Speaking Spark's Protocols

Another approach is for `comet-executor` to speak Spark's own protocols, so that Spark tasks and
tasks on the cluster exchange data the way Spark executors do. A Spark executor uses two protocols,
and they differ in how practical they are to implement.

The control plane runs over Spark's Netty RPC. Its messages, such as `RegisterExecutor`,
`LaunchTask`, `StatusUpdate` and heartbeats, are Scala case classes in Java serialization. A
`LaunchTask` carries a serialized `ShuffleMapTask` or `ResultTask`, whose RDD chain and closures
are JVM code, and the formats change between Spark versions.

The data plane is Spark's block transfer protocol in `network-common` (`OpenBlocks`,
`FetchShuffleBlocks`, `StreamHandle`, `ChunkFetchRequest` and so on). Its messages have a compact
binary encoding rather than Java serialization, the protocol has been stable for many releases, and
the external shuffle service speaks it too. Shuffle data sits in Spark's sort-based layout of a
data file and an index file per map task. Comet's native shuffle already writes that layout: the
data comes from Rust and `CometNativeShuffleWriter` commits the index through Spark's
`IndexShuffleBlockResolver`.

That suggests three options:

- **A Spark executor without a JVM.** Not practical. Running a Spark task means deserializing Java
  objects and running JVM closures. Even a task that only wraps a Comet plan would need its plan
  extracted from a Java-serialized object graph, separately for each Spark version.
- **A data plane in Rust, with Spark coordinating.** `comet-executor` writes shuffle output in
  Spark's layout, serves those shuffle blocks over the block transfer protocol, and fetches them
  from Spark executors and the external shuffle service. When a stage on the cluster finishes, the
  driver registers its map outputs with Spark's `MapOutputTrackerMaster`, and downstream Spark
  stages read them as they would any other shuffle. In the other direction, the driver gives a
  stage on the cluster the locations of the shuffle blocks of the Spark shuffle it reads. This
  provides a shuffle both engines can read and write without a separate shuffle service. The work
  includes Spark's authentication and encryption (SASL, or `spark.network.crypto`), which a first
  version could require to be off, mapping fetch failures onto Spark's `FetchFailed` handling and
  stage retries, and possibly push-based shuffle later. A shuffle block fetch client in Rust would
  also benefit Comet on Spark, by moving shuffle reads out of the JVM.
- **An embedded JVM for the control plane.** `comet-executor` hosts a small JVM that runs a
  cut-down `CoarseGrainedExecutorBackend`. It registers with the driver, deserializes tasks, passes
  each task's Comet plan to the Rust side, and reports map statuses and accumulators. This works,
  but the result is a Spark executor with a small heap whose tasks run in Rust: Comet on Spark with
  a data plane that lives more in Rust. Spark schedules every task one stage at a time, so the
  cluster's streaming of data between stages and its lower per-task overhead are lost. An embedded
  JVM is more useful as an evaluator for expressions that have no Rust implementation, as described
  above, where Spark's protocols are not involved.

The data-plane option combines with either way of placing the work. The cluster's executors could
join a Spark application as executors, which needs the embedded JVM for their control plane, with
stage-level scheduling sending the stages that run entirely in the Comet pipeline to them through a
`ResourceProfile` that asks for a resource only they offer. Or the driver could keep offloading
whole graphs of stages to the cluster, which keeps its own scheduling and its streaming of data
between stages, and use a Spark-compatible shuffle as the boundary between Spark stages and stages
on the cluster. The second keeps more of what the cluster provides.

### A First Experiment: the Block Transfer Protocol

A first experiment ([#7384](https://github.com/apache/datafusion-comet/issues/7384)) is a Rust
client and server for `OpenBlocks` and `FetchShuffleBlocks`, tested against a Spark executor and
the external shuffle service by reading a shuffle that Comet's native shuffle wrote. That shows
whether the data plane works independently of any scheduling decision.

### When Splitting Pays Off

Whichever approach is used, the driver should decide whether to split a query based on cost. If
most of a query's work has to run in Spark, running all of it with Comet on Spark is likely to beat
sending part of it to the cluster and reading the results back over the network.
