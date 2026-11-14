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

This page is for contributors. It covers the Spark driver side: how to run a query on a cluster and
what does not work yet. The driver never links Ballista. It sends the stage graph to the
scheduler's Arrow Flight service through a small client in libcomet, and the Spark tasks that read
the results use the same client. The cluster features that some queries rely on are in
[#7289](https://github.com/apache/datafusion-comet/pull/7289) and are not part of the cluster in
this repository yet. [Current limitations](#current-limitations) says which.

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
   usual, with native shuffle (`spark.comet.shuffle.mode=native`, as in the
   [installation guide](../user-guide/latest/installation.md)), because the driver turns only
   native shuffle exchanges into the edges of the stage graph.

4. Check that the query ran on the cluster: its physical plan has a `CometNativeExecutors` leaf
   (the `CometNativeExecutorsExec` class) where the Comet plan would otherwise be. A query that
   cannot run entirely in the Comet pipeline and on the cluster fails instead, with a message that
   names the operator and Comet's reasons for it.

`CometNativeExecutorsSuite` runs queries against a cluster it starts from the binaries in
`COMET_NATIVE_EXECUTORS_BIN_DIR`. Its header comment gives the commands. It is not part of CI. Its
tests that need a cluster are skipped when the variable is not set. The tests that check what the
driver refuses to plan run without a cluster, but they still need a libcomet built with the
`native-executors` feature. Its TPC-H tests need the cluster of
[#7289](https://github.com/apache/datafusion-comet/pull/7289): on the cluster in this repository,
only q1, q6, q11 and q15 pass. `CometNativeExecutorsPlanSuite` runs in CI. It takes the plan Comet
would execute and checks the stage graph, the queries the driver refuses, and how a scalar
subquery's result goes into the plan, with neither a cluster nor the feature.

## Current limitations

Comet Native Executors are a first version. The sub-issues of
[#7288](https://github.com/apache/datafusion-comet/issues/7288) track what is missing:

- Whole queries only. See [below](#whole-queries-only).
- Only native shuffle exchanges. A query whose exchanges use JVM shuffle is refused.
- `LIMIT` and `show()`: [#7365](https://github.com/apache/datafusion-comet/issues/7365).
- Global sorts. The cluster in this repository reads a range exchange as one partition, so every
  global sort runs in a single task.
  [#7289](https://github.com/apache/datafusion-comet/pull/7289) adds a cluster that sorts the
  partitions in parallel and merges them, except for sorts by floats or nested types, which still
  run in a single task: [#7366](https://github.com/apache/datafusion-comet/issues/7366).
- Hash partitioning is DataFusion's, not Spark-compatible:
  [#7368](https://github.com/apache/datafusion-comet/issues/7368).
- A small broadcast side runs in every task that reads it:
  [#7369](https://github.com/apache/datafusion-comet/issues/7369). Moving a join whose build side
  turns out small into the stage that produces its probe side is in
  [#7289](https://github.com/apache/datafusion-comet/pull/7289).
- Dynamic partition pruning: [#7370](https://github.com/apache/datafusion-comet/issues/7370). A scan
  with a dynamic pruning filter is refused.
- Writes: [#7371](https://github.com/apache/datafusion-comet/issues/7371). Commands and writes stay
  in Spark.
- Cancellation: [#7372](https://github.com/apache/datafusion-comet/issues/7372). The driver thread
  waits for the cluster in native code until the query finishes, so cancelling the Spark job or
  interrupting the thread does not stop the wait, and a killed Spark job leaves its query running
  on the cluster. A killed Spark task that reads a result partition stops at the next batch; a
  read blocked waiting for a batch still waits for it.
- Errors. A failure on the cluster reaches Spark as a `CometNativeException` with the cluster's
  message, not as the exception Spark would raise. For example, a division by zero with ANSI mode on
  is not a `SparkArithmeticException` with the `DIVIDE_BY_ZERO` error class.
- Spark UI metrics: [#7373](https://github.com/apache/datafusion-comet/issues/7373).
- Security. The scheduler and the executors have no authentication and no TLS, and a query names
  the files and object stores the executors read. Run a cluster only on a trusted network.
- Result expiry. A query's results stay on the cluster for
  `COMET_SCHEDULER_RESULT_RETENTION_SECONDS` after it ends (five minutes by default), whether or
  not Spark has read them. The scheduler gives each result partition its expiration time, and a
  Spark task that reads a partition after that fails with a message that names the setting.

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
