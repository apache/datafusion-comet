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
Spark driver plans each query with Catalyst and Comet, then submits the plan as a stage graph of
Comet native blocks to a cluster of one `comet-scheduler` and any number of `comet-executor`
processes, built on [Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista).
The design is in [issue #7288](https://github.com/apache/datafusion-comet/issues/7288), and
`native/native-executors/README.md` describes the cluster, how to build it and how to test it.

## Current limitation: whole queries only

The driver offloads a query only if every operator and expression in it can run natively
(`CometNativeExecutors` in `CometNativeExecutorsExec.scala`). Otherwise it refuses the query,
naming the operator that couldn't run and Comet's fallback reasons for it. That includes plans that
call back into the JVM, such as JVM scalar UDFs and Arrow Python UDFs, because the cluster has no
JVM.

Data already crosses between Spark and the cluster in two places:

- **Scalar subqueries.** Spark runs each scalar subquery, and the driver writes its result into the
  plan as a literal before submitting the query. Data flows from Spark to the cluster, but only one
  value.
- **Query results.** The driver reads the final stage's output over Arrow Flight, one endpoint per
  result partition (`ResultEndpoint`). Data flows from the cluster to Spark, but only at the root
  of the plan.

## Possible future research: hybrid Spark and native execution

A query could run partly on the cluster and partly in Spark, so that a query with one unsupported
operator or UDF can still run most of its work natively. The approaches below generalize the two
crossings above. They differ mainly in which way data crosses the boundary. Moving data from the
cluster into Spark is much easier than moving data from Spark into the cluster.

### A native subtree as a leaf of the Spark plan

The driver could offload the largest native subtrees under an operator that must run in Spark,
rather than requiring the whole plan to be native. Spark would see each offloaded subtree as a leaf
with one partition per result endpoint, and each Spark task would read its partition from the
cluster over Flight. The batches arrive as Arrow, so Comet's columnar operators above the leaf can
consume them without converting them to rows.

This reuses the result path that already works. What remains:

- Serving partitioned results in pipelined mode. Today the scheduler coalesces a pipelined job's
  results to one partition.
- Reporting the cluster's output statistics to Spark's adaptive query execution.
- Choosing where the native region should end.

It suits the common shape where the scans, joins and partial aggregates are native, and an
unsupported operator or UDF sits near the top of the plan.

### Small Spark outputs as inputs to native stages

Spark could compute a whole relation that fits in a broadcast, such as the build side of a
broadcast join, rather than a single value. The driver would encode the relation as Arrow IPC and
send it with the plan or upload it to the scheduler, where a native leaf reads it. This only
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

### Running JVM expressions on native executors

Instead of splitting the plan, `comet-executor` could embed a JVM to evaluate the expressions and
UDFs that have no native implementation, reusing Comet's existing bridges for calling back into
the JVM. The plan would stay fully native and scheduling would not change. The costs are that every
executor needs Spark's classpath and enough session state to evaluate the expressions, and that the
plan must carry those expressions in serialized Spark form.

### When splitting pays off

Whichever approach is used, the driver should decide whether to split a query based on cost. If
most of a query's work has to run in Spark, running all of it with Comet on Spark is likely to beat
sending part of it to the cluster and reading the results back over the network.
