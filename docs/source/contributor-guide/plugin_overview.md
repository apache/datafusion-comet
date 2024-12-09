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

# Comet Plugin Architecture

## Comet SQL Plugin

The entry point to Comet is the `org.apache.spark.CometPlugin` class, which can be registered with Spark by adding the
following setting to the Spark configuration when launching `spark-shell` or `spark-submit`:

```
--conf spark.plugins=org.apache.spark.CometPlugin
```

On initialization, this class registers two physical plan optimization rules with Spark: `CometScanRule`
and `CometExecRule`. These rules run whenever a query stage is being planned during Adaptive Query Execution, and
run once for the entire plan when Adaptive Query Execution is disabled.

## CometScanRule

`CometScanRule` replaces any Parquet scans with Comet operators. There are different paths for Spark v1 and v2 data sources.

When reading from Parquet v1 data sources, Comet replaces `FileSourceScanExec` with a `CometScanExec`, and for v2
data sources, `BatchScanExec` is replaced with `CometBatchScanExec`. In both cases, Comet replaces Spark's Parquet
reader with a custom vectorized Parquet reader. This is similar to Spark's vectorized Parquet reader used by the v2
Parquet data source but leverages native code for decoding Parquet row groups directly into Arrow format.

Comet only supports a subset of data types and will fall back to Spark's scan if unsupported types
exist. Comet can still accelerate the rest of the query execution in this case because `CometSparkToColumnarExec` will
convert the output from Spark's can to Arrow arrays. Note that both `spark.comet.exec.enabled=true` and
`spark.comet.convert.parquet.enabled=true` must be set to enable this conversion.

Refer to the [Supported Spark Data Types](https://datafusion.apache.org/comet/user-guide/datatypes.html) section
in the contributor guide to see a list of currently supported data types.

## CometExecRule

This rule traverses bottom-up from the original Spark plan and attempts to replace each operator with a Comet equivalent.
For example, a `ProjectExec` will be replaced by `CometProjectExec`.

When replacing a node, various checks are performed to determine if Comet can support the operator and its expressions.
If an operator, expression, or data type is not supported by Comet then the reason will be stored in a tag on the
underlying Spark node and the plan will not be converted.

Comet does not support partially replacing subsets of the plan within a query stage because this would involve adding
transitions to convert between row-based and columnar data between Spark operators and Comet operators and the overhead
of this could outweigh the benefits of running parts of the query stage natively in Comet.

## Query Execution

Once the plan has been transformed, any consecutive Comet operators are combined into a `CometNativeExec` which contains
a serialized version of the plan (the serialization code can be found in `QueryPlanSerde`). When this operator is
executed, the serialized plan is passed to the native code when calling `Native.createPlan`.

In the native code there is a `PhysicalPlanner` struct (in `planner.rs`) which converts the serialized plan into an
Apache DataFusion `ExecutionPlan`. In some cases, Comet provides specialized physical operators and expressions to
override the DataFusion versions to ensure compatibility with Apache Spark.

`CometExecIterator` will invoke `Native.executePlan` to pull the next batch from the native plan. This is repeated
until no more batches are available (meaning that all data has been processed by the native plan).

The leaf nodes in the physical plan are always `ScanExec` and these operators consume batches of Arrow data that were
prepared before the plan is executed. When `CometExecIterator` invokes `Native.executePlan` it passes the memory
addresses of these Arrow arrays to the native code.

![Diagram of Comet Native Execution](../../_static/images/CometOverviewDetailed.drawio.svg)

## End to End Flow

The following diagram shows the end-to-end flow.

![Diagram of Comet Native Parquet Scan](../../_static/images/CometNativeParquetReader.drawio.svg)
