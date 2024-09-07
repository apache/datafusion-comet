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
and `CometExecRule`. These rules run whenever a query stage is being planned during Adaptive Query Execution.

## CometScanRule

`CometScanRule` replaces any Parquet scans with Comet Parquet scan classes.

When the V1 data source API is being used, `FileSourceScanExec` is replaced with `CometScanExec`.

When the V2 data source API is being used, `BatchScanExec` is replaced with `CometBatchScanExec`.

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

Once the plan has been transformed, it is serialized into Comet protocol buffer format by the `QueryPlanSerde` class
and this serialized plan is passed into the native code by `CometExecIterator`.

In the native code there is a `PhysicalPlanner` struct (in `planner.rs`) which converts the serialized plan into an
Apache DataFusion physical plan. In some cases, Comet provides specialized physical operators and expressions to
override the DataFusion versions to ensure compatibility with Apache Spark.

`CometExecIterator` will invoke `Native.executePlan` to fetch the next batch from the native plan. This is repeated 
until no more batches are available (meaning that all data has been processed by the native plan).

The leaf nodes in the physical plan are always `ScanExec` and these operators consume batches of Arrow data that were 
prepared before the plan is executed. When `CometExecIterator` invokes `Native.executePlan` it passes the memory 
addresses of these Arrow arrays to the native code.

The following section on Parquet support provides a diagram showing the complete execution flow.

## Parquet Support

### Native Parquet Scan with v1 Data Source

When reading from Parquet v1 data sources, Comet provides JVM code for performing the reads from disk and 
implementing predicate pushdown to skip row groups and then delegates to native code for decoding Parquet pages and 
row groups into Arrow arrays.

![Diagram of Comet Native Parquet Scan](../../_static/images/CometNativeParquetScan.drawio.png)

`CometScanRule` replaces `FileSourceScanExec` with `CometScanExec`.

`CometScanExec.doExecuteColumnar` creates an instance of `CometParquetPartitionReaderFactory` and passes it either 
into a `DataSourceRDD` (if prefetch is enabled) or a `FileScanRDD`. It then calls `mapPartitionsInternal` on the 
`RDD` and wraps the resulting `Iterator[ColumnarBatch]` in another iterator that collects metrics such as `scanTime`
and `numOutputRows`.

`CometParquetPartitionReaderFactory` will create a `org.apache.comet.parquet.BatchReader` which in turn creates one
column reader per column. There are different column reader implementations for different data types and encodings. The
column readers invoke methods on the `org.apache.comet.parquet.Native` class such as `resetBatch`, `readBatch`, 
and `currentBatch`.

The `CometScanExec` provides batches that will be read by the `ScanExec` native plan leaf node. `CometScanExec` is 
wrapped in a `CometBatchIterator` that will convert Spark's `ColumnarBatch` into Arrow Arrays. This is then wrapped in
a `CometExecIterator` that will consume the Arrow Arrays and execute the native plan via methods on 
`org.apache.comet.Native` such as `createPlan`, `executePlan`, and `releasePlan`. The memory addresses for each batch of
Arrow Arrays are passed to the call to `executePlan` and are then consumed by the plan's `ScanExec` leaf nodes.

An execution plan can contain multiple `ScanExec` nodes and the call to `createPlan` passes an array of 
`ColumnBatchIterator`.

### Parquet Scan with v2 Data Source

`CometScanRule` replaces `BatchScanExec` with `CometBatchScanExec`.

Comet does not provide native acceleration for decoding Parquet files when using v2 data source and instead uses 
Spark's vectorized reader. 

### Parquet Scan using Spark's vectorized reader

When Comet's native scan is disabled (by setting `spark.comet.scan.enabled=false`), Comet will use 
Spark's `FileSourceScanExec` or `BatchScanExec` and wrap these operators in `CometSparkToColumnarExec` which will
convert instances of Spark's `ColumnarBatch` into Arrow Arrays. 

Note that both `spark.comet.exec.enabled=true` and `spark.comet.convert.parquet.enabled=true` must be set to enable 
this feature.