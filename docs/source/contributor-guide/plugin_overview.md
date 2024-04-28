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

# Comet Plugin Overview

The entry point to Comet is the `org.apache.comet.CometSparkSessionExtensions` class, which can be registered with Spark by adding the following setting to the Spark configuration when launching `spark-shell` or `spark-submit`:

```
--conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions
```

On initialization, this class registers two physical plan optimization rules with Spark: `CometScanRule` and `CometExecRule`. These rules run whenever a query stage is being planned.

## CometScanRule

`CometScanRule` replaces any Parquet scans with Comet Parquet scan classes.

When the V1 data source API is being used, `FileSourceScanExec` is replaced with `CometScanExec`.

When the V2 data source API is being used, `BatchScanExec` is replaced with `CometBatchScanExec`.

## CometExecRule

`CometExecRule` attempts to transform a Spark physical plan into a Comet plan.

This rule traverses bottom-up from the original Spark plan and attempts to replace each node with a Comet equivalent. For example, a `ProjectExec` will be replaced by `CometProjectExec`.

When replacing a node, various checks are performed to determine if Comet can support the operator and its expressions. If an operator or expression is not supported by Comet then the reason will be stored in a tag on the underlying Spark node. Running `explain` on a query will show any reasons that prevented the plan from being executed natively in Comet. If any part of the plan is not supported in Comet then the original Spark plan will be returned.

Comet does not support partially replacing subsets of the plan because this would involve adding transitions to convert between row-based and columnar data between Spark operators and Comet operators and the overhead of this could outweigh the benefits of running parts of the plan natively in Comet.

Once the plan has been transformed, it is serialized into Comet protocol buffer format by the `QueryPlanSerde` class and this serialized plan is passed into the native code by `CometExecIterator`.

In the native code there is a `PhysicalPlanner` struct (in `planner.rs`) which converts the serialized plan into an Apache DataFusion physical plan. In some cases, Comet provides specialized physical operators and expressions to override the DataFusion versions to ensure compatibility with Apache Spark.
