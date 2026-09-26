<!---
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

# Spark Operator Support

This page is the complete reference for how Apache Comet handles each Spark physical operator.
Comet replaces supported operators with native equivalents. Comet runs whole subtrees of native
operators together, so if a query stage contains an operator Comet does not support, that stage
falls back to regular Spark execution. Results are unaffected.

Operators marked ✅ Supported are enabled by default. Each can be turned off individually with
`spark.comet.exec.OPERATOR.enabled=false` (for example `spark.comet.exec.sort.enabled=false`), and
all native execution can be turned off with `spark.comet.exec.enabled=false`. See the
[Comet Configuration Guide](configs.md) for the full list.

## Status legend

| Status                 | Meaning                                                                                                                           |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| ✅ Supported           | Native implementation, enabled by default; works in the common case. Some inputs or forms may fall back to Spark.                 |
| ⚠️ Supported (caveats) | Experimental or disabled by default, or accelerates only a limited subset. See the [Compatibility Guide](compatibility/index.md). |
| 🔜 Planned             | Intended; tracked by an open issue or pull request.                                                                               |

## Not currently planned

The following operator families fall back to Spark and are not on the current roadmap. They are
omitted from the tables below and may be reconsidered based on demand:

- **Structured Streaming operators** (`StateStoreSaveExec`, `StateStoreRestoreExec`, `StreamingSymmetricHashJoinExec`, and similar): Comet targets batch execution.
- **Cartesian / cross joins** (`CartesianProductExec`): rare and expensive, with little acceleration benefit.
- **Range generation** (`RangeExec`): niche leaf operator.
- **Pickled (non-Arrow) Python UDFs** (`BatchEvalPythonExec`): Comet accelerates Arrow-based Python UDFs only ([#4234](https://github.com/apache/datafusion-comet/pull/4234)).

## Scans

| Operator                | Status | Notes                                                                                                                                                                                                                                                                                           |
| ----------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FileSourceScanExec`    | ✅     | Parquet only. Some types and configurations fall back. See [Parquet Scan Compatibility](compatibility/scans.md).                                                                                                                                                                                |
| `BatchScanExec`         | ✅     | Apache Iceberg Parquet scans run natively. Native CSV scans are experimental and disabled by default. DataSource V2 Parquet scans are not accelerated. See [Parquet Scan Compatibility](compatibility/scans.md) and the [Iceberg Guide](iceberg.md).                                            |
| `LocalTableScanExec`    | ⚠️     | Disabled by default; there is no acceleration advantage and this operator is typically only used in test code. Can be opted into via config ([#4393](https://github.com/apache/datafusion-comet/pull/4393)).                                                                                    |
| `EmptyRelationExec`     | ✅     | Spark 4.0 and later. See [Empty Relations](compatibility/operators.md#empty-relations) for native-input support and writer fallback.                                                                                                                                                            |
| `InMemoryTableScanExec` | ⚠️     | Experimental, disabled by default. Set `spark.comet.exec.inMemoryCache.enabled=true` before the application starts so Comet installs its Arrow cache serializer. Relations with unsupported column types stay in Spark's cache format and fall back. See [In-Memory Cache](in-memory-cache.md). |

## Projection and filtering

| Operator      | Status | Notes                                                                                        |
| ------------- | ------ | -------------------------------------------------------------------------------------------- |
| `ProjectExec` | ✅     |                                                                                              |
| `FilterExec`  | ✅     |                                                                                              |
| `SampleExec`  | ⚠️     | Sampling without replacement only. See [Operator Compatibility](compatibility/operators.md). |

## Sorting and limiting

| Operator                    | Status | Notes |
| --------------------------- | ------ | ----- |
| `SortExec`                  | ✅     |       |
| `GlobalLimitExec`           | ✅     |       |
| `LocalLimitExec`            | ✅     |       |
| `CollectLimitExec`          | ✅     |       |
| `TakeOrderedAndProjectExec` | ✅     |       |

## Aggregation

| Operator                  | Status | Notes                                                                                                                                                                                                                                                                                                                              |
| ------------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HashAggregateExec`       | ✅     |                                                                                                                                                                                                                                                                                                                                    |
| `ObjectHashAggregateExec` | ✅     | Runs the object-buffer aggregates Comet supports, such as `collect_list`, `collect_set`, `percentile`, `approx_percentile`, `mode`, `bloom_filter_agg`, and (Spark 4.0+) `listagg`. Falls back when Comet shuffle is disabled, which would otherwise split the aggregate across Comet and Spark. See [Shuffle](tuning.md#shuffle). |
| `SortAggregateExec`       | 🔜     | Falls back today; Comet currently accelerates hash aggregates.                                                                                                                                                                                                                                                                     |

## Joins

| Operator                      | Status | Notes                                                                                                                                                                         |
| ----------------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BroadcastHashJoinExec`       | ✅     |                                                                                                                                                                               |
| `ShuffledHashJoinExec`        | ✅     |                                                                                                                                                                               |
| `SortMergeJoinExec`           | ✅     | Supports `BINARY` join keys. Nested-type (struct, array, map) and collated-string join keys fall back to Spark.                                                               |
| `BroadcastNestedLoopJoinExec` | ✅     | Falls back to Spark when the preserved side is broadcast (for example LEFT OUTER with BROADCAST on the left) ([#4429](https://github.com/apache/datafusion-comet/pull/4429)). |

## Exchanges

| Operator                | Status | Notes |
| ----------------------- | ------ | ----- |
| `ShuffleExchangeExec`   | ✅     |       |
| `BroadcastExchangeExec` | ✅     |       |

## Window

| Operator               | Status | Notes                                                                                                                                                                                            |
| ---------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `WindowExec`           | ⚠️     | Runs natively and is enabled by default. A broad set of window functions is accelerated; unsupported shapes fall back to Spark. See [window function compatibility](compatibility/operators.md). |
| `WindowGroupLimitExec` | ✅     | Spark 3.5 and later. Streaming per-partition top-K pushdown for `ROW_NUMBER`, `RANK`, and `DENSE_RANK`.                                                                                          |

## Generators and set operations

| Operator       | Status | Notes                                                                                                            |
| -------------- | ------ | ---------------------------------------------------------------------------------------------------------------- |
| `GenerateExec` | ✅     | Supports `explode`, `explode_outer`, `posexplode`, `posexplode_outer` over arrays. `inline` / `stack` fall back. |
| `ExpandExec`   | ✅     |                                                                                                                  |
| `UnionExec`    | ✅     |                                                                                                                  |
| `CoalesceExec` | ✅     |                                                                                                                  |

## Writes

| Operator                                                                                           | Status | Notes                                                                                                                                                                             |
| -------------------------------------------------------------------------------------------------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WriteFilesExec`                                                                                   | ⚠️     | Spark 4.0+. Experimental native Parquet writes, disabled by default (opt-in). Non-partitioned, non-bucketed writes only, and not when `spark.sql.files.maxRecordsPerFile` is set. |
| `DataWritingCommandExec`                                                                           | ⚠️     | Spark 3.4/3.5 only. Experimental native Parquet writes, disabled by default (opt-in). Replaced by `WriteFilesExec` on Spark 4.0+ and removed with Spark 3.x support.              |
| `AppendDataExec`, `OverwriteByExpressionExec`, `OverwritePartitionsDynamicExec`, `ReplaceDataExec` | ⚠️     | Apache Iceberg tables only. Experimental, disabled by default. See [Iceberg Writes](iceberg-writes.md).                                                                           |

## Python and UDF

| Operator                                           | Status | Notes                                                                                                                                                                                                    |
| -------------------------------------------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MapInArrowExec`, `MapInPandasExec`                | ⚠️     | Spark 4.0 and later. Experimental, disabled by default (`spark.comet.exec.pyarrowUDF.enabled`). See [PyArrow UDF Acceleration](pyarrow-udfs.md).                                                         |
| `ArrowEvalPythonExec`, `FlatMapGroupsInPandasExec` | 🔜     | Scalar `@pandas_udf` ([#5386](https://github.com/apache/datafusion-comet/issues/5386)) and grouped `applyInPandas` ([#5123](https://github.com/apache/datafusion-comet/issues/5123)) fall back to Spark. |

## See also

- [Comet Compatibility Guide](compatibility/index.md) - known incompatibilities and edge cases.
- [Supported Spark Expressions](expressions.md) - the equivalent reference for expressions.
