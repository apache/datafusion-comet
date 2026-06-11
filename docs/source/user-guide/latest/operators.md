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
- **Sampling and range generation** (`SampleExec`, `RangeExec`): niche leaf operators.
- **Pickled (non-Arrow) Python UDFs** (`BatchEvalPythonExec`): Comet accelerates Arrow-based Python UDFs only ([#4234](https://github.com/apache/datafusion-comet/issues/4234)).

## Scans

| Operator                | Status | Notes                                                                                                                                                  |
| ----------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `FileSourceScanExec`    | ✅     | Parquet only. Some types and configurations fall back. See [Parquet Scan Compatibility](compatibility/scans.md).                                       |
| `BatchScanExec`         | ✅     | Parquet, Apache Iceberg Parquet, and CSV (native) scans. See [Parquet Scan Compatibility](compatibility/scans.md) and the [Iceberg Guide](iceberg.md). |
| `LocalTableScanExec`    | ⚠️     | Disabled by default; there is no acceleration advantage and this operator is typically only used in test code. Can be opted into via config (#4393).   |
| `InMemoryTableScanExec` | 🔜     | Cached / in-memory table scans fall back today.                                                                                                        |

## Projection and filtering

| Operator      | Status | Notes |
| ------------- | ------ | ----- |
| `ProjectExec` | ✅     |       |
| `FilterExec`  | ✅     |       |

## Sorting and limiting

| Operator                    | Status | Notes |
| --------------------------- | ------ | ----- |
| `SortExec`                  | ✅     |       |
| `GlobalLimitExec`           | ✅     |       |
| `LocalLimitExec`            | ✅     |       |
| `CollectLimitExec`          | ✅     |       |
| `TakeOrderedAndProjectExec` | ✅     |       |

## Aggregation

| Operator                  | Status | Notes                                                             |
| ------------------------- | ------ | ----------------------------------------------------------------- |
| `HashAggregateExec`       | ✅     |                                                                   |
| `ObjectHashAggregateExec` | ✅     | Supports a limited set of aggregates, such as `bloom_filter_agg`. |
| `SortAggregateExec`       | 🔜     | Falls back today; Comet currently accelerates hash aggregates.    |

## Joins

| Operator                      | Status | Notes                                                                                                                 |
| ----------------------------- | ------ | --------------------------------------------------------------------------------------------------------------------- |
| `BroadcastHashJoinExec`       | ✅     |                                                                                                                       |
| `ShuffledHashJoinExec`        | ✅     |                                                                                                                       |
| `SortMergeJoinExec`           | ✅     |                                                                                                                       |
| `BroadcastNestedLoopJoinExec` | ✅     | Falls back to Spark when the preserved side is broadcast (for example LEFT OUTER with BROADCAST on the left) (#4429). |

## Exchanges

| Operator                | Status | Notes |
| ----------------------- | ------ | ----- |
| `ShuffleExchangeExec`   | ✅     |       |
| `BroadcastExchangeExec` | ✅     |       |

## Window

| Operator               | Status | Notes                                                                                                                                            |
| ---------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `WindowExec`           | ⚠️     | Runs natively, but only a subset of window functions is accelerated. The rest fall back. See the [expression reference](expressions.md) (#2721). |
| `WindowGroupLimitExec` | 🔜     | Window-based limit pushdown falls back today.                                                                                                    |

## Generators and set operations

| Operator       | Status | Notes                                                                                                                      |
| -------------- | ------ | -------------------------------------------------------------------------------------------------------------------------- |
| `GenerateExec` | ✅     | Supports `explode` and `posexplode` over arrays. The `_outer` variants are incompatible, and `inline` / `stack` fall back. |
| `ExpandExec`   | ✅     |                                                                                                                            |
| `UnionExec`    | ✅     |                                                                                                                            |
| `CoalesceExec` | ✅     |                                                                                                                            |

## Writes

| Operator                 | Status | Notes                                                             |
| ------------------------ | ------ | ----------------------------------------------------------------- |
| `DataWritingCommandExec` | ⚠️     | Experimental native Parquet writes, disabled by default (opt-in). |

## Python and UDF

| Operator                                                                                | Status | Notes                                                                |
| --------------------------------------------------------------------------------------- | ------ | -------------------------------------------------------------------- |
| `ArrowEvalPythonExec`, `MapInArrowExec`, `MapInPandasExec`, `FlatMapGroupsInPandasExec` | 🔜     | Experimental accelerated PyArrow UDF support is in progress (#4234). |

## See also

- [Comet Compatibility Guide](compatibility/index.md) - known incompatibilities and edge cases.
- [Supported Spark Expressions](expressions.md) - the equivalent reference for expressions.
