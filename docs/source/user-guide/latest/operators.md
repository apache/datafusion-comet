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

Every concrete operator in `org.apache.spark.sql.execution` has a row in one of the tables below,
across all Spark versions Comet builds against (3.4 through 4.2). Operators that exist only in
some versions are annotated. `CometOperatorDocSuite` fails the build if Spark adds an operator
that is not listed here.

Operators marked ✅ Supported are enabled by default. Each can be turned off individually with
`spark.comet.exec.OPERATOR.enabled=false` (for example `spark.comet.exec.sort.enabled=false`), and
all native execution can be turned off with `spark.comet.exec.enabled=false`. See the
[Comet Configuration Guide](configs.md) for the full list.

## Status legend

| Status                 | Meaning                                                                                                                                                                                                                            |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ✅ Supported           | Native implementation, enabled by default; works in the common case. Some inputs or forms may fall back to Spark.                                                                                                                  |
| ⚠️ Supported (caveats) | Experimental or disabled by default, or accelerates only a limited subset. See the [Compatibility Guide](compatibility/index.md).                                                                                                  |
| 🔜 Planned             | Intended; tracked by an open issue or pull request.                                                                                                                                                                                |
| ❌ Not supported       | Falls back to Spark today. Native execution would be worthwhile, so these are genuine gaps.                                                                                                                                        |
| ➖ Not applicable      | Falls back to Spark by design. Either there is no meaningful native work to do — catalog commands, streaming, arbitrary JVM closures, driver-side operations — or the operator is plan plumbing that Comet handles while planning. |

Neither ❌ nor ➖ is a correctness concern: Comet falls back rather than producing a different
answer. Set [`spark.comet.explainFallback.enabled=true`](tuning.md) to see which operator in your
plan caused a fallback.

## Scans

| Operator                | Status | Notes                                                                                                                                                                                                            |
| ----------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FileSourceScanExec`    | ✅     | Parquet only. Some types and configurations fall back. See [Parquet Scan Compatibility](compatibility/scans.md).                                                                                                 |
| `BatchScanExec`         | ✅     | Parquet, Apache Iceberg Parquet, and CSV (native) scans. See [Parquet Scan Compatibility](compatibility/scans.md) and the [Iceberg Guide](iceberg.md).                                                           |
| `LocalTableScanExec`    | ⚠️     | Disabled by default; there is no acceleration advantage and this operator is typically only used in test code. Can be opted into via config ([#4393](https://github.com/apache/datafusion-comet/pull/4393)).     |
| `InMemoryTableScanExec` | 🔜     | Cached / in-memory table scans fall back today. The output can still be fed into a native stage with `spark.comet.sparkToColumnar.enabled=true` (see [Bridging non-native leaves](#bridging-non-native-leaves)). |
| `UnionLoopExec`         | ❌     | Spark 4.1+. The recursion driver for `WITH RECURSIVE` common table expressions.                                                                                                                                  |
| `RowDataSourceScanExec` | ➖     | V1 sources that are not file-based, most commonly JDBC. Rows arrive from the external system already materialized, so there is no columnar read to accelerate.                                                   |
| `RDDScanExec`           | ➖     | `createDataFrame` over an RDD or local collection. Can be bridged into a native stage (see below); it is in the default bridge list.                                                                             |
| `ExternalRDDScanExec`   | ➖     | `Dataset` built from an RDD of JVM objects. Rows arrive as deserialized objects, so there is nothing columnar to read.                                                                                           |
| `OneRowRelationExec`    | ➖     | Spark 4.1+. A single-row source, for example `SELECT 1`. Can be bridged into a native stage; it is in the default bridge list.                                                                                   |
| `RangeExec`             | ➖     | `spark.range(...)`. Generating a sequence is not a bottleneck, but it is in the default bridge list, so a native stage above it does not have to fall back.                                                      |
| `EmptyRelationExec`     | ➖     | Spark 4.0+. An empty relation folded out by AQE. There are no rows to process.                                                                                                                                   |

### Bridging non-native leaves

Some non-native leaf operators can still feed a native stage: Comet inserts a
`CometSparkToColumnarExec` that converts their row output to Arrow, so the operators above them do
not have to fall back. This is disabled by default. Enable it with
`spark.comet.sparkToColumnar.enabled=true`; the set of eligible operators is
`spark.comet.sparkToColumnar.supportedOperatorList`, which defaults to
`Range,InMemoryTableScan,RDDScan,OneRowRelation`. The conversion itself costs CPU, so it only pays
off when enough native work sits above the leaf.

## Projection and filtering

| Operator             | Status | Notes                                                                                                                                                                    |
| -------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ProjectExec`        | ✅     |                                                                                                                                                                          |
| `FilterExec`         | ✅     |                                                                                                                                                                          |
| `SampleExec`         | ❌     | `df.sample(...)` and `TABLESAMPLE`. Matching Spark's output row-for-row requires reproducing its per-partition RNG stream, which is why this has not been attempted yet. |
| `CollectMetricsExec` | ❌     | `df.observe(...)`. Falls back, and because it sits in the middle of a plan it also splits the surrounding native stage in two.                                           |

## Sorting and limiting

| Operator                    | Status | Notes                                                                                                       |
| --------------------------- | ------ | ----------------------------------------------------------------------------------------------------------- |
| `SortExec`                  | ✅     |                                                                                                             |
| `GlobalLimitExec`           | ✅     |                                                                                                             |
| `LocalLimitExec`            | ✅     |                                                                                                             |
| `CollectLimitExec`          | ✅     |                                                                                                             |
| `TakeOrderedAndProjectExec` | ✅     |                                                                                                             |
| `CollectTailExec`           | ➖     | `df.tail(n)`. Reads from the end of the last partition; a driver-side operation with nothing to accelerate. |

## Aggregation

| Operator                  | Status | Notes                                                                                      |
| ------------------------- | ------ | ------------------------------------------------------------------------------------------ |
| `HashAggregateExec`       | ✅     |                                                                                            |
| `ObjectHashAggregateExec` | ✅     | Supports a limited set of aggregates, such as `bloom_filter_agg`.                          |
| `SortAggregateExec`       | 🔜     | Falls back today; Comet currently accelerates hash aggregates.                             |
| `MergingSessionsExec`     | ❌     | Session-window aggregation (`session_window`). Used in batch as well as streaming queries. |
| `UpdatingSessionsExec`    | ❌     | Assigns rows to session windows ahead of a session-window aggregate.                       |

## Joins

| Operator                      | Status | Notes                                                                                                                                                                         |
| ----------------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BroadcastHashJoinExec`       | ✅     |                                                                                                                                                                               |
| `ShuffledHashJoinExec`        | ✅     |                                                                                                                                                                               |
| `SortMergeJoinExec`           | ✅     |                                                                                                                                                                               |
| `BroadcastNestedLoopJoinExec` | ✅     | Falls back to Spark when the preserved side is broadcast (for example LEFT OUTER with BROADCAST on the left) ([#4429](https://github.com/apache/datafusion-comet/pull/4429)). |
| `CartesianProductExec`        | ➖     | Cross joins. Runtime is dominated by output size rather than per-row cost, so there is little for a native implementation to win.                                             |

## Exchanges

| Operator                | Status | Notes |
| ----------------------- | ------ | ----- |
| `ShuffleExchangeExec`   | ✅     |       |
| `BroadcastExchangeExec` | ✅     |       |

## Window

| Operator               | Status | Notes                                                                                                                                                                                            |
| ---------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `WindowExec`           | ⚠️     | Runs natively and is enabled by default. A broad set of window functions is accelerated; unsupported shapes fall back to Spark. See [window function compatibility](compatibility/operators.md). |
| `WindowGroupLimitExec` | 🔜     | Spark 3.5+. Window-based limit pushdown falls back today ([#4837](https://github.com/apache/datafusion-comet/issues/4837)).                                                                      |

## Generators and set operations

| Operator       | Status | Notes                                                                                                                      |
| -------------- | ------ | -------------------------------------------------------------------------------------------------------------------------- |
| `GenerateExec` | ✅     | Supports `explode` and `posexplode` over arrays. The `_outer` variants are incompatible, and `inline` / `stack` fall back. |
| `ExpandExec`   | ✅     |                                                                                                                            |
| `UnionExec`    | ✅     |                                                                                                                            |
| `CoalesceExec` | ✅     |                                                                                                                            |

## Typed Dataset operators

Operators from the typed `Dataset[T]` API all fall back. They evaluate arbitrary JVM closures over
deserialized objects, so there is no expression tree for Comet to translate and no columnar
representation to work on. This is worth knowing about even though there is nothing for Comet to
do: a single `ds.map(...)` takes its whole query stage back to Spark. Prefer the DataFrame / SQL
API on hot paths.

| Operator                        | Status | Notes                                                                          |
| ------------------------------- | ------ | ------------------------------------------------------------------------------ |
| `DeserializeToObjectExec`       | ➖     | Converts rows to JVM objects at the start of a typed section of a plan.        |
| `SerializeFromObjectExec`       | ➖     | Converts JVM objects back to rows at the end of a typed section.               |
| `MapPartitionsExec`             | ➖     | `ds.mapPartitions(...)`.                                                       |
| `MapElementsExec`               | ➖     | `ds.map(...)` / `ds.flatMap(...)` / `ds.filter(func)`.                         |
| `MapGroupsExec`                 | ➖     | `ds.groupByKey(...).mapGroups(...)` / `flatMapGroups(...)`.                    |
| `CoGroupExec`                   | ➖     | `KeyValueGroupedDataset.cogroup(...)`.                                         |
| `AppendColumnsExec`             | ➖     | Computes the grouping key added by `groupByKey`.                               |
| `AppendColumnsWithObjectExec`   | ➖     | Object-input variant of the same.                                              |
| `SparkScriptTransformationExec` | ➖     | `TRANSFORM ... USING 'script'`. Rows are piped to an external process as text. |

## Python, pandas, and R UDFs

Comet can keep data in Arrow format across `mapInArrow` and `mapInPandas`, avoiding the
Arrow→row→Arrow round trip Spark performs. This is experimental and disabled by default; enable it
with `spark.comet.exec.pyarrowUdf.enabled=true` and see the
[PyArrow UDF guide](pyarrow-udfs.md) for limitations, including that it requires Spark 4.0+ and
Comet's native shuffle.

The remaining Arrow-based UDF operators are marked ❌ rather than ➖ because the same optimization
applies to them in principle — they already exchange Arrow batches with the Python worker. Pickled
UDFs and SparkR are marked ➖ because they serialize row at a time, with no columnar boundary to
preserve.

| Operator                                                                                 | Status | Notes                                                                                                                                                         |
| ---------------------------------------------------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PythonMapInArrowExec`, `MapInArrowExec`                                                 | ⚠️     | `df.mapInArrow(...)`. Native path is opt-in via `spark.comet.exec.pyarrowUdf.enabled` and requires Spark 4.0+. Named `PythonMapInArrowExec` before Spark 4.0. |
| `MapInPandasExec`                                                                        | ⚠️     | `df.mapInPandas(...)`. Same opt-in and version requirement.                                                                                                   |
| `ArrowEvalPythonExec`                                                                    | 🔜     | Scalar `@pandas_udf` ([#4234](https://github.com/apache/datafusion-comet/pull/4234)).                                                                         |
| `FlatMapGroupsInPandasExec`                                                              | 🔜     | `df.groupBy(...).applyInPandas(...)` ([#4234](https://github.com/apache/datafusion-comet/pull/4234)).                                                         |
| `AggregateInPandasExec`, `ArrowAggregatePythonExec`                                      | ❌     | Grouped-aggregate pandas UDFs. Renamed to `ArrowAggregatePythonExec` in Spark 4.1.                                                                            |
| `WindowInPandasExec`, `ArrowWindowPythonExec`                                            | ❌     | Window pandas UDFs. Renamed to `ArrowWindowPythonExec` in Spark 4.1.                                                                                          |
| `ArrowEvalPythonUDTFExec`                                                                | ❌     | Spark 3.5+. Arrow-based Python UDTFs (`udtf`).                                                                                                                |
| `FlatMapGroupsInArrowExec`                                                               | ❌     | Spark 4.0+. `applyInArrow` on a grouped DataFrame.                                                                                                            |
| `FlatMapCoGroupsInPandasExec`                                                            | ❌     | `cogroup(...).applyInPandas(...)`.                                                                                                                            |
| `FlatMapCoGroupsInArrowExec`                                                             | ❌     | Spark 4.0+. `cogroup(...).applyInArrow(...)`.                                                                                                                 |
| `AttachDistributedSequenceExec`                                                          | ❌     | Assigns the distributed sequence index used by the pandas API on Spark (`pyspark.pandas`).                                                                    |
| `BatchEvalPythonExec`                                                                    | ➖     | Pickled (non-Arrow) Python UDFs. Rows are pickled one at a time, so there is no columnar boundary to exploit.                                                 |
| `BatchEvalPythonUDTFExec`                                                                | ➖     | Spark 3.5+. Pickled Python UDTFs.                                                                                                                             |
| `PythonWorkerLogsExec`                                                                   | ➖     | Spark 4.1+. Surfaces Python worker logs as a relation. Metadata, not user data.                                                                               |
| `FlatMapGroupsInRExec`, `FlatMapGroupsInRWithArrowExec`, `MapPartitionsInRWithArrowExec` | ➖     | SparkR UDFs (`gapply`, `dapply`). Comet has no R integration.                                                                                                 |

## Writes

| Operator                                                                                                   | Status | Notes                                                                                                                            |
| ---------------------------------------------------------------------------------------------------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `DataWritingCommandExec`                                                                                   | ⚠️     | Experimental native Parquet writes, disabled by default (opt-in).                                                                |
| `WriteFilesExec`                                                                                           | ⚠️     | The V1 write operator that `DataWritingCommandExec` wraps. Comet converts the pair together; it is never accelerated on its own. |
| `AppendDataExec`, `OverwriteByExpressionExec`, `OverwritePartitionsDynamicExec`, `WriteToDataSourceV2Exec` | ❌     | DataSource V2 writes, including Iceberg writes. Comet accelerates Iceberg reads only.                                            |
| `ReplaceDataExec`, `WriteDeltaExec`, `MergeRowsExec`, `InsertOnlyMergeExec`, `DeleteFromTableExec`         | ❌     | Row-level `MERGE` / `UPDATE` / `DELETE` plans. `MergeRowsExec` is Spark 3.5+, `InsertOnlyMergeExec` is Spark 4.2+.               |

## Plan infrastructure

These operators are how Spark stitches a plan together rather than places where work happens. Comet
rewrites, wraps, or plans around each of them; none is an acceleration target in its own right.
They are listed so that seeing one in a plan does not read as an undocumented gap.

| Operator                                                                   | Status | Notes                                                                                                                                  |
| -------------------------------------------------------------------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| `WholeStageCodegenExec`                                                    | ➖     | Comet replaces the operators inside a stage before codegen, so a fully native stage has no codegen wrapper.                            |
| `ColumnarToRowExec`, `RowToColumnarExec`                                   | ➖     | Comet substitutes `CometColumnarToRowExec` and `CometSparkToColumnarExec`, and removes transition pairs that cancel out.               |
| `AdaptiveSparkPlanExec`                                                    | ➖     | Comet's rules run on each AQE stage as it is planned, so conversion happens inside AQE rather than around it.                          |
| `AQEShuffleReadExec`                                                       | ➖     | Reads the output of a `CometShuffleExchangeExec` and is preserved across conversion, so AQE coalescing and skew splitting still apply. |
| `ShuffleQueryStageExec`, `BroadcastQueryStageExec`, `ResultQueryStageExec` | ➖     | AQE stage wrappers. Comet unwraps them to find the underlying exchange. `ResultQueryStageExec` is Spark 4.0+.                          |
| `TableCacheQueryStageExec`                                                 | ➖     | AQE wrapper around a cached-table scan; see `InMemoryTableScanExec` above.                                                             |
| `ReusedExchangeExec`                                                       | ➖     | Reuse of an already-materialized exchange, including reuse of a Comet exchange.                                                        |
| `SubqueryExec`, `ReusedSubqueryExec`                                       | ➖     | Scalar subqueries. The subquery plan is converted independently on its own merits.                                                     |
| `InSubqueryExec`, `SubqueryBroadcastExec`, `SubqueryAdaptiveBroadcastExec` | ➖     | Dynamic partition pruning. Comet rewrites these so a DPP filter still resolves when the broadcast side became a Comet exchange.        |
| `CommandResultExec`, `ExecutedCommandExec`, `MultiResultExec`              | ➖     | Hold already-computed command results. `MultiResultExec` is Spark 4.1+ (SQL scripting).                                                |
| `GroupPartitionsExec`                                                      | ➖     | Spark 4.2+. Regroups DataSource V2 input partitions for storage-partitioned joins.                                                     |

## Commands

DDL, catalog, and metadata commands operate on catalog state rather than row data and execute once
on the driver, so there is nothing for a columnar engine to accelerate. This is not expected to
change.

| Operator group   | Status | Operators                                                                                                                                                                                                                                                                                                                                                           |
| ---------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Table DDL        | ➖     | `CreateTableExec`, `CreateTableAsSelectExec`, `AtomicCreateTableAsSelectExec`, `ReplaceTableExec`, `AtomicReplaceTableExec`, `ReplaceTableAsSelectExec`, `AtomicReplaceTableAsSelectExec`, `DropTableExec`, `RenameTableExec`, `AlterTableExec`, `TruncateTableExec`, `RefreshTableExec`, `CreateTableLikeExec` (Spark 4.2+), `AddCheckConstraintExec` (Spark 4.1+) |
| Partition DDL    | ➖     | `AddPartitionExec`, `DropPartitionExec`, `RenamePartitionExec`, `TruncatePartitionExec`                                                                                                                                                                                                                                                                             |
| Namespace DDL    | ➖     | `CreateNamespaceExec`, `DropNamespaceExec`, `AlterNamespaceSetPropertiesExec`, `SetCatalogAndNamespaceExec`                                                                                                                                                                                                                                                         |
| View DDL         | ➖     | `DropViewExec`, and Spark 4.2+ V2 views: `CreateV2ViewExec`, `CreateV2MetricViewExec`, `AlterV2ViewExec`, `AlterV2ViewSetPropertiesExec`, `AlterV2ViewUnsetPropertiesExec`, `AlterV2ViewSchemaBindingExec`, `RenameV2ViewExec`                                                                                                                                      |
| Index DDL        | ➖     | `CreateIndexExec`, `DropIndexExec`                                                                                                                                                                                                                                                                                                                                  |
| Metadata queries | ➖     | `DescribeTableExec`, `DescribeColumnExec`, `DescribeNamespaceExec`, `DescribeTablePartitionExec` (Spark 4.2+), `ShowTablesExec`, `ShowTablesExtendedExec` (Spark 4.0+), `ShowTablePropertiesExec`, `ShowTablePartitionExec`, `ShowPartitionsExec`, `ShowColumnsExec`, `ShowNamespacesExec`, `ShowFunctionsExec`, `ShowCreateTableExec`                              |
| View metadata    | ➖     | `ShowViewsExec`, `DescribeV2ViewExec`, `DescribeV2ViewColumnExec`, `ShowCreateV2ViewExec`, `ShowV2ViewColumnsExec`, `ShowV2ViewPropertiesExec` (Spark 4.2+)                                                                                                                                                                                                         |
| Caching          | ➖     | `CacheTableExec`, `CacheTableAsSelectExec`, `UncacheTableExec`                                                                                                                                                                                                                                                                                                      |
| SQL variables    | ➖     | `CreateVariableExec`, `SetVariableExec`, `DropVariableExec` (Spark 4.0+)                                                                                                                                                                                                                                                                                            |
| Cursors          | ➖     | `DeclareCursorExec`, `OpenCursorExec`, `FetchCursorExec`, `CloseCursorExec` (Spark 4.2+)                                                                                                                                                                                                                                                                            |

## Structured Streaming

Comet targets batch execution. Streaming-specific operators, and the stateful operators that back
them, fall back to Spark. This is not on the roadmap. A streaming query can still benefit from
Comet where it uses batch operators, for example a `FilterExec` over a Parquet source.

| Operator group     | Status | Operators                                                                                                                                                                                                |
| ------------------ | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Streaming sources  | ➖     | `StreamingRelationExec`, `MicroBatchScanExec`, `ContinuousScanExec`, `RealTimeStreamScanExec` (Spark 4.1+)                                                                                               |
| Streaming sinks    | ➖     | `WriteToContinuousDataSourceExec`                                                                                                                                                                        |
| State store        | ➖     | `StateStoreSaveExec`, `StateStoreRestoreExec`, `SessionWindowStateStoreSaveExec`, `SessionWindowStateStoreRestoreExec`                                                                                   |
| Stateful transform | ➖     | `FlatMapGroupsWithStateExec`, `FlatMapGroupsInPandasWithStateExec`, `TransformWithStateExec` (Spark 4.0+), `TransformWithStateInPandasExec` (Spark 4.0+), `TransformWithStateInPySparkExec` (Spark 4.1+) |
| Streaming joins    | ➖     | `StreamingSymmetricHashJoinExec`                                                                                                                                                                         |
| Deduplication      | ➖     | `StreamingDeduplicateExec`, `StreamingDeduplicateWithinWatermarkExec` (Spark 3.5+)                                                                                                                       |
| Streaming limits   | ➖     | `StreamingGlobalLimitExec`, `StreamingLocalLimitExec`                                                                                                                                                    |
| Watermarks         | ➖     | `EventTimeWatermarkExec`, `UpdateEventTimeColumnExec` (Spark 4.0+)                                                                                                                                       |

## See also

- [Comet Compatibility Guide](compatibility/index.md) - known incompatibilities and edge cases.
- [Supported Spark Expressions](expressions.md) - the equivalent reference for expressions.
- [PyArrow UDF Acceleration](pyarrow-udfs.md) - the opt-in native path for `mapInArrow` / `mapInPandas`.
