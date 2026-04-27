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

# Understanding Comet Plans

This guide explains how to read a Spark query plan once Comet is enabled, what
happens when parts of a plan fall back to Spark, and which configs to use to
inspect that behavior.

## Overview

When Comet is enabled, the `CometSparkSessionExtensions` rules walk the
physical plan bottom-up and replace Spark operators with Comet equivalents
where possible. Consecutive native operators are combined into a single block
that is serialized as protobuf and executed by DataFusion on the executor.
Operators that Comet does not support remain as their original Spark form.

As a result, a plan can mix three kinds of nodes:

- **`Comet*` nodes** that run natively in Rust (for example `CometProject`,
  `CometHashAggregate`).
- **`Comet*` nodes that run on the JVM** but are still part of the Comet
  pipeline (for example `CometBroadcastExchange`, `CometColumnarExchange`).
- **Standard Spark nodes** (for example `Project`, `HashAggregate`) where
  Comet either does not support the operator or has fallen back due to an
  unsupported expression, data type, or configuration.

Wherever data crosses between columnar and row-based execution, Comet inserts
a transition node such as `CometColumnarToRow` or `CometSparkRowToColumnar`.

## Reading a Plan

You can print a plan with `df.explain(true)` or `EXPLAIN FORMATTED <sql>`, and
the same plan is shown in the Spark SQL UI. When reading a plan, look for:

- **Node prefix.** `Comet*` nodes are accelerated by Comet. Anything without
  the prefix is unmodified Spark.
- **Transitions.** `CometColumnarToRow`, `CometNativeColumnarToRow`, and
  `CometSparkRowToColumnar` mark boundaries between columnar Comet execution
  and row-based Spark execution. Frequent transitions usually indicate
  fallback inside the plan.
- **Exchange type.** `CometExchange` is the native shuffle path,
  `CometColumnarExchange` is the JVM columnar shuffle path, and a plain
  `Exchange` means Spark shuffle. See [Shuffle Operators](#shuffle-operators)
  below.

## Fallback

A "fallback" happens when Comet cannot translate part of a plan into native
execution. Fallback can be partial (a subtree falls back while the rest stays
native) or full (no Comet nodes appear).

Common reasons:

- The Spark operator is not supported by Comet.
- An expression inside an otherwise supported operator is not supported, or
  is marked incompatible and the per-expression opt-in
  `spark.comet.expression.<ExpressionName>.allowIncompatible=true` is not
  set. Operators have an equivalent
  `spark.comet.operator.<OperatorName>.allowIncompatible` opt-in.
- A data type is not supported by the operator.
- A configuration setting disables a specific operator or expression.

See [Supported Spark Operators](operators.md) and [Supported Expressions](expressions.md)
for current coverage, and the [Compatibility Guide](compatibility/index.md) for
incompatibility details.

## Configs for Inspecting Plans and Fallback

Comet provides four configs for understanding what is happening in a plan.
They serve different purposes and produce output in different places.

| Config                                   | Output destination                 | What you see                                                                                  |
| ---------------------------------------- | ---------------------------------- | --------------------------------------------------------------------------------------------- |
| `spark.comet.explainFallback.enabled`    | Driver log (only when fallback)    | A WARN with the list of reasons each query stage could not run natively.                      |
| `spark.comet.logFallbackReasons.enabled` | Driver log                         | One WARN per fallback reason as it is encountered, without surrounding plan context.          |
| `spark.comet.explain.format`             | Spark SQL UI (Spark 4.0 and newer) | Annotated plan or fallback-reason list, depending on `verbose` (default) or `fallback` value. |
| `spark.comet.explain.native.enabled`     | Executor logs, per task            | The DataFusion native plan with metrics, useful for inspecting native execution.              |

### `spark.comet.explainFallback.enabled`

Logs a single WARN listing the reasons each query stage could not be executed
natively. Nothing is logged when the entire stage runs in Comet. Useful as a
low-noise check that fallback is or is not happening.

### `spark.comet.logFallbackReasons.enabled`

Logs every fallback reason as it is encountered, one WARN per reason. Use this
when you want to see all reasons, including ones that
`spark.comet.explainFallback.enabled` may aggregate or omit. The output does
not include the surrounding plan, so it is best for accumulating diagnostics
across many queries.

### `spark.comet.explain.format`

This config is read by `org.apache.comet.ExtendedExplainInfo`, which Spark
loads via the `spark.sql.extendedExplainProviders` mechanism added in Spark
4.0. Add the provider:

```shell
--conf spark.sql.extendedExplainProviders=org.apache.comet.ExtendedExplainInfo
```

The Spark SQL UI then shows an additional section under the detailed plan.
The format is controlled by `spark.comet.explain.format`:

- `verbose` (default): the full plan annotated with fallback reasons, plus a
  summary of how much of the plan is accelerated.
- `fallback`: a list of fallback reasons only.

This is the most convenient option on Spark 4.0 because the output is shown
inline in the UI. Earlier Spark versions do not have the
`extendedExplainProviders` extension point, so this provider is not used and
the config has no effect there.

### `spark.comet.explain.native.enabled`

When enabled, each executor task logs the DataFusion native plan it executes,
along with metrics. This is verbose because there is one plan per task, but it
is the only way to see the native plan as DataFusion sees it (including how
operators were arranged after Comet's serialization). See the
[Metrics Guide](metrics.md) for details on the native metrics that appear in
this output.

## Comet Operator Reference

The following sections describe the Comet nodes you will see in plans, grouped
by role. Names match what is shown in the plan output.

### Scans

| Node                     | Description                                                                                     |
| ------------------------ | ----------------------------------------------------------------------------------------------- |
| `CometScan`              | V1 Parquet scan driven by Spark's file-source path through Comet's Parquet reader. Decoding runs in native code; the resulting Arrow batches cross JNI into the native plan. The active scan implementation is shown in brackets, e.g. `CometScan [native_iceberg_compat]`. |
| `CometBatchScan`         | DataSource V2 scan, including Iceberg Parquet, that produces Arrow batches consumed by Comet.   |
| `CometNativeScan`        | Fully native Parquet scan that runs entirely in DataFusion (no JVM Parquet reader involvement). |
| `CometIcebergNativeScan` | Fully native Iceberg Parquet scan.                                                              |
| `CometCsvNativeScan`     | Fully native CSV scan (experimental).                                                           |

### Native Execution Operators

These run natively in DataFusion. When several appear consecutively in a plan,
they execute as a single fused native block.

| Node                         | Spark equivalent                               |
| ---------------------------- | ---------------------------------------------- |
| `CometProject`               | `ProjectExec`                                  |
| `CometFilter`                | `FilterExec`                                   |
| `CometSort`                  | `SortExec`                                     |
| `CometLocalLimit`            | `LocalLimitExec`                               |
| `CometGlobalLimit`           | `GlobalLimitExec`                              |
| `CometExpand`                | `ExpandExec`                                   |
| `CometExplode`               | `GenerateExec` (for `explode` only)            |
| `CometHashAggregate`         | `HashAggregateExec`, `ObjectHashAggregateExec` |
| `CometHashJoin`              | `ShuffledHashJoinExec`                         |
| `CometBroadcastHashJoin`     | `BroadcastHashJoinExec`                        |
| `CometSortMergeJoin`         | `SortMergeJoinExec`                            |
| `CometWindow`                | `WindowExec`                                   |
| `CometTakeOrderedAndProject` | `TakeOrderedAndProjectExec`                    |

### JVM-Side Operators

These keep their data on the JVM but participate in the Comet pipeline.

| Node                     | Notes                                                                                 |
| ------------------------ | ------------------------------------------------------------------------------------- |
| `CometUnion`             | JVM-side union of Comet inputs. The native side reads each branch as a separate scan. |
| `CometCoalesce`          | JVM-side partition coalesce.                                                          |
| `CometCollectLimit`      | JVM-side collect limit, equivalent to `CollectLimitExec`.                             |
| `CometBroadcastExchange` | Broadcast exchange producing serialized Arrow batches that the consumer can decode.   |
| `CometSubqueryBroadcast` | Companion to `CometBroadcastExchange` for dynamic partition pruning subqueries.       |

### Shuffle Operators

Comet has two shuffle implementations and the plan tells you which one is in
use:

- **`CometExchange`** is the **native shuffle** path. The child must already
  produce columnar Arrow batches and the partitioning keys must be primitive
  types. The node calls `executeColumnar()` on its child and the partition,
  encode, and compress steps run in native code.
- **`CometColumnarExchange`** is the **JVM columnar shuffle** path. It accepts
  either Spark row-based input or Comet columnar input (Comet children are
  converted to rows automatically) and supports more partitioning schemes
  (`HashPartitioning`, `RoundRobinPartitioning`, `RangePartitioning`,
  `SinglePartitioning`) and more partitioning key types, including complex
  types. It is the fallback when native shuffle cannot be used but Comet
  shuffle is still enabled.

The choice between the two is automatic. See the
[Tuning Guide shuffle section](tuning.md#shuffle) for how to enable Comet
shuffle and choose between the implementations.

### Columnar/Row Transitions

Comet inserts these nodes wherever data has to cross the columnar/row boundary.
Multiple implementations exist because the optimal strategy depends on what
produced the columnar data.

| Node                           | Direction           | Notes                                                                                                                                                                                                                         |
| ------------------------------ | ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CometColumnarToRow`           | columnar → row      | JVM-based row conversion. A fork of Spark's `ColumnarToRowExec` that includes the SPARK-50235 fix.                                                                                                                            |
| `CometNativeColumnarToRow`     | columnar → row      | Native row conversion that decodes broadcast Arrow batches via `NativeColumnarToRowConverter`. Used downstream of `CometBroadcastExchange`. Zero-copy for variable-length types and avoids an extra JVM materialization step. |
| `CometSparkColumnarToColumnar` | columnar → columnar | Converts a Spark columnar input (a non-Comet `ColumnarBatch`) into Comet's Arrow batches.                                                                                                                                     |
| `CometSparkRowToColumnar`      | row → columnar      | Converts a Spark row input into Comet's Arrow batches.                                                                                                                                                                        |

The two `CometSpark*` names come from a single `CometSparkToColumnarExec`
operator that picks the node name based on whether its child supports
columnar.
