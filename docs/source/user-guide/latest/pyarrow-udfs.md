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

# PyArrow UDF Acceleration

Comet can accelerate Python UDFs that use PyArrow-backed batch processing, such as `mapInArrow` and `mapInPandas`.
These APIs are commonly used for ML inference, feature engineering, and data transformation workloads.

## Background

Spark's `mapInArrow` and `mapInPandas` APIs allow users to apply Python functions that operate on Arrow
RecordBatches or Pandas DataFrames. Under the hood, Spark communicates with the Python worker process
using the Arrow IPC format.

Without Comet, the execution path for these UDFs involves unnecessary data conversions:

1. Comet reads data in Arrow columnar format (via CometScan)
2. Spark inserts a ColumnarToRow transition (converts Arrow to UnsafeRow)
3. The Python runner converts those rows back to Arrow to send to Python
4. Python executes the UDF on Arrow batches
5. Results are returned as Arrow and then converted back to rows

Steps 2 and 3 are redundant since the data starts and ends in Arrow format.

## How Comet Optimizes This

When enabled, Comet detects `PythonMapInArrowExec` / `MapInArrowExec` and `MapInPandasExec`
operators in the physical plan and replaces them with `CometMapInBatchExec`, which:

- Reads Arrow columnar batches directly from the upstream Comet operator
- Feeds them to the Python runner without the expensive UnsafeProjection copy
- Keeps the Python output in columnar format for downstream operators

This eliminates the ColumnarToRow transition and the output row conversion, reducing CPU overhead
and memory allocations. The internal row-to-Arrow IPC re-encoding inside Spark's
`ArrowPythonRunner` is unchanged in this version; full round-trip elimination is tracked in
[#4240](https://github.com/apache/datafusion-comet/issues/4240).

### Plan flow

Without Comet's optimization:

```
PythonMapInArrow / MapInArrow / MapInPandas
+- ColumnarToRow         <- Arrow -> Row copy
   +- CometNativeExec    <- Arrow batch
      +- CometScan
```

With the optimization enabled:

```
CometMapInBatch          <- Arrow batch in/out, Python runner attached
+- CometNativeExec
   +- CometScan
```

## Configuration

The optimization is experimental and disabled by default. Enable it with:

```
spark.comet.exec.pyarrowUdf.enabled=true
```

The default is `false` while the feature stabilizes.

## Supported APIs

| PySpark API                      | Spark Plan Node             | Supported |
| -------------------------------- | --------------------------- | --------- |
| `df.mapInArrow(func, schema)`    | `PythonMapInArrowExec`      | Yes       |
| `df.mapInPandas(func, schema)`   | `MapInPandasExec`           | Yes       |
| `@pandas_udf` (scalar)           | `ArrowEvalPythonExec`       | Not yet   |
| `df.applyInPandas(func, schema)` | `FlatMapGroupsInPandasExec` | Not yet   |

## Example

```python
import pyarrow as pa
from pyspark.sql import SparkSession, types as T

spark = SparkSession.builder \
    .config("spark.plugins", "org.apache.spark.CometPlugin") \
    .config("spark.comet.enabled", "true") \
    .config("spark.comet.exec.enabled", "true") \
    .config("spark.comet.exec.pyarrowUdf.enabled", "true") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .getOrCreate()

df = spark.read.parquet("data.parquet")

def transform(batch: pa.RecordBatch) -> pa.RecordBatch:
    # Your transformation logic here
    table = batch.to_pandas()
    table["new_col"] = table["value"] * 2
    return pa.RecordBatch.from_pandas(table)

output_schema = T.StructType([
    T.StructField("value", T.DoubleType()),
    T.StructField("new_col", T.DoubleType()),
])

result = df.mapInArrow(transform, output_schema)
```

## Verifying the Optimization

Use `explain()` to verify that `CometMapInBatch` appears in your plan:

```python
result.explain(mode="extended")
```

You should see:

```
CometMapInBatch ...
+- CometNativeExec ...
   +- CometScan ...
```

Instead of the unoptimized plan:

```
PythonMapInArrow ...
+- ColumnarToRow
   +- CometNativeExec ...
      +- CometScan ...
```

When AQE is enabled (the Spark default) and the query contains a shuffle, the
optimization is applied during stage materialization. Calling `explain()` before
running an action will show the unoptimized plan:

```
AdaptiveSparkPlan isFinalPlan=false
+- PythonMapInArrow ...
   +- CometExchange ...
```

To see the optimized plan, run an action first (for example `result.collect()` or
`result.cache(); result.count()`) and then call `explain()`. The post-execution
plan shows the materialized stages and includes `CometMapInBatch` if the
optimization fired.

## Barrier execution

`mapInArrow(..., barrier=True)` and `mapInPandas(..., barrier=True)` are honored: the
optimized operator propagates `isBarrier` through `RDD.barrier()`, so all tasks are
gang-scheduled and `BarrierTaskContext.barrier()` works inside the UDF the same way it does
on the unoptimized path.

## Limitations

- The optimization currently applies only to `mapInArrow` and `mapInPandas`. Scalar pandas UDFs
  (`@pandas_udf`) and grouped operations (`applyInPandas`) are not yet supported.
- The internal row-to-Arrow conversion inside the Python runner is still present in this version.
  Comet currently routes columnar input through `ColumnarBatch.rowIterator()` so that the existing
  `ArrowPythonRunner` can re-encode the rows back to Arrow IPC. A future optimization will write
  Arrow batches directly to the Python IPC stream, eliminating the remaining round-trip and
  achieving near zero-copy data transfer.
- The optimization requires Arrow data on the input side. If a shuffle sits between the upstream
  Comet operator and the Python UDF, you need Comet's native shuffle for the optimization to
  apply. Set `spark.shuffle.manager` to
  `org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager` and enable
  `spark.comet.exec.shuffle.enabled=true` at session startup. With a vanilla Spark `Exchange`
  in the plan the data leaves the shuffle as rows and the optimization cannot fire.
- Spark 3.4 lacks several APIs the optimization depends on (`MapInBatchExec.isBarrier`,
  `arrowUseLargeVarTypes`, `JobArtifactSet`, the modern `ArrowPythonRunner` constructor). On
  Spark 3.4 the feature is a no-op even when enabled. Spark 3.5+ is required.
