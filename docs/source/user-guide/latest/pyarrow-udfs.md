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

# Python UDF Acceleration

Comet can accelerate Python UDFs that exchange Arrow batches with the Python worker: the batch APIs
`mapInArrow` and `mapInPandas`, and scalar UDFs (an Arrow-optimized `udf()`, a `@pandas_udf`, or a
`@arrow_udf`). These APIs are commonly used for ML inference, feature engineering, and data
transformation workloads.

## Background

These APIs let users apply Python functions that operate on Arrow RecordBatches, Pandas
DataFrames, or Pandas Series. Under the hood, Spark communicates with the Python worker process
using the Arrow IPC format.

Without Comet, the execution path for these UDFs involves unnecessary data conversions:

1. Comet reads data in Arrow columnar format (via CometScan)
2. Spark inserts a ColumnarToRow transition (converts Arrow to UnsafeRow)
3. The Python runner converts those rows back to Arrow to send to Python
4. Python executes the UDF on Arrow batches
5. Results are returned as Arrow and then converted back to rows

Steps 2 and 3 are redundant since the data starts and ends in Arrow format.

## How Comet Optimizes This

### Batch APIs: `mapInArrow` and `mapInPandas`

When enabled, Comet detects `PythonMapInArrowExec` / `MapInArrowExec` and `MapInPandasExec`
operators in the physical plan and replaces them with `CometMapInBatchExec`, which:

- Reads Arrow columnar batches directly from the upstream Comet operator
- Feeds them to the Python runner without the expensive UnsafeProjection copy
- Keeps the Python output in columnar format for downstream operators

This eliminates the ColumnarToRow transition and the output row conversion, reducing CPU overhead
and memory allocations. The row-to-Arrow re-encoding that Spark's `ArrowPythonRunner` performed on
the input side is also gone: `CometArrowPythonRunner` consumes `ColumnarBatch` directly, so batches
are written straight from Comet's vectors into the IPC root. See [Limitations](#limitations) for the
copies that remain.

### Scalar UDFs

Scalar Python UDFs are planned as `ArrowEvalPythonExec`, which Comet replaces with
`CometArrowEvalPythonExec`. Spark's operator is considerably more row-bound than the batch APIs:
for every input row it copies the row into a `HybridRowQueue` (which spills to disk under memory
pressure), runs a `MutableProjection` to materialize the UDF arguments, and on the way back joins
the queued row with the worker's output through a `JoinedRow` and an `UnsafeProjection`.

Comet's operator instead sends the UDF argument columns straight from the input batch to the
worker, and appends the columns the worker returns to the input batch's own columns. No row is
materialized, and the row queue and its spill path are gone.

One operator covers three user-facing UDF families, because Spark routes all of them to
`ArrowEvalPythonExec`:

- a plain `udf()` when `spark.sql.execution.pythonUDF.arrow.enabled=true`
- `@pandas_udf` (scalar)
- `@arrow_udf` (Spark 4.1+)

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

For a scalar UDF, without the optimization:

```
ArrowEvalPython          <- row queue, projections, JoinedRow
+- ColumnarToRow         <- Arrow -> Row copy
   +- CometNativeExec    <- Arrow batch
      +- CometScan
```

and with it:

```
CometArrowEvalPython     <- argument columns out, result columns appended
+- CometNativeExec
   +- CometScan
```

## Configuration

The optimization is experimental and disabled by default. Enable it with:

```
spark.comet.exec.pyarrowUDF.enabled=true
```

The default is `false` while the feature stabilizes.

### Relationship to Spark's PySpark Arrow conversion conf

`spark.comet.exec.pyarrowUDF.enabled` is **not** the same as PySpark's
[`spark.sql.execution.arrow.pyspark.enabled`](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html#enabling-for-conversion-to-from-pandas).
That conf controls whether Spark uses Arrow when materializing a DataFrame to a Pandas DataFrame
(`toPandas()`) or constructing one from Pandas. The Comet conf controls a planner rewrite for
`mapInArrow` / `mapInPandas`, and only affects how Comet's columnar batches feed the Python
worker. Both confs can be set independently.

### Enabling Arrow-optimized `udf()`

A plain `udf()` is a pickled, row-at-a-time UDF (`BatchEvalPythonExec`) unless
[`spark.sql.execution.pythonUDF.arrow.enabled`](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
is set, which is off by default in Spark. Only with it enabled does a plain `udf()` become an
`ArrowEvalPythonExec` that Comet can accelerate.

Comet does not set that conf on your behalf. It changes Spark's own type coercion and error
semantics, not just the transport, so turning it on is a decision about UDF behaviour rather than
about performance.

## Supported APIs

| PySpark API                                    | Spark Plan Node             | Supported |
| ---------------------------------------------- | --------------------------- | --------- |
| `df.mapInArrow(func, schema)`                  | `PythonMapInArrowExec`      | Yes       |
| `df.mapInPandas(func, schema)`                 | `MapInPandasExec`           | Yes       |
| `udf()` with `pythonUDF.arrow.enabled`         | `ArrowEvalPythonExec`       | Yes       |
| `@pandas_udf` (scalar)                         | `ArrowEvalPythonExec`       | Yes       |
| `@arrow_udf` (scalar, Spark 4.1+)              | `ArrowEvalPythonExec`       | Yes       |
| `@pandas_udf` / `@arrow_udf` (scalar iterator) | `ArrowEvalPythonExec`       | Not yet   |
| `udf()` without `pythonUDF.arrow.enabled`      | `BatchEvalPythonExec`       | No        |
| `df.applyInPandas(func, schema)`               | `FlatMapGroupsInPandasExec` | Not yet   |

## Example

```python
import pyarrow as pa
from pyspark.sql import SparkSession, types as T

spark = SparkSession.builder \
    .config("spark.plugins", "org.apache.spark.CometPlugin") \
    .config("spark.comet.enabled", "true") \
    .config("spark.comet.exec.enabled", "true") \
    .config("spark.comet.exec.pyarrowUDF.enabled", "true") \
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

And for a scalar UDF:

```python
import pandas as pd
from pyspark.sql import functions as F, types as T

# Required for a plain `udf()` to be planned as ArrowEvalPythonExec.
spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")

df = spark.read.parquet("data.parquet")

@F.pandas_udf(T.DoubleType())
def scale(values: pd.Series) -> pd.Series:
    return values * 2

result = df.withColumn("scaled", scale("value"))
```

## Verifying the Optimization

Use `explain()` to verify that `CometMapInBatch` (or `CometArrowEvalPython`, for a scalar UDF)
appears in your plan:

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

- Grouped operations (`applyInPandas`, `applyInArrow`, grouped-aggregate and window pandas UDFs,
  cogroup, Arrow UDTFs) are not yet supported.
- Scalar _iterator_ UDFs (`@pandas_udf` / `@arrow_udf` over an `Iterator`) are not accelerated.
  They guarantee only that the worker returns the same total number of rows, not the same
  batching, and Comet pairs each output batch with the input batch that produced it.
- A scalar UDF is accelerated only when every one of its arguments is a plain column of the
  operator's child. `udf(col("a"))` qualifies; `udf(col("a") + 1)` does not, because Spark does
  not project the expression below the operator. Chained UDFs (`f(g(x))`, which Spark folds into
  one operator) and keyword arguments are excluded for the same reason. These shapes fall back to
  vanilla Spark rather than failing.
- Pickled row-at-a-time UDFs (`BatchEvalPythonExec`, what a plain `udf()` produces by default)
  are out of scope. There is no columnar boundary to preserve: per-row boxing, pickling, and
  interpreter cost dominate. See
  [Enabling Arrow-optimized `udf()`](#enabling-arrow-optimized-udf).
- The optimization requires Arrow data on the input side. If a shuffle sits between the upstream
  Comet operator and the Python UDF, you need Comet's native shuffle for the optimization to
  apply. Set `spark.shuffle.manager` to
  `org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager` and enable
  `spark.comet.shuffle.enabled=true` at session startup. With a vanilla Spark `Exchange`
  in the plan the data leaves the shuffle as rows and the optimization cannot fire.
- Spark 4.0 or newer is required. On Spark 3.4 and 3.5 the optimization is a no-op even when
  enabled; vanilla `PythonMapInArrowExec` / `MapInPandasExec` handle the operation. The Spark 3.5
  `PythonArrowInput` trait has a different contract than 4.x and a separate implementation has
  not been written. Track 3.5 support as a future follow-on if there is user demand.
- Timestamps are presented to the UDF with a `UTC` time zone rather than the session time zone.
  Comet normalizes timestamps to UTC internally, and the accelerated path builds the Arrow schema
  it sends to Python from Comet's own vectors, so a `TimestampType` column reaches the worker
  labelled `Timestamp(MICROSECOND, "UTC")`. Vanilla Spark instead labels it with
  `spark.sql.session.timeZone`. The stored value is the same absolute instant either way, so a
  passthrough or value-based UDF round-trips identically. The difference is only observable to a
  UDF that reads the Arrow field's time zone or localizes to wall-clock time (for example a
  `mapInPandas` UDF that strips the tz and treats the value as naive local time): under a non-UTC
  session time zone such a UDF can diverge from the unoptimized path. Set
  `spark.comet.exec.pyarrowUDF.enabled=false` for those UDFs.
- `spark.sql.execution.arrow.useLargeVarTypes=true` is not supported. With this conf enabled,
  Spark supplies `large_string` and `large_binary` input columns with 8-byte offsets. Native
  Comet vectors use 4-byte offsets, and direct serialization advertises their matching `string`
  and `binary` types. This produces a valid IPC stream, but does not preserve the input types
  requested by the configuration. `EliminateRedundantTransitions` therefore skips the rewrite
  and vanilla Spark handles the operation. Comet can read `large_string` and `large_binary`
  columns returned by a Python worker; that output support does not widen the input vectors.
- Comet writes input Arrow IPC record batches directly from its existing vector buffers. For
  `mapInArrow` / `mapInPandas` the only additional Arrow buffer is the validity bitmap for the
  non-null struct that wraps the input columns. Writing the IPC bytes to the Python worker's pipe
  still requires one copy; that copy is inherent to Spark's process-based Python transport.
- The scalar UDF path additionally copies each input batch once. Its output batch is the input
  batch's columns plus the worker's result columns, so the input has to stay valid until the
  worker replies, while Comet's native operators recycle the buffers behind consecutive batches.
  That bulk `memcpy` per batch replaces Spark's per-row `UnsafeRow` copy into a spillable row
  queue, so it remains well ahead of the unoptimized path.
