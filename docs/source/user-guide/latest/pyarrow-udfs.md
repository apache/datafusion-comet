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
and memory allocations. The row-to-Arrow re-encoding that Spark's `ArrowPythonRunner` performed on
the input side is also gone: `CometArrowPythonRunner` consumes `ColumnarBatch` directly, so batches
are written straight from Comet's vectors into the IPC root. See [Limitations](#limitations) for the
copies that remain.

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
spark.comet.exec.pyarrowUDF.enabled=true
```

The default is `false` while the feature stabilizes.

### Native scalar Arrow UDFs (Spark 4.1+)

Scalar `@arrow_udf` in Spark 4.1 and later can run inside Comet's Rust execution pipeline when
the native library is built with the `python-udf` Cargo feature and this separate option is enabled:

```
spark.comet.exec.nativeArrowPythonUDF.enabled=true
```

Build the native library with a Python interpreter that matches the major and minor version used
by the executors' PySpark workers. That interpreter needs its development headers (for example,
`python3-dev` on Debian and Ubuntu) and a shared `libpython` (`libpython3.x.so` on Linux). A Python
built with pyenv may need `PYTHON_CONFIGURE_OPTS="--enable-shared"` when it is installed. For example:

```sh
PYO3_PYTHON=/path/to/python make release COMET_FEATURES=python-udf
```

Install the matching shared `libpython` on every executor and make it discoverable by the dynamic
linker. On Linux, the JVM loads `libcomet` with local symbols, so Comet promotes the already-loaded
shared `libpython` to the global namespace before importing Python extensions such as PyArrow. A
statically linked Python cannot provide those symbols this way, and importing PyArrow can fail with
an undefined-symbol error. A library built with `python-udf` depends on `libpython` as soon as
`libcomet` is loaded: if an executor cannot find it, **Comet itself fails to load**, even when a
query does not use an Arrow UDF.

Comet passes each argument as a `pyarrow.Array` through the Arrow C Data Interface, invokes the
pickled Python function with PyO3, and appends the result array to the input batch. It checks the
result length and safely casts it to the declared return type, matching Spark's scalar Arrow UDF
serializer. A native worker is created per partition.

Each partition unpickles its own callable. Imported modules and their global state are shared by
concurrent tasks in the executor's embedded Python interpreter.

The executor's embedded Python must be able to import `pyspark`, `pyarrow`, and the user's Python
modules. Spark serializes the callable and a PySpark return type with `pyspark.cloudpickle`, so
`pyspark` is required even when the callable itself only uses PyArrow. Build the `python-udf`
feature against the same Python major/minor version used by PySpark workers. Install those packages
into that Python environment and ensure the executor process can find them through the embedded
interpreter's `sys.path` (for example, by setting `PYTHONPATH` before launching the executor).
`PYSPARK_PYTHON` selects the external worker executable; it does not select or configure the
embedded interpreter. The worker-only `pyspark.zip` path is not automatically added to it. The
feature and config are disabled by default. Without either, `ArrowEvalPythonExec` stays on Spark's
normal path.

The initial native path accepts scalar `@arrow_udf` calls with regular or named arguments and
multiple independent UDFs in one `ArrowEvalPythonExec`. Chained Python UDFs, broadcast variables,
Python includes, per-function environment overrides, and
`spark.sql.execution.arrow.useLargeVarTypes=true` stay on Spark's path. Iterator Arrow UDFs,
ordinary `udf(..., useArrow=True)`, scalar pandas UDFs, and `mapInArrow` are separate execution
types; `mapInArrow` retains the columnar runner described above.

The native path accepts boolean, byte, short, integer, long, float, double, plain string, binary,
decimal, date, and timestamp without time zone. Other input or result types and an
enabled `spark.sql.pyspark.udf.profiler` stay on Spark's path. Spark labels `TimestampType` with the
session time zone, while Comet uses UTC; nested Arrow field names can also differ. This allow-list
keeps types with unverified Arrow schemas on Spark's path. `TimeType` also stays on Spark's path:
Spark 4.2's Arrow UDF row converter rejects it even though PySpark can describe its Arrow type.

The embedded interpreter is shared by tasks. Pure Python code contends on its global interpreter
lock, so multiple partitions may be slower than Spark's separate Python workers; PyArrow kernels
that release the lock can still run concurrently. Python execution stays synchronous on JVM input
paths and hands off other async tasks when it runs on a Tokio worker. `pyspark.TaskContext.get()`
returns `None` inside a native UDF. A native extension crash or `os._exit` terminates the executor
process. PyArrow allocations made in Python are outside Comet's memory pool and are not limited by
`spark.executor.pyspark.memory`.

### Relationship to Spark's PySpark Arrow conversion conf

`spark.comet.exec.pyarrowUDF.enabled` is **not** the same as PySpark's
[`spark.sql.execution.arrow.pyspark.enabled`](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html#enabling-for-conversion-to-from-pandas).
That conf controls whether Spark uses Arrow when materializing a DataFrame to a Pandas DataFrame
(`toPandas()`) or constructing one from Pandas. The Comet conf controls a planner rewrite for
`mapInArrow` / `mapInPandas`, and only affects how Comet's columnar batches feed the Python
worker. Both confs can be set independently.

## Supported APIs

| PySpark API                      | Spark Plan Node             | Supported                |
| -------------------------------- | --------------------------- | ------------------------ |
| `df.mapInArrow(func, schema)`    | `PythonMapInArrowExec`      | Yes                      |
| `df.mapInPandas(func, schema)`   | `MapInPandasExec`           | Yes                      |
| scalar `@arrow_udf` (Spark 4.1+) | `ArrowEvalPythonExec`       | Experimental native path |
| `udf(..., useArrow=True)`        | `ArrowEvalPythonExec`       | Not yet                  |
| `@pandas_udf` (scalar)           | `ArrowEvalPythonExec`       | Not yet                  |
| `df.applyInPandas(func, schema)` | `FlatMapGroupsInPandasExec` | Not yet                  |

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
    .config("spark.executor.memoryOverhead", "2g") \
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

- The columnar Python runner applies to `mapInArrow` and `mapInPandas`. The separate native path
  applies to scalar `@arrow_udf` on Spark 4.1+. Scalar pandas UDFs (`@pandas_udf`) and grouped
  operations (`applyInPandas`) are not yet supported.
- The optimization requires Arrow data on the input side. If a shuffle sits between the upstream
  Comet operator and the Python UDF, use Comet's columnar shuffle for the optimization to apply.
  Both the `jvm` and `native` shuffle modes can feed `CometMapInBatch`. Set
  `spark.shuffle.manager` to
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
- Comet applies `spark.sql.execution.arrow.maxRecordsPerBatch` to every input batch, including
  batches with only plain columns. Before decoding dictionary-encoded shuffle columns, Comet also
  compares their estimated decoded size with `spark.sql.execution.arrow.maxBytesPerBatch`.
  When either threshold requires splitting, every column is sliced at the same row boundaries.
  Temporary slices and decoded dictionary vectors are released after each synchronous write.
  Comet returns control to Spark after each slice so Spark can drain its Python transport buffer;
  small slices may share that buffer until Spark reaches its buffering threshold. The source
  batch remains alive until its last slice has been written.
- The byte estimate covers only the logical buffers of decoded dictionary columns: values,
  offsets, and validity bits. It excludes plain columns and is a soft limit: the row that crosses
  the threshold stays in the batch, and a single oversized row remains intact. A separate guard
  prevents combining rows whose estimated decoded dictionary size exceeds Arrow's signed 32-bit
  limit (2 GiB minus 1 byte). This guard cannot split an individually oversized row and does not
  guarantee that Arrow allocations stay below that limit. Arrow rounds buffer capacities up, so
  an allocation can approach twice its logical size; existing input buffers and other overhead
  also consume memory. `maxBytesPerBatch` is therefore not a ceiling on actual memory use.
- Dictionary-encoded values nested inside a struct, list, or map are not supported on the
  optimized input path. Comet rejects them with an error naming the field path. Comet's current
  shuffle does not produce these nested dictionaries.
- Comet writes input Arrow IPC record batches directly from plain vector buffers. For an unsplit
  plain batch, the only additional Arrow buffer is the validity bitmap for the non-null struct
  that wraps the input columns. Slicing may allocate offset or validity buffers. Writing the IPC
  bytes to the Python worker's pipe still requires one copy; that copy is inherent to Spark's
  process-based Python transport. Borrowed buffers are not transferred between Arrow allocators or
  given new ownership.
