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

# Comet Debugging Guide

This HOWTO describes how to debug JVM code and Native code concurrently. The guide assumes you have:

1. IntelliJ as the Java IDE
2. CLion as the Native IDE. For Rust code, the CLion Rust language plugin is required. Note that the
   IntelliJ Rust plugin is not sufficient.
3. CLion/LLDB as the native debugger. CLion ships with a bundled LLDB and the Rust community has
   its own packaging of LLDB (`lldb-rust`). Both provide a better display of Rust symbols than plain
   LLDB or the LLDB that is bundled with XCode. We will use the LLDB packaged with CLion for this guide.
4. We will use a Comet _unit_ test as the canonical use case.

_Caveat: The steps here have only been tested with JDK 11_ on Mac (M1)

## Debugging for Advanced Developers

Add a `.lldbinit` to comet/native. This is not strictly necessary but will be useful if you want to
use advanced `lldb` debugging. For example, we can ignore some exceptions and signals that are not relevant
to our debugging and would otherwise cause the debugger to stop.

```
settings set platform.plugin.darwin.ignored-exceptions EXC_BAD_ACCESS|EXC_BAD_INSTRUCTION
process handle -n true -p true -s false SIGBUS SIGSEGV SIGILL
```

### In IntelliJ

1. Set a breakpoint in `NativeBase.load()`, at a point _after_ the Comet library has been loaded.

1. Add a Debug Configuration for the unit test

1. In the Debug Configuration for that unit test add `-Xint` as a JVM parameter. This option is
   undocumented _magic_. Without this, the LLDB debugger hits a EXC_BAD_ACCESS (or EXC_BAD_INSTRUCTION) from
   which one cannot recover.

1. Add a println to the unit test to print the PID of the JVM process. (jps can also be used but this is less error prone if you have multiple jvm processes running)

   ```scala
        println("Waiting for Debugger: PID - ", ManagementFactory.getRuntimeMXBean().getName())
   ```

   This will print something like : `PID@your_machine_name`.

   For JDK9 and newer

   ```scala
        println("Waiting for Debugger: PID - ", ProcessHandle.current.pid)
   ```

   ==> Note the PID

1. Debug-run the test in IntelliJ and wait for the breakpoint to be hit

### In CLion

1. After the breakpoint is hit in IntelliJ, in Clion (or LLDB from terminal or editor) -
   1. Attach to the jvm process (make sure the PID matches). In CLion, this is `Run -> Attach to process`

   1. Put your breakpoint in the native code

1. Go back to IntelliJ and resume the process.

1. Most debugging in CLion is similar to IntelliJ. For advanced LLDB based debugging the LLDB command line can be accessed from the LLDB tab in the Debugger view. Refer to the [LLDB manual](https://lldb.llvm.org/use/tutorial.html) for LLDB commands.

### After your debugging is done

1. In CLion, detach from the process if not already detached

2. In IntelliJ, the debugger might have lost track of the process. If so, the debugger tab
   will show the process as running (even if the test/job is shown as completed).

3. Close the debugger tab, and if the IDS asks whether it should terminate the process,
   click Yes.

4. In terminal, use jps to identify the process with the process id you were debugging. If
   it shows up as running, kill -9 [pid]. If that doesn't remove the process, don't bother,
   the process will be left behind as a zombie and will consume no (significant) resources.
   Eventually it will be cleaned up when you reboot possibly after a software update.

### Additional Info

OpenJDK mailing list on debugging the JDK on MacOS
<https://mail.openjdk.org/pipermail/hotspot-dev/2019-September/039429.html>

Detecting the debugger
<https://stackoverflow.com/questions/5393403/can-a-java-application-detect-that-a-debugger-is-attached#:~:text=No.,to%20let%20your%20app%20continue.&text=I%20know%20that%20those%20are,meant%20with%20my%20first%20phrase>).

## Verbose debug

### Enabling Native Backtraces

By default, Comet does not show native backtraces when exceptions happen in native code:

```scala
scala> spark.sql("my_failing_query").show(false)

24/03/05 17:00:07 ERROR Executor: Exception in task 0.0 in stage 0.0 (TID 0)/ 1]
org.apache.comet.CometNativeException: Internal error: MIN/MAX is not expected to receive scalars of incompatible types (Date32("NULL"), Int32(15901)).
This was likely caused by a bug in DataFusion's code and we would welcome that you file an bug report in our issue tracker
        at org.apache.comet.Native.executePlan(Native Method)
        at org.apache.comet.CometExecIterator.executeNative(CometExecIterator.scala:65)
        at org.apache.comet.CometExecIterator.getNextBatch(CometExecIterator.scala:111)
        at org.apache.comet.CometExecIterator.hasNext(CometExecIterator.scala:126)

```

Comet can be built with DataFusion's [backtrace] feature enabled, which will include native back traces in `CometNativeException`.

[backtrace]: https://arrow.apache.org/datafusion/user-guide/example-usage.html#enable-backtraces

To build Comet with this feature enabled:

```shell
make release COMET_FEATURES=backtrace
```

Set `RUST_BACKTRACE=1` for the Spark worker/executor process, or for `spark-submit` if running in local mode.

```console
RUST_BACKTRACE=1 $SPARK_HOME/spark-shell --jars spark/target/comet-spark-spark4.1_2.13-$COMET_VERSION.jar --conf spark.plugins=org.apache.spark.CometPlugin --conf spark.comet.enabled=true --conf spark.comet.exec.enabled=true
```

Get the expanded exception details

```scala
scala> spark.sql("my_failing_query").show(false)
24/03/05 17:00:49 ERROR Executor: Exception in task 0.0 in stage 0.0 (TID 0)
org.apache.comet.CometNativeException: Internal error: MIN/MAX is not expected to receive scalars of incompatible types (Date32("NULL"), Int32(15901))

backtrace:
  0: std::backtrace::Backtrace::create
  1: datafusion::physical_expr::aggregate::min_max::min
  2: <datafusion::physical_expr::aggregate::min_max::MinAccumulator as datafusion_expr::accumulator::Accumulator>::update_batch
  3: <futures_util::stream::stream::fuse::Fuse<S> as futures_core::stream::Stream>::poll_next
  4: comet::execution::jni_api::Java_org_apache_comet_Native_executePlan::{{closure}}
  5: _Java_org_apache_comet_Native_executePlan
  (reduced)

This was likely caused by a bug in DataFusion's code and we would welcome that you file an bug report in our issue tracker
        at org.apache.comet.Native.executePlan(Native Method)
at org.apache.comet.CometExecIterator.executeNative(CometExecIterator.scala:65)
at org.apache.comet.CometExecIterator.getNextBatch(CometExecIterator.scala:111)
at org.apache.comet.CometExecIterator.hasNext(CometExecIterator.scala:126)
(reduced)

```

Note:

- The backtrace coverage in DataFusion is still improving. So there is a chance the error still not covered, if so feel free to file a [ticket](https://github.com/apache/arrow-datafusion/issues)
- The backtrace evaluation comes with performance cost and intended mostly for debugging purposes

### Native log configuration

By default, Comet emits native-side logs at the `INFO` level to `stderr`.

You can use the `COMET_LOG_LEVEL` environment variable to specify the log level. Supported values are: `OFF`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`.

For example, to configure native logs at the `DEBUG` level on spark executor:

```
spark.executorEnv.COMET_LOG_LEVEL=DEBUG
```

This produces output like the following:

```
25/09/15 20:17:42 INFO core/src/lib.rs: Comet native library version 0.11.0 initialized
25/09/15 20:17:44 DEBUG /xxx/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-execution-49.0.2/src/disk_manager.rs: Created local dirs [TempDir { path: "/private/var/folders/4p/9gtjq1s10fd6frkv9kzy0y740000gn/T/blockmgr-ba524f95-a792-4d79-b49c-276ba324941e/datafusion-qrpApx" }] as DataFusion working directory
25/09/15 20:17:44 DEBUG /xxx/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-functions-nested-49.0.2/src/lib.rs: Overwrite existing UDF: array_to_string
25/09/15 20:17:44 DEBUG /xxx/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-functions-nested-49.0.2/src/lib.rs: Overwrite existing UDF: string_to_array
25/09/15 20:17:44 DEBUG /xxx/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-functions-nested-49.0.2/src/lib.rs: Overwrite existing UDF: range
...
```

Additionally, you can place a `log4rs.yaml` configuration file inside the Comet configuration directory specified by the `COMET_CONF_DIR` environment variable to enable more advanced logging configurations. This file uses the [log4rs YAML configuration format](https://docs.rs/log4rs/latest/log4rs/#configuration-via-a-yaml-file).
For example, see: [log4rs.yaml](https://github.com/apache/datafusion-comet/blob/main/conf/log4rs.yaml).

### Debugging Memory Reservations

Set `spark.comet.debug.memory=true` to log all calls that grow or shrink memory reservations.

Example log output:

```
[Task 486] MemoryPool[ExternalSorter[6]].try_grow(256232960) returning Ok
[Task 486] MemoryPool[ExternalSorter[6]].try_grow(256375168) returning Ok
[Task 486] MemoryPool[ExternalSorter[6]].try_grow(256899456) returning Ok
[Task 486] MemoryPool[ExternalSorter[6]].try_grow(257296128) returning Ok
[Task 486] MemoryPool[ExternalSorter[6]].try_grow(257820416) returning Err
[Task 486] MemoryPool[ExternalSorterMerge[6]].shrink(10485760)
[Task 486] MemoryPool[ExternalSorter[6]].shrink(150464)
[Task 486] MemoryPool[ExternalSorter[6]].shrink(146688)
[Task 486] MemoryPool[ExternalSorter[6]].shrink(137856)
[Task 486] MemoryPool[ExternalSorter[6]].shrink(141952)
[Task 486] MemoryPool[ExternalSorterMerge[6]].try_grow(0) returning Ok
[Task 486] MemoryPool[ExternalSorterMerge[6]].try_grow(0) returning Ok
[Task 486] MemoryPool[ExternalSorter[6]].shrink(524288)
[Task 486] MemoryPool[ExternalSorterMerge[6]].try_grow(0) returning Ok
[Task 486] MemoryPool[ExternalSorterMerge[6]].try_grow(68928) returning Ok
```

When backtraces are enabled (see earlier section) then backtraces will be included for failed allocations.

### Dumping native stream output with a `DbgExec` wrapper

When a native operator is suspected of producing wrong data (wrong values, wrong
nullability, wrong row count) but the JVM-side observable output is just a DataFrame
mismatch, it is useful to inspect the `RecordBatch`es that the operator actually
emits. A small `ExecutionPlan` wrapper that prints every batch as it flows through
makes this easy. Comet does not ship this wrapper in the source tree — paste it
into a convenient module (for example `native/core/src/parquet/parquet_exec.rs`)
for the duration of your debugging session and remove it before committing.

#### Reference implementation

```rust
use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

/// Wraps a `SendableRecordBatchStream` to print each batch as it flows through.
/// Returns a new `SendableRecordBatchStream` that yields the same batches.
pub fn dbg_batch_stream(stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
    use futures::StreamExt;
    let schema = stream.schema();
    let printing_stream = stream.map(|batch_result| {
        match &batch_result {
            Ok(batch) => {
                dbg!(batch, batch.schema());
                for (col_idx, column) in batch.columns().iter().enumerate() {
                    dbg!(col_idx, column, column.nulls());
                }
            }
            Err(e) => {
                println!("batch error: {:?}", e);
            }
        }
        batch_result
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, printing_stream))
}

/// Execution plan wrapper that prints each batch produced by `inner` at
/// runtime. Wrap an operator (e.g. `WindowAggExec`) with this to see what
/// its output stream actually contains during JVM test execution.
#[derive(Debug)]
pub struct DbgExec {
    label: String,
    inner: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
}

impl DbgExec {
    pub fn new(
        label: impl Into<String>,
        inner: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Self {
        Self {
            label: label.into(),
            inner,
        }
    }
}

impl datafusion::physical_plan::DisplayAs for DbgExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "DbgExec[{}]", self.label)
    }
}

impl datafusion::physical_plan::ExecutionPlan for DbgExec {
    fn name(&self) -> &str {
        "DbgExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Ok(Arc::new(DbgExec::new(
            self.label.clone(),
            Arc::clone(&children[0]),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        eprintln!(
            "[comet-debug] DbgExec[{}] execute(partition={})",
            self.label, partition
        );
        let stream = self.inner.execute(partition, context)?;
        Ok(dbg_batch_stream(stream))
    }
}
```

`DbgExec` forwards `schema`, `properties`, `children`, and `with_new_children` to the
inner plan, so slotting it in does not change operator semantics — it only adds
printing. Because it is itself an `ExecutionPlan` it can be inserted anywhere in
the physical plan tree built by `PhysicalPlanner::create_plan` in
`native/core/src/execution/planner.rs`.

#### Using `DbgExec` to inspect an operator's output

Import the wrapper in `planner.rs` (assuming you pasted it into
`parquet_exec.rs`):

```rust
use crate::parquet::parquet_exec::DbgExec;
```

Then wrap whichever `Arc<dyn ExecutionPlan>` you want to see the batches of. For
example, to dump the output of the window operator:

```rust
let window_agg: Arc<dyn ExecutionPlan> = Arc::new(WindowAggExec::try_new(
    window_expr?,
    Arc::clone(&child.native_plan),
    !partition_exprs.is_empty(),
)?);

// TEMPORARY: print every batch emitted by the window operator.
let window_agg: Arc<dyn ExecutionPlan> =
    Arc::new(DbgExec::new("window", window_agg));
```

Rebuild with `make core` and run the JVM test that exercises the operator. Each
emitted batch is printed to stderr with `dbg!`. The output looks like this for a
`LEAD(c) IGNORE NULLS OVER (PARTITION BY a ORDER BY b)` query over five rows:

```text
[comet-debug] DbgExec[window] execute(partition=0)
[core/src/parquet/parquet_exec.rs:225:17] batch = RecordBatch {
    schema: Schema { fields: [ ... "col_0" Int32, "col_1" Int32, "col_2" Int32, "lead" Int32 ] },
    columns: [
        PrimitiveArray<Int32> [1, 1, 1, 2, 2],                // partition key a
        PrimitiveArray<Int32> [1, 2, 3, 1, 2],                // order key  b
        PrimitiveArray<Int32> [10, null, 30, null, 20],       // value      c
        PrimitiveArray<Int32> [null, null, null, null, null], // lead(c)
    ],
    row_count: 5,
}
[core/src/parquet/parquet_exec.rs:227:21] col_idx = 3
[core/src/parquet/parquet_exec.rs:227:21] column.nulls() = Some(
    NullBuffer { ..., null_count: 5 },
)
```

From the dump you can see immediately that the native `lead` column is all-null
(`null_count: 5` out of 5 rows) while the inputs are correctly sorted — pinpointing
the bug to the native operator rather than to the scan, the plan shape, or the
JVM-side comparison.

#### Where to place `DbgExec`

Good insertion points when debugging:

- **Right after the operator under test**, to see exactly what that operator emits.
- **Right before the operator under test**, to confirm what its input looked like
  (rules out upstream issues).
- **Both**, with different labels (`DbgExec::new("window-in", ...)` /
  `DbgExec::new("window-out", ...)`), to diff input vs output.

Because `DbgExec` is a one-line wrap, it is fine to scatter several throughout the
plan during a debugging session and remove them afterwards. The `label` is part of
every line it emits, so multiple wrappers stay easy to tell apart.

`DbgExec` is a debugging aid — remove the wrapper definition, any wraps, and the
`use crate::parquet::parquet_exec::DbgExec;` import before committing.

### Dumping expression inputs and outputs with a `DbgExpr` wrapper

`DbgExec` works at the operator level. When the suspect is a single
**expression** — a cast, a binary op, a `CASE WHEN` predicate, a UDF — an
`ExecutionPlan` wrapper is too coarse. The equivalent trick at the
`PhysicalExpr` level is a small wrapper that forwards `evaluate()` to an inner
expression and prints both the input `RecordBatch` and the resulting
`ColumnarValue`. Like `DbgExec`, this wrapper is not shipped in the source tree
— paste it in for a debugging session and remove it before committing.

#### Reference implementation

```rust
use std::any::Any;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

/// `PhysicalExpr` wrapper that prints every `evaluate()` call:
/// - the input `RecordBatch` (rows / columns / schema),
/// - the resulting `ColumnarValue` (array + null buffer, or scalar).
///
/// Wrap any `Arc<dyn PhysicalExpr>` produced by `PhysicalPlanner::create_expr`
/// in `planner.rs` to see exactly what it receives and returns at runtime.
#[derive(Debug)]
pub struct DbgExpr {
    label: String,
    inner: Arc<dyn PhysicalExpr>,
}

impl DbgExpr {
    pub fn new(label: impl Into<String>, inner: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            label: label.into(),
            inner,
        }
    }
}

impl fmt::Display for DbgExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DbgExpr[{}]({})", self.label, self.inner)
    }
}

impl PartialEq for DbgExpr {
    fn eq(&self, other: &Self) -> bool {
        self.label == other.label && self.inner.eq(&other.inner)
    }
}
impl Eq for DbgExpr {}
impl Hash for DbgExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label.hash(state);
        self.inner.hash(state);
    }
}

impl PhysicalExpr for DbgExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        eprintln!(
            "[comet-debug] DbgExpr[{}].evaluate(rows={}, cols={})",
            self.label,
            batch.num_rows(),
            batch.num_columns()
        );
        dbg!(batch, batch.schema());
        let out = self.inner.evaluate(batch)?;
        match &out {
            ColumnarValue::Array(arr) => {
                dbg!(arr.len(), arr.nulls(), arr);
            }
            ColumnarValue::Scalar(s) => {
                dbg!(s);
            }
        }
        Ok(out)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(DbgExpr::new(
            self.label.clone(),
            Arc::clone(&children[0]),
        )))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_sql(f)
    }
}
```

`DbgExpr` forwards `data_type`, `nullable`, `children`, and `with_new_children`
to the inner expression, so wrapping it does not change semantics — it only adds
printing on every `evaluate()` call.

#### Using `DbgExpr` in `planner.rs`

`PhysicalPlanner::create_expr` returns `Arc<dyn PhysicalExpr>`, so any expression
produced during plan building can be one-line-wrapped. For example, to dump what
a `CASE WHEN` predicate sees and produces:

```rust
use crate::parquet::parquet_exec::DbgExpr; // or wherever you pasted it

// Before:
let predicate = self.create_expr(when_expr, Arc::clone(&input_schema))?;

// After — TEMPORARY, remove before committing:
let predicate: Arc<dyn PhysicalExpr> =
    Arc::new(DbgExpr::new("case-when-predicate", predicate));
```

To trace every argument of a suspicious scalar function, wrap each child as it
is built:

```rust
let args: Vec<Arc<dyn PhysicalExpr>> = expr
    .children
    .iter()
    .enumerate()
    .map(|(i, c)| {
        let child = self.create_expr(c, Arc::clone(&input_schema))?;
        Ok::<_, ExecutionError>(Arc::new(DbgExpr::new(
            format!("arg{i}"),
            child,
        )) as Arc<dyn PhysicalExpr>)
    })
    .collect::<Result<_, _>>()?;
```

Rebuild with `make core` and run the JVM test. Sample output for a `BinaryExpr`
computing `a + b` over a three-row batch:

```text
[comet-debug] DbgExpr[add].evaluate(rows=3, cols=2)
[core/src/execution/planner.rs:…] batch = RecordBatch { columns: [Int32[1,2,3], Int32[10,20,30]], row_count: 3 }
[core/src/execution/planner.rs:…] arr.len() = 3
[core/src/execution/planner.rs:…] arr.nulls() = None
[core/src/execution/planner.rs:…] arr = PrimitiveArray<Int32> [11, 22, 33]
```

#### When to reach for `DbgExpr` vs `DbgExec`

- Use **`DbgExec`** when you suspect an *operator* (scan, window, sort, aggregate)
  is emitting wrong batches — you want to see what crosses operator boundaries.
- Use **`DbgExpr`** when the operator looks fine but a specific *expression*
  inside a projection, filter, or window function is returning wrong values or
  nullability — you want to see what one expression receives and computes.
- They compose: wrap the suspect expression with `DbgExpr`, and wrap the
  operator that evaluates it with `DbgExec`, to correlate per-expression
  behavior with the batches the operator is producing overall.

`DbgExpr` is a debugging aid — remove the wrapper definition, any wraps, and the
`use crate::parquet::parquet_exec::DbgExpr;` import before committing.
