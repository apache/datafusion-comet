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

Add a `.lldbinit` to comet/core. This is not strictly necessary but will be useful if you want to
use advanced `lldb` debugging.

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

   1. Attach to the jvm process (make sure the PID matches). In CLion, this is `Run -> Atttach to process`

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

### Exception details

By default, Comet outputs the exception details specific for Comet.

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

There is a verbose exception option by leveraging DataFusion [backtraces](https://arrow.apache.org/datafusion/user-guide/example-usage.html#enable-backtraces)
This option allows to append native DataFusion stack trace to the original error message.
To enable this option with Comet it is needed to include `backtrace` feature in [Cargo.toml](https://github.com/apache/arrow-datafusion-comet/blob/main/core/Cargo.toml) for DataFusion dependencies

```toml
datafusion-common = { version = "36.0.0", features = ["backtrace"] }
datafusion = { default-features = false, version = "36.0.0", features = ["unicode_expressions", "backtrace"] }
```

Then build the Comet as [described](https://github.com/apache/arrow-datafusion-comet/blob/main/README.md#getting-started)

Start Comet with `RUST_BACKTRACE=1`

```console
RUST_BACKTRACE=1 $SPARK_HOME/spark-shell --jars spark/target/comet-spark-spark3.5_2.12-$COMET_VERSION.jar --conf spark.plugins=org.apache.spark.CometPlugin --conf spark.comet.enabled=true --conf spark.comet.exec.enabled=true
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

By default, Comet emits native-side logs at the **INFO** level to `stderr`.

You can use the `COMET_LOG_LEVEL` environment variable to specify the log level. Supported values are: `OFF`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`.

For example, to configure native logs at the `DEBUG` level:

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

Additionally, you can place a `log4rs.yaml` configuration file inside the Comet configuration directory specified by the `COMET_CONF_DIR` environment variable to enable more advanced logging configurations. This file uses the  [log4rs YAML configuration format](https://docs.rs/log4rs/latest/log4rs/#configuration-via-a-yaml-file).
For example, see: [log4rs.yaml](https://github.com/apache/datafusion-comet/blob/main/conf/log4rs.yaml).
