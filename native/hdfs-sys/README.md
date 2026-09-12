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

# hdfs-sys

Bindings to the Apache Hadoop `libhdfs` C API, carrying the
[HDFS-16021](https://issues.apache.org/jira/browse/HDFS-16021) thread-ownership fix.

This crate substitutes for the crates.io [`hdfs-sys`](https://github.com/Xuanwo/hdfs-sys) crate
through a `[patch.crates-io]` entry in `native/Cargo.toml`. It is not published and is not
intended for use outside Comet.

## Why it exists

`libhdfs` registers a pthread thread-local destructor, `hdfsThreadDestructor`, which detaches the
current thread from the JVM. It does so for every thread that has a cached `JNIEnv`, including
threads that libhdfs did not attach. Comet attaches each of its Tokio worker threads with
`AttachCurrentThreadAsDaemon` and detaches them itself on thread stop, so by the time the pthread
destructor runs the `JNIEnv` has already been freed. Dereferencing it jumps through a null function
pointer and takes the JVM down with `SIGSEGV at pc=0x0`.

That is [apache/datafusion-comet#5023](https://github.com/apache/datafusion-comet/issues/5023),
which reproduces on both Linux and macOS in the `[scans]` CI bucket. `ParquetReadFromFakeHadoopFsSuite`
is what routes a read through `libhdfs`, but the destructor fires whenever one of those threads
later exits, so the crash lands in an unrelated suite sharing the same JVM.

There is no released `hdfs-sys` with the fix, and no way to avoid the dependency: it arrives through
`opendal`'s `services-hdfs` feature by way of `hdrs`. The last crates.io release is 0.3.0 from July
2023, and a fix merged upstream in January 2026 is still unreleased, so waiting is not a strategy.

## Provenance

| Component             | Source                                                                                                                                   |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `libhdfs/hdfs_3_3/**` | Apache Hadoop, via the `hdfs-sys` 0.3.0 crate's vendored copy of `hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs` |
| `libhdfs/config.h`    | Written for Comet. Hadoop generates this with CMake; the upstream crate ships it empty                                                   |
| `src/lib.rs`          | Written for Comet, transcribed from the vendored `libhdfs/hdfs_3_3/include/hdfs/hdfs.h`                                                  |
| `build.rs`            | Written for Comet, following the file list in Hadoop's `hadoop-hdfs-native-client/src/CMakeLists.txt`                                    |

The C sources are Apache Hadoop's own, carry their original ASF license headers, and are
unmodified apart from the changes listed below. No code authored by the `hdfs-sys` maintainer is
copied here: the Rust binding layer and the build script were written for Comet against Hadoop's
public header, which is why only the API surface `hdrs` uses is declared.

Relative to the upstream crate this copy also drops everything Comet does not build: the twelve
vendored Hadoop versions older than 3.3, the Windows platform layer, and the bundled `libdirent`
(MIT). Comet ships no Windows native artifacts.

## Modifications

Three files differ from Hadoop's originals. Each carries a notice at the top of the file, as
required by section 4(b) of the Apache License.

- `libhdfs/hdfs_3_3/os/thread_local_storage.h` — adds an `attachedByLibhdfs` flag to
  `struct ThreadLocalState`.
- `libhdfs/hdfs_3_3/jni_helper.c` — `getGlobalJNIEnv` reports whether it attached the current
  thread, and calls `GetEnv` before `AttachCurrentThread` so that an attachment made by the JVM or
  by the embedding application is reused rather than claimed.
- `libhdfs/hdfs_3_3/os/posix/thread_local_storage.c` — `hdfsThreadDestructor` detaches only when
  `attachedByLibhdfs` is set, and `threadLocalStorageCreate` initialises the two fields it
  previously left holding `malloc` garbage.

The first and third come from the patch attached to HDFS-16021. The `GetEnv` check in the second
does not, and is the part that matters for Comet: `AttachCurrentThread` succeeds on an
already-attached thread and returns the same `JNIEnv`, so without it libhdfs would still record
itself as the owner of an attachment Comet made.

The same changes are proposed upstream as
[Xuanwo/hdfs-sys#47](https://github.com/Xuanwo/hdfs-sys/pull/47).

## Removal condition

Delete this directory, drop the `[patch.crates-io]` entry from `native/Cargo.toml`, and remove the
NOTICE.txt stanza once a crates.io release of `hdfs-sys` contains the fix. Hadoop's own copy is not
sufficient on its own: HDFS-16021 is still open, and trunk still has the unguarded destructor.

## A system libhdfs will not carry the fix

`build.rs` keeps the upstream resolution order, so setting `HDFS_LIB_DIR` or `HADOOP_HOME` links a
prebuilt `libhdfs` instead of compiling these sources, and that library has whatever behaviour its
own build gave it. The `vendored` feature, which Comet enables on macOS through `hdrs`, skips the
search. On Linux neither variable is set in CI, so the vendored sources are built there too.
