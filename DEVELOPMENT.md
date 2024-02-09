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

# Comet Development Guide

## Project Layout

```
├── common     <- common Java/Scala code
├── conf       <- configuration files
├── core       <- core native code, in Rust
├── spark      <- Spark integration
```

## Development Setup

1. Make sure `JAVA_HOME` is set and point to JDK 11 installation.
2. Install Rust toolchain. The easiest way is to use
   [rustup](https://rustup.rs).

## Build & Test

A few common commands are specified in project's `Makefile`:

- `make`: compile the entire project, but don't run tests
- `make test`: compile the project and run tests in both Rust and Java
  side.
- `make release`: compile the project and creates a release build. This
  is useful when you want to test Comet local installation in another project
  such as Spark.
- `make clean`: clean up the workspace
- `bin/comet-spark-shell -d . -o spark/target/` run Comet spark shell for V1 datasources
- `bin/comet-spark-shell -d . -o spark/target/ --conf spark.sql.sources.useV1SourceList=""` run Comet spark shell for V2 datasources

## Benchmark

There's a `make` command to run micro benchmarks in the repo. For
instance:

```
make benchmark-org.apache.spark.sql.benchmark.CometReadBenchmark
```

To run TPC-H or TPC-DS micro benchmarks, please follow the instructions
in the respective source code, e.g., `CometTPCHQueryBenchmark`.

## Debugging
Comet is a multi-language project with native code written in Rust and JVM code written in Java and Scala.
It is possible to debug both native and JVM code concurrently as described in the [DEBUGGING guide](DEBUGGING.md)
