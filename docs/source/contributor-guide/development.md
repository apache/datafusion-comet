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

1. Make sure `JAVA_HOME` is set and point to JDK 8/11/17 installation.
2. Install Rust toolchain. The easiest way is to use
   [rustup](https://rustup.rs).

## Build & Test

A few common commands are specified in project's `Makefile`:

- `make`: compile the entire project, but don't run tests
- `make test-rust`: compile the project and run tests in Rust side
- `make test-java`: compile the project and run tests in Java side
- `make test`: compile the project and run tests in both Rust and Java
  side.
- `make release`: compile the project and creates a release build. This
  is useful when you want to test Comet local installation in another project
  such as Spark.
- `make clean`: clean up the workspace
- `bin/comet-spark-shell -d . -o spark/target/` run Comet spark shell for V1 datasources
- `bin/comet-spark-shell -d . -o spark/target/ --conf spark.sql.sources.useV1SourceList=""` run Comet spark shell for V2 datasources

## Development Environment

Comet is a multi-language project with native code written in Rust and JVM code written in Java and Scala.
For Rust code, the CLion IDE is recommended. For JVM code, IntelliJ IDEA is recommended.

Before opening the project in an IDE, make sure to run `make` first to generate the necessary files for the IDEs. Currently, it's mostly about
generating protobuf message classes for the JVM side. It's only required to run `make` once after cloning the repo.

### IntelliJ IDEA

First make sure to install the Scala plugin in IntelliJ IDEA.
After that, you can open the project in IntelliJ IDEA. The IDE should automatically detect the project structure and import as a Maven project.

### CLion

First make sure to install the Rust plugin in CLion or you can use the dedicated Rust IDE: RustRover.
After that you can open the project in CLion. The IDE should automatically detect the project structure and import as a Cargo project.

### Running Tests in IDEA

Like other Maven projects, you can run tests in IntelliJ IDEA by right-clicking on the test class or test method and selecting "Run" or "Debug".
However if the tests is related to the native side. Please make sure to run `make core` or `cd core && cargo build` before running the tests in IDEA.

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
It is possible to debug both native and JVM code concurrently as described in the [DEBUGGING guide](debugging)

## Submitting a Pull Request

Comet uses `cargo fmt`, [Scalafix](https://github.com/scalacenter/scalafix) and [Spotless](https://github.com/diffplug/spotless/tree/main/plugin-maven) to
automatically format the code. Before submitting a pull request, you can simply run `make format` to format the code.
