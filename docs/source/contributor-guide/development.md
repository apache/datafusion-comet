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
├── native     <- native code, in Rust
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
- `make test-jvm`: compile the project and run tests in Java side
- `make test`: compile the project and run tests in both Rust and Java
  side.
- `make release`: compile the project and creates a release build. This
  is useful when you want to test Comet local installation in another project
  such as Spark.
- `make clean`: clean up the workspace

### Using SBT:
The general command is `sbt <definitions> assembly`, with the following definitions currently supported:
- `-Drelease` / `-Dapache-release` - Sets the optimization level for native code(Does not perform any other release-related changes)
- `-Dspark<version>` - Supported versions are "3.3", "3.4", "3.5", "4.0"(experimental)
- `-Dscala<version>` - Supported versions are "2.12" and "2.13", usually, simply choosing the Spark version automatically selects the correct scala.
- `-Djdk<version>` - Supported targets are "17", "11", and "1.8"(JDK 8), ensure your JDK supports compilation for that target
- `-D<platform>` - Which platform and architecture to build for, options are "Win-x86", "Darwin-x86", "Darwin-aarch64", "Linux-amd64", "Linux-aarch64"\
In most cases, when not cross compiling, its best to let the auto-selection work.

SBT supports "test" and "testOnly".
Do note that the generated artifact from an sbt build will be in `target/scala-2.12/comet-assembly-0.6.0-SNAPSHOT.jar`, which differs from the `make` output

## Development Environment

Comet is a multi-language project with native code written in Rust and JVM code written in Java and Scala.
For Rust code, the CLion IDE is recommended. For JVM code, IntelliJ IDEA is recommended.

Before opening the project in an IDE, make sure to run `make` first to generate the necessary files for the IDEs. Currently, it's mostly about
generating protobuf message classes for the JVM side. It's only required to run `make` once after cloning the repo.

### IntelliJ IDEA

First make sure to install the Scala plugin in IntelliJ IDEA.
After that, you can open the project in IntelliJ IDEA. The IDE should automatically detect the project structure and import as a Maven project.

Comet uses generated source files that are too large for IntelliJ's default size limit for code inspections. To avoid IDE errors
(missing definitions, etc.) caused by IntelliJ skipping these generated files, modify
[IntelliJ's Platform Properties](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties)
by going to `Help -> Edit Custom Properties...`. For example, adding `idea.max.intellisense.filesize=16384` increases the file
size limit to 16 MB.

### CLion

First make sure to install the Rust plugin in CLion or you can use the dedicated Rust IDE: RustRover.
After that you can open the project in CLion. The IDE should automatically detect the project structure and import as a Cargo project.

### Running Tests in IDEA

Like other Maven projects, you can run tests in IntelliJ IDEA by right-clicking on the test class or test method and selecting "Run" or "Debug".
However if the tests is related to the native side. Please make sure to run `make core` or `cd native && cargo build` before running the tests in IDEA.

### Running Tests from command line

It is possible to specify which ScalaTest suites you want to run from the CLI using the `suites`
argument, for example if you only want to execute the test cases that contains *valid*
in their name in `org.apache.comet.CometCastSuite` you can use

```sh
./mvnw test -Dtest=none -Dsuites="org.apache.comet.CometCastSuite valid"
```

Other options for selecting specific suites are described in the [ScalaTest Maven Plugin documentation](https://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin)

## Plan Stability Testing

Comet has a plan stability testing framework that can be used to test the stability of the query plans generated by Comet.
The plan stability testing framework is located in the `spark` module and can be run using the following command:

```sh
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -nsu test
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-3.5 -nsu test
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-4.0 -nsu test
```

and
```sh
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -nsu test
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-3.5 -nsu test
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-4.0 -nsu test
```

If your pull request changes the query plans generated by Comet, you should regenerate the golden files.
To regenerate the golden files, you can run the following command:

```sh
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -nsu test
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-3.5 -nsu test
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-4.0 -nsu test
```

and
```sh
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -nsu test
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-3.5 -nsu test
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-4.0 -nsu test
```

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
