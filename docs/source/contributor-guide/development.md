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

1. Make sure `JAVA_HOME` is set and point to JDK using [support matrix](../user-guide/latest/installation.md)
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

## Common Build and Test Pitfalls

### Native Code Must Be Built First

The native Rust code must be compiled before running JVM tests. If you skip this step, tests will
fail because they cannot find the native library. Always run `make core` (or `cd native && cargo build`)
before running Maven tests.

```sh
# Correct order
make core                    # Build native code first
./mvnw test -Dsuites="..."   # Then run JVM tests
```

### Debug vs Release Mode

There is no need to use release mode (`make release`) during normal development. Debug builds
are faster to compile and provide better error messages. Only use release mode when:

- Running benchmarks
- Testing performance
- Creating a release build for deployment

For regular development and testing, use `make` or `make core` which build in debug mode.

### Running Rust Tests

When running Rust tests directly with `cargo test`, the JVM library (`libjvm.so`) must be on
your library path. Set the `LD_LIBRARY_PATH` environment variable to include your JDK's `lib/server`
directory:

```sh
# Find your libjvm.so location (example for typical JDK installation)
export LD_LIBRARY_PATH=$JAVA_HOME/lib/server:$LD_LIBRARY_PATH

# Now you can run Rust tests
cd native && cargo test
```

Alternatively, use `make test-rust` which handles the JVM compilation dependency automatically.

### Avoid Using `-pl` to Select Modules

When running Maven tests, avoid using `-pl spark` to select only the spark module. This can cause
Maven to pick up the `common` module from your local Maven repository instead of using the current
codebase, leading to inconsistent test results:

```sh
# Avoid this - may use stale common module from local repo
./mvnw test -pl spark -Dsuites="..."

# Do this instead - builds and tests with current code
./mvnw test -Dsuites="..."
```

### Use `wildcardSuites` for Running Tests

When running specific test suites, use `wildcardSuites` instead of `suites` for more flexible
matching. The `wildcardSuites` parameter allows partial matching of suite names:

```sh
# Run all suites containing "CometCast"
./mvnw test -DwildcardSuites="CometCast"

# Run specific suite with filter
./mvnw test -Dsuites="org.apache.comet.CometCastSuite valid"
```

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
argument, for example if you only want to execute the test cases that contains _valid_
in their name in `org.apache.comet.CometCastSuite` you can use

```sh
./mvnw test -Dtest=none -Dsuites="org.apache.comet.CometCastSuite valid"
```

Other options for selecting specific suites are described in the [ScalaTest Maven Plugin documentation](https://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin)

## Plan Stability Testing

Comet has a plan stability testing framework that can be used to test the stability of the query plans generated by Comet.
The plan stability testing framework is located in the `spark` module.

### Using the Helper Script

The easiest way to regenerate golden files is to use the provided script:

```sh
# Regenerate golden files for all Spark versions
./dev/regenerate-golden-files.sh

# Regenerate only for a specific Spark version
./dev/regenerate-golden-files.sh --spark-version 3.5
```

The script verifies that JDK 17+ is configured (required for Spark 4.0), installs Comet for each
Spark version, and runs the plan stability tests with `SPARK_GENERATE_GOLDEN_FILES=1`.

### Manual Instructions

Alternatively, you can run the tests manually using the following commands.

First, Comet needs to be installed for each Spark version to be tested:

```sh
./mvnw install -DskipTests -Pspark-3.4
./mvnw install -DskipTests -Pspark-3.5
# note that Spark 4.0 requires JDK 17 or later
./mvnw install -DskipTests -Pspark-4.0
```

Note that the output files get written to `$SPARK_HOME`.

The tests can be run with:

```sh
export SPARK_HOME=`pwd`
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-3.4 -nsu test
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-3.5 -nsu test
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-4.0 -nsu test
```

and

```sh
export SPARK_HOME=`pwd`
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-3.4 -nsu test
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-3.5 -nsu test
./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-4.0 -nsu test
```

If your pull request changes the query plans generated by Comet, you should regenerate the golden files.
To regenerate the golden files, you can run the following commands.

```sh
export SPARK_HOME=`pwd`
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-3.4 -nsu test
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-3.5 -nsu test
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-4.0 -nsu test
```

and

```sh
export SPARK_HOME=`pwd`
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-3.4 -nsu test
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

Before submitting a pull request, follow this checklist to ensure your changes are ready:

### 1. Format Your Code

Comet uses `cargo fmt`, [Scalafix](https://github.com/scalacenter/scalafix) and [Spotless](https://github.com/diffplug/spotless/tree/main/plugin-maven) to
automatically format the code. Run the following command to format all code:

```sh
make format
```

### 2. Build and Verify

After formatting, run a full build to ensure everything compiles correctly and generated
documentation is up to date:

```sh
make
```

This builds both native and JVM code. Fix any compilation errors before proceeding.

### 3. Run Clippy (Recommended)

It's strongly recommended to run Clippy locally to catch potential issues before the CI/CD pipeline does. You can run the same Clippy checks used in CI/CD with:

```bash
cd native
cargo clippy --color=never --all-targets --workspace -- -D warnings
```

Make sure to resolve any Clippy warnings before submitting your pull request, as the CI/CD pipeline will fail if warnings are present.

### 4. Run Tests

Run the relevant tests for your changes:

```sh
# Run all tests
make test

# Or run only Rust tests
make test-rust

# Or run only JVM tests (native must be built first)
make test-jvm
```

### Pre-PR Summary

```sh
make format   # Format code
make          # Build everything and update generated docs
make test     # Run tests (optional but recommended)
```

## How to format `.md` document

We are using `prettier` to format `.md` files.

You can either use `npm i -g prettier` to install it globally or use `npx` to run it as a standalone binary. Using `npx` required a working node environment. Upgrading to the latest prettier is recommended (by adding `--upgrade` to the `npm` command).

```bash
$ prettier --version
2.3.0
```

After you've confirmed your prettier version, you can format all the `.md` files:

```bash
npx prettier "**/*.md" --write
```
