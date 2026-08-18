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

# Micro Benchmarking on AWS EC2

Comet has around 35 micro benchmark suites that measure individual expressions and operators
against their Spark equivalents. They live in
`spark/src/test/scala/org/apache/spark/sql/benchmark` and generate their own data, so no TPC
dataset is needed.

Running them on a laptop gives noisy numbers because of thermal throttling, efficiency cores, and
background processes. This guide covers running them on a dedicated EC2 instance using
`benchmarks/micro/run.py`, which installs the prerequisites, builds Comet, runs the suites, and
collects the results into a form that can be published in a pull request.

## Create the EC2 Instance

Create the instance yourself, from the console or the AWS CLI. Recommended settings:

| Setting        | Value                                   |
| -------------- | --------------------------------------- |
| Instance type  | `m7i.xlarge` (4 vCPU, 16 GiB)           |
| AMI            | Amazon Linux 2023 (x86_64)              |
| Root volume    | 100 GiB `gp3`                           |
| Security group | Inbound SSH (port 22) from your IP only |
| Key pair       | An existing key pair you can SSH with   |

Notes on the instance type:

- `m7i.xlarge` is the default that published results are expected to use. It has fixed performance
  Intel Sapphire Rapids cores, unlike burstable `t3`/`t4g` types whose CPU credits make timings
  drift over a long run.
- A larger type such as `m7i.2xlarge` works too, and shortens the wall clock time of a full run.
  Results are only comparable against other runs on the same instance type, so record which type
  was used. `run.py collect` does this automatically.
- 100 GiB of disk gives room for the Rust and Maven build output plus the temporary Parquet files
  the suites generate. A full build tree is roughly 20 GiB.

Connect to the instance:

```shell
ssh -i /path/to/key.pem ec2-user@<public-ip>
```

## Quick Start

The runner script is self-contained and can be downloaded before the repository is cloned:

```shell
curl -sSLO https://raw.githubusercontent.com/apache/datafusion-comet/main/benchmarks/micro/run.py
python3 run.py all
```

`all` installs the prerequisites, clones `apache/datafusion-comet` into `~/datafusion-comet`,
builds it in release mode, runs every default suite, and copies the results into
`~/datafusion-comet/benchmarks/results/micro`.

A full run takes a few hours, so run it under `tmux` or `nohup` to survive a dropped SSH
connection:

```shell
tmux new -s bench
python3 run.py all 2>&1 | tee ~/bench.log
```

Detach with `Ctrl-b d` and reattach later with `tmux attach -t bench`.

## Manual Setup

`python3 run.py setup` performs the steps in this section. They are listed here so that the
environment can also be prepared by hand, or reproduced on a different Linux distribution.

### System Packages

On Amazon Linux 2023:

```shell
sudo dnf install -y git make cmake gcc gcc-c++ protobuf-compiler python3 \
  java-17-amazon-corretto-devel
```

On Ubuntu or Debian:

```shell
sudo apt-get update
sudo apt-get install -y git make cmake build-essential protobuf-compiler python3 curl \
  openjdk-17-jdk
```

What each is for:

- `git` clones the repository
- `make` drives the build through the top level `Makefile`
- `gcc`, `gcc-c++`, `cmake` build the native dependencies of the Rust crates
- `protobuf-compiler` provides `protoc`, needed to generate the plan serialization code
- `java-17-amazon-corretto-devel` provides the JDK. JDK 17 or later is required for Spark 4.0 and
  above, which is the default build profile
- `python3` runs the benchmark script

### Protobuf Compiler

The native build compiles the Comet protobuf definitions with `prost-build`, which needs a `protoc`
binary on the `PATH`. Without one, `make release` fails with:

```
Error: Custom { kind: NotFound, error: "Could not find `protoc`. ..." }
```

The JVM side is unaffected, since it downloads its own `protoc` through
`protoc-jar-maven-plugin`.

Install it from the distribution packages and check that it resolves:

```shell
sudo dnf install -y protobuf-compiler
protoc --version
```

If the package is unavailable, install the binary from the protobuf releases instead. Use
`linux-aarch_64` in place of `linux-x86_64` on Graviton instances:

```shell
sudo dnf install -y unzip
PROTOC_VERSION=25.5
curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip
sudo unzip -o protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /usr/local bin/protoc
protoc --version
```

`run.py setup` does this automatically: it installs the distribution package, and if `protoc` is
still missing afterwards it downloads the release archive and installs the binary into
`/usr/local/bin`.

### Java

Set `JAVA_HOME` so that the build and the benchmarks find the JDK:

```shell
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto' >> ~/.bashrc
```

Check that the JDK on the `PATH` is the same one, since Maven and the Rust build both use it:

```shell
java -version
javac -version
```

JDK 17 or later is required. Comet targets Java 17 bytecode whenever the JDK in use is 17 or
newer, through the `jdk17` profile in the pom, so no profile needs to be passed by hand. `run.py`
detects `JAVA_HOME` when it is not set, preferring the newest JDK under `/usr/lib/jvm`, and refuses
to run with anything older than 17.

### Maven

Maven does not need to be installed. The repository ships the `./mvnw` wrapper, which downloads a
pinned Maven version on first use. Installing `maven` from the distribution packages is optional
and only useful for running `mvn` directly.

### Rust

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

The toolchain version is pinned by `rust-toolchain.toml` in the repository, so `rustup` installs
the correct version automatically on the first build.

### Clone and Build

```shell
git clone https://github.com/apache/datafusion-comet.git
cd datafusion-comet
make release
```

`make release` builds the native library with `-Ctarget-cpu=native` and installs the JVM artifacts.
Expect it to take 15 to 30 minutes on an `m7i.xlarge`. Benchmarks must be run against a release
build; a debug build reports numbers that are several times slower.

To benchmark a specific branch or commit, pass it to the script instead:

```shell
python3 run.py setup --ref my-branch
python3 run.py setup --repo https://github.com/my-user/datafusion-comet.git --ref my-branch
```

## Running the Suites

```shell
cd ~/datafusion-comet
python3 benchmarks/micro/run.py run
```

Each suite runs in its own JVM, sequentially, with `SPARK_GENERATE_BENCHMARK_FILES=1` so that
Spark's benchmark framework writes result files to `spark/benchmarks`. Per-suite logs and a
`summary.json` are written to `~/comet-bench-runs/<timestamp>/`.

A suite that fails or times out does not stop the run. The first exception in its log is printed
immediately, and its status is recorded in the summary and in `RUN-INFO.md`.

`--timeout` defaults to 60 minutes per suite. `CometShuffleBenchmark` runs far longer than the
others and is given 180 minutes automatically.

Useful options:

| Option                  | Purpose                                                         |
| ----------------------- | --------------------------------------------------------------- |
| `--list`                | Print the selected suites and exit                              |
| `--only Cast`           | Run only suites matching this regular expression, repeatable    |
| `--skip Iceberg`        | Skip suites matching this regular expression, repeatable        |
| `--suites FILE`         | Run the suites listed in a file, one per line, `#` for comments |
| `--heap 12g`            | JVM max heap per suite, default `8g`                            |
| `--timeout 90`          | Per-suite timeout in minutes, `0` disables it                   |
| `--profile -Pspark-3.5` | Build and run against a different Spark version                 |
| `--dry-run`             | Print the commands without running them                         |

### Suites That Are Not Run by Default

The suites are discovered from the sources in
`spark/src/test/scala/org/apache/spark/sql/benchmark`, so a newly added benchmark is picked up
without any change to `run.py`. A few need something the runner cannot provide, and are skipped:

| Suite                      | Reason                                       |
| -------------------------- | -------------------------------------------- |
| `CometTPCHQueryBenchmark`  | Needs TPC-H data via `--data-location`       |
| `CometTPCDSQueryBenchmark` | Needs TPC-DS data via `--data-location`      |
| `CometTPCDSMicroBenchmark` | Needs TPC-DS data via `--data-location`      |
| `CometC2RIsolatedBench`    | Prints to stdout only, writes no result file |
| `CometReadHdfsBenchmark`   | Starts a local HDFS mini cluster             |

They can still be run by naming them in a `--suites` file. `run --list` prints both the selected
suites and the skipped ones with their reasons.

For TPC-H and TPC-DS benchmarking see [Comet Benchmarking in EC2](benchmarking_aws_ec2.md).

### Running a Single Suite by Hand

The `Makefile` target works without the script:

```shell
BENCH_HEAP=8g SPARK_GENERATE_BENCHMARK_FILES=1 \
  make benchmark-org.apache.spark.sql.benchmark.CometStringExpressionBenchmark
```

`BENCH_HEAP` defaults to `20g`, which is more than an `m7i.xlarge` has. Set it to `8g` there, or
leave it alone on a larger instance. Note that this target rebuilds Comet first, which is why
`run.py` builds once and then invokes the benchmarks directly. It reads the invocation from the
`Makefile` through `make print-benchmark-args`, so the two stay in step.

## Collecting Results

```shell
python3 benchmarks/micro/run.py collect
```

This copies the result files written by the most recent run from `spark/benchmarks` into
`benchmarks/results/micro`, and writes `RUN-INFO.md` recording:

- the instance type and availability zone, read from EC2 instance metadata
- CPU model, vCPU count, memory, OS, JDK and Rust versions
- the Comet branch and commit that was benchmarked
- the Maven profile and heap size used
- the status and duration of every suite in the run

Only files modified by that run are copied, so stale results from an earlier build are left behind.
Pass `--all-results` to copy everything in `spark/benchmarks`, or `--run-dir` to collect a specific
run other than the most recent one.

`spark/benchmarks` is in `.gitignore` and acts as a scratch directory.
`benchmarks/results/micro` is tracked and is what gets published.

## Publishing Results

```shell
python3 benchmarks/micro/run.py publish
```

By default this creates a branch, commits the collected results, and prints the push and pull
request commands for you to run. To do the whole thing on the instance, install and authenticate
the GitHub CLI first:

```shell
sudo dnf install -y gh
gh auth login
python3 benchmarks/micro/run.py publish --push --open-pr
```

Because the checkout on the instance is a clone of `apache/datafusion-comet`, pushing requires
either commit access or a fork. To use a fork, add it as a remote and name it:

```shell
git remote add fork https://github.com/my-user/datafusion-comet.git
python3 benchmarks/micro/run.py publish --remote fork --push --open-pr
```

`collect --publish` runs the collect and publish steps together.

Alternatively, copy the results back to your own machine and open the pull request from there:

```shell
rsync -av -e "ssh -i /path/to/key.pem" \
  ec2-user@<public-ip>:datafusion-comet/benchmarks/results/micro/ \
  benchmarks/results/micro/
```

## Getting Comparable Numbers

- Use the same instance type for every run that is going to be compared. Timings from an
  `m7i.xlarge` and an `m7i.2xlarge` are not interchangeable.
- Do not run anything else on the instance while benchmarking, including the build itself. The
  suites use `local[1]`, so a single busy core is enough to skew the numbers.
- Benchmark a release build. `run.py` always builds with `make release`.
- Comet is built with `-Ctarget-cpu=native`, so a build produced on one instance family should not
  be reused on another.
- When investigating a change, benchmark the base commit and the change on the same instance in the
  same session rather than comparing against previously published results.

## Troubleshooting

**The JVM is killed partway through a suite.** The heap is larger than the instance has memory for.
Lower it with `--heap 6g`.

**`linker 'cc' not found` during the Rust build.** A fresh Amazon Linux 2023 image has no C
compiler. Install one and rerun `make release`:

```shell
sudo dnf install -y gcc gcc-c++ make
```

**`Could not find protoc` during the native build.** See
[Protobuf Compiler](#protobuf-compiler) above, then rerun `make release`.

**`JAVA_HOME could not be determined`.** Install a JDK and export `JAVA_HOME`, or pass a checkout
that has one configured.

**`Class java.lang.Record not found - continuing with a stub`.** The Scala compiler is targeting the
Java 11 API, which has no `Record`, while the Spark 4.x sources need Java 17. This happens on a JDK
older than 17, where the `jdk17` profile does not activate and `java.version` stays at its default
of 11. Use JDK 17 or later:

```shell
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
export PATH=$JAVA_HOME/bin:$PATH
./mvnw clean
make release
```

Run `./mvnw clean` first: classes compiled against the wrong API stay in `target/` and break the
next build.

**`cargo: command not found` after `setup`.** `rustup` installs into `~/.cargo/bin`. Run
`source "$HOME/.cargo/env"`, or start a new shell.

**A suite times out.** Raise `--timeout`, or exclude the suite with `--skip` and note the exclusion
when publishing results.

## Shutting Down

Benchmark instances are billed per second while running. Stop or terminate the instance when the
run is finished and the results have been copied off it.
