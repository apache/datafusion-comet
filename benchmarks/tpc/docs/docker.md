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

# Running Benchmarks with Docker Compose

A Docker Compose setup is provided in `infra/docker/` for running benchmarks in an isolated
Spark standalone cluster. The Docker image supports both **Linux (amd64)** and **macOS (arm64)**
via architecture-agnostic Java symlinks created at build time.

See the [main README](../README.md) for general benchmark usage and options.

## Build the image

The image must be built for the correct platform to match the native libraries in the
engine JARs (e.g. Comet bundles `libcomet.so` for a specific OS/arch).

```shell
docker build -t comet-bench -f benchmarks/tpc/infra/docker/Dockerfile .
```

## Building a compatible Comet JAR

The Comet JAR contains platform-specific native libraries (`libcomet.so` / `libcomet.dylib`).
A JAR built on the host may not work inside the Docker container due to OS, architecture,
or glibc version mismatches. Use `Dockerfile.build-comet` to build a JAR with compatible
native libraries:

- **macOS (Apple Silicon):** The host JAR contains `darwin/aarch64` libraries which
  won't work in Linux containers. You **must** use the build Dockerfile.
- **Linux:** If your host glibc version differs from the container's, the native library
  will fail to load with a `GLIBC_x.xx not found` error. The build Dockerfile uses
  Ubuntu 20.04 (glibc 2.31) for broad compatibility. Use it if you see
  `UnsatisfiedLinkError` mentioning glibc when running benchmarks.

```shell
mkdir -p output
docker build -t comet-builder \
    -f benchmarks/tpc/infra/docker/Dockerfile.build-comet .
docker run --rm -v $(pwd)/output:/output comet-builder
export COMET_JAR=$(pwd)/output/comet-spark-spark3.5_2.12-*.jar
```

## Platform notes

**macOS (Apple Silicon):** Docker Desktop is required.

- **Memory:** Docker Desktop defaults to a small memory allocation (often 8 GB) which
  is not enough for Spark benchmarks. Go to **Docker Desktop > Settings > Resources >
  Memory** and increase it to at least 48 GB (each worker requests 16 GB for its executor
  plus overhead, and the driver needs 8 GB). Without enough memory, executors will be
  OOM-killed (exit code 137).
- **File Sharing:** You may need to add your data directory (e.g. `/opt`) to
  **Docker Desktop > Settings > Resources > File Sharing** before mounting host volumes.

**Linux (amd64):** Docker uses cgroup memory limits directly without a VM layer. No
special Docker configuration is needed, but you may still need to build the Comet JAR
using `Dockerfile.build-comet` (see above) if your host glibc version doesn't match
the container's.

The Docker image auto-detects the container architecture (amd64/arm64) and sets up
arch-agnostic Java symlinks. The compose file uses `BENCH_JAVA_HOME` (not `JAVA_HOME`)
to avoid inheriting the host's Java path into the container.

## Start the cluster

Set environment variables pointing to your host paths, then start the Spark master and
two workers:

```shell
export DATA_DIR=/mnt/bigdata/tpch/sf100
export RESULTS_DIR=/tmp/bench-results
export COMET_JAR=/opt/comet/comet-spark-spark3.5_2.12-0.10.0.jar

mkdir -p $RESULTS_DIR/spark-events
docker compose -f benchmarks/tpc/infra/docker/docker-compose.yml up -d
```

Set `COMET_JAR`, `GLUTEN_JAR`, or `ICEBERG_JAR` to the host path of the engine JAR you
want to use. Each JAR is mounted individually into the container, so you can easily switch
between versions by changing the path and restarting.

## Run benchmarks

Use `docker compose run --rm` to execute benchmarks. The `--rm` flag removes the
container when it exits, preventing port conflicts on subsequent runs. Pass
`--no-restart` since the cluster is already managed by Compose, and `--output /results`
so that output files land in the mounted results directory:

```shell
docker compose -f benchmarks/tpc/infra/docker/docker-compose.yml \
    run --rm -p 4040:4040 bench \
    python3 /opt/benchmarks/run.py \
    --engine comet --benchmark tpch --output /results --no-restart
```

The `-p 4040:4040` flag exposes the Spark Application UI on the host. The following
UIs are available during a benchmark run:

| UI                | URL                    |
| ----------------- | ---------------------- |
| Spark Master      | http://localhost:8080  |
| Worker 1          | http://localhost:8081  |
| Worker 2          | http://localhost:8082  |
| Spark Application | http://localhost:4040  |
| History Server    | http://localhost:18080 |

> **Note:** The Master UI links to the Application UI using the container's internal
> hostname, which is not reachable from the host. Use `http://localhost:4040` directly
> to access the Application UI.

The Spark Application UI is only available while a benchmark is running. To inspect
completed runs, uncomment the `history-server` service in `docker-compose.yml` and
restart the cluster. The History Server reads event logs from `$RESULTS_DIR/spark-events`.

For Gluten (requires Java 8), you must restart the **entire cluster** with `JAVA_HOME`
set so that all services (master, workers, and bench) use Java 8:

```shell
export BENCH_JAVA_HOME=/usr/lib/jvm/java-8-openjdk
docker compose -f benchmarks/tpc/infra/docker/docker-compose.yml down
docker compose -f benchmarks/tpc/infra/docker/docker-compose.yml up -d

docker compose -f benchmarks/tpc/infra/docker/docker-compose.yml \
    run --rm bench \
    python3 /opt/benchmarks/run.py \
    --engine gluten --benchmark tpch --output /results --no-restart
```

> **Important:** Only passing `-e JAVA_HOME=...` to the `bench` container is not
> sufficient -- the workers also need Java 8 or Gluten will fail at runtime with
> `sun.misc.Unsafe` errors. Unset `BENCH_JAVA_HOME` (or switch it back to Java 17)
> and restart the cluster before running Comet or Spark benchmarks.

## Memory limits

Two compose files are provided for different hardware profiles:

| File                        | Workers | Total memory | Use case                       |
| --------------------------- | ------- | ------------ | ------------------------------ |
| `docker-compose.yml`        | 2       | ~74 GB       | SF100+ on a workstation/server |
| `docker-compose-laptop.yml` | 1       | ~12 GB       | SF1–SF10 on a laptop           |

**`docker-compose.yml`** (workstation default):

| Container      | Container limit (`mem_limit`) | Spark JVM allocation      |
| -------------- | ----------------------------- | ------------------------- |
| spark-worker-1 | 32 GB                         | 16 GB executor + overhead |
| spark-worker-2 | 32 GB                         | 16 GB executor + overhead |
| bench (driver) | 10 GB                         | 8 GB driver               |
| **Total**      | **74 GB**                     |                           |

Configure via environment variables: `WORKER_MEM_LIMIT` (default: 32g per worker),
`BENCH_MEM_LIMIT` (default: 10g), `WORKER_MEMORY` (default: 16g, Spark executor memory),
`WORKER_CORES` (default: 8).

## Running on a laptop with small scale factors

For local development or testing with small scale factors (e.g. SF1 or SF10), use the
laptop compose file which runs a single worker with reduced memory:

```shell
docker compose -f benchmarks/tpc/infra/docker/docker-compose-laptop.yml up -d
```

This starts one worker (4 GB executor inside an 8 GB container) and a 4 GB bench
container, totaling approximately **12 GB** of memory.

The benchmark scripts request 2 executor instances and 16 max cores by default
(`run.py`). Spark will simply use whatever resources are available on the single worker,
so no script changes are needed.
