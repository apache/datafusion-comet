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

# Comet Benchmark Suite

Unified benchmark infrastructure for Apache DataFusion Comet. Supports
TPC-H/TPC-DS and shuffle benchmarks across multiple engines (Spark, Comet,
Gluten) with composable configuration and optional memory profiling.

## Quick Start

```bash
# Run TPC-H with Comet on a standalone cluster
python benchmarks/run.py \
    --engine comet --profile standalone-tpch --restart-cluster \
    -- tpc --benchmark tpch --data $TPCH_DATA --queries $TPCH_QUERIES \
       --output . --iterations 1

# Preview the spark-submit command without executing
python benchmarks/run.py \
    --engine comet --profile standalone-tpch --dry-run \
    -- tpc --benchmark tpch --data $TPCH_DATA --queries $TPCH_QUERIES \
       --output . --iterations 1
```

## Directory Layout

```
benchmarks/
├── run.py                 # Entry point — builds and runs spark-submit
├── conf/
│   ├── engines/           # Per-engine configs (comet, spark, gluten, ...)
│   └── profiles/          # Per-environment configs (local, standalone, docker, k8s)
├── runner/
│   ├── cli.py             # Python CLI passed to spark-submit (subcommands: tpc, shuffle, micro)
│   ├── config.py          # Config file loader and merger
│   ├── spark_session.py   # SparkSession builder
│   └── profiling.py       # Level 1 JVM metrics via Spark REST API
├── suites/
│   ├── tpc.py             # TPC-H / TPC-DS benchmark suite
│   ├── shuffle.py         # Shuffle benchmark suite (hash, round-robin)
│   └── micro.py           # Microbenchmark suite (string expressions, ...)
├── analysis/
│   ├── compare.py         # Generate comparison charts from result JSON
│   └── memory_report.py   # Generate memory reports from profiling CSV
├── infra/
│   ├── docker/            # Dockerfile, docker-compose, metrics collector
│   └── k8s/               # Kubernetes manifests (namespace, RBAC, PVCs, Job)
├── create-iceberg-tpch.py # Utility: convert TPC-H Parquet to Iceberg tables
└── drop-caches.sh         # Utility: drop OS page caches before benchmarks
```

## How It Works

`run.py` is the single entry point. It:

1. Reads a **profile** config (cluster shape, memory, master URL)
2. Reads an **engine** config (plugin JARs, shuffle manager, engine-specific settings)
3. Applies any `--conf key=value` CLI overrides (highest precedence)
4. Builds and executes the `spark-submit` command

The merge order is: **profile < engine < CLI overrides**, so engine configs
can override profile defaults (e.g., an engine can set `offHeap.enabled=false`
even though the profile enables it).

### Wrapper arguments (before `--`)

| Flag                | Description                                     |
| ------------------- | ----------------------------------------------- |
| `--engine NAME`     | Engine config from `conf/engines/NAME.conf`     |
| `--profile NAME`    | Profile config from `conf/profiles/NAME.conf`   |
| `--conf key=value`  | Extra Spark/runner config override (repeatable) |
| `--restart-cluster` | Stop/start Spark standalone master + worker     |
| `--dry-run`         | Print spark-submit command without executing    |

### Suite arguments (after `--`)

Everything after `--` is passed to `runner/cli.py`. See per-suite docs:

- [TPC-H / TPC-DS](suites/TPC.md)
- [Shuffle](suites/SHUFFLE.md)
- [Microbenchmarks](suites/MICRO.md)

## Available Engines

| Engine                 | Config file                         | Description                       |
| ---------------------- | ----------------------------------- | --------------------------------- |
| `spark`                | `engines/spark.conf`                | Vanilla Spark (no accelerator)    |
| `comet`                | `engines/comet.conf`                | DataFusion Comet with native scan |
| `comet-iceberg`        | `engines/comet-iceberg.conf`        | Comet + native Iceberg scanning   |
| `gluten`               | `engines/gluten.conf`               | Gluten (Velox backend) — Java 8   |
| `spark-shuffle`        | `engines/spark-shuffle.conf`        | Spark baseline for shuffle tests  |
| `comet-jvm-shuffle`    | `engines/comet-jvm-shuffle.conf`    | Comet with JVM shuffle mode       |
| `comet-native-shuffle` | `engines/comet-native-shuffle.conf` | Comet with native shuffle         |

## Available Profiles

| Profile            | Config file                      | Description                    |
| ------------------ | -------------------------------- | ------------------------------ |
| `local`            | `profiles/local.conf`            | `local[*]` mode, no cluster    |
| `standalone-tpch`  | `profiles/standalone-tpch.conf`  | 1 executor, 8 cores, S3A       |
| `standalone-tpcds` | `profiles/standalone-tpcds.conf` | 2 executors, 16 cores, S3A     |
| `docker`           | `profiles/docker.conf`           | For docker-compose deployments |
| `k8s`              | `profiles/k8s.conf`              | Spark-on-Kubernetes            |

## Environment Variables

The config files use `${VAR}` references that are expanded from the
environment at load time:

| Variable       | Used by              | Description                       |
| -------------- | -------------------- | --------------------------------- |
| `SPARK_HOME`   | `run.py`             | Path to Spark installation        |
| `SPARK_MASTER` | standalone profiles  | Spark master URL                  |
| `COMET_JAR`    | comet engines        | Path to Comet JAR                 |
| `GLUTEN_JAR`   | gluten engine        | Path to Gluten JAR                |
| `ICEBERG_JAR`  | comet-iceberg engine | Path to Iceberg Spark runtime JAR |
| `K8S_MASTER`   | k8s profile          | K8s API server URL                |

## Profiling

Add `--profile` (the flag, not the config) to any suite command to enable
Level 1 JVM metrics collection via the Spark REST API:

```bash
python benchmarks/run.py --engine comet --profile standalone-tpch \
    -- tpc --benchmark tpch --data $TPCH_DATA --queries $TPCH_QUERIES \
       --output . --iterations 1 --profile --profile-interval 1.0
```

This writes a `{name}-{benchmark}-metrics.csv` alongside the result JSON.

For container-level memory profiling, use the constrained docker-compose
overlay — see [Docker infrastructure](infra/docker/).

## Generating Charts

```bash
# Compare two result JSON files
python -m benchmarks.analysis.compare \
    comet-tpch-*.json spark-tpch-*.json \
    --labels Comet Spark --benchmark tpch \
    --title "TPC-H SF100" --output-dir ./charts

# Generate memory reports
python -m benchmarks.analysis.memory_report \
    --spark-csv comet-tpch-metrics.csv \
    --container-csv container-metrics.csv \
    --output-dir ./charts
```

## Running in Docker

See [infra/docker/](infra/docker/) for docker-compose setup with optional
memory-constrained overlays and cgroup metrics collection.

The Docker image includes both Java 8 and Java 17 runtimes. Java 17 is the
default (`JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`), which is required
by Comet. Gluten requires Java 8, so override `JAVA_HOME` for all containers
when running Gluten benchmarks:

```bash
# Start the cluster with Java 8 for Gluten
docker compose -f benchmarks/infra/docker/docker-compose.yml up -d

# Run Gluten benchmark (override JAVA_HOME on all containers)
docker compose -f benchmarks/infra/docker/docker-compose.yml run --rm \
    -e JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
    -e GLUTEN_JAR=/jars/gluten.jar \
    bench bash -c 'python3 /opt/benchmarks/run.py \
        --engine gluten --profile docker \
        -- tpc --name gluten --benchmark tpch --data /data \
           --queries /queries --output /results --iterations 1'
```

> **Note:** The Spark worker must also run Java 8 for Gluten. Use a
> docker-compose override file to set `JAVA_HOME` on `spark-master` and
> `spark-worker` services before starting the cluster, or restart the
> cluster between engine switches.

## Running on Kubernetes

See [infra/k8s/](infra/k8s/) for K8s manifests (namespace, RBAC, PVCs,
benchmark Job template).
