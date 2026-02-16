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

# Comet Benchmarking Scripts

This directory contains scripts used for generating benchmark results that are published in this repository and in
the Comet documentation.

For full instructions on running these benchmarks on an EC2 instance, see the [Comet Benchmarking on EC2 Guide].

[Comet Benchmarking on EC2 Guide]: https://datafusion.apache.org/comet/contributor-guide/benchmarking_aws_ec2.html

## Usage

All benchmarks are run via `run.py`:

```
python3 run.py --engine <engine> --benchmark <tpch|tpcds> [options]
```

| Option          | Description                              |
| --------------- | ---------------------------------------- |
| `--engine`      | Engine name (matches a TOML file in `engines/`) |
| `--benchmark`   | `tpch` or `tpcds`                        |
| `--iterations`  | Number of iterations (default: 1)        |
| `--output`      | Output directory (default: `.`)          |
| `--query`       | Run a single query number                |
| `--no-restart`  | Skip Spark master/worker restart         |
| `--dry-run`     | Print the spark-submit command without executing |

Available engines: `spark`, `comet`, `comet-iceberg`, `gluten`, `blaze`

## Example usage

Set Spark environment variables:

```shell
export SPARK_HOME=/opt/spark-3.5.3-bin-hadoop3/
export SPARK_MASTER=spark://yourhostname:7077
```

Set path to queries and data:

```shell
export TPCH_QUERIES=/mnt/bigdata/tpch/queries/
export TPCH_DATA=/mnt/bigdata/tpch/sf100/
```

Run Spark benchmark:

```shell
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
sudo ./drop-caches.sh
python3 run.py --engine spark --benchmark tpch
```

Run Comet benchmark:

```shell
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export COMET_JAR=/opt/comet/comet-spark-spark3.5_2.12-0.10.0.jar
sudo ./drop-caches.sh
python3 run.py --engine comet --benchmark tpch
```

Run Gluten benchmark:

```shell
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export GLUTEN_JAR=/opt/gluten/gluten-velox-bundle-spark3.5_2.12-linux_amd64-1.4.0.jar
sudo ./drop-caches.sh
python3 run.py --engine gluten --benchmark tpch
```

Preview a command without running it:

```shell
python3 run.py --engine comet --benchmark tpch --dry-run
```

Generating charts:

```shell
python3 generate-comparison.py --benchmark tpch --labels "Spark 3.5.3" "Comet 0.9.0" "Gluten 1.4.0" --title "TPC-H @ 100 GB (single executor, 8 cores, local Parquet files)" spark-tpch-1752338506381.json comet-tpch-1752337818039.json gluten-tpch-1752337474344.json
```

## Engine Configuration

Each engine is defined by a TOML file in `engines/`. The config specifies JARs, Spark conf overrides,
required environment variables, and optional defaults/exports. See existing files for examples.

## Iceberg Benchmarking

Comet includes native Iceberg support via iceberg-rust integration. This enables benchmarking TPC-H queries
against Iceberg tables with native scan acceleration.

### Prerequisites

Download the Iceberg Spark runtime JAR (required for running the benchmark):

```shell
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.8.1/iceberg-spark-runtime-3.5_2.12-1.8.1.jar
export ICEBERG_JAR=/path/to/iceberg-spark-runtime-3.5_2.12-1.8.1.jar
```

Note: Table creation uses `--packages` which auto-downloads the dependency.

### Create Iceberg TPC-H tables

Convert existing Parquet TPC-H data to Iceberg format:

```shell
export ICEBERG_WAREHOUSE=/mnt/bigdata/iceberg-warehouse
export ICEBERG_CATALOG=${ICEBERG_CATALOG:-local}

$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1 \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=8 \
    --conf spark.cores.max=8 \
    --conf spark.executor.memory=16g \
    --conf spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.${ICEBERG_CATALOG}.type=hadoop \
    --conf spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=$ICEBERG_WAREHOUSE \
    create-iceberg-tpch.py \
    --parquet-path $TPCH_DATA \
    --catalog $ICEBERG_CATALOG \
    --database tpch
```

### Run Iceberg benchmark

```shell
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export COMET_JAR=/opt/comet/comet-spark-spark3.5_2.12-0.10.0.jar
export ICEBERG_JAR=/path/to/iceberg-spark-runtime-3.5_2.12-1.8.1.jar
export ICEBERG_WAREHOUSE=/mnt/bigdata/iceberg-warehouse
export TPCH_QUERIES=/mnt/bigdata/tpch/queries/
sudo ./drop-caches.sh
python3 run.py --engine comet-iceberg --benchmark tpch
```

The benchmark uses `spark.comet.scan.icebergNative.enabled=true` to enable Comet's native iceberg-rust
integration. Verify native scanning is active by checking for `CometIcebergNativeScanExec` in the
physical plan output.

### Iceberg-specific options

| Environment Variable | Default    | Description                         |
| -------------------- | ---------- | ----------------------------------- |
| `ICEBERG_CATALOG`    | `local`    | Iceberg catalog name                |
| `ICEBERG_DATABASE`   | `tpch`     | Database containing TPC-H tables    |
| `ICEBERG_WAREHOUSE`  | (required) | Path to Iceberg warehouse directory |

### Comparing Parquet vs Iceberg performance

Run both benchmarks and compare:

```shell
python3 generate-comparison.py --benchmark tpch \
    --labels "Comet (Parquet)" "Comet (Iceberg)" \
    --title "TPC-H @ 100 GB: Parquet vs Iceberg" \
    comet-tpch-*.json comet-iceberg-tpch-*.json
```
