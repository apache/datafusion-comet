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

# TPC-DS Benchmarking with spark-sql-perf

This guide explains how to generate TPC-DS data and run TPC-DS benchmarks using the
[databricks/spark-sql-perf](https://github.com/databricks/spark-sql-perf) framework, then run those benchmarks
with the Comet benchmarking scripts.

The spark-sql-perf approach uses the TPC-DS `dsdgen` tool to generate data directly through Spark, which handles
partitioning and writing to Parquet format automatically.

## Prerequisites

- Java 17 (for Spark 3.5+)
- Apache Spark 3.5.x
- SBT (Scala Build Tool)
- C compiler toolchain (`gcc`, `make`, `flex`, `bison`, `byacc`)

## Step 1: Build tpcds-kit

The `dsdgen` tool from [databricks/tpcds-kit](https://github.com/databricks/tpcds-kit) is required for data
generation. This is a modified fork of the official TPC-DS toolkit that outputs to stdout, allowing Spark to
ingest the data directly.

**Linux (Ubuntu/Debian):**

```shell
sudo apt-get install -y gcc make flex bison byacc git
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX
```

**Linux (CentOS/RHEL/Amazon Linux):**

```shell
sudo yum install -y gcc make flex bison byacc git
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX
```

**macOS:**

```shell
xcode-select --install
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=MACOS
```

Verify the build succeeded:

```shell
ls -l dsdgen
```

## Step 2: Build spark-sql-perf

```shell
git clone https://github.com/databricks/spark-sql-perf.git
cd spark-sql-perf
sbt package
```

This produces a JAR file at `target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar` (the exact version may
vary). Note the path to this JAR for later use.

## Step 3: Install and Start Spark

If you do not already have Spark installed:

```shell
export SPARK_VERSION=3.5.6
wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz
tar xzf spark-$SPARK_VERSION-bin-hadoop3.tgz
sudo mv spark-$SPARK_VERSION-bin-hadoop3 /opt
export SPARK_HOME=/opt/spark-$SPARK_VERSION-bin-hadoop3/
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
mkdir -p /tmp/spark-events
```

Start Spark in standalone mode:

```shell
$SPARK_HOME/sbin/start-master.sh
export SPARK_MASTER=spark://$(hostname):7077
$SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER
```

## Step 4: Generate TPC-DS Data

Launch `spark-shell` with the spark-sql-perf JAR loaded:

```shell
$SPARK_HOME/bin/spark-shell \
    --master $SPARK_MASTER \
    --jars /path/to/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=8 \
    --conf spark.executor.memory=16g
```

In the Spark shell, run the following to generate data. Adjust `scaleFactor` and paths as needed:

```scala
import com.databricks.spark.sql.perf.tpcds.TPCDSTables

val tpcdsKit = "/path/to/tpcds-kit/tools"
val scaleFactor = "100"    // 100 GB
val dataDir = "/path/to/tpcds-data"
val format = "parquet"
val numPartitions = 32     // adjust based on cluster size

val tables = new TPCDSTables(spark.sqlContext, dsdgenDir = tpcdsKit, scaleFactor = scaleFactor)
tables.genData(
  location = dataDir,
  format = format,
  overwrite = true,
  partitionTables = false,
  clusterByPartitionColumns = false,
  filterOutNullPartitionValues = false,
  numPartitions = numPartitions
)
```

Data generation for SF100 typically takes 20-60 minutes depending on hardware. When complete, exit the shell:

```scala
:quit
```

Verify the data was generated:

```shell
ls /path/to/tpcds-data/
```

You should see directories for each TPC-DS table (`store_sales`, `catalog_sales`, `web_sales`, `customer`,
`date_dim`, etc.).

Set the `TPCDS_DATA` environment variable:

```shell
export TPCDS_DATA=/path/to/tpcds-data
```

## Step 5: Run TPC-DS Benchmarks with Comet

The Comet benchmarking scripts in `benchmarks/tpc/` can run TPC-DS queries against the generated data.

### Run Spark Baseline

```shell
cd benchmarks/tpc
sudo ./drop-caches.sh
python3 run.py --engine spark --benchmark tpcds
```

### Run Comet

Build Comet from source if you have not already:

```shell
make release
export COMET_JAR=$(pwd)/spark/target/comet-spark-spark3.5_2.12-*.jar
```

Then run the benchmark:

```shell
cd benchmarks/tpc
sudo ./drop-caches.sh
python3 run.py --engine comet --benchmark tpcds
```

### Compare Results

Generate a comparison chart from the benchmark output JSON files:

```shell
python3 generate-comparison.py --benchmark tpcds \
    --labels "Spark" "Comet" \
    --title "TPC-DS @ 100 GB" \
    spark-tpcds-*.json comet-tpcds-*.json
```

## Alternative: Command-Line Data Generation

You can also generate TPC-DS data without the Spark shell using `spark-submit`:

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --class com.databricks.spark.sql.perf.tpcds.GenTPCDSData \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=8 \
    --conf spark.executor.memory=16g \
    /path/to/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
    -d /path/to/tpcds-kit/tools \
    -s 100 \
    -l /path/to/tpcds-data \
    -f parquet
```

## Troubleshooting

### dsdgen not found

Ensure `tpcds-kit/tools/dsdgen` exists and is executable. The `dsdgenDir` parameter in the Spark shell (or `-d`
flag in the CLI) must point to the directory containing the `dsdgen` binary, not the binary itself.

### Out of memory during data generation

For large scale factors (SF1000+), increase executor memory and the number of partitions:

```shell
--conf spark.executor.memory=32g
```

And in the Spark shell, use a higher `numPartitions` value (e.g., 200+).

### Schema mismatch with benchmark queries

The Comet TPC-DS benchmark queries in `benchmarks/tpc/queries/tpcds/` expect table names matching the TPC-DS
specification (e.g., `store_sales`, `date_dim`). The spark-sql-perf data generation produces directories with
these exact names, so they should work with the benchmark scripts without modification.
