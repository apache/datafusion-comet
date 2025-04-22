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

# Comet Benchmarking on macOS

This guide is for setting up TPC-H benchmarks locally on macOS using the 100 GB dataset.

Note that running this benchmark on macOS is not ideal because we cannot force Spark or Comet to use performance 
cores rather than efficiency cores, and background processes are sharing these cores. Also, power and thermal 
management may throttle CPU cores.  

## Prerequisites

Java and Rust must be installed locally.

## Data Generation

```shell
cargo install tpchgen-rs
tpchgen-cli -s 100 --format=parquet
```

## Clone the DataFusion Benchmarks Repository

```shell
git clone https://github.com/apache/datafusion-benchmarks.git
```

## Install Spark

Install Spark

```shell
wget https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz
tar xzf spark-3.5.4-bin-hadoop3.tgz
sudo mv spark-3.5.4-bin-hadoop3 /opt
export SPARK_HOME=/opt/spark-3.5.4-bin-hadoop3/
mkdir /tmp/spark-events
```


Start Spark in standalone mode:

```shell
$SPARK_HOME/sbin/start-master.sh
```

Set `SPARK_MASTER` env var (host name will need to be edited):

```shell
export SPARK_MASTER=spark://Andys-MacBook-Pro.local:7077
```


```shell
$SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER
```


## Run Spark Benchmarks

Run the following command (the `--data` parameter will need to be updated to point to your TPC-H data):

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=8 \
    --conf spark.cores.max=8 \
    --conf spark.executor.memory=16g \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=16g \
    --conf spark.eventLog.enabled=true \
    /path/to/datafusion-benchmarks/runners/datafusion-comet/tpcbench.py \
    --name spark \
    --benchmark tpch \
    --data /Users/andy/Data/tpch/sf100 \
    --queries /path/to/datafusion-benchmarks/tpch/queries \
    --output . \
    --iterations 1
```

## Run Comet Benchmarks

Build Comet from source, with `mimalloc` enabled.

```shell
make release COMET_FEATURES="mimalloc"
```

Set `COMET_JAR` to point to the location of the Comet jar file.

```shell
export COMET_JAR=`pwd`/spark/target/comet-spark-spark3.5_2.12-0.8.0-SNAPSHOT.jar
```

Run the following command (the `--data` parameter will need to be updated to point to your S3 bucket):

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=8 \
    --conf spark.cores.max=8 \
    --conf spark.executor.memory=16g \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=16g \
    --conf spark.eventLog.enabled=true \
    --jars $COMET_JAR \
    --driver-class-path $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.shuffle.enableFastEncoding=true \
    --conf spark.comet.exec.shuffle.fallbackToColumnar=true \
    --conf spark.comet.exec.replaceSortMergeJoin=true \
    --conf spark.comet.cast.allowIncompatible=true \
    /path/to/datafusion-benchmarks/runners/datafusion-comet/tpcbench.py \
    --name comet \
    --benchmark tpch \
    --data /path/to/tpch-data/ \
    --queries /path/to/datafusion-benchmarks//tpch/queries \
    --output . \
    --iterations 1
```
