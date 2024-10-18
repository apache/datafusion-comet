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

# Comet Benchmarking Guide

To track progress on performance, we regularly run benchmarks derived from TPC-H and TPC-DS. Data generation and 
benchmarking documentation and scripts are available in the [DataFusion Benchmarks](https://github.com/apache/datafusion-benchmarks) GitHub repository.

We also have many micro benchmarks that can be run from an IDE located [here]()https://github.com/apache/datafusion-comet/tree/main/spark/src/test/scala/org/apache/spark/sql/benchmark). 

Here are example commands for running the benchmarks against a Spark cluster. This command will need to be 
adapted based on the Spark environment and location of data files.

These commands are intended to be run from the `runners/datafusion-comet` directory in the `datafusion-benchmarks` 
repository.

## Running Benchmarks Against Apache Spark

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=32G \
    --conf spark.executor.cores=8 \
    --conf spark.cores.max=8 \
    tpcbench.py \
    --benchmark tpch \
    --data /mnt/bigdata/tpch/sf100/ \
    --queries ../../tpch/queries \
    --iterations 3
```

## Running Benchmarks Against Apache Spark with Apache DataFusion Comet Enabled

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=32G \
    --conf spark.executor.cores=8 \
    --conf spark.cores.max=8 \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=10g \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.comet.cast.allowIncompatible=true \
    --conf spark.comet.exec.shuffle.enabled=true \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    tpcbench.py \
    --benchmark tpch \
    --data /mnt/bigdata/tpch/sf100/ \
    --queries ../../tpch/queries \
    --iterations 3
```

## Current Benchmark Results

- [Benchmarks derived from TPC-H](benchmark-results/tpc-h)
- [Benchmarks derived from TPC-DS](benchmark-results/tpc-ds)



