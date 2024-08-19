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
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    org.apache.spark.CometPlugin \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.enabled=true \
    --conf spark.comet.exec.all.enabled=true \
    --conf spark.comet.cast.allowIncompatible=true \
    --conf spark.comet.exec.shuffle.enabled=true \
    --conf spark.comet.exec.shuffle.mode=auto \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    tpcbench.py \
    --benchmark tpch \
    --data /mnt/bigdata/tpch/sf100/ \
    --queries ../../tpch/queries \
    --iterations 3
```

## Current Performance

Comet is not yet achieving full DataFusion speeds in all cases, but with future work we aim to provide a 2x-4x speedup
for many use cases.

The following benchmarks were performed on a Linux workstation with PCIe 5, AMD 7950X CPU (16 cores), 128 GB RAM, and 
data stored locally on NVMe storage. Performance characteristics will vary in different environments and we encourage 
you to run these benchmarks in your own environments.

### TPC-H

Comet currently provides a 54% speedup for TPC-H @ SF=100GB.

![](../../_static/images/benchmark-results/2024-07-19/tpch_allqueries.png)

Here is a breakdown showing relative performance of Spark, Comet, and DataFusion for each query.

![](../../_static/images/benchmark-results/2024-07-19/tpch_queries_compare.png)

The following chart shows how much Comet currently accelerates each query from the benchmark. 

![](../../_static/images/benchmark-results/2024-07-19/tpch_queries_speedup.png)

The raw results of these benchmarks in JSON format is available here:

- [Spark](./benchmark-results/2024-07-19/spark-tpch.json)
- [Comet](./benchmark-results/2024-07-19/comet-tpch.json)
- [DataFusion](./benchmark-results/2024-07-19/datafusion-tpch.json)
