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

Here are example commands for running the benchmarks against a Spark cluster. This command will need to be 
adapted based on the Spark environment and location of data files.

This command assumes that `datafusion-benchmarks` is checked out in a parallel directory to `datafusion-comet`.

## Running Benchmarks Against Apache Spark

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.driver.memory=8G \
    --conf spark.executor.memory=64G \
    --conf spark.executor.cores=16 \
    --conf spark.cores.max=16 \
    --conf spark.eventLog.enabled=true \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    tpcbench.py \
    --benchmark tpch \
    --data /mnt/bigdata/tpch/sf100/ \
    --queries ../../tpch/queries \
    --iterations 5
```

## Running Benchmarks Against Apache Spark with Apache DataFusion Comet Enabled

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.driver.memory=8G \
    --conf spark.executor.memory=64G \
    --conf spark.executor.cores=16 \
    --conf spark.cores.max=16 \
    --conf spark.eventLog.enabled=true \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.enabled=true \
    --conf spark.comet.exec.all.enabled=true \
    --conf spark.comet.cast.allowIncompatible=true \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.comet.parquet.io.enabled=false \
    --conf spark.comet.batchSize=8192 \
    --conf spark.comet.exec.shuffle.enabled=true \
    --conf spark.comet.exec.shuffle.mode=auto \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.sql.adaptive.coalescePartitions.enabled=false \
    tpcbench.py \
    --benchmark tpch \
    --data /mnt/bigdata/tpch/sf100/ \
    --queries ../../tpch/queries \
    --iterations 5
```