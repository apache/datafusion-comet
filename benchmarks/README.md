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

# Running Comet Benchmarks in Microk8s

This guide explains how to run benchmarks derived from TPC-H and TPC-DS in Apache DataFusion Comet deployed in a
local Microk8s cluster.

## Use Microk8s locally

Install Micro8s following the instructions at https://microk8s.io/docs/getting-started and then perform these
additional steps, ensuring that any existing kube config is backed up first.

```shell
mkdir -p ~/.kube
microk8s config > ~/.kube/config

microk8s enable dns
microk8s enable registry

microk8s kubectl create serviceaccount spark
```

## Build Comet Docker Image

Run the following command from the root of this repository to build the Comet Docker image, or use a published
Docker image from https://github.com/orgs/apache/packages?repo_name=datafusion-comet

```shell
docker build -t apache/datafusion-comet -f kube/Dockerfile .
```

## Build Comet Benchmark Docker Image

Build the benchmark Docker image and push to the Microk8s Docker registry.

```shell
docker build -t apache/datafusion-comet-tpcbench  .
docker tag apache/datafusion-comet-tpcbench localhost:32000/apache/datafusion-comet-tpcbench:latest
docker push localhost:32000/apache/datafusion-comet-tpcbench:latest
```

## Run benchmarks

```shell
export SPARK_MASTER=k8s://https://127.0.0.1:16443
export COMET_DOCKER_IMAGE=localhost:32000/apache/datafusion-comet-tpcbench:latest
# Location of Comet JAR within the Docker image
export COMET_JAR=/opt/spark/jars/comet-spark-spark3.4_2.12-0.5.0-SNAPSHOT.jar

$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --deploy-mode cluster  \
    --name comet-tpcbench \
    --driver-memory 8G \
    --conf spark.driver.memory=8G \
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=32G \
    --conf spark.executor.cores=8 \
    --conf spark.cores.max=8 \
    --conf spark.task.cpus=1 \
    --conf spark.executor.memoryOverhead=3G \
    --jars local://$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.enabled=true \
    --conf spark.comet.exec.all.enabled=true \
    --conf spark.comet.cast.allowIncompatible=true \
    --conf spark.comet.exec.shuffle.enabled=true \
    --conf spark.comet.exec.shuffle.mode=auto \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.kubernetes.namespace=default \
    --conf spark.kubernetes.driver.pod.name=tpcbench  \
    --conf spark.kubernetes.container.image=$COMET_DOCKER_IMAGE \
    --conf spark.kubernetes.driver.volumes.hostPath.tpcdata.mount.path=/mnt/bigdata/tpcds/sf100/ \
    --conf spark.kubernetes.driver.volumes.hostPath.tpcdata.options.path=/mnt/bigdata/tpcds/sf100/ \
    --conf spark.kubernetes.executor.volumes.hostPath.tpcdata.mount.path=/mnt/bigdata/tpcds/sf100/ \
    --conf spark.kubernetes.executor.volumes.hostPath.tpcdata.options.path=/mnt/bigdata/tpcds/sf100/ \
    --conf spark.kubernetes.authenticate.caCertFile=/var/snap/microk8s/current/certs/ca.crt \
    local:///opt/datafusion-benchmarks/runners/datafusion-comet/tpcbench.py \
    --benchmark tpcds \
    --data /mnt/bigdata/tpcds/sf100/ \
    --queries /opt/datafusion-benchmarks/tpcds/queries-spark \
    --iterations 1
```

