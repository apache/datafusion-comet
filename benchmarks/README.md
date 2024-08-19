# Running Comet in Microk8s

This guide explains how to run benchmarks derived from TPC-H and TPC-DS in Apache DataFusion Comet deployed in a
local Microk8s cluster.

## Use Microk8s locally

Install Micro8s following the instructions at https://microk8s.io/docs/getting-started and then perform these
additional steps, ensuring that any existing kube config is backed up first.

```shell
mkdir -p ~/.kube
microk8s config > ~/.kube/config

microk8s.enable dns
microk8s enable registry

microk8s.kubectl create serviceaccount spark
```

## Build Comet Docker Image

Run the following command from the root of this repository to build the Comet Docker image.

```shell
docker build -t apache/datafusion-comet -f kube/Dockerfile .
```

## Build Comet Benchmark Docker Image

Create a Dockerfile for the benchmarks, using Comet as the base image. This Dockerfile also exists in the repository
in the benchmarks directory.

```dockerfile
FROM apache/datafusion-comet:latest

RUN apt update \
    && apt install -y git python3 python3-pip \
    && apt clean

RUN cd /opt \
    && git clone https://github.com/andygrove/datafusion-benchmarks.git \
    && cd datafusion-benchmarks \
    && git checkout tpcds
```

Build the benchmark Docker image and push to the Microk8s Docker registry.

```shell
docker build -t apache/datafusion-comet-tpcbench  .
docker tag apache/datafusion-comet-tpcbench localhost:32000/apache/datafusion-comet-tpcbench:latest
docker push localhost:32000/apache/datafusion-comet-tpcbench:latest
```

## Run benchmarks

```shell
export COMET_DOCKER_IMAGE=localhost:32000/apache/datafusion-comet-tpcbench:latest

$SPARK_HOME/bin/spark-submit \
    --master k8s://https://127.0.0.1:16443 \
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
    --jars local:///opt/spark/jars/comet-spark-spark3.4_2.12-0.2.0-SNAPSHOT.jar \
    --conf spark.executor.extraClassPath=/opt/spark/jars/comet-spark-spark3.4_2.12-0.2.0-SNAPSHOT.jar \
    --conf spark.driver.extraClassPath=/opt/spark/jars/comet-spark-spark3.4_2.12-0.2.0-SNAPSHOT.jar \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.enabled=true \
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
    --queries /opt-datafusion-benchmarks/tpcds/queries-spark \
    --iterations 1
    
```

