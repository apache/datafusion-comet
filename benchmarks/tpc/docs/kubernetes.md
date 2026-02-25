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

# Running Benchmarks on Kubernetes

The benchmark infrastructure supports Spark's native Kubernetes integration. Instead of
a standalone Spark cluster, the driver and executors run as K8s pods. Results are written
to S3 (or S3-compatible storage) since the driver pod has no shared local filesystem with
the submitter.

See the [main README](../README.md) for general benchmark usage and options.

## Prerequisites

- A Kubernetes cluster with `kubectl` configured
- The benchmark Docker image pushed to a registry accessible from the cluster
- A service account with permissions to create pods (see RBAC setup below)
- An S3 bucket (or S3-compatible storage) for benchmark results
- `SPARK_HOME` pointing to a local Spark installation (for `spark-submit`)

## Setup

**1. Create RBAC resources:**

```shell
kubectl apply -f benchmarks/tpc/infra/k8s/rbac.yaml -n <namespace>
```

**2. Build and push the Docker image:**

```shell
docker build -t <registry>/comet-bench -f benchmarks/tpc/infra/docker/Dockerfile .
docker push <registry>/comet-bench
```

**3. Set environment variables:**

```shell
export SPARK_HOME=/opt/spark-3.5.3-bin-hadoop3/
export SPARK_MASTER=k8s://https://<k8s-api-server>:6443
export BENCH_IMAGE=<registry>/comet-bench
export TPCH_DATA=s3a://my-bucket/tpch/sf100/
```

## Running benchmarks

```shell
python3 run.py --engine comet --benchmark tpch \
    --output s3a://my-bucket/bench-results --no-restart
```

The runner auto-detects K8s mode from the `SPARK_MASTER` prefix (`k8s://`) and:
- Uses `--deploy-mode cluster` (driver runs as a K8s pod)
- Sets `spark.kubernetes.container.image` from `BENCH_IMAGE`
- References `tpcbench.py` at its container path (`/opt/benchmarks/tpcbench.py`)
- Skips local Spark master/worker restart

Preview the generated command:

```shell
python3 run.py --engine comet --benchmark tpch \
    --output s3a://my-bucket/bench-results --dry-run
```

## Retrieving results

Results are written as JSON to the S3 output path:

```shell
aws s3 ls s3://my-bucket/bench-results/
aws s3 cp s3://my-bucket/bench-results/comet-tpch-*.json .
```

## Optional: PVC for data

If you prefer to serve TPC data from a PersistentVolumeClaim instead of S3, use the
provided pod template:

```shell
# Create a PVC named "tpc-data" with your dataset, then:
python3 run.py --engine comet --benchmark tpch \
    --output s3a://my-bucket/bench-results --no-restart
```

Add these Spark configs to your engine TOML or pass them via `--conf`:

```
spark.kubernetes.driver.podTemplateFile=benchmarks/tpc/infra/k8s/pod-template.yaml
spark.kubernetes.executor.podTemplateFile=benchmarks/tpc/infra/k8s/pod-template.yaml
```

## Optional: File upload path

If your engine JARs are local (not baked into the image), set `SPARK_UPLOAD_PATH` to an
S3 path where Spark can stage files for K8s pods:

```shell
export SPARK_UPLOAD_PATH=s3a://my-bucket/spark-uploads/
```
