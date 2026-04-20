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

# Apache DataFusion Comet: Benchmarks Derived From TPC-DS

The following benchmarks were performed on an EKS cluster (`r61.2xlarge` instances with EBS storage) with data stored in S3.

The tracking issue for improving TPC-DS performance is [#858](https://github.com/apache/datafusion-comet/issues/858).

## Configuration

Common:

```properties
spark.executor.instances=32
spark.executor.cores=16
spark.memory.fraction=0.6
spark.memory.storageFraction=0.2
# Kubernetes CPU constraints
spark.kubernetes.executor.request.cores=8
spark.kubernetes.executor.limit.cores=8
spark.kubernetes.driver.request.cores=32
spark.kubernetes.driver.limit.cores=32
spark.driver.memory=128G
```

Spark:

```properties
spark.executor.memory=64G
spark.executor.memoryOverhead=10G
# K8s executor pod memory limit: 74G (64G heap + 10G overhead)
```

Comet:

```properties
spark.executor.memory=64G
spark.executor.memoryOverhead=10G
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=32G
# K8s executor pod memory limit: 106G (64G heap + 10G overhead + 32G off-heap)
spark.comet.memoryPool.fraction=0.8
spark.comet.scan.impl=auto
spark.comet.exec.shuffle.enabled=true
```

## Benchmark Results

![](../../_static/images/benchmark-results/0.15.0/tpcds_allqueries.png)

Here is a breakdown showing relative performance of Spark and Comet for each query.

![](../../_static/images/benchmark-results/0.15.0/tpcds_queries_compare.png)

The following chart shows how much Comet currently accelerates each query from the benchmark in relative terms.

![](../../_static/images/benchmark-results/0.15.0/tpcds_queries_speedup_rel.png)

The following chart shows how much Comet currently accelerates each query from the benchmark in absolute terms.

![](../../_static/images/benchmark-results/0.15.0/tpcds_queries_speedup_abs.png)
