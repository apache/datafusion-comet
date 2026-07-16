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

# Apache DataFusion Comet: Benchmarks Derived From TPC-H

The following benchmarks were performed on an EKS cluster (`r6i.24xlarge` instances with EBS storage) with data stored in S3.

## Benchmark Results

Total time to run all queries (lower is better).

![](../../_static/images/benchmark-results/0.16.0/tpch_allqueries_with_tuned.png)

The following charts are based on the tuned run using hash join.

Per-query breakdown showing the relative performance of Spark and Comet.

![](../../_static/images/benchmark-results/0.16.0/tpch_queries_compare.png)

How much Comet accelerates each query in relative terms.

![](../../_static/images/benchmark-results/0.16.0/tpch_queries_speedup_rel.png)

How much Comet accelerates each query in absolute terms.

![](../../_static/images/benchmark-results/0.16.0/tpch_queries_speedup_abs.png)

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
```

Spark:

```properties
spark.executor.memory=64G
spark.executor.memoryOverhead=10G
```

Comet:

```properties
spark.executor.memory=32G
spark.executor.memoryOverhead=10G
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=32G
```

### Comet (Tuned)

```properties
spark.comet.exec.replaceSortMergeJoin=true
spark.comet.memoryPool.fraction=0.8
```
