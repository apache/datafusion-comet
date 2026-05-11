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

## Configuration

<!-- AUTO-GENERATED:config:START -->
### Common

| Property | Value |
| --- | --- |
| spark.cores.max | 16 |
| spark.driver.memory | 8G |
| spark.eventLog.dir | /tmp/spark-events |
| spark.eventLog.enabled | true |
| spark.executor.cores | 8 |
| spark.executor.instances | 2 |
| spark.executor.memory | 16g |
| spark.hadoop.fs.s3a.aws.credentials.provider | com.amazonaws.auth.DefaultAWSCredentialsProviderChain |
| spark.hadoop.fs.s3a.impl | org.apache.hadoop.fs.s3a.S3AFileSystem |
| spark.memory.offHeap.enabled | true |
| spark.memory.offHeap.size | 16g |
| spark.rdd.compress | True |
| spark.serializer.objectStreamReset | 100 |
| spark.sql.warehouse.dir | file:/home/andy/git/apache/datafusion-comet/benchmarks/tpc/spark-warehouse |

### Spark

| Property | Value |
| --- | --- |
| spark.app.submitTime | 1776548361806 |

### Comet

| Property | Value |
| --- | --- |
| spark.app.initial.jar.urls | spark://10.0.0.118:37357/jars/comet-spark-spark3.5_2.12-0.15.0.jar |
| spark.app.submitTime | 1776548868968 |
| spark.comet.expression.Cast.allowIncompatible | true |
| spark.comet.scan.impl | native_datafusion |
| spark.plugins | org.apache.spark.CometPlugin |
| spark.shuffle.manager | org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager |
| spark.sql.extensions | org.apache.comet.CometSparkSessionExtensions |
<!-- AUTO-GENERATED:config:END -->

### Comet (Tuned)

Hand-tuned configuration used for the "with tuned" chart below:

```properties
spark.comet.exec.replaceSortMergeJoin=true
spark.comet.memoryPool.fraction=0.8
```

## Benchmark Results

<!-- AUTO-GENERATED:charts:START -->
![](../../_static/images/benchmark-results/0.16.0/tpch_allqueries.png)

![](../../_static/images/benchmark-results/0.16.0/tpch_queries_compare.png)

![](../../_static/images/benchmark-results/0.16.0/tpch_queries_speedup_rel.png)

![](../../_static/images/benchmark-results/0.16.0/tpch_queries_speedup_abs.png)
<!-- AUTO-GENERATED:charts:END -->

### With hand-tuned Comet configuration

The following chart adds a "Comet (Tuned)" run on top of the default Spark vs. Comet comparison.

![](../../_static/images/benchmark-results/0.15.0/tpch_allqueries_with_tuned.png)
