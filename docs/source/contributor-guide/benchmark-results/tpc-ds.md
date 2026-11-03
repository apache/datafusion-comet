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

# TPC-DS

The following benchmarks were performed on an EKS cluster (`r6i` instances with EBS storage) with data stored in S3,
using Apache Spark 4.1.3 (Scala 2.13), Comet 1.2.0, and Apache Iceberg 1.11.0 tables (Parquet data files compressed with zstd,
Hadoop catalog). Both engines read the same scale factor 1000 (1 TB) tables. Each query was run twice and the mean time is
reported. Comet reads the Iceberg tables with its native Iceberg scan, and Spark reads them with the Iceberg Spark runtime.

Across all 103 queries, Comet finished in 827 seconds compared with 1,460 seconds for Spark, a 1.8x speedup.

## Benchmark Results

Total time to run all queries (lower is better).

![](../../_static/images/benchmark-results/1.2.0/tpcds_allqueries.png)

Per-query breakdown showing the relative performance of Spark and Comet.

![](../../_static/images/benchmark-results/1.2.0/tpcds_queries_compare.png)

How much Comet accelerates each query in relative terms.

![](../../_static/images/benchmark-results/1.2.0/tpcds_queries_speedup_rel.png)

How much Comet accelerates each query in absolute terms.

![](../../_static/images/benchmark-results/1.2.0/tpcds_queries_speedup_abs.png)

## Configuration

Only settings that differ from the defaults are listed.

Common:

```properties
spark.driver.memory=128G
spark.driver.maxResultSize=16g
spark.executor.instances=32
spark.executor.cores=16
spark.memory.fraction=0.6
spark.memory.storageFraction=0.2
spark.sql.shuffle.partitions=512
spark.sql.broadcastTimeout=900
spark.sql.sources.ignoreDataLocality=true
spark.hadoop.parquet.io.stats.enabled=false
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type=hadoop
# Kubernetes CPU constraints
spark.kubernetes.driver.request.cores=32
spark.kubernetes.driver.limit.cores=32
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
spark.executor.memory=64G
spark.executor.memoryOverhead=10G
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=32G
spark.plugins=org.apache.spark.CometPlugin
spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.comet.CometSparkSessionExtensions
spark.comet.exec.memoryPool.fraction=0.8
spark.comet.scan.icebergNative.dataFileConcurrencyLimit=4
```
