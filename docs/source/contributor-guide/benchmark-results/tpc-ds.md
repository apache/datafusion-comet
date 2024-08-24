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

The following benchmarks were performed on a Linux workstation with PCIe 5, AMD 7950X CPU (16 cores), 128 GB RAM, and
data stored locally on NVMe storage. Performance characteristics will vary in different environments and we encourage
you to run these benchmarks in your own environments.

Comet can not yet run all TPC-DS queries natively. The tracking issue is [#858](https://github.com/apache/datafusion-comet/issues/858).

Comet currently provides a 21% speedup for TPC-DS @ SF=100GB.

![](../../_static/images/benchmark-results/2024-08-23/tpcds_allqueries.png)

Here is a breakdown showing relative performance of Spark and Comet for each query.

![](../../_static/images/benchmark-results/2024-08-23/tpcds_queries_compare.png)

The following chart shows how much Comet currently accelerates each query from the benchmark.

![](../../_static/images/benchmark-results/2024-08-23/tpcds_queries_speedup_abs.png)

The raw results of these benchmarks in JSON format is available here:

- [Spark](./2024-08-23/spark-tpcds.json)
- [Comet](./2024-08-23/comet-tpcds.json)
