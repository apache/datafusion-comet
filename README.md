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

# Apache DataFusion Comet

[![Apache licensed][license-badge]][license-url]
[![Discord chat][discord-badge]][discord-url]
[![Pending PRs][pending-pr-badge]][pending-pr-url]
[![Maven Central][maven-badge]][maven-url]

[license-badge]: https://img.shields.io/badge/license-Apache%20v2-blue.svg
[license-url]: https://github.com/apache/datafusion-comet/blob/main/LICENSE.txt
[discord-badge]: https://img.shields.io/discord/885562378132000778.svg?logo=discord&style=flat-square
[discord-url]: https://discord.gg/3EAr4ZX6JK
[pending-pr-badge]: https://img.shields.io/github/issues-search/apache/datafusion-comet?query=is%3Apr+is%3Aopen+draft%3Afalse+review%3Arequired+-status%3Afailure&label=Pending%20PRs&logo=github
[pending-pr-url]: https://github.com/apache/datafusion-comet/pulls?q=is%3Apr+is%3Aopen+draft%3Afalse+review%3Arequired+-status%3Afailure+sort%3Aupdated-desc
[maven-badge]: https://img.shields.io/maven-central/v/org.apache.datafusion/comet-spark-spark4.0_2.13
[maven-url]: https://search.maven.org/search?q=g:org.apache.datafusion%20AND%20comet-spark

<img src="docs/source/_static/images/DataFusionComet-Logo-Light.png" width="512" alt="logo"/>

Apache DataFusion Comet is a high-performance accelerator for Apache Spark. Comet keeps Spark queries
**Arrow-native end-to-end**: operators, expressions, shuffle, and broadcast all stay in Apache Arrow
columnar format, avoiding the per-row overhead of Spark's row-based engine. Within the Arrow-native
pipeline, operators and expressions execute as Rust code (via the [Apache DataFusion] query engine)
or as JVM code that operates directly on Arrow batches. Comet integrates with the Spark ecosystem
without requiring any code changes.

**Comet provides a ~2x speedup for TPC-DS @ SF 1000 (1TB), resulting in ~50% cost savings.**

That 2x speedup gives you a choice: finish the same Spark workload in half the time on the cluster you already have,
or match your current Spark performance on roughly half the resources. Either way, the gain translates directly into
lower cloud bills, reduced on-prem capacity, and lower energy usage, with no changes to your existing Spark SQL,
DataFrame, or PySpark code. Comet runs on commodity hardware: no GPUs, FPGAs, or other specialized accelerators are
required, so the savings come from better utilization of the infrastructure you already run on.

![](docs/source/_static/images/benchmark-results/0.16.0/tpcds_allqueries.png)

![](docs/source/_static/images/benchmark-results/0.16.0/tpcds_queries_speedup_rel.png)

See the [Comet Benchmarking Guide](https://datafusion.apache.org/comet/contributor-guide/benchmarking.html) for more details.

[Apache DataFusion]: https://datafusion.apache.org

## What Comet Accelerates

Comet accelerates Spark workloads by replacing Spark operators and expressions with high-performance implementations that process Apache Arrow columnar data directly. Most operators are powered by native Rust execution built on Apache DataFusion, while others run efficiently in the JVM on Arrow batches. This unified columnar execution model keeps processing within the Comet engine end-to-end, reducing overhead and delivering faster, more efficient query execution without reverting to Spark's traditional row-based engine.

- **Parquet scans**: native Parquet reader integrated with Spark's query planner
- **Apache Iceberg**: accelerated Parquet scans when reading Iceberg tables from Spark
  (see the [Iceberg guide](https://datafusion.apache.org/comet/user-guide/iceberg.html))
- **Shuffle**: Arrow-IPC columnar shuffle with support for hash and range partitioning, in a native Rust
  implementation paired with a JVM fallback for unsupported partition key types
- **Expressions**: hundreds of supported Spark expressions across math, string, datetime, array,
  map, JSON, hash, and predicate categories
- **Aggregations**: hash aggregate with support for `FILTER (WHERE ...)` clauses
- **Joins**: hash join, sort-merge join, and broadcast join
- **Scala/Java UDFs**: support for keeping Scala/Java scalar UDFs in the Comet pipeline
  via Spark's whole-stage codegen (see the
  [Scala UDF guide](https://datafusion.apache.org/comet/user-guide/scala_java_udfs.html))

For the authoritative lists, see the [supported expressions](https://datafusion.apache.org/comet/user-guide/expressions.html)
and [supported operators](https://datafusion.apache.org/comet/user-guide/operators.html) pages.

## Drop-In Integration

Comet is designed as a drop-in accelerator for Apache Spark, allowing you to integrate Comet into your existing
Spark deployments and workflows seamlessly. With no code changes required, you can immediately harness the
benefits of Comet's acceleration capabilities without disrupting your Spark applications.

## Getting Started

Comet supports Apache Spark 3.4, 3.5, 4.0, and 4.1, and provides experimental support for Spark 4.2. See the
[installation guide](https://datafusion.apache.org/comet/user-guide/installation.html) for the detailed
version, Java, and Scala compatibility matrix.

Install Comet by adding the jar for your Spark and Scala version to the Spark classpath and enabling the plugin.
A typical configuration looks like:

```shell
export COMET_JAR=/path/to/comet-spark-spark3.5_2.12-<version>.jar

$SPARK_HOME/bin/spark-shell \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=4g
```

For full installation instructions, published jar downloads, and configuration reference, see the
[installation guide](https://datafusion.apache.org/comet/user-guide/installation.html) and the
[configuration reference](https://datafusion.apache.org/comet/user-guide/configs.html).

## Community

Join the [DataFusion Slack and Discord channels](https://datafusion.apache.org/contributor-guide/communication.html)
to connect with other users, ask questions, and share your experiences with Comet.

## Contributing

We welcome contributions from the community to help improve and enhance Apache DataFusion Comet. Whether it's fixing
bugs, adding new features, writing documentation, or optimizing performance, your contributions are invaluable in
shaping the future of Comet. Check out our
[contributor guide](https://datafusion.apache.org/comet/contributor-guide/contributing.html) to get started.

## License

Apache DataFusion Comet is licensed under the Apache License 2.0. See the [LICENSE.txt](LICENSE.txt) file for details.
