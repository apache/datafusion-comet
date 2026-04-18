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
[pending-pr-badge]: https://img.shields.io/github/issues-search/apache/datafusion-comet?query=is%3Apr+is%3Aopen+draft%3Afalse+review%3Arequired+status%3Asuccess&label=Pending%20PRs&logo=github
[pending-pr-url]: https://github.com/apache/datafusion-comet/pulls?q=is%3Apr+is%3Aopen+draft%3Afalse+review%3Arequired+status%3Asuccess+sort%3Aupdated-desc
[maven-badge]: https://img.shields.io/maven-central/v/org.apache.datafusion/comet-spark-spark4.0_2.13
[maven-url]: https://search.maven.org/search?q=g:org.apache.datafusion%20AND%20comet-spark

<img src="docs/source/_static/images/DataFusionComet-Logo-Light.png" width="512" alt="logo"/>

Apache DataFusion Comet is a high-performance accelerator for Apache Spark, built on top of the powerful
[Apache DataFusion] query engine. Comet is designed to significantly enhance the
performance of Apache Spark workloads while leveraging commodity hardware and seamlessly integrating with the
Spark ecosystem without requiring any code changes.

[Apache DataFusion]: https://datafusion.apache.org

## Supported Spark Versions

Comet supports Apache Spark 3.4 and 3.5, and provides experimental support for Spark 4.0. See the
[installation guide](https://datafusion.apache.org/comet/user-guide/installation.html) for the detailed
version, Java, and Scala compatibility matrix.

## What Comet Accelerates

Comet replaces Spark operators and expressions with native Rust implementations that run on Apache DataFusion.
It uses Apache Arrow for zero-copy data transfer between the JVM and native code.

- **Parquet scans**: native Parquet reader integrated with Spark's query planner
- **Apache Iceberg**: accelerated Parquet scans when reading Iceberg tables from Spark
  (see the [Iceberg guide](https://datafusion.apache.org/comet/user-guide/iceberg.html))
- **Shuffle**: native columnar shuffle with support for hash and range partitioning
- **Expressions**: hundreds of supported Spark expressions across math, string, datetime, array,
  map, JSON, hash, and predicate categories
- **Aggregations**: hash aggregate with support for `FILTER (WHERE ...)` clauses
- **Joins**: hash join, sort-merge join, and broadcast join

For the authoritative lists, see the [supported expressions](https://datafusion.apache.org/comet/user-guide/expressions.html)
and [supported operators](https://datafusion.apache.org/comet/user-guide/operators.html) pages.

## Benefits of Using Comet

### Reduce Your Spark Compute Costs

Comet's performance improvements translate directly into infrastructure savings. In many cases, jobs that previously
required a given cluster size can run on roughly half the compute with Comet, producing meaningful cost reductions on
cloud bills, on-prem hardware, and energy usage, with no changes to your existing Spark SQL, DataFrame, or
PySpark code.

### Do More with Commodity Hardware

Comet runs on the hardware you already have. There is no requirement for GPUs, FPGAs, or other specialized
accelerators, so the savings above come from better utilization of the same commodity infrastructure rather than
from hardware upgrades.

### Spark Compatibility

Comet aims for 100% compatibility with all supported versions of Apache Spark, allowing you to integrate Comet into
your existing Spark deployments and workflows seamlessly. With no code changes required, you can immediately harness
the benefits of Comet's acceleration capabilities without disrupting your Spark applications.

## Getting Started

Install Comet by adding the jar for your Spark and Scala version to the Spark classpath and enabling the plugin.
A typical configuration looks like:

```shell
export COMET_JAR=/path/to/comet-spark-spark3.5_2.12-<verison>.jar

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
