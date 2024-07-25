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

[license-badge]: https://img.shields.io/badge/license-Apache%20v2-blue.svg
[license-url]: https://github.com/apache/datafusion-comet/blob/main/LICENSE.txt
[discord-badge]: https://img.shields.io/discord/885562378132000778.svg?logo=discord&style=flat-square
[discord-url]: https://discord.gg/3EAr4ZX6JK

<img src="docs/source/_static/images/DataFusionComet-Logo-Light.png" width="512" alt="logo"/>

Apache DataFusion Comet is a high-performance accelerator for Apache Spark, built on top of the powerful
[Apache DataFusion](https://datafusion.apache.org) query engine. Comet is designed to significantly enhance the
performance of Apache Spark workloads while leveraging commodity hardware and seamlessly integrating with the
Spark ecosystem without requiring any code changes.

# Benefits of Using Comet

## Run Spark Queries at DataFusion Speeds

Comet delivers a performance speedup for many queries, enabling faster data processing and shorter time-to-insights.

The following chart shows the time it takes to run the 22 TPC-H queries against 100 GB of data in Parquet format 
using a single executor with 8 cores. See the [Comet Benchmarking Guide](https://datafusion.apache.org/comet/contributor-guide/benchmarking.html)
for details of the environment used for these benchmarks.

When using Comet, the overall run time is reduced from 649 seconds to 433 seconds, a 1.5x speedup, with some queries
showing a 2x-3x speedup.

Running the same queries with DataFusion standalone (without Spark) using the same number of cores results in a 3.9x 
speedup compared to Spark.

Comet is not yet achieving full DataFusion speeds in all cases, but with future work we aim to provide a 2x-4x speedup 
for a broader set of queries.

![](docs/source/_static/images/benchmark-results/2024-07-19/tpch_allqueries.png)

Here is a breakdown showing relative performance of Spark, Comet, and DataFusion for each TPC-H query.

![](docs/source/_static/images/benchmark-results/2024-07-19/tpch_queries_compare.png)

The following chart shows how much Comet currently accelerates each query from the benchmark. Performance optimization
is an ongoing task, and we welcome contributions from the community to help achieve even greater speedups in the future.

![](docs/source/_static/images/benchmark-results/2024-07-19/tpch_queries_speedup.png)

These benchmarks can be reproduced in any environment using the documentation in the 
[Comet Benchmarking Guide](https://datafusion.apache.org/comet/contributor-guide/benchmarking.html). We encourage 
you to run your own benchmarks.

## Use Commodity Hardware

Comet leverages commodity hardware, eliminating the need for costly hardware upgrades or
specialized hardware accelerators, such as GPUs or FGPA. By maximizing the utilization of commodity hardware, Comet 
ensures cost-effectiveness and scalability for your Spark deployments.

## Spark Compatibility

Comet aims for 100% compatibility with all supported versions of Apache Spark, allowing you to integrate Comet into
your existing Spark deployments and workflows seamlessly. With no code changes required, you can immediately harness
the benefits of Comet's acceleration capabilities without disrupting your Spark applications.

## Tight Integration with Apache DataFusion

Comet tightly integrates with the core Apache DataFusion project, leveraging its powerful execution engine. With
seamless interoperability between Comet and DataFusion, you can achieve optimal performance and efficiency in your
Spark workloads.

## Active Community

Comet boasts a vibrant and active community of developers, contributors, and users dedicated to advancing the
capabilities of Apache DataFusion and accelerating the performance of Apache Spark.

## Getting Started

To get started with Apache DataFusion Comet, follow the
[installation instructions](https://datafusion.apache.org/comet/user-guide/installation.html). Join the
[DataFusion Slack and Discord channels](https://datafusion.apache.org/contributor-guide/communication.html) to connect
with other users, ask questions, and share your experiences with Comet.

## Contributing

We welcome contributions from the community to help improve and enhance Apache DataFusion Comet. Whether it's fixing
bugs, adding new features, writing documentation, or optimizing performance, your contributions are invaluable in
shaping the future of Comet. Check out our
[contributor guide](https://datafusion.apache.org/comet/contributor-guide/contributing.html) to get started.

## License

Apache DataFusion Comet is licensed under the Apache License 2.0. See the [LICENSE.txt](LICENSE.txt) file for details.

## How to Release

Here are the steps to cut a new Comet release.

1. After everything is committed to the `main-apple` branch. Checkout
   `main-apple-release` branch and rebase it on top of `main-apple`:
   ```
   git rebase main-apple
   ```
   And then push the `main-apple-release` branch to remote. This will trigger the
   release job.

2. Make sure the [release job](https://rio.apple.com/projects/aci-ipr-apache-arrow-datafusion-comet)
   finishes successfully.

3. Go back to the `main-apple` branch and bump the SNAPSHOT version in the `pom.xml` file and `rio.yaml` file, and
   push the `main-apple` branch to remote.

4. Create release notes in [Releases](https://github.pie.apple.com/IPR/apache-arrow-datafusion-comet/releases).

## Acknowledgments

We would like to express our gratitude to the Apache DataFusion community for their support and contributions to
Comet. Together, we're building a faster, more efficient future for big data processing with Apache Spark.
