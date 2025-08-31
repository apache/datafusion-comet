<!---
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

# Comet Overview

Apache DataFusion Comet is a high-performance accelerator for Apache Spark, built on top of the powerful
[Apache DataFusion] query engine. Comet is designed to significantly enhance the
performance of Apache Spark workloads while leveraging commodity hardware and seamlessly integrating with the
Spark ecosystem without requiring any code changes.

[Apache DataFusion]: https://datafusion.apache.org

The following diagram provides an overview of Comet's architecture.

![Comet Overview](../_static/images/comet-overview.png)

## Architecture

The following diagram shows how Comet integrates with Apache Spark.

![Comet System Diagram](../_static/images/comet-system-diagram.png)

## Feature Parity with Apache Spark

The project strives to keep feature parity with Apache Spark, that is,
users should expect the same behavior (w.r.t features, configurations,
query results, etc) with Comet turned on or turned off in their Spark
jobs. In addition, Comet extension should automatically detect unsupported
features and fallback to Spark engine.

## Comparison with other open-source Spark accelerators

There are two other major open-source Spark accelerators:

- [Apache Gluten (incubating)](https://github.com/apache/incubator-gluten)
- [NVIDIA Spark RAPIDS](https://github.com/NVIDIA/spark-rapids)

We have a detailed guide [comparing Apache DataFusion Comet with Apache Gluten].

Spark RAPIDS is a solution that provides hardware acceleration on NVIDIA GPUs. Comet does not require specialized 
hardware.

[comparing Apache DataFusion Comet with Apache Gluten]: latest/gluten_comparison.md

## Getting Started

Refer to the [Comet Installation Guide] to get started.

[Comet Installation Guide]: latest/installation.md
