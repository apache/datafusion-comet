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

# Comet Benchmarking Guide

To track progress on performance, we regularly run benchmarks derived from TPC-H and TPC-DS. Data generation and 
benchmarking documentation and scripts are available in the [DataFusion Benchmarks](https://github.com/apache/datafusion-benchmarks) GitHub repository.

Available benchmarking guides:

- [Benchmarking on macOS](benchmarking_macos.md) 
- [Benchmarking on AWS EC2](benchmarking_aws_ec2) 

We also have many micro benchmarks that can be run from an IDE located [here](https://github.com/apache/datafusion-comet/tree/main/spark/src/test/scala/org/apache/spark/sql/benchmark). 

## Current Benchmark Results

- [Benchmarks derived from TPC-H](benchmark-results/tpc-h)
- [Benchmarks derived from TPC-DS](benchmark-results/tpc-ds)



