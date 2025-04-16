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

# Benchmarks

[WIP] the purpose of this document / folder is to automate running the 100 GB benchmark in AWS so that anyone 
can run this as part of the release process and update the charts that we use in the repo.

This is separate from the general benchmarking advice that we have in the contributor guide but does duplicate much of it.

The documentation assumes that we are using instance type `m6id.2xlarge` (subject to change). 

For now I am using 1024 GB EBS but this can probably be much smaller.

Connect to the instance and clone this repo (or the fork/branch to be used for benchmarking).

```shell
git clone https://github.com/apache/datafusion-comet
```

## Install Prerequisites

This is not fully tested yet. I am copying and pasting from the script for testing. 

```shell
cd dev/benchmarks
./setup.sh
```

## Generate data locally

TODO this is using the new tpchgen-rs project, which is much more convenient that the previous approach, but it 
only generates a single Parquet file per table by default. I have not looked into any performance impact that this may
have on the benchmark.

```shell
cargo install tpchgen-cli
tpchgen-cli -s 100 --format parquet
```

## Spark Benchmark

run `spark-tpch.sh` to run Spark benchmark. This will take around TBD minutes.

## Comet Benchmark

run `comet-tpch.sh`. This will take around TBD minutes.

## Produce Charts

TBD
