# Benchmarks

[WIP] the purpose of this document / folder is to automate running the 100 GB benchmark in AWS so that anyone 
can run this as part of the release process and update the charts that we use in the repo.

This is separate from the general benchmarking advice that we have in the contributor guide but does duplicate much of it.

The documentation assumes that we are using instance type `m5.2xlarge` (subject to change). 

For now I am using 1024 GB EBS but this can probably be much smaller.


Connect to the instance and clone this repo (or the fork/branch to be used for benchmarking).

```shell
git clone https://github.com/apache/datafusion-comet
```

## Install Prerequisites

This is not tested yet. I am copying and pasting from the script for testing. 

```shell
cd dev/benchmarks
./setup.sh
```
Generate data locally

```shell
cargo install tpchgen-cli
tpchgen-cli -s 100 --format parquet
```

## Spark Benchmark

run `spark-tpch.sh` to run Spark benchmark. This will take around one hour.

## Comet Benchmark

TBD

## Produce Charts

TBD
