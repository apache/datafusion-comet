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

To track progress on performance, we regularly run benchmarks derived from TPC-H and TPC-DS.

The benchmarking scripts are contained at [https://github.com/apache/datafusion-comet/tree/main/dev/benchmarks](https://github.com/apache/datafusion-comet/tree/main/dev/benchmarks).

Data generation scripts are available in the [DataFusion Benchmarks](https://github.com/apache/datafusion-benchmarks) GitHub repository.

## Current Benchmark Results

The published benchmarks are performed on a Linux workstation with PCIe 5, AMD 7950X CPU (16 cores), 128 GB RAM, and
data stored locally in Parquet format on NVMe storage. Performance characteristics will vary in different environments
and we encourage you to run these benchmarks in your own environments.

The operating system used was Ubuntu 22.04.5 LTS.

- [Benchmarks derived from TPC-H](benchmark-results/tpc-h)
- [Benchmarks derived from TPC-DS](benchmark-results/tpc-ds)

## Benchmarking Guides

Available benchmarking guides:

- [Benchmarking on macOS](benchmarking_macos.md)
- [Benchmarking on AWS EC2](benchmarking_aws_ec2)

## Comet Benchmark Suite

Comet includes a comprehensive benchmark suite located in `spark/src/test/scala/org/apache/spark/sql/benchmark/` that provides detailed performance measurements for various aspects of query execution. The benchmark suite compares Spark native execution with Comet acceleration to measure performance improvements across different workload patterns.

### Benchmark Categories

#### **Expression-Level Benchmarks (Micro)**
These benchmarks focus on specific expression types and operations:

- **CometArithmeticBenchmark**: Tests arithmetic operations (add, subtract, multiply, divide) on integer and decimal types
- **CometStringExpressionBenchmark**: Tests string operations including substring, space, trim, length, concat, and regex_replace
- **CometConditionalExpressionBenchmark**: Tests conditional expressions like CASE WHEN and IF statements
- **CometPredicateExpressionBenchmark**: Tests predicate operations including IN expressions and filtering
- **CometDatetimeExpressionBenchmark**: Tests date/time operations including truncation at various granularities (year, month, day, hour, etc.)

#### **Operator-Level Benchmarks**
These benchmarks focus on query execution operators:

- **CometAggregateBenchmark**: Tests aggregation operations (SUM, MIN, MAX, COUNT, COUNT DISTINCT) with varying cardinalities
- **CometShuffleBenchmark**: Tests shuffle operations using CometShuffleManager with various data types and partition counts
- **CometExecBenchmark**: Tests general execution performance with project and filter operations at different selectivity levels

#### **I/O and Scan Benchmarks**
- **CometReadBenchmark**: Comprehensive scan performance testing including:
  - Direct Parquet reader performance across all data types
  - Iceberg table format support
  - Encrypted Parquet column scanning
  - Dictionary encoding optimizations
  - Wide table scanning (single column from tables with 10-100 columns)
  - Filter pushdown with varying selectivity

#### **Full Query Suite Benchmarks**
- **CometTPCHQueryBenchmark**: Runs all 22 TPC-H queries with configurable scale factors
- **CometTPCDSQueryBenchmark**: Runs TPC-DS queries (v1.4 and v2.7.0 variants) with configurable scale factors
- **CometTPCDSMicroBenchmark**: Runs 25 targeted micro-benchmarks using TPC-DS data, focusing on specific operations like decimal handling, joins, aggregations, and string functions

### Running Benchmarks

#### **Basic Command Structure**
```bash
SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-<CLASS_NAME> [-- --args]
```

The `SPARK_GENERATE_BENCHMARK_FILES=1` environment variable is required to output benchmark results to files in the `spark/benchmarks/` directory.

#### **Expression and Operator Benchmark Examples**
```bash
# Arithmetic operations
SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometArithmeticBenchmark

# String operations
SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometStringExpressionBenchmark

# Aggregation operations
SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometAggregateBenchmark

# Shuffle operations
SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometShuffleBenchmark

# Scan operations
SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometReadBenchmark
```

#### **TPC Benchmark Examples**

First, generate the required test data:

```bash
# Generate TPC-H data (1GB scale factor)
cd $COMET_HOME
make benchmark-org.apache.spark.sql.GenTPCHData -- --location /tmp --scaleFactor 1

# Generate TPC-DS data (requires tpcds-kit)
cd /tmp && git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools && make OS=MACOS  # Use OS=LINUX on Linux
cd $COMET_HOME && mkdir /tmp/tpcds
make benchmark-org.apache.spark.sql.GenTPCDSData -- --dsdgenDir /tmp/tpcds-kit/tools --location /tmp/tpcds --scaleFactor 1
```

Then run the benchmarks:

```bash
# Run TPC-H benchmarks
SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometTPCHQueryBenchmark -- --data-location /tmp/tpch/sf1_parquet

# Run TPC-DS benchmarks
SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometTPCDSQueryBenchmark -- --data-location /tmp/tpcds

# Run TPC-DS micro benchmarks
SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometTPCDSMicroBenchmark -- --data-location /tmp/tpcds
```

#### **Benchmark Options**
Many benchmarks support additional options:

```bash
# Filter specific queries (e.g., only run TPC-H queries 3, 5, and 13)
SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometTPCHQueryBenchmark -- --data-location /tmp/tpch/sf1_parquet --query-filter q3,q5,q13

# Enable Cost-Based Optimization
SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometTPCDSQueryBenchmark -- --data-location /tmp/tpcds --cbo

# Use larger scale factors
make benchmark-org.apache.spark.sql.GenTPCHData -- --location /tmp --scaleFactor 10
```

### Understanding Benchmark Results

#### **Execution Modes Compared**
The benchmarks typically compare several execution modes:

1. **Spark Native**: Standard Spark execution without Comet
2. **Comet Scan**: Comet scan acceleration with Spark execution
3. **Comet Scan + Exec**: Full Comet acceleration (scan + execution operators)
4. **Comet Native DataFusion**: Full native DataFusion execution path
5. **Comet Native Iceberg**: Iceberg-compatible native execution

#### **Key Metrics**
- **Execution Time**: Wall clock time for query execution
- **Throughput**: Rows processed per second
- **Speedup Ratio**: Performance improvement over Spark native
- **Memory Usage**: Peak memory consumption during execution

#### **Output Location**
Benchmark results are written to:
- Directory: `spark/benchmarks/`
- File format: `<BenchmarkClass>-results.txt`
- Content: Detailed timing results and performance comparisons

### Advanced Configuration

#### **Environment Variables**
- `SPARK_GENERATE_BENCHMARK_FILES=1`: Required for file output
- `MAVEN_OPTS`: JVM options (default: `-Xmx20g`)
- `PROFILES`: Maven profiles for Spark versions (default: Spark 3.5)

#### **Key Configuration Options**
Benchmarks can be tuned using Spark configuration:

```bash
# Memory configuration
--conf spark.executor.memory=8g
--conf spark.executor.memoryOverhead=2g

# Shuffle configuration
--conf spark.sql.shuffle.partitions=200
--conf spark.comet.columnar.shuffle.async.thread.num=4
--conf spark.comet.columnar.shuffle.spill.threshold=134217728

# Comet-specific settings
--conf spark.comet.enabled=true
--conf spark.comet.exec.enabled=true
--conf spark.comet.nativeScan.impl=COMET  # or DATAFUSION, ICEBERG_COMPAT
```

### Benchmark Development

All benchmarks extend `CometBenchmarkBase`, which provides:

- **Test Data Generation**: Helper methods for creating Parquet tables with various compression and encryption options
- **Comet Integration**: Automatic setup of SparkSession with Comet extensions
- **Comparison Framework**: `runWithComet()` method to run tests with Comet both enabled and disabled
- **Result Collection**: Standardized timing and metrics collection

When developing new benchmarks, consider:

1. **Test Data Variety**: Include different data types, null patterns, and cardinalities
2. **Realistic Workloads**: Base tests on common query patterns
3. **Multiple Scenarios**: Test various configurations (dictionary encoding, compression, etc.)
4. **Clear Naming**: Use descriptive benchmark and case names
5. **Resource Management**: Ensure proper cleanup of temporary tables and files

### Benchmark Suite Summary

| Benchmark Type | Count | Focus Area | Example Use Case |
|---|---|---|---|
| Expression-Level | 5 | Individual operations | Optimize arithmetic functions |
| Operator-Level | 3 | Query operators | Optimize aggregation algorithms |
| I/O & Scan | 1 | Data access | Optimize Parquet reading |
| Full Query Suite | 3 | End-to-end | Validate overall performance |

The benchmark suite provides comprehensive coverage from low-level expression evaluation to full TPC query execution, enabling both targeted optimization and overall performance validation.
