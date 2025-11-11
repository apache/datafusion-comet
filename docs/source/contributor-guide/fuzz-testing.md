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

# Fuzz Testing

## Overview

Comet Fuzz is a standalone property-based testing framework located in the `fuzz-testing/` directory. It helps ensure compatibility between Comet and Apache Spark by automatically generating random data and queries, then executing them against both Spark (with Comet disabled) and Comet (with Comet enabled) to detect discrepancies.

The framework has proven valuable in discovering numerous bugs and edge cases that might not be caught by traditional unit or integration tests.

## Inspiration

Comet Fuzz is inspired by the [SparkFuzz](https://ir.cwi.nl/pub/30222) paper from Databricks and CWI, which demonstrates the effectiveness of property-based testing for finding bugs in query engines.

## Architecture

The fuzz testing framework consists of several key components:

### Core Components

- **`Meta.scala`**: Defines metadata about supported data types, expressions, and operators
- **`QueryGen.scala`**: Generates random SQL queries based on available test data
- **`QueryRunner.scala`**: Executes queries against Spark with Comet disabled and enabled, comparing results
- **`Utils.scala`**: Utility functions for data generation and comparison
- **`Main.scala`**: Entry point with CLI for different fuzz testing modes
- **`ComparisonTool.scala`**: Standalone tool for comparing existing datasets

### Testing Workflow

1. **Data Generation**: Create random Parquet files with various data types
2. **Query Generation**: Generate random SQL queries that operate on the test data
3. **Execution**: Run queries twice - once with Comet disabled (Spark baseline) and once with Comet enabled
4. **Comparison**: Compare results and report any discrepancies

## Getting Started

### Prerequisites

Before running fuzz tests, ensure you have:

- Comet built and installed: `mvn install -DskipTests` from the project root
- `SPARK_HOME` pointing to your Spark installation
- `SPARK_MASTER` set to your Spark cluster endpoint (or `local[*]` for local testing)
- `COMET_JAR` pointing to the Comet jar file

### Building the Fuzz Testing Jar

From the `fuzz-testing/` directory:

```shell
mvn package
```

This creates a fat jar with all dependencies:
```
target/comet-fuzz-spark3.5_2.12-0.12.0-SNAPSHOT-jar-with-dependencies.jar
```

## Usage

### 1. Generate Test Data

Create random Parquet files to serve as test datasets:

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --class org.apache.comet.fuzz.Main \
    target/comet-fuzz-spark3.5_2.12-0.12.0-SNAPSHOT-jar-with-dependencies.jar \
    data --num-files=2 --num-rows=200 --exclude-negative-zero --generate-arrays --generate-structs --generate-maps
```

#### Options

- `--num-files`: Number of Parquet files to generate
- `--num-rows`: Number of rows per file
- `--exclude-negative-zero`: Exclude `-0.0` from generated data (useful since Rust and Java handle this edge case differently)
- `--generate-arrays`: Include array columns
- `--generate-structs`: Include struct columns
- `--generate-maps`: Include map columns

### 2. Generate Random Queries

Generate SQL queries based on the available test files:

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --class org.apache.comet.fuzz.Main \
    target/comet-fuzz-spark3.5_2.12-0.12.0-SNAPSHOT-jar-with-dependencies.jar \
    queries --num-files=2 --num-queries=500
```

#### Options

- `--num-files`: Number of data files to use (should match data generation)
- `--num-queries`: Number of random queries to generate

**Note**: Queries are currently saved to a hard-coded filename: `queries.sql`

### 3. Execute and Compare

Run the generated queries with both Spark and Comet, comparing results:

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=16G \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.comet.enabled=true \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.exec.shuffle.enabled=true \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --class org.apache.comet.fuzz.Main \
    target/comet-fuzz-spark3.5_2.12-0.12.0-SNAPSHOT-jar-with-dependencies.jar \
    run --num-files=2 --filename=queries.sql
```

Results are saved to: `results-${System.currentTimeMillis()}.md`

### 4. Compare Existing Datasets (Optional)

To compare pre-existing Parquet datasets (e.g., TPC-H results):

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --class org.apache.comet.fuzz.ComparisonTool \
    target/comet-fuzz-spark3.5_2.12-0.12.0-SNAPSHOT-jar-with-dependencies.jar \
    compareParquet --input-spark-folder=/tmp/tpch/spark --input-comet-folder=/tmp/tpch/comet
```

The tool recursively compares all Parquet datasets in matching subfolders.

## Supported Features

### Data Types

The fuzzer can generate the following data types:
- Primitive types: Boolean, Byte, Short, Integer, Long, Float, Double, String, Binary, Date, Timestamp, Decimal
- Complex types: Array, Struct, Map (when corresponding flags are enabled)

### Query Operations

Generated queries may include:
- SELECT with various expressions
- WHERE clauses with filter predicates
- GROUP BY with aggregations
- ORDER BY with sorting
- JOIN operations (limited support currently)
- Aggregate functions (SUM, AVG, COUNT, MIN, MAX, etc.)

## Interpreting Results

### Successful Run

When all queries produce matching results between Spark and Comet, the output will indicate success.

### Discrepancies Found

When discrepancies are found, the results file will contain:
- The failing query
- Expected results (from Spark)
- Actual results (from Comet)
- Diff highlighting differences

This information is invaluable for debugging and fixing compatibility issues.

## Known Limitations and Roadmap

Current limitations (see `fuzz-testing/README.md` for the latest):

- **ANSI mode**: Not fully tested
- **Expression coverage**: Not all Comet-supported expressions are included in query generation
- **Conditional expressions**: IF and CASE WHEN support is limited
- **Complex expressions**: Deeply nested expressions are not well-tested
- **Literal values**: Queries primarily use column references, not literal scalars
- **Floating-point grouping/sorting**: May want to add option to avoid grouping/sorting on float/double columns due to known edge cases
- **Join support**:
  - Limited to simple equi-joins
  - No support for joins without join keys
  - No support for composite or multiple join keys
  - No support for join conditions using complex expressions

## Best Practices

### Running Regularly

Fuzz testing should be run regularly during development to catch regressions early:

```shell
# Quick sanity check
mvn package && ./run-fuzz-tests.sh --num-files=2 --num-rows=100 --num-queries=100

# Comprehensive overnight run
mvn package && ./run-fuzz-tests.sh --num-files=10 --num-rows=10000 --num-queries=10000
```

### Targeting Specific Features

When implementing a new feature, generate data and queries specifically for that feature:

```shell
# Test array functionality
spark-submit ... data --generate-arrays --num-files=5 --num-rows=1000
spark-submit ... queries --num-queries=1000
# Edit queries.sql to focus on array operations
spark-submit ... run --filename=queries.sql
```

### Reproducing Issues

When a fuzz test finds an issue:

1. Extract the failing query from the results file
2. Create a minimal reproduction case
3. Add as a unit test in the appropriate test suite
4. Fix the bug
5. Verify the fuzz test passes

### Debugging Failures

Use Spark's explain to understand query plans:

```scala
// In Spark shell with Comet enabled
val df = spark.sql("SELECT ...")
df.explain(extended = true)
```

Enable verbose logging to see why Comet falls back to Spark:

```
--conf spark.comet.explain.verbose=true
--conf spark.comet.logFallbackReasons=true
```

## Contributing Improvements

The fuzz testing framework is continually evolving. Contributions are welcome in several areas:

- **More expression types**: Add support for expressions not yet covered
- **Better query generation**: Improve randomization strategies
- **Data generators**: Add generators for edge cases and corner cases
- **Performance testing**: Add timing comparisons between Spark and Comet
- **Automated CI integration**: Run fuzz tests automatically on PRs

See the roadmap in `fuzz-testing/README.md` for planned improvements.
