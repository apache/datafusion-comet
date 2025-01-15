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

# Comet Fuzz

Comet Fuzz is a standalone project for generating random data and queries and executing queries against Spark 
with Comet disabled and enabled and checking for incompatibilities.

Although it is a simple tool it has already been useful in finding many bugs.

Comet Fuzz is inspired by the [SparkFuzz](https://ir.cwi.nl/pub/30222) paper from Databricks and CWI.

## Roadmap

Planned areas of improvement:

- ANSI mode
- Support for all data types, expressions, and operators supported by Comet
- IF and CASE WHEN expressions
- Complex (nested) expressions
- Literal scalar values in queries
- Add option to avoid grouping and sorting on floating-point columns
- Improve join query support:
  - Support joins without join keys
  - Support composite join keys
  - Support multiple join keys
  - Support join conditions that use expressions

## Usage

Build the jar file first.

```shell
mvn package
```

Set appropriate values for `SPARK_HOME`, `SPARK_MASTER`, and `COMET_JAR` environment variables and then use
`spark-submit` to run CometFuzz against a Spark cluster.

### Generating Data Files

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --class org.apache.comet.fuzz.Main \
    target/comet-fuzz-spark3.4_2.12-0.6.0-SNAPSHOT-jar-with-dependencies.jar \
    data --num-files=2 --num-rows=200 --num-columns=100 --exclude-negative-zero
```

There is an optional `--exclude-negative-zero` flag for excluding `-0.0` from the generated data, which is 
sometimes useful because we already know that we often have different behavior for this edge case due to 
differences between Rust and Java handling of this value.

### Generating Queries

Generate random queries that are based on the available test files.

```shell
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --class org.apache.comet.fuzz.Main \
    target/comet-fuzz-spark3.4_2.12-0.6.0-SNAPSHOT-jar-with-dependencies.jar \
    queries --num-files=2 --num-queries=500
```

Note that the output filename is currently hard-coded as `queries.sql`

### Execute Queries

```shell
export COMET_JAR=`pwd`/../spark/target/comet-spark-spark3.4_2.12-0.6.0-SNAPSHOT.jar
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.enabled=true \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.exec.shuffle.enabled=true \
    --conf spark.comet.exec.shuffle.mode=auto \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --class org.apache.comet.fuzz.Main \
    target/comet-fuzz-spark3.4_2.12-0.6.0-SNAPSHOT-jar-with-dependencies.jar \
    run --num-files=2 --filename=queries.sql
```

Note that the output filename is currently hard-coded as `results-${System.currentTimeMillis()}.md`
