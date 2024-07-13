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

# Installing DataFusion Comet

Make sure the following requirements are met and software installed on your machine.

## Supported Platforms

- Linux
- Apple OSX (Intel and Apple Silicon)

## Requirements

- Apache Spark 3.3, or 3.4
- JDK 8 and up
- GLIBC 2.17 (Centos 7) and up

## Using a Published Release

There are no public releases available yet, so it is necessary to build from source as described in the next section.

## Building From Source

Clone the repository:

```console
git clone https://github.com/apache/datafusion-comet.git
```

Build Comet for a specific Spark version:

```console
cd datafusion-comet
make release PROFILES="-Pspark-3.4"
```

Note that the project builds for Scala 2.12 by default but can be built for Scala 2.13 using an additional profile:

```console
make release PROFILES="-Pspark-3.4 -Pscala-2.13"
```

To build Comet from the source distribution on an isolated environment without an access to `github.com` it is necessary to disable `git-commit-id-maven-plugin`, otherwise you will face errors that there is no access to the git during the build process. In that case you may use:

```console
make release-nogit PROFILES="-Pspark-3.4"
```

## Run Spark Shell with Comet enabled

Make sure `SPARK_HOME` points to the same Spark version as Comet was built for.

```console
export COMET_JAR=spark/target/comet-spark-spark3.4_2.12-0.1.0-SNAPSHOT.jar

$SPARK_HOME/bin/spark-shell \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.enabled=true \
    --conf spark.comet.exec.all.enabled=true \
    --conf spark.comet.explainFallback.enabled=true
```

### Verify Comet enabled for Spark SQL query

Create a test Parquet source

```scala
scala> (0 until 10).toDF("a").write.mode("overwrite").parquet("/tmp/test")
```

Query the data from the test source and check:

- INFO message shows the native Comet library has been initialized.
- The query plan reflects Comet operators being used for this query instead of Spark ones

```scala
scala> spark.read.parquet("/tmp/test").createOrReplaceTempView("t1")
scala> spark.sql("select * from t1 where a > 5").explain
INFO src/lib.rs: Comet native library initialized
== Physical Plan ==
        *(1) ColumnarToRow
        +- CometFilter [a#14], (isnotnull(a#14) AND (a#14 > 5))
          +- CometScan parquet [a#14] Batched: true, DataFilters: [isnotnull(a#14), (a#14 > 5)],
             Format: CometParquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/test], PartitionFilters: [],
             PushedFilters: [IsNotNull(a), GreaterThan(a,5)], ReadSchema: struct<a:int>
```

With the configuration `spark.comet.explainFallback.enabled=true`, Comet will log any reasons that prevent a plan from
being executed natively.

```scala
scala> Seq(1,2,3,4).toDF("a").write.parquet("/tmp/test.parquet")
WARN CometSparkSessionExtensions$CometExecRule: Comet cannot execute some parts of this plan natively because:
  - LocalTableScan is not supported
  - WriteFiles is not supported
  - Execute InsertIntoHadoopFsRelationCommand is not supported
```

### Enable Comet shuffle

Comet shuffle feature is disabled by default. To enable it, please add related configs:

```
--conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
--conf spark.comet.exec.shuffle.enabled=true
```

Above configs enable Comet native shuffle which only supports hash partition and single partition.
Comet native shuffle doesn't support complex types yet.

Comet doesn't have official release yet so currently the only way to test it is to build jar and include it in your
Spark application. Depending on your deployment mode you may also need to set the driver & executor class path(s) to
explicitly contain Comet otherwise Spark may use a different class-loader for the Comet components than its internal
components which will then fail at runtime. For example:

```
--driver-class-path spark/target/comet-spark-spark3.4_2.12-0.1.0-SNAPSHOT.jar
```

Some cluster managers may require additional configuration, see <https://spark.apache.org/docs/latest/cluster-overview.html>

To enable columnar shuffle which supports all partitioning and basic complex types, one more config is required:

```
--conf spark.comet.columnar.shuffle.enabled=true
```
