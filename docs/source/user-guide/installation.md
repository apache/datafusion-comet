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

## Prerequisites

Make sure the following requirements are met and software installed on your machine.

### Supported Operating Systems

- Linux
- Apple OSX (Intel and Apple Silicon)

### Supported Spark Versions

Comet currently supports the following versions of Apache Spark:

- 3.3.x (Java 8/11/17, Scala 2.12/2.13)
- 3.4.x (Java 8/11/17, Scala 2.12/2.13)
- 3.5.x (Java 8/11/17, Scala 2.12/2.13)

Experimental support is provided for the following versions of Apache Spark and is intended for development/testing
use only and should not be used in production yet.

- 4.0.0-preview1 (Java 17/21, Scala 2.13)

Note that Comet may not fully work with proprietary forks of Apache Spark such as the Spark versions offered by
Cloud Service Providers.

## Using a Published JAR File

Comet jar files are available in [Maven Central](https://central.sonatype.com/namespace/org.apache.datafusion) for amd64 and arm64 architectures for Linux. For Apple OSX, it 
is currently necessary to build from source.

Here are the direct links for downloading the Comet jar file.

- [Comet plugin for Spark 3.3 / Scala 2.12](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.3_2.12/0.3.0/comet-spark-spark3.3_2.12-0.3.0.jar)
- [Comet plugin for Spark 3.3 / Scala 2.13](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.3_2.13/0.3.0/comet-spark-spark3.3_2.13-0.3.0.jar)
- [Comet plugin for Spark 3.4 / Scala 2.12](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.4_2.12/0.3.0/comet-spark-spark3.4_2.12-0.3.0.jar)
- [Comet plugin for Spark 3.4 / Scala 2.13](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.4_2.13/0.3.0/comet-spark-spark3.4_2.13-0.3.0.jar)
- [Comet plugin for Spark 3.5 / Scala 2.12](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.5_2.12/0.3.0/comet-spark-spark3.5_2.12-0.3.0.jar)
- [Comet plugin for Spark 3.5 / Scala 2.13](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.5_2.13/0.3.0/comet-spark-spark3.5_2.13-0.3.0.jar)

## Building from source

Refer to the [Building from Source] guide for instructions from building Comet from source, either from official
source releases, or from the latest code in the GitHub repository.

[Building from Source]: source.md

## Deploying to Kubernetes

See the [Comet Kubernetes Guide](kubernetes.md) guide.

## Run Spark Shell with Comet enabled

Make sure `SPARK_HOME` points to the same Spark version as Comet was built for.

```console
export COMET_JAR=spark/target/comet-spark-spark3.4_2.12-0.3.0-SNAPSHOT.jar

$SPARK_HOME/bin/spark-shell \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=16g \
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

## Additional Configuration

Depending on your deployment mode you may also need to set the driver & executor class path(s) to
explicitly contain Comet otherwise Spark may use a different class-loader for the Comet components than its internal
components which will then fail at runtime. For example:

```
--driver-class-path spark/target/comet-spark-spark3.4_2.12-0.3.0-SNAPSHOT.jar
```

Some cluster managers may require additional configuration, see <https://spark.apache.org/docs/latest/cluster-overview.html>

### Memory tuning

In addition to Apache Spark memory configuration parameters, Comet introduces additional parameters to configure memory
allocation for native execution. See [Comet Memory Tuning](./tuning.md) for details.
