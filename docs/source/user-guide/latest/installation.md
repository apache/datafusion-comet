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
- Apple macOS (Intel and Apple Silicon)

### Supported Spark Versions

Comet $COMET_VERSION supports the following versions of Apache Spark.

We recommend only using Comet with Spark versions where we currently have both Comet and Spark tests enabled in CI.
Other versions may work well enough for development and evaluation purposes.

| Spark Version | Java Version | Scala Version | Comet Tests in CI | Spark SQL Tests in CI |
|---------------| ------------ | ------------- |-------------------|-----------------------|
| 3.4.3         | 11/17        | 2.12/2.13     | Yes               | Yes                   |
| 3.5.4         | 11/17        | 2.12/2.13     | Yes               | No                    |
| 3.5.5         | 11/17        | 2.12/2.13     | Yes               | No                    |
| 3.5.6         | 11/17        | 2.12/2.13     | Yes               | Yes                   |

Note that we do not test the full matrix of supported Java and Scala versions in CI for every Spark version.

Experimental support is provided for the following versions of Apache Spark and is intended for development/testing
use only and should not be used in production yet.

| Spark Version  | Java Version | Scala Version | Comet Tests in CI | Spark SQL Tests in CI |
| -------------- | ------------ | ------------- | ----------------- |-----------------------|
| 4.0.0 | 17           | 2.13          | Yes               | Yes                   |

Note that Comet may not fully work with proprietary forks of Apache Spark such as the Spark versions offered by
Cloud Service Providers.

## Using a Published JAR File

Comet jar files are available in [Maven Central](https://central.sonatype.com/namespace/org.apache.datafusion) for amd64 and arm64 architectures for Linux. For Apple macOS, it
is currently necessary to build from source.

Here are the direct links for downloading the Comet 0.9.1 jar file.

- [Comet plugin for Spark 3.4 / Scala 2.12](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.4_2.12/0.9.1/comet-spark-spark3.4_2.12-0.9.1.jar)
- [Comet plugin for Spark 3.4 / Scala 2.13](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.4_2.13/0.9.1/comet-spark-spark3.4_2.13-0.9.1.jar)
- [Comet plugin for Spark 3.5 / Scala 2.12](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.5_2.12/0.9.1/comet-spark-spark3.5_2.12-0.9.1.jar)
- [Comet plugin for Spark 3.5 / Scala 2.13](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.5_2.13/0.9.1/comet-spark-spark3.5_2.13-0.9.1.jar)

## Building from source

Refer to the [Building from Source] guide for instructions from building Comet from source, either from official
source releases, or from the latest code in the GitHub repository.

[Building from Source]: source.md

## Deploying to Kubernetes

See the [Comet Kubernetes Guide](kubernetes.md) guide.

## Run Spark Shell with Comet enabled

Make sure `SPARK_HOME` points to the same Spark version as Comet was built for.

```shell
export COMET_JAR=spark/target/comet-spark-spark3.5_2.12-$COMET_VERSION.jar

$SPARK_HOME/bin/spark-shell \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.sql.extendedExplainProviders=org.apache.comet.ExtendedExplainInfo \
    --conf spark.comet.explain.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=2g
```

### Verify Comet enabled for Spark SQL query

Create a test Parquet source. Note that Comet will not accelerate writing the Parquet file.

```
scala> (0 until 10).toDF("a").write.mode("overwrite").parquet("/tmp/test")

WARN CometExecRule: Comet cannot accelerate some parts of this plan (set spark.comet.explain.enabled=false to disable this logging):
Execute InsertIntoHadoopFsRelationCommand [COMET: Execute InsertIntoHadoopFsRelationCommand is not supported]
+-  WriteFiles [COMET: WriteFiles is not supported]
+-  LocalTableScan [COMET: LocalTableScan is not supported]

Comet accelerated 0% of eligible operators (sparkOperators=3, cometOperators=0, transitions=0, wrappers=0).
```

Create a view from the Parquet file. Again, Comet will not accelerate this part.

```
scala> spark.read.parquet("/tmp/test").createOrReplaceTempView("t1")
```

Executing a simple SELECT query should be fully accelerated by Comet:

```
scala> spark.sql("select * from t1 where a > 5").explain

WARN CometExecRule: Comet fully accelerated this plan (set spark.comet.explain.enabled=false to disable this logging):
CometFilter
+- CometScanWrapper

Comet accelerated 100% of eligible operators (sparkOperators=0, cometOperators=2, transitions=0, wrappers=0).
== Physical Plan ==
  *(1) CometColumnarToRow
  +- CometFilter [a#7], (isnotnull(a#7) AND (a#7 > 5))
+- CometScan [native_iceberg_compat] parquet [a#7] Batched: true, DataFilters: [isnotnull(a#7), (a#7 > 5)], Format: CometParquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/test], PartitionFilters: [], PushedFilters: [IsNotNull(a), GreaterThan(a,5)], ReadSchema: struct<a:int>
```

## Additional Configuration

Depending on your deployment mode you may also need to set the driver & executor class path(s) to
explicitly contain Comet otherwise Spark may use a different class-loader for the Comet components than its internal
components which will then fail at runtime. For example:

```
--driver-class-path spark/target/comet-spark-spark3.5_2.12-$COMET_VERSION.jar
```

Some cluster managers may require additional configuration, see <https://spark.apache.org/docs/latest/cluster-overview.html>

### Memory tuning

In addition to Apache Spark memory configuration parameters, Comet introduces additional parameters to configure memory
allocation for native execution. See [Comet Memory Tuning](./tuning.md) for details.
