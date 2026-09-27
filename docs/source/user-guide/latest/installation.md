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

The published Comet jar files in Maven Central bundle native libraries for Linux only (amd64 and arm64). macOS
users must [build from source](source.md).

| Operating System            | Published Maven Jars | Build from Source |
| --------------------------- | -------------------- | ----------------- |
| Linux (amd64)               | Yes                  | Yes               |
| Linux (arm64)               | Yes                  | Yes               |
| Apple macOS (Apple Silicon) | No                   | Yes               |

### Supported Spark Versions

Comet $COMET_VERSION supports the following versions of Apache Spark. Refer to the [Spark Version Compatibility] page
in the [Compatibility Guide] for more information, such as known limitations per Spark version.

[Spark Version Compatibility]: compatibility/spark-versions.md
[Compatibility Guide]: compatibility/index.md

We recommend only using Comet with Spark versions where we currently have both Comet and Spark tests enabled in CI.
Other versions may work well enough for development and evaluation purposes.

Comet requires JDK 17 or later. JDK 11 is no longer supported as of the 1.1.0 release.

```{warning}
Spark 3.4 support is deprecated as of the 1.0.0 release and will be removed in a future release.
Apache Spark's own SQL test suite no longer runs against Spark 3.4 automatically; it runs only on
demand. We recommend moving to Spark 3.5 or later.
```

| Spark Version | Java Version | Scala Version | Comet Tests in CI | Spark SQL Tests in CI |
| ------------- | ------------ | ------------- | ----------------- | --------------------- |
| 3.4.3         | 17           | 2.12/2.13     | Nightly           | On demand             |
| 3.5.9         | 17           | 2.12/2.13     | Nightly           | Nightly               |
| 4.0.4         | 17/21        | 2.13          | Nightly           | Nightly               |
| 4.1.3         | 17/21        | 2.13          | Before merge      | Before merge          |

Note that we do not test the full matrix of supported Java and Scala versions in CI for every Spark version.

"Before merge" in the table above means the suite must pass before a change is merged. "Nightly" means
the suite runs once a day against the `main` branch, so a regression it finds is caught after the change
has been merged rather than before. "On demand" means the suite does not run automatically at all. A
contributor can still run it against an individual pull request, but Spark 3.4 is no longer covered by
default.

Experimental support is provided for the following versions of Apache Spark and is intended for development/testing
use only and should not be used in production yet.

| Spark Version | Java Version | Scala Version | Comet Tests in CI | Spark SQL Tests in CI |
| ------------- | ------------ | ------------- | ----------------- | --------------------- |
| 4.2.0         | 17           | 2.13          | Nightly           | No                    |

Note that Comet may not fully work with proprietary forks of Apache Spark such as the Spark versions offered by
Cloud Service Providers.

## Using a Published JAR File

<!-- IF_SNAPSHOT -->

This documentation is for the current development version of Comet, which has not been released. Nightly snapshot
jar files for this version are published to the
[ASF snapshot repository](https://repository.apache.org/content/repositories/snapshots/org/apache/datafusion/) for the
amd64 and arm64 architectures for Linux. For Apple macOS, it is currently necessary to
[build from source](source.md).

Snapshots are unreleased development builds provided for testing and evaluation only. They are not Apache releases,
have not been voted on, and should not be used in production. Older snapshots are removed from the repository
periodically.

A new snapshot is published each night that new commits land on the `main` branch. Every snapshot carries the same
version, `$COMET_VERSION`, so Maven-based tooling resolves the most recent one automatically. The
[Publish Snapshot](https://github.com/apache/datafusion-comet/actions/workflows/publish_snapshot.yml) workflow log
records the commit each snapshot was built from.

The following artifacts are published:

- `comet-spark-spark3.4_2.12`
- `comet-spark-spark3.5_2.12`
- `comet-spark-spark4.0_2.13`
- `comet-spark-spark4.1_2.13`

To download a snapshot jar, browse to the artifact directory in the snapshot repository, for example
[comet-spark-spark4.1_2.13/$COMET_VERSION](https://repository.apache.org/content/repositories/snapshots/org/apache/datafusion/comet-spark-spark4.1_2.13/$COMET_VERSION/),
and pick the jar with the newest timestamp. Then use it as described in
[Run Spark Shell with Comet enabled](#run-spark-shell-with-comet-enabled).

Alternatively, let Spark resolve the newest snapshot directly:

```shell
$SPARK_HOME/bin/spark-shell \
    --repositories https://repository.apache.org/content/repositories/snapshots/ \
    --packages org.apache.datafusion:comet-spark-spark4.1_2.13:$COMET_VERSION \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.explain.fallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=4g \
    --conf spark.executor.memoryOverhead=2g
```

<!-- ENDIF -->

<!-- IF_RELEASE -->

Comet jar files are available in [Maven Central](https://central.sonatype.com/namespace/org.apache.datafusion) for amd64 and arm64 architectures for Linux. For Apple macOS, it
is currently necessary to build from source.

For performance reasons, published Comet jar files target baseline CPUs available in modern data centers. For example,
the amd64 build uses the `x86-64-v3` target that adds CPU instructions (_e.g._, AVX2) common after 2013. Similarly, the
arm64 build uses the `neoverse-n1` target, which is a common baseline for ARM cores found in AWS (Graviton2+), GCP, and
Azure after 2019. If the Comet library fails for SIGILL (illegal instruction), please open an issue on the GitHub
repository describing your environment, and [build from source] for your target architecture.

Here are the direct links for downloading the Comet $COMET_VERSION jar file.

- [Comet plugin for Spark 3.4 / Scala 2.12](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.4_2.12/$COMET_VERSION/comet-spark-spark3.4_2.12-$COMET_VERSION.jar)
- [Comet plugin for Spark 3.4 / Scala 2.13](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.4_2.13/$COMET_VERSION/comet-spark-spark3.4_2.13-$COMET_VERSION.jar)
- [Comet plugin for Spark 3.5 / Scala 2.12](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.5_2.12/$COMET_VERSION/comet-spark-spark3.5_2.12-$COMET_VERSION.jar)
- [Comet plugin for Spark 3.5 / Scala 2.13](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark3.5_2.13/$COMET_VERSION/comet-spark-spark3.5_2.13-$COMET_VERSION.jar)
- [Comet plugin for Spark 4.0 / Scala 2.13](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark4.0_2.13/$COMET_VERSION/comet-spark-spark4.0_2.13-$COMET_VERSION.jar)
- [Comet plugin for Spark 4.1 / Scala 2.13](https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark4.1_2.13/$COMET_VERSION/comet-spark-spark4.1_2.13-$COMET_VERSION.jar)

<!-- ENDIF -->

## Building from source

Refer to the [Building from source] guide for instructions from building Comet from source, either from official
source releases, or from the latest code in the GitHub repository.

[Building from source]: source.md

## Deploying to Kubernetes

See the [Comet Kubernetes Guide](kubernetes.md) guide.

## Run Spark Shell with Comet enabled

Make sure `SPARK_HOME` points to the same Spark version as Comet was built for.

```shell
export COMET_JAR=spark/target/comet-spark-spark4.1_2.13-$COMET_VERSION.jar

$SPARK_HOME/bin/spark-shell \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.explain.fallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=4g \
    --conf spark.executor.memoryOverhead=2g
```

### Verify Comet enabled for Spark SQL query

Create a test Parquet source

```scala
scala> (0 until 10).toDF("a").write.mode("overwrite").parquet("/tmp/test")
```

Comet will log output similar to this on Spark 4.0 and later:

```shell
INFO core/src/lib.rs: Comet native library version $COMET_VERSION initialized
WARN CometExecRule: Comet cannot execute some parts of this plan natively (set spark.comet.explain.fallback.enabled=false to disable this logging):
  Execute InsertIntoHadoopFsRelationCommand
+- WriteFiles [COMET: Native support for operator WriteFilesExec is disabled. Set spark.comet.parquet.write.enabled=true to enable it.]
   +-  LocalTableScan [COMET: Native support for operator LocalTableScanExec is disabled. Set spark.comet.exec.localTableScan.enabled=true to enable it.]
```

On Spark 3.4 and 3.5 the native writer replaces the whole write command rather than just the
per-task write, so the same message appears on `Execute InsertIntoHadoopFsRelationCommand` and
names `DataWritingCommandExec`.

Query the data from the test source and check:

- INFO message shows the native Comet library has been initialized.
- The query plan reflects Comet operators being used for this query instead of Spark ones

```scala
scala> spark.read.parquet("/tmp/test").createOrReplaceTempView("t1")
scala> spark.sql("select * from t1 where a > 5").explain
```

Comet will log output similar to:

```shell
== Physical Plan ==
CometColumnarToRow
+- CometFilter [a#6], (isnotnull(a#6) AND (a#6 > 5))
   +- CometNativeScan parquet [a#6] Batched: true, DataFilters: [isnotnull(a#6), (a#6 > 5)], Format: CometParquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/test], PartitionFilters: [], PushedFilters: [IsNotNull(a), GreaterThan(a,5)], ReadSchema: struct<a:int>
```

## Checking the Comet Version

When the Comet plugin is loaded, it exposes its build version as the Spark config
`spark.comet.version`. This can be queried at runtime from any supported language, for example:

```scala
scala> spark.conf.get("spark.comet.version")
```

```sql
SET spark.comet.version;
```

The same value is available programmatically on the JVM classpath, along with additional build
metadata that is useful when reporting issues:

```scala
scala> import org.apache.comet.{COMET_VERSION, COMET_BRANCH, COMET_REVISION}
scala> println(COMET_VERSION)
```

Comet also logs its version when the native library is initialized:

```shell
INFO core/src/lib.rs: Comet native library version <version> initialized
```

## Additional Configuration

Depending on your deployment mode you may also need to set the driver & executor class path(s) to
explicitly contain Comet otherwise Spark may use a different class-loader for the Comet components than its internal
components which will then fail at runtime. For example:

```shell
--driver-class-path spark/target/comet-spark-spark4.1_2.13-$COMET_VERSION.jar
```

Some cluster managers may require additional configuration, see <https://spark.apache.org/docs/latest/cluster-overview.html>

### Memory tuning

In addition to Apache Spark memory configuration parameters, Comet introduces additional parameters to configure memory
allocation for native execution. See [Comet Memory Tuning](./tuning/memory.md) for details.

### Kryo serialization

If the application uses Kryo (`spark.serializer=org.apache.spark.serializer.KryoSerializer`) with
`spark.kryo.registrationRequired=true`, also register Comet's classes with Kryo:

```shell
--conf spark.kryo.registrator=org.apache.comet.CometKryoRegistrator
```

Without it, any query that uses Comet's native broadcast exchange, which is enabled by default,
fails with Kryo's "Class is not registered" error, for example on the first broadcast hash join.
The [in-memory cache](in-memory-cache.md#kryo) needs the same registrator. Set it before the
`SparkContext` is created: `KryoSerializer` reads it before Comet's plugin runs, so Comet cannot
add it for you. `spark.kryo.registrator` accepts a comma-separated list, so an application with
its own registrator can list both. Comet logs a warning at startup when Kryo requires registration
and this registrator is missing.
