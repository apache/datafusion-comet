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

# Accelerating Apache Iceberg Parquet Scans using Comet (Experimental)

**Note: Iceberg integration is a work-in-progress. Comet currently has two distinct Iceberg
code paths: 1) a hybrid reader (native Parquet decoding, JVM otherwise) that requires
building Iceberg from source rather than using available artifacts in Maven, and 2) fully-native
reader (based on [iceberg-rust](https://github.com/apache/iceberg-rust)). Directions for both
designs are provided below.**

## Hybrid Reader

### Build Comet

Run a Maven install so that we can compile Iceberg against latest Comet:

```shell
mvn install -DskipTests
```

Build the release JAR to be used from Spark:

```shell
make release
```

Set `COMET_JAR` env var:

```shell
export COMET_JAR=`pwd`/spark/target/comet-spark-spark3.5_2.12-$COMET_VERSION.jar
```

### Build Iceberg

Clone the Iceberg repository and apply code changes needed by Comet

```shell
git clone git@github.com:apache/iceberg.git
cd iceberg
git checkout apache-iceberg-1.8.1
git apply ../datafusion-comet/dev/diffs/iceberg/1.8.1.diff
```

Perform a clean build

```shell
./gradlew clean build -x test -x integrationTest
```

### Test

Set `ICEBERG_JAR` environment variable.

```shell
export ICEBERG_JAR=`pwd`/spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.12-1.9.0-SNAPSHOT.jar
```

Launch Spark Shell:

```shell
$SPARK_HOME/bin/spark-shell \
    --driver-class-path $COMET_JAR:$ICEBERG_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR:$ICEBERG_JAR \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hadoop \
    --conf spark.sql.catalog.spark_catalog.warehouse=/tmp/warehouse \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.sql.iceberg.parquet.reader-type=COMET \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=2g \
    --conf spark.comet.use.lazyMaterialization=false \
    --conf spark.comet.schemaEvolution.enabled=true
```

Create an Iceberg table. Note that Comet will not accelerate this part.

```
scala> spark.sql(s"CREATE TABLE IF NOT EXISTS t1 (c0 INT, c1 STRING) USING iceberg")
scala> spark.sql(s"INSERT INTO t1 VALUES ${(0 until 10000).map(i => (i, i)).mkString(",")}")
```

Comet should now be able to accelerate reading the table:

```
scala> spark.sql(s"SELECT * from t1").show()
```

This should produce the following output:

```
scala> spark.sql(s"SELECT * from t1").show()
+---+---+
| c0| c1|
+---+---+
|  0|  0|
|  1|  1|
|  2|  2|
|  3|  3|
|  4|  4|
|  5|  5|
|  6|  6|
|  7|  7|
|  8|  8|
|  9|  9|
| 10| 10|
| 11| 11|
| 12| 12|
| 13| 13|
| 14| 14|
| 15| 15|
| 16| 16|
| 17| 17|
| 18| 18|
| 19| 19|
+---+---+
only showing top 20 rows
```

Confirm that the query was accelerated by Comet:

```
scala> spark.sql(s"SELECT * from t1").explain()
== Physical Plan ==
*(1) CometColumnarToRow
+- CometBatchScan spark_catalog.default.t1[c0#26, c1#27] spark_catalog.default.t1 (branch=null) [filters=, groupedBy=] RuntimeFilters: []
```

### Known issues

- Spark Runtime Filtering isn't [working](https://github.com/apache/datafusion-comet/issues/2116)
  - You can bypass the issue by either setting `spark.sql.adaptive.enabled=false` or `spark.comet.exec.broadcastExchange.enabled=false`

## Native Reader

Comet's fully-native Iceberg integration does not require modifying Iceberg source
code. Instead, Comet relies on reflection to extract `FileScanTask`s from Iceberg, which are
then serialized to Comet's native execution engine (see
[PR #2528](https://github.com/apache/datafusion-comet/pull/2528)).

The example below uses Spark's package downloader to retrieve Comet 0.12.0 and Iceberg
1.8.1, but Comet has been tested with Iceberg 1.5, 1.7, 1.8, and 1.10. The key configuration
to enable fully-native Iceberg is `spark.comet.scan.icebergNative.enabled=true`. This
configuration should **not** be used with the hybrid Iceberg configuration
`spark.sql.iceberg.parquet.reader-type=COMET` from above.

```shell
$SPARK_HOME/bin/spark-shell \
    --packages org.apache.datafusion:comet-spark-spark3.5_2.12:0.12.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-core:1.8.1 \
    --repositories https://repo1.maven.org/maven2/ \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hadoop \
    --conf spark.sql.catalog.spark_catalog.warehouse=/tmp/warehouse \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.comet.scan.icebergNative.enabled=true \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=2g
```

The same sample queries from above can be used to test Comet's fully-native Iceberg integration,
however the scan node to look for is `CometIcebergNativeScan`.

### Current limitations

The following scenarios are not yet supported, but are work in progress:

- Iceberg table spec v3 scans will fall back.
- Iceberg writes will fall back.
- Iceberg table scans backed by Avro or ORC data files will fall back.
- Iceberg table scans partitioned on `BINARY` or `DECIMAL` (with precision >28) columns will fall back.
- Iceberg scans with residual filters (_i.e._, filter expressions that are not partition values,
  and are evaluated on the column values at scan time) of `truncate`, `bucket`, `year`, `month`,
  `day`, `hour` will fall back.
