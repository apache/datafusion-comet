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

**Note: Iceberg integration is a work-in-progress. It is currently necessary to build Iceberg from
source rather than using available artifacts in Maven**

## Build Comet

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
export COMET_JAR=`pwd`/spark/target/comet-spark-spark3.5_2.12-0.10.0-SNAPSHOT.jar
```

## Build Iceberg

Clone the Iceberg repository and apply code changes needed by Comet

```shell
git clone git@github.com:apache/iceberg.git
cd iceberg
git checkout apache-iceberg-1.8.1
git apply ../datafusion-comet/dev/diffs/iceberg/1.8.1.diff
```

Perform a clean build

```shell
./gradlew clean
./gradlew build -x test -x integrationTest
```

## Test

Set `ICEBERG_JAR` environment variable.

```shell
export ICEBERG_JAR=`pwd`/spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.12-1.9.0-SNAPSHOT.jar
```

Launch Spark Shell:

```shell
$SPARK_HOME/bin/spark-shell \
    --jars $COMET_JAR,$ICEBERG_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR,$ICEBERG_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR,$ICEBERG_JAR \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hadoop \
    --conf spark.sql.catalog.spark_catalog.warehouse=/tmp/warehouse \
    --conf spark.plugins=org.apache.spark.CometPlugin \
    --conf spark.comet.exec.shuffle.enabled=false \
    --conf spark.sql.iceberg.parquet.reader-type=COMET \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=2g \
    --conf spark.comet.use.lazyMaterialization=false \
    --conf spark.comet.schemaEvolution.enabled=true \
    --conf spark.comet.exec.broadcastExchange.enabled=false
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

## Known issues
 - We temporarily disable Comet when there are delete files in Iceberg scan, see Iceberg [1.8.1 diff](../../../dev/diffs/iceberg/1.8.1.diff)
   - Iceberg scan w/ delete files lead to [runtime exceptions](https://github.com/apache/datafusion-comet/issues/2117) and [incorrect results](https://github.com/apache/datafusion-comet/issues/2118)
 - Enabling `CometShuffleManager` leads to [runtime exceptions](https://github.com/apache/datafusion-comet/issues/2086)
 - Enabling `CometBroadcastExchangeExec` leads to [runtime exceptions](https://github.com/apache/datafusion-comet/issues/2116)
