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

**Note: Iceberg integration is a work-in-progress. It may be necessary to build Iceberg from
source rather than using available artifacts in Maven**

Download compatible Spark, Comet, and Iceberg versions from Maven, or compile projects from source.

- [Comet Artifacts](https://central.sonatype.com/artifact/org.apache.datafusion/comet-spark-spark3.5_2.12)
- [Iceberg Artifacts](https://central.sonatype.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.5_2.12/overview)

Set `COMET_JAR` and `ICEBERG_JAR` environment variables.

```shell
export COMET_JAR=/path/to/comet-spark-spark3.5_2.12-0.8.0.jar
export ICEBERG_JAR=/path/to/iceberg-spark-runtime-3.5_2.12-1.8.1.jar
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
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.scan.impl=native_iceberg_compat \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=16g
```

```shell
scala> spark.sql(s"CREATE TABLE IF NOT EXISTS t1 (c0 INT, c1 STRING) USING iceberg")
scala> spark.sql(s"INSERT INTO t1 VALUES ${(0 until 10000).map(i => (i, i)).mkString(",")}")
scala> spark.sql(s"SELECT * from t1").show()
```

**Note: this is not actually accelerating the read**

```
25/04/26 08:52:56 WARN CometSparkSessionExtensions$CometExecRule: Comet cannot execute some parts of this plan natively (set spark.comet.explainFallback.enabled=false to disable this logging):
 CollectLimit [COMET: CollectLimit is not supported]
+-  Project [COMET: Project is not native because the following children are not native (BatchScan spark_catalog.default.t1)]
   +-  BatchScan spark_catalog.default.t1 [COMET: Comet Scan only supports Parquet and Iceberg Parquet file formats, BatchScan spark_catalog.default.t1 is not supported]
```
