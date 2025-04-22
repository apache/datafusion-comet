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

# Supported Spark Data Sources

## Parquet

When `spark.comet.scan.enabled` is enabled, Parquet scans will be performed natively by Comet if all data types
in the schema are supported. When this option is not enabled, the scan will fall back to Spark. In this case,
enabling `spark.comet.convert.parquet.enabled` will immediately convert the data into Arrow format, allowing native 
execution to happen after that, but the process may not be efficient.

## CSV

Comet does not provide native CSV scan, but when `spark.comet.convert.csv.enabled` is enabled, data is immediately
converted into Arrow format, allowing native execution to happen after that.

## JSON

Comet does not provide native JSON scan, but when `spark.comet.convert.json.enabled` is enabled, data is immediately
converted into Arrow format, allowing native execution to happen after that.

# Supported Storages

## Local
In progress

## HDFS

Apache DataFusion Comet native reader seamlessly scans files from remote HDFS for [supported formats](#supported-spark-data-sources)

### Using experimental native DataFusion reader
Unlike to native Comet reader the Datafusion reader fully supports nested types processing. This reader is currently experimental only

To build Comet with native DataFusion reader and remote HDFS support it is required to have a JDK installed

Example:
Build a Comet for `spark-3.5` provide a JDK path in `JAVA_HOME` 
Provide the JRE linker path in `RUSTFLAGS`, the path can vary depending on the system. Typically JRE linker is a part of installed JDK

```shell
export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
make release PROFILES="-Pspark-3.5" COMET_FEATURES=hdfs RUSTFLAGS="-L $JAVA_HOME/libexec/openjdk.jdk/Contents/Home/lib/server"
```

Start Comet with experimental reader and HDFS support as [described](installation.md/#run-spark-shell-with-comet-enabled)
and add additional parameters 

```shell
--conf spark.comet.scan.impl=native_datafusion \
--conf spark.hadoop.fs.defaultFS="hdfs://namenode:9000" \
--conf spark.hadoop.dfs.client.use.datanode.hostname = true \
--conf dfs.client.use.datanode.hostname = true
```

Query a struct type from Remote HDFS 
```shell
spark.read.parquet("hdfs://namenode:9000/user/data").show(false)

root
 |-- id: integer (nullable = true)
 |-- first_name: string (nullable = true)
 |-- personal_info: struct (nullable = true)
 |    |-- firstName: string (nullable = true)
 |    |-- lastName: string (nullable = true)
 |    |-- ageInYears: integer (nullable = true)

25/01/30 16:50:43 INFO core/src/lib.rs: Comet native library version 0.7.0 initialized
== Physical Plan ==
* CometColumnarToRow (2)
+- CometNativeScan:  (1)


(1) CometNativeScan: 
Output [3]: [id#0, first_name#1, personal_info#4]
Arguments: [id#0, first_name#1, personal_info#4]

(2) CometColumnarToRow [codegen id : 1]
Input [3]: [id#0, first_name#1, personal_info#4]


25/01/30 16:50:44 INFO fs-hdfs-0.1.12/src/hdfs.rs: Connecting to Namenode (hdfs://namenode:9000)
+---+----------+-----------------+
|id |first_name|personal_info    |
+---+----------+-----------------+
|2  |Jane      |{Jane, Smith, 34}|
|1  |John      |{John, Doe, 28}  |
+---+----------+-----------------+



```

Verify the native scan type should be `CometNativeScan`.

More on [HDFS Reader](../../../native/hdfs/README.md)

### Local HDFS development

- Configure local machine network. Add hostname to `/etc/hosts`
```commandline
127.0.0.1	localhost   namenode datanode1 datanode2 datanode3
::1             localhost namenode datanode1 datanode2 datanode3
```

- Start local HDFS cluster, 3 datanodes, namenode url is `namenode:9000` 
```commandline
docker compose -f kube/local/hdfs-docker-compose.yml up
```

- Check the local namenode is up and running on `http://localhost:9870/dfshealth.html#tab-overview`
- Build a project with HDFS support
```commandline
JAVA_HOME="/opt/homebrew/opt/openjdk@11" make release PROFILES="-Pspark-3.5" COMET_FEATURES=hdfs RUSTFLAGS="-L /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home/lib/server"
```

- Run local test 
```scala

    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION,
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      "fs.defaultFS" -> "hdfs://namenode:9000",
      "dfs.client.use.datanode.hostname" -> "true") {
      val df = spark.read.parquet("/tmp/2")
      df.show(false)
      df.explain("extended")
    }
  }
```
Or use `spark-shell` with HDFS support as described [above](#using-experimental-native-datafusion-reader)
## S3
In progress 
