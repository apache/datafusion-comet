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

# Comet Benchmarking Scripts

This directory contains scripts used for generating benchmark results that are published in this repository and in 
the Comet documentation.

## Example usage

Set Spark environment variables:

```shell
export SPARK_HOME=/opt/spark-3.5.3-bin-hadoop3/
export SPARK_MASTER=spark://yourhostname:7077
```

Set path to queries and data:

```shell
export TPCH_QUERIES=/mnt/bigdata/tpch/queries/
export TPCH_DATA=/mnt/bigdata/tpch/sf100/
```

Run Spark benchmark:

```shell
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

Run Comet benchmark:

```shell
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export COMET_JAR=/opt/comet/comet-spark-spark3.5_2.12-0.9.0.jar
```

Run Gluten benchmark:

```shell
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export GLUTEN_JAR=/opt/gluten/gluten-velox-bundle-spark3.5_2.12-linux_amd64-1.4.0.jar
```
