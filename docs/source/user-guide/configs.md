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

# Comet Configuration Settings

Comet provides the following configuration settings.

| Config | Description | Default Value |
|--------|-------------|---------------|
| spark.comet.batchSize | The columnar batch size, i.e., the maximum number of rows that a batch can contain. | 8192 |
| spark.comet.cast.allowIncompatible | Comet is not currently fully compatible with Spark for all cast operations. Set this config to true to allow them anyway. See compatibility guide for more information. | false |
| spark.comet.columnar.shuffle.async.enabled | Whether to enable asynchronous shuffle for Arrow-based shuffle. By default, this config is false. | false |
| spark.comet.columnar.shuffle.async.max.thread.num | Maximum number of threads on an executor used for Comet async columnar shuffle. By default, this config is 100. This is the upper bound of total number of shuffle threads per executor. In other words, if the number of cores * the number of shuffle threads per task `spark.comet.columnar.shuffle.async.thread.num` is larger than this config. Comet will use this config as the number of shuffle threads per executor instead. | 100 |
| spark.comet.columnar.shuffle.async.thread.num | Number of threads used for Comet async columnar shuffle per shuffle task. By default, this config is 3. Note that more threads means more memory requirement to buffer shuffle data before flushing to disk. Also, more threads may not always improve performance, and should be set based on the number of cores available. | 3 |
| spark.comet.columnar.shuffle.memory.factor | Fraction of Comet memory to be allocated per executor process for Comet shuffle. Comet memory size is specified by `spark.comet.memoryOverhead` or calculated by `spark.comet.memory.overhead.factor` * `spark.executor.memory`. By default, this config is 1.0. | 1.0 |
| spark.comet.debug.enabled | Whether to enable debug mode for Comet. By default, this config is false. When enabled, Comet will do additional checks for debugging purpose. For example, validating array when importing arrays from JVM at native side. Note that these checks may be expensive in performance and should only be enabled for debugging purpose. | false |
| spark.comet.enabled | Whether to enable Comet extension for Spark. When this is turned on, Spark will use Comet to read Parquet data source. Note that to enable native vectorized execution, both this config and 'spark.comet.exec.enabled' need to be enabled. By default, this config is the value of the env var `ENABLE_COMET` if set, or true otherwise. | true |
| spark.comet.exceptionOnDatetimeRebase | Whether to throw exception when seeing dates/timestamps from the legacy hybrid (Julian + Gregorian) calendar. Since Spark 3, dates/timestamps were written according to the Proleptic Gregorian calendar. When this is true, Comet will throw exceptions when seeing these dates/timestamps that were written by Spark version before 3.0. If this is false, these dates/timestamps will be read as if they were written to the Proleptic Gregorian calendar and will not be rebased. | false |
| spark.comet.exec.all.enabled | Whether to enable all Comet operators. By default, this config is false. Note that this config precedes all separate config 'spark.comet.exec.<operator_name>.enabled'. That being said, if this config is enabled, separate configs are ignored. | false |
| spark.comet.exec.enabled | Whether to enable Comet native vectorized execution for Spark. This controls whether Spark should convert operators into their Comet counterparts and execute them in native space. Note: each operator is associated with a separate config in the format of 'spark.comet.exec.<operator_name>.enabled' at the moment, and both the config and this need to be turned on, in order for the operator to be executed in native. By default, this config is false. | false |
| spark.comet.exec.memoryFraction | The fraction of memory from Comet memory overhead that the native memory manager can use for execution. The purpose of this config is to set aside memory for untracked data structures, as well as imprecise size estimation during memory acquisition. Default value is 0.7. | 0.7 |
| spark.comet.exec.shuffle.codec | The codec of Comet native shuffle used to compress shuffle data. Only zstd is supported. | zstd |
| spark.comet.exec.shuffle.enabled | Whether to enable Comet native shuffle. By default, this config is false. Note that this requires setting 'spark.shuffle.manager' to 'org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager'. 'spark.shuffle.manager' must be set before starting the Spark application and cannot be changed during the application. | false |
| spark.comet.exec.shuffle.mode | The mode of Comet shuffle. This config is only effective if Comet shuffle is enabled. Available modes are 'native', 'jvm', and 'auto'. 'native' is for native shuffle which has best performance in general. 'jvm' is for jvm-based columnar shuffle which has higher coverage than native shuffle. 'auto' is for Comet to choose the best shuffle mode based on the query plan. By default, this config is 'jvm'. | jvm |
| spark.comet.explainFallback.enabled | When this setting is enabled, Comet will provide logging explaining the reason(s) why a query stage cannot be executed natively. | false |
| spark.comet.memory.overhead.factor | Fraction of executor memory to be allocated as additional non-heap memory per executor process for Comet. Default value is 0.2. | 0.2 |
| spark.comet.memory.overhead.min | Minimum amount of additional memory to be allocated per executor process for Comet, in MiB. | 402653184b |
| spark.comet.nativeLoadRequired | Whether to require Comet native library to load successfully when Comet is enabled. If not, Comet will silently fallback to Spark when it fails to load the native lib. Otherwise, an error will be thrown and the Spark job will be aborted. | false |
| spark.comet.parquet.enable.directBuffer | Whether to use Java direct byte buffer when reading Parquet. By default, this is false | false |
| spark.comet.rowToColumnar.supportedOperatorList | A comma-separated list of row-based operators that will be converted to columnar format when 'spark.comet.rowToColumnar.enabled' is true | Range,InMemoryTableScan |
| spark.comet.scan.enabled | Whether to enable Comet scan. When this is turned on, Spark will use Comet to read Parquet data source. Note that to enable native vectorized execution, both this config and 'spark.comet.exec.enabled' need to be enabled. By default, this config is true. | true |
| spark.comet.scan.preFetch.enabled | Whether to enable pre-fetching feature of CometScan. By default is disabled. | false |
| spark.comet.scan.preFetch.threadNum | The number of threads running pre-fetching for CometScan. Effective if spark.comet.scan.preFetch.enabled is enabled. By default it is 2. Note that more pre-fetching threads means more memory requirement to store pre-fetched row groups. | 2 |
| spark.comet.shuffle.preferDictionary.ratio | The ratio of total values to distinct values in a string column to decide whether to prefer dictionary encoding when shuffling the column. If the ratio is higher than this config, dictionary encoding will be used on shuffling string column. This config is effective if it is higher than 1.0. By default, this config is 10.0. Note that this config is only used when 'spark.comet.columnar.shuffle.enabled' is true. | 10.0 |
| spark.comet.xxhash64.enabled | The xxhash64 implementation is not optimized yet and may cause performance issues. | false |
