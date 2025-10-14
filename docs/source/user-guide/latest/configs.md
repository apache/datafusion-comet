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

## Scan Configuration Settings

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[scan]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| spark.comet.convert.csv.enabled | When enabled, data from Spark (non-native) CSV v1 and v2 scans will be converted to Arrow format. Note that to enable native vectorized execution, both this config and 'spark.comet.exec.enabled' need to be enabled. | false |
| spark.comet.convert.json.enabled | When enabled, data from Spark (non-native) JSON v1 and v2 scans will be converted to Arrow format. Note that to enable native vectorized execution, both this config and 'spark.comet.exec.enabled' need to be enabled. | false |
| spark.comet.convert.parquet.enabled | When enabled, data from Spark (non-native) Parquet v1 and v2 scans will be converted to Arrow format. Note that to enable native vectorized execution, both this config and 'spark.comet.exec.enabled' need to be enabled. | false |
| spark.comet.scan.allowIncompatible | Some Comet scan implementations are not currently fully compatible with Spark for all datatypes. Set this config to true to allow them anyway. For more information, refer to the Comet Compatibility Guide (https://datafusion.apache.org/comet/user-guide/compatibility.html). | false |
| spark.comet.scan.enabled | Whether to enable native scans. When this is turned on, Spark will use Comet to read supported data sources (currently only Parquet is supported natively). Note that to enable native vectorized execution, both this config and 'spark.comet.exec.enabled' need to be enabled. | true |
| spark.comet.scan.preFetch.enabled | Whether to enable pre-fetching feature of CometScan. | false |
| spark.comet.scan.preFetch.threadNum | The number of threads running pre-fetching for CometScan. Effective if spark.comet.scan.preFetch.enabled is enabled. Note that more pre-fetching threads means more memory requirement to store pre-fetched row groups. | 2 |
| spark.comet.sparkToColumnar.enabled | Whether to enable Spark to Arrow columnar conversion. When this is turned on, Comet will convert operators in `spark.comet.sparkToColumnar.supportedOperatorList` into Arrow columnar format before processing. | false |
| spark.comet.sparkToColumnar.supportedOperatorList | A comma-separated list of operators that will be converted to Arrow columnar format when 'spark.comet.sparkToColumnar.enabled' is true | Range,InMemoryTableScan,RDDScan |
| spark.hadoop.fs.comet.libhdfs.schemes | Defines filesystem schemes (e.g., hdfs, webhdfs) that the native side accesses via libhdfs, separated by commas. Valid only when built with hdfs feature enabled. | |
<!--END:CONFIG_TABLE-->

## Parquet Reader Configuration Settings

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[parquet]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| spark.comet.parquet.enable.directBuffer | Whether to use Java direct byte buffer when reading Parquet. | false |
| spark.comet.parquet.read.io.adjust.readRange.skew | In the parallel reader, if the read ranges submitted are skewed in sizes, this option will cause the reader to break up larger read ranges into smaller ranges to reduce the skew. This will result in a slightly larger number of connections opened to the file system but may give improved performance. | false |
| spark.comet.parquet.read.io.mergeRanges | When enabled the parallel reader will try to merge ranges of data that are separated by less than 'comet.parquet.read.io.mergeRanges.delta' bytes. Longer continuous reads are faster on cloud storage. | true |
| spark.comet.parquet.read.io.mergeRanges.delta | The delta in bytes between consecutive read ranges below which the parallel reader will try to merge the ranges. The default is 8MB. | 8388608 |
| spark.comet.parquet.read.parallel.io.enabled | Whether to enable Comet's parallel reader for Parquet files. The parallel reader reads ranges of consecutive data in a  file in parallel. It is faster for large files and row groups but uses more resources. | true |
| spark.comet.parquet.read.parallel.io.thread-pool.size | The maximum number of parallel threads the parallel reader will use in a single executor. For executors configured with a smaller number of cores, use a smaller number. | 16 |
| spark.comet.parquet.respectFilterPushdown | Whether to respect Spark's PARQUET_FILTER_PUSHDOWN_ENABLED config. This needs to be respected when running the Spark SQL test suite but the default setting results in poor performance in Comet when using the new native scans, disabled by default | false |
<!--END:CONFIG_TABLE-->

## Query Execution Settings

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[exec]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| spark.comet.caseConversion.enabled | Java uses locale-specific rules when converting strings to upper or lower case and Rust does not, so we disable upper and lower by default. | false |
| spark.comet.debug.enabled | Whether to enable debug mode for Comet. When enabled, Comet will do additional checks for debugging purpose. For example, validating array when importing arrays from JVM at native side. Note that these checks may be expensive in performance and should only be enabled for debugging purpose. | false |
| spark.comet.dppFallback.enabled | Whether to fall back to Spark for queries that use DPP. | true |
| spark.comet.enabled | Whether to enable Comet extension for Spark. When this is turned on, Spark will use Comet to read Parquet data source. Note that to enable native vectorized execution, both this config and 'spark.comet.exec.enabled' need to be enabled. By default, this config is the value of the env var `ENABLE_COMET` if set, or true otherwise. | true |
| spark.comet.exceptionOnDatetimeRebase | Whether to throw exception when seeing dates/timestamps from the legacy hybrid (Julian + Gregorian) calendar. Since Spark 3, dates/timestamps were written according to the Proleptic Gregorian calendar. When this is true, Comet will throw exceptions when seeing these dates/timestamps that were written by Spark version before 3.0. If this is false, these dates/timestamps will be read as if they were written to the Proleptic Gregorian calendar and will not be rebased. | false |
| spark.comet.exec.aggregate.enabled | Whether to enable aggregate by default. | true |
| spark.comet.exec.broadcastExchange.enabled | Whether to enable broadcastExchange by default. | true |
| spark.comet.exec.broadcastHashJoin.enabled | Whether to enable broadcastHashJoin by default. | true |
| spark.comet.exec.coalesce.enabled | Whether to enable coalesce by default. | true |
| spark.comet.exec.collectLimit.enabled | Whether to enable collectLimit by default. | true |
| spark.comet.exec.enabled | Whether to enable Comet native vectorized execution for Spark. This controls whether Spark should convert operators into their Comet counterparts and execute them in native space. Note: each operator is associated with a separate config in the format of 'spark.comet.exec.<operator_name>.enabled' at the moment, and both the config and this need to be turned on, in order for the operator to be executed in native. | true |
| spark.comet.exec.expand.enabled | Whether to enable expand by default. | true |
| spark.comet.exec.filter.enabled | Whether to enable filter by default. | true |
| spark.comet.exec.globalLimit.enabled | Whether to enable globalLimit by default. | true |
| spark.comet.exec.hashJoin.enabled | Whether to enable hashJoin by default. | true |
| spark.comet.exec.localLimit.enabled | Whether to enable localLimit by default. | true |
| spark.comet.exec.project.enabled | Whether to enable project by default. | true |
| spark.comet.exec.replaceSortMergeJoin | Experimental feature to force Spark to replace SortMergeJoin with ShuffledHashJoin for improved performance. This feature is not stable yet. For more information, refer to the Comet Tuning Guide (https://datafusion.apache.org/comet/user-guide/tuning.html). | false |
| spark.comet.exec.sort.enabled | Whether to enable sort by default. | true |
| spark.comet.exec.sortMergeJoin.enabled | Whether to enable sortMergeJoin by default. | true |
| spark.comet.exec.sortMergeJoinWithJoinFilter.enabled | Experimental support for Sort Merge Join with filter | false |
| spark.comet.exec.stddev.enabled | Whether to enable stddev by default. stddev is slower than Spark's implementation. | true |
| spark.comet.exec.takeOrderedAndProject.enabled | Whether to enable takeOrderedAndProject by default. | true |
| spark.comet.exec.union.enabled | Whether to enable union by default. | true |
| spark.comet.exec.window.enabled | Whether to enable window by default. | true |
| spark.comet.explain.native.enabled | When this setting is enabled, Comet will provide a tree representation of the native query plan before execution and again after execution, with metrics. | false |
| spark.comet.explain.verbose.enabled | When this setting is enabled, Comet's extended explain output will provide the full query plan annotated with fallback reasons as well as a summary of how much of the plan was accelerated by Comet. When this setting is disabled, a list of fallback reasons will be provided instead. | false |
| spark.comet.explainFallback.enabled | When this setting is enabled, Comet will provide logging explaining the reason(s) why a query stage cannot be executed natively. Set this to false to reduce the amount of logging. | false |
| spark.comet.expression.allowIncompatible | Comet is not currently fully compatible with Spark for all expressions. Set this config to true to allow them anyway. For more information, refer to the Comet Compatibility Guide (https://datafusion.apache.org/comet/user-guide/compatibility.html). | false |
| spark.comet.logFallbackReasons.enabled | When this setting is enabled, Comet will log warnings for all fallback reasons. | false |
| spark.comet.maxTempDirectorySize | The maximum amount of data (in bytes) stored inside the temporary directories. | 107374182400b |
| spark.comet.metrics.updateInterval | The interval in milliseconds to update metrics. If interval is negative, metrics will be updated upon task completion. | 3000 |
| spark.comet.nativeLoadRequired | Whether to require Comet native library to load successfully when Comet is enabled. If not, Comet will silently fallback to Spark when it fails to load the native lib. Otherwise, an error will be thrown and the Spark job will be aborted. | false |
| spark.comet.regexp.allowIncompatible | Comet is not currently fully compatible with Spark for all regular expressions. Set this config to true to allow them anyway. For more information, refer to the Comet Compatibility Guide (https://datafusion.apache.org/comet/user-guide/compatibility.html). | false |
<!--END:CONFIG_TABLE-->

## Enabling or Disabling Individual Expressions

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[enable_expr]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| Acos | Enable Comet acceleration for Acos | true |
| Add | Enable Comet acceleration for Add | true |
| Alias | Enable Comet acceleration for Alias | true |
| And | Enable Comet acceleration for And | true |
| ArrayAppend | Enable Comet acceleration for ArrayAppend | true |
| ArrayCompact | Enable Comet acceleration for ArrayCompact | true |
| ArrayContains | Enable Comet acceleration for ArrayContains | true |
| ArrayDistinct | Enable Comet acceleration for ArrayDistinct | true |
| ArrayExcept | Enable Comet acceleration for ArrayExcept | true |
| ArrayFilter | Enable Comet acceleration for ArrayFilter | true |
| ArrayInsert | Enable Comet acceleration for ArrayInsert | true |
| ArrayIntersect | Enable Comet acceleration for ArrayIntersect | true |
| ArrayJoin | Enable Comet acceleration for ArrayJoin | true |
| ArrayMax | Enable Comet acceleration for ArrayMax | true |
| ArrayMin | Enable Comet acceleration for ArrayMin | true |
| ArrayRemove | Enable Comet acceleration for ArrayRemove | true |
| ArrayRepeat | Enable Comet acceleration for ArrayRepeat | true |
| ArrayUnion | Enable Comet acceleration for ArrayUnion | true |
| ArraysOverlap | Enable Comet acceleration for ArraysOverlap | true |
| Ascii | Enable Comet acceleration for Ascii | true |
| Asin | Enable Comet acceleration for Asin | true |
| Atan | Enable Comet acceleration for Atan | true |
| Atan2 | Enable Comet acceleration for Atan2 | true |
| AttributeReference | Enable Comet acceleration for AttributeReference | true |
| BitLength | Enable Comet acceleration for BitLength | true |
| BitwiseAnd | Enable Comet acceleration for BitwiseAnd | true |
| BitwiseCount | Enable Comet acceleration for BitwiseCount | true |
| BitwiseGet | Enable Comet acceleration for BitwiseGet | true |
| BitwiseNot | Enable Comet acceleration for BitwiseNot | true |
| BitwiseOr | Enable Comet acceleration for BitwiseOr | true |
| BitwiseXor | Enable Comet acceleration for BitwiseXor | true |
| CaseWhen | Enable Comet acceleration for CaseWhen | true |
| Cast | Enable Comet acceleration for Cast | true |
| Ceil | Enable Comet acceleration for Ceil | true |
| CheckOverflow | Enable Comet acceleration for CheckOverflow | true |
| Chr | Enable Comet acceleration for Chr | true |
| Coalesce | Enable Comet acceleration for Coalesce | true |
| ConcatWs | Enable Comet acceleration for ConcatWs | true |
| Contains | Enable Comet acceleration for Contains | true |
| Cos | Enable Comet acceleration for Cos | true |
| CreateArray | Enable Comet acceleration for CreateArray | true |
| CreateNamedStruct | Enable Comet acceleration for CreateNamedStruct | true |
| DateAdd | Enable Comet acceleration for DateAdd | true |
| DateSub | Enable Comet acceleration for DateSub | true |
| DayOfMonth | Enable Comet acceleration for DayOfMonth | true |
| DayOfWeek | Enable Comet acceleration for DayOfWeek | true |
| DayOfYear | Enable Comet acceleration for DayOfYear | true |
| Divide | Enable Comet acceleration for Divide | true |
| ElementAt | Enable Comet acceleration for ElementAt | true |
| EndsWith | Enable Comet acceleration for EndsWith | true |
| EqualNullSafe | Enable Comet acceleration for EqualNullSafe | true |
| EqualTo | Enable Comet acceleration for EqualTo | true |
| Exp | Enable Comet acceleration for Exp | true |
| Expm1 | Enable Comet acceleration for Expm1 | true |
| Flatten | Enable Comet acceleration for Flatten | true |
| Floor | Enable Comet acceleration for Floor | true |
| FromUnixTime | Enable Comet acceleration for FromUnixTime | true |
| GetArrayItem | Enable Comet acceleration for GetArrayItem | true |
| GetArrayStructFields | Enable Comet acceleration for GetArrayStructFields | true |
| GetMapValue | Enable Comet acceleration for GetMapValue | true |
| GetStructField | Enable Comet acceleration for GetStructField | true |
| GreaterThan | Enable Comet acceleration for GreaterThan | true |
| GreaterThanOrEqual | Enable Comet acceleration for GreaterThanOrEqual | true |
| Hex | Enable Comet acceleration for Hex | true |
| Hour | Enable Comet acceleration for Hour | true |
| If | Enable Comet acceleration for If | true |
| In | Enable Comet acceleration for In | true |
| InSet | Enable Comet acceleration for InSet | true |
| InitCap | Enable Comet acceleration for InitCap | true |
| IntegralDivide | Enable Comet acceleration for IntegralDivide | true |
| IsNaN | Enable Comet acceleration for IsNaN | true |
| IsNotNull | Enable Comet acceleration for IsNotNull | true |
| IsNull | Enable Comet acceleration for IsNull | true |
| Length | Enable Comet acceleration for Length | true |
| LessThan | Enable Comet acceleration for LessThan | true |
| LessThanOrEqual | Enable Comet acceleration for LessThanOrEqual | true |
| Like | Enable Comet acceleration for Like | true |
| Literal | Enable Comet acceleration for Literal | true |
| Log | Enable Comet acceleration for Log | true |
| Log10 | Enable Comet acceleration for Log10 | true |
| Log2 | Enable Comet acceleration for Log2 | true |
| Lower | Enable Comet acceleration for Lower | true |
| MapEntries | Enable Comet acceleration for MapEntries | true |
| MapFromArrays | Enable Comet acceleration for MapFromArrays | true |
| MapKeys | Enable Comet acceleration for MapKeys | true |
| MapValues | Enable Comet acceleration for MapValues | true |
| Md5 | Enable Comet acceleration for Md5 | true |
| Minute | Enable Comet acceleration for Minute | true |
| MonotonicallyIncreasingID | Enable Comet acceleration for MonotonicallyIncreasingID | true |
| Month | Enable Comet acceleration for Month | true |
| Multiply | Enable Comet acceleration for Multiply | true |
| Murmur3Hash | Enable Comet acceleration for Murmur3Hash | true |
| Not | Enable Comet acceleration for Not | true |
| OctetLength | Enable Comet acceleration for OctetLength | true |
| Or | Enable Comet acceleration for Or | true |
| Pow | Enable Comet acceleration for Pow | true |
| Quarter | Enable Comet acceleration for Quarter | true |
| RLike | Enable Comet acceleration for RLike | true |
| Rand | Enable Comet acceleration for Rand | true |
| Randn | Enable Comet acceleration for Randn | true |
| Remainder | Enable Comet acceleration for Remainder | true |
| Reverse | Enable Comet acceleration for Reverse | true |
| Round | Enable Comet acceleration for Round | true |
| Second | Enable Comet acceleration for Second | true |
| Sha2 | Enable Comet acceleration for Sha2 | true |
| ShiftLeft | Enable Comet acceleration for ShiftLeft | true |
| ShiftRight | Enable Comet acceleration for ShiftRight | true |
| Signum | Enable Comet acceleration for Signum | true |
| Sin | Enable Comet acceleration for Sin | true |
| SparkPartitionID | Enable Comet acceleration for SparkPartitionID | true |
| Sqrt | Enable Comet acceleration for Sqrt | true |
| StartsWith | Enable Comet acceleration for StartsWith | true |
| StringInstr | Enable Comet acceleration for StringInstr | true |
| StringLPad | Enable Comet acceleration for StringLPad | true |
| StringRPad | Enable Comet acceleration for StringRPad | true |
| StringRepeat | Enable Comet acceleration for StringRepeat | true |
| StringReplace | Enable Comet acceleration for StringReplace | true |
| StringSpace | Enable Comet acceleration for StringSpace | true |
| StringTranslate | Enable Comet acceleration for StringTranslate | true |
| StringTrim | Enable Comet acceleration for StringTrim | true |
| StringTrimBoth | Enable Comet acceleration for StringTrimBoth | true |
| StringTrimLeft | Enable Comet acceleration for StringTrimLeft | true |
| StringTrimRight | Enable Comet acceleration for StringTrimRight | true |
| StructsToJson | Enable Comet acceleration for StructsToJson | true |
| Substring | Enable Comet acceleration for Substring | true |
| Subtract | Enable Comet acceleration for Subtract | true |
| Tan | Enable Comet acceleration for Tan | true |
| TruncDate | Enable Comet acceleration for TruncDate | true |
| TruncTimestamp | Enable Comet acceleration for TruncTimestamp | true |
| UnaryMinus | Enable Comet acceleration for UnaryMinus | true |
| Unhex | Enable Comet acceleration for Unhex | true |
| Upper | Enable Comet acceleration for Upper | true |
| WeekDay | Enable Comet acceleration for WeekDay | true |
| WeekOfYear | Enable Comet acceleration for WeekOfYear | true |
| XxHash64 | Enable Comet acceleration for XxHash64 | true |
| Year | Enable Comet acceleration for Year | true |
<!--END:CONFIG_TABLE-->

## Shuffle Configuration Settings

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[shuffle]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| spark.comet.columnar.shuffle.async.enabled | Whether to enable asynchronous shuffle for Arrow-based shuffle. | false |
| spark.comet.columnar.shuffle.async.max.thread.num | Maximum number of threads on an executor used for Comet async columnar shuffle. This is the upper bound of total number of shuffle threads per executor. In other words, if the number of cores * the number of shuffle threads per task `spark.comet.columnar.shuffle.async.thread.num` is larger than this config. Comet will use this config as the number of shuffle threads per executor instead. | 100 |
| spark.comet.columnar.shuffle.async.thread.num | Number of threads used for Comet async columnar shuffle per shuffle task. Note that more threads means more memory requirement to buffer shuffle data before flushing to disk. Also, more threads may not always improve performance, and should be set based on the number of cores available. | 3 |
| spark.comet.columnar.shuffle.batch.size | Batch size when writing out sorted spill files on the native side. Note that this should not be larger than batch size (i.e., `spark.comet.batchSize`). Otherwise it will produce larger batches than expected in the native operator after shuffle. | 8192 |
| spark.comet.exec.shuffle.compression.codec | The codec of Comet native shuffle used to compress shuffle data. lz4, zstd, and snappy are supported. Compression can be disabled by setting spark.shuffle.compress=false. | lz4 |
| spark.comet.exec.shuffle.compression.zstd.level | The compression level to use when compressing shuffle files with zstd. | 1 |
| spark.comet.exec.shuffle.enabled | Whether to enable Comet native shuffle. Note that this requires setting 'spark.shuffle.manager' to 'org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager'. 'spark.shuffle.manager' must be set before starting the Spark application and cannot be changed during the application. | true |
| spark.comet.native.shuffle.partitioning.hash.enabled | Whether to enable hash partitioning for Comet native shuffle. | true |
| spark.comet.native.shuffle.partitioning.range.enabled | Whether to enable range partitioning for Comet native shuffle. | true |
| spark.comet.shuffle.preferDictionary.ratio | The ratio of total values to distinct values in a string column to decide whether to prefer dictionary encoding when shuffling the column. If the ratio is higher than this config, dictionary encoding will be used on shuffling string column. This config is effective if it is higher than 1.0. Note that this config is only used when `spark.comet.exec.shuffle.mode` is `jvm`. | 10.0 |
| spark.comet.shuffle.sizeInBytesMultiplier | Comet reports smaller sizes for shuffle due to using Arrow's columnar memory format and this can result in Spark choosing a different join strategy due to the estimated size of the exchange being smaller. Comet will multiple sizeInBytes by this amount to avoid regressions in join strategy. | 1.0 |
<!--END:CONFIG_TABLE-->

## Memory & Tuning Configuration Settings

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[tuning]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| spark.comet.batchSize | The columnar batch size, i.e., the maximum number of rows that a batch can contain. | 8192 |
| spark.comet.exec.memoryPool | The type of memory pool to be used for Comet native execution. When running Spark in on-heap mode, available pool types are 'greedy', 'fair_spill', 'greedy_task_shared', 'fair_spill_task_shared', 'greedy_global', 'fair_spill_global', and `unbounded`. When running Spark in off-heap mode, available pool types are 'greedy_unified' and `fair_unified`. The default pool type is `greedy_task_shared` for on-heap mode and `unified` for off-heap mode. For more information, refer to the Comet Tuning Guide (https://datafusion.apache.org/comet/user-guide/tuning.html). | default |
| spark.comet.tracing.enabled | Enable fine-grained tracing of events and memory usage. For more information, refer to the Comet Tracing Guide (https://datafusion.apache.org/comet/user-guide/tracing.html). | false |
<!--END:CONFIG_TABLE-->

## Configuration Settings Used In Tests

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[testing]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| spark.comet.exec.onHeap.enabled | Whether to allow Comet to run in on-heap mode. Required for running Spark SQL tests. | false |
| spark.comet.memory.overhead.factor | Fraction of executor memory to be allocated as additional memory for Comet when running Spark in on-heap mode. For more information, refer to the Comet Tuning Guide (https://datafusion.apache.org/comet/user-guide/tuning.html). | 0.2 |
| spark.comet.memory.overhead.min | Minimum amount of additional memory to be allocated per executor process for Comet, in MiB, when running Spark in on-heap mode. For more information, refer to the Comet Tuning Guide (https://datafusion.apache.org/comet/user-guide/tuning.html). | 402653184b |
| spark.comet.memoryOverhead | The amount of additional memory to be allocated per executor process for Comet, in MiB, when running Spark in on-heap mode. This config is optional. If this is not specified, it will be set to `spark.comet.memory.overhead.factor` * `spark.executor.memory`. For more information, refer to the Comet Tuning Guide (https://datafusion.apache.org/comet/user-guide/tuning.html). | |
<!--END:CONFIG_TABLE-->
