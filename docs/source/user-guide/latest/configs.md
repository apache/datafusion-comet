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
| `spark.comet.scan.allowIncompatible` | Some Comet scan implementations are not currently fully compatible with Spark for all datatypes. Set this config to true to allow them anyway. For more information, refer to the [Comet Compatibility Guide](https://datafusion.apache.org/comet/user-guide/compatibility.html). | false |
| `spark.comet.scan.enabled` | Whether to enable native scans. When this is turned on, Spark will use Comet to read supported data sources (currently only Parquet is supported natively). Note that to enable native vectorized execution, both this config and `spark.comet.exec.enabled` need to be enabled. | true |
| `spark.comet.scan.preFetch.enabled` | Whether to enable pre-fetching feature of CometScan. | false |
| `spark.comet.scan.preFetch.threadNum` | The number of threads running pre-fetching for CometScan. Effective if spark.comet.scan.preFetch.enabled is enabled. Note that more pre-fetching threads means more memory requirement to store pre-fetched row groups. | 2 |
| `spark.hadoop.fs.comet.libhdfs.schemes` | Defines filesystem schemes (e.g., hdfs, webhdfs) that the native side accesses via libhdfs, separated by commas. Valid only when built with hdfs feature enabled. | |
<!--END:CONFIG_TABLE-->

## Parquet Reader Configuration Settings

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[parquet]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| `spark.comet.parquet.enable.directBuffer` | Whether to use Java direct byte buffer when reading Parquet. | false |
| `spark.comet.parquet.read.io.adjust.readRange.skew` | In the parallel reader, if the read ranges submitted are skewed in sizes, this option will cause the reader to break up larger read ranges into smaller ranges to reduce the skew. This will result in a slightly larger number of connections opened to the file system but may give improved performance. | false |
| `spark.comet.parquet.read.io.mergeRanges` | When enabled the parallel reader will try to merge ranges of data that are separated by less than `comet.parquet.read.io.mergeRanges.delta` bytes. Longer continuous reads are faster on cloud storage. | true |
| `spark.comet.parquet.read.io.mergeRanges.delta` | The delta in bytes between consecutive read ranges below which the parallel reader will try to merge the ranges. The default is 8MB. | 8388608 |
| `spark.comet.parquet.read.parallel.io.enabled` | Whether to enable Comet's parallel reader for Parquet files. The parallel reader reads ranges of consecutive data in a  file in parallel. It is faster for large files and row groups but uses more resources. | true |
| `spark.comet.parquet.read.parallel.io.thread-pool.size` | The maximum number of parallel threads the parallel reader will use in a single executor. For executors configured with a smaller number of cores, use a smaller number. | 16 |
| `spark.comet.parquet.respectFilterPushdown` | Whether to respect Spark's PARQUET_FILTER_PUSHDOWN_ENABLED config. This needs to be respected when running the Spark SQL test suite but the default setting results in poor performance in Comet when using the new native scans, disabled by default | false |
<!--END:CONFIG_TABLE-->

## Query Execution Settings

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[exec]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| `spark.comet.caseConversion.enabled` | Java uses locale-specific rules when converting strings to upper or lower case and Rust does not, so we disable upper and lower by default. | false |
| `spark.comet.debug.enabled` | Whether to enable debug mode for Comet. When enabled, Comet will do additional checks for debugging purpose. For example, validating array when importing arrays from JVM at native side. Note that these checks may be expensive in performance and should only be enabled for debugging purpose. | false |
| `spark.comet.dppFallback.enabled` | Whether to fall back to Spark for queries that use DPP. | true |
| `spark.comet.enabled` | Whether to enable Comet extension for Spark. When this is turned on, Spark will use Comet to read Parquet data source. Note that to enable native vectorized execution, both this config and `spark.comet.exec.enabled` need to be enabled. Can be overridden by environment variable `ENABLE_COMET`. | true |
| `spark.comet.exceptionOnDatetimeRebase` | Whether to throw exception when seeing dates/timestamps from the legacy hybrid (Julian + Gregorian) calendar. Since Spark 3, dates/timestamps were written according to the Proleptic Gregorian calendar. When this is true, Comet will throw exceptions when seeing these dates/timestamps that were written by Spark version before 3.0. If this is false, these dates/timestamps will be read as if they were written to the Proleptic Gregorian calendar and will not be rebased. | false |
| `spark.comet.exec.enabled` | Whether to enable Comet native vectorized execution for Spark. This controls whether Spark should convert operators into their Comet counterparts and execute them in native space. Note: each operator is associated with a separate config in the format of `spark.comet.exec.<operator_name>.enabled` at the moment, and both the config and this need to be turned on, in order for the operator to be executed in native. | true |
| `spark.comet.exec.replaceSortMergeJoin` | Experimental feature to force Spark to replace SortMergeJoin with ShuffledHashJoin for improved performance. This feature is not stable yet. For more information, refer to the [Comet Tuning Guide](https://datafusion.apache.org/comet/user-guide/tuning.html). | false |
| `spark.comet.expression.allowIncompatible` | Comet is not currently fully compatible with Spark for all expressions. Set this config to true to allow them anyway. For more information, refer to the [Comet Compatibility Guide](https://datafusion.apache.org/comet/user-guide/compatibility.html). | false |
| `spark.comet.maxTempDirectorySize` | The maximum amount of data (in bytes) stored inside the temporary directories. | 107374182400b |
| `spark.comet.metrics.updateInterval` | The interval in milliseconds to update metrics. If interval is negative, metrics will be updated upon task completion. | 3000 |
| `spark.comet.nativeLoadRequired` | Whether to require Comet native library to load successfully when Comet is enabled. If not, Comet will silently fallback to Spark when it fails to load the native lib. Otherwise, an error will be thrown and the Spark job will be aborted. | false |
| `spark.comet.regexp.allowIncompatible` | Comet is not currently fully compatible with Spark for all regular expressions. Set this config to true to allow them anyway. For more information, refer to the [Comet Compatibility Guide](https://datafusion.apache.org/comet/user-guide/compatibility.html). | false |
<!--END:CONFIG_TABLE-->

## Viewing Explain Plan & Fallback Reasons

These settings can be used to determine which parts of the plan are accelerated by Comet and to see why some parts of the plan could not be supported by Comet.

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[exec_explain]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| `spark.comet.explain.format` | Choose extended explain output. The default format of 'verbose' will provide the full query plan annotated with fallback reasons as well as a summary of how much of the plan was accelerated by Comet. The format 'fallback' provides a list of fallback reasons instead. | verbose |
| `spark.comet.explain.native.enabled` | When this setting is enabled, Comet will provide a tree representation of the native query plan before execution and again after execution, with metrics. | false |
| `spark.comet.explain.rules` | When this setting is enabled, Comet will log all plan transformations performed in physical optimizer rules. Default: false | false |
| `spark.comet.explainFallback.enabled` | When this setting is enabled, Comet will provide logging explaining the reason(s) why a query stage cannot be executed natively. Set this to false to reduce the amount of logging. | false |
| `spark.comet.logFallbackReasons.enabled` | When this setting is enabled, Comet will log warnings for all fallback reasons. Can be overridden by environment variable `ENABLE_COMET_LOG_FALLBACK_REASONS`. | true |
<!--END:CONFIG_TABLE-->

## Shuffle Configuration Settings

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[shuffle]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| `spark.comet.columnar.shuffle.async.enabled` | Whether to enable asynchronous shuffle for Arrow-based shuffle. | false |
| `spark.comet.columnar.shuffle.async.max.thread.num` | Maximum number of threads on an executor used for Comet async columnar shuffle. This is the upper bound of total number of shuffle threads per executor. In other words, if the number of cores * the number of shuffle threads per task `spark.comet.columnar.shuffle.async.thread.num` is larger than this config. Comet will use this config as the number of shuffle threads per executor instead. | 100 |
| `spark.comet.columnar.shuffle.async.thread.num` | Number of threads used for Comet async columnar shuffle per shuffle task. Note that more threads means more memory requirement to buffer shuffle data before flushing to disk. Also, more threads may not always improve performance, and should be set based on the number of cores available. | 3 |
| `spark.comet.columnar.shuffle.batch.size` | Batch size when writing out sorted spill files on the native side. Note that this should not be larger than batch size (i.e., `spark.comet.batchSize`). Otherwise it will produce larger batches than expected in the native operator after shuffle. | 8192 |
| `spark.comet.exec.shuffle.compression.codec` | The codec of Comet native shuffle used to compress shuffle data. lz4, zstd, and snappy are supported. Compression can be disabled by setting spark.shuffle.compress=false. | lz4 |
| `spark.comet.exec.shuffle.compression.zstd.level` | The compression level to use when compressing shuffle files with zstd. | 1 |
| `spark.comet.exec.shuffle.enabled` | Whether to enable Comet native shuffle. Note that this requires setting `spark.shuffle.manager` to `org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager`. `spark.shuffle.manager` must be set before starting the Spark application and cannot be changed during the application. | true |
| `spark.comet.native.shuffle.partitioning.hash.enabled` | Whether to enable hash partitioning for Comet native shuffle. | true |
| `spark.comet.native.shuffle.partitioning.range.enabled` | Whether to enable range partitioning for Comet native shuffle. | true |
| `spark.comet.shuffle.preferDictionary.ratio` | The ratio of total values to distinct values in a string column to decide whether to prefer dictionary encoding when shuffling the column. If the ratio is higher than this config, dictionary encoding will be used on shuffling string column. This config is effective if it is higher than 1.0. Note that this config is only used when `spark.comet.exec.shuffle.mode` is `jvm`. | 10.0 |
| `spark.comet.shuffle.sizeInBytesMultiplier` | Comet reports smaller sizes for shuffle due to using Arrow's columnar memory format and this can result in Spark choosing a different join strategy due to the estimated size of the exchange being smaller. Comet will multiple sizeInBytes by this amount to avoid regressions in join strategy. | 1.0 |
<!--END:CONFIG_TABLE-->

## Memory & Tuning Configuration Settings

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[tuning]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| `spark.comet.batchSize` | The columnar batch size, i.e., the maximum number of rows that a batch can contain. | 8192 |
| `spark.comet.exec.memoryPool` | The type of memory pool to be used for Comet native execution when running Spark in off-heap mode. Available pool types are `greedy_unified` and `fair_unified`. For more information, refer to the [Comet Tuning Guide](https://datafusion.apache.org/comet/user-guide/tuning.html). | fair_unified |
| `spark.comet.exec.memoryPool.fraction` | Fraction of off-heap memory pool that is available to Comet. Only applies to off-heap mode. For more information, refer to the [Comet Tuning Guide](https://datafusion.apache.org/comet/user-guide/tuning.html). | 1.0 |
| `spark.comet.tracing.enabled` | Enable fine-grained tracing of events and memory usage. For more information, refer to the [Comet Tracing Guide](https://datafusion.apache.org/comet/user-guide/tracing.html). | false |
<!--END:CONFIG_TABLE-->

## Development & Testing Settings

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[testing]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| `spark.comet.columnar.shuffle.memory.factor` | Fraction of Comet memory to be allocated per executor process for columnar shuffle when running in on-heap mode. For more information, refer to the [Comet Tuning Guide](https://datafusion.apache.org/comet/user-guide/tuning.html). | 1.0 |
| `spark.comet.convert.csv.enabled` | When enabled, data from Spark (non-native) CSV v1 and v2 scans will be converted to Arrow format. This is an experimental feature and has known issues with non-UTC timezones. | false |
| `spark.comet.convert.json.enabled` | When enabled, data from Spark (non-native) JSON v1 and v2 scans will be converted to Arrow format. This is an experimental feature and has known issues with non-UTC timezones. | false |
| `spark.comet.convert.parquet.enabled` | When enabled, data from Spark (non-native) Parquet v1 and v2 scans will be converted to Arrow format.  This is an experimental feature and has known issues with non-UTC timezones. | false |
| `spark.comet.exec.onHeap.enabled` | Whether to allow Comet to run in on-heap mode. Required for running Spark SQL tests. Can be overridden by environment variable `ENABLE_COMET_ONHEAP`. | false |
| `spark.comet.exec.onHeap.memoryPool` | The type of memory pool to be used for Comet native execution when running Spark in on-heap mode. Available pool types are `greedy`, `fair_spill`, `greedy_task_shared`, `fair_spill_task_shared`, `greedy_global`, `fair_spill_global`, and `unbounded`. | greedy_task_shared |
| `spark.comet.memoryOverhead` | The amount of additional memory to be allocated per executor process for Comet, in MiB, when running Spark in on-heap mode. | 1024 MiB |
| `spark.comet.sparkToColumnar.enabled` | Whether to enable Spark to Arrow columnar conversion. When this is turned on, Comet will convert operators in `spark.comet.sparkToColumnar.supportedOperatorList` into Arrow columnar format before processing. This is an experimental feature and has known issues with non-UTC timezones. | false |
| `spark.comet.sparkToColumnar.supportedOperatorList` | A comma-separated list of operators that will be converted to Arrow columnar format when `spark.comet.sparkToColumnar.enabled` is true. | Range,InMemoryTableScan,RDDScan |
| `spark.comet.testing.strict` | Experimental option to enable strict testing, which will fail tests that could be more comprehensive, such as checking for a specific fallback reason. Can be overridden by environment variable `ENABLE_COMET_STRICT_TESTING`. | false |
<!--END:CONFIG_TABLE-->

## Enabling or Disabling Individual Operators

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[enable_exec]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| `spark.comet.exec.aggregate.enabled` | Whether to enable aggregate by default. | true |
| `spark.comet.exec.broadcastExchange.enabled` | Whether to enable broadcastExchange by default. | true |
| `spark.comet.exec.broadcastHashJoin.enabled` | Whether to enable broadcastHashJoin by default. | true |
| `spark.comet.exec.coalesce.enabled` | Whether to enable coalesce by default. | true |
| `spark.comet.exec.collectLimit.enabled` | Whether to enable collectLimit by default. | true |
| `spark.comet.exec.expand.enabled` | Whether to enable expand by default. | true |
| `spark.comet.exec.filter.enabled` | Whether to enable filter by default. | true |
| `spark.comet.exec.globalLimit.enabled` | Whether to enable globalLimit by default. | true |
| `spark.comet.exec.hashJoin.enabled` | Whether to enable hashJoin by default. | true |
| `spark.comet.exec.localLimit.enabled` | Whether to enable localLimit by default. | true |
| `spark.comet.exec.localTableScan.enabled` | Whether to enable localTableScan by default. | false |
| `spark.comet.exec.project.enabled` | Whether to enable project by default. | true |
| `spark.comet.exec.sort.enabled` | Whether to enable sort by default. | true |
| `spark.comet.exec.sortMergeJoin.enabled` | Whether to enable sortMergeJoin by default. | true |
| `spark.comet.exec.sortMergeJoinWithJoinFilter.enabled` | Experimental support for Sort Merge Join with filter | false |
| `spark.comet.exec.takeOrderedAndProject.enabled` | Whether to enable takeOrderedAndProject by default. | true |
| `spark.comet.exec.union.enabled` | Whether to enable union by default. | true |
| `spark.comet.exec.window.enabled` | Whether to enable window by default. | true |
<!--END:CONFIG_TABLE-->

## Enabling or Disabling Individual Scalar Expressions

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[enable_expr]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| `spark.comet.expression.Abs.enabled` | Enable Comet acceleration for `Abs` | true |
| `spark.comet.expression.Acos.enabled` | Enable Comet acceleration for `Acos` | true |
| `spark.comet.expression.Add.enabled` | Enable Comet acceleration for `Add` | true |
| `spark.comet.expression.Alias.enabled` | Enable Comet acceleration for `Alias` | true |
| `spark.comet.expression.And.enabled` | Enable Comet acceleration for `And` | true |
| `spark.comet.expression.ArrayAppend.enabled` | Enable Comet acceleration for `ArrayAppend` | true |
| `spark.comet.expression.ArrayCompact.enabled` | Enable Comet acceleration for `ArrayCompact` | true |
| `spark.comet.expression.ArrayContains.enabled` | Enable Comet acceleration for `ArrayContains` | true |
| `spark.comet.expression.ArrayDistinct.enabled` | Enable Comet acceleration for `ArrayDistinct` | true |
| `spark.comet.expression.ArrayExcept.enabled` | Enable Comet acceleration for `ArrayExcept` | true |
| `spark.comet.expression.ArrayFilter.enabled` | Enable Comet acceleration for `ArrayFilter` | true |
| `spark.comet.expression.ArrayInsert.enabled` | Enable Comet acceleration for `ArrayInsert` | true |
| `spark.comet.expression.ArrayIntersect.enabled` | Enable Comet acceleration for `ArrayIntersect` | true |
| `spark.comet.expression.ArrayJoin.enabled` | Enable Comet acceleration for `ArrayJoin` | true |
| `spark.comet.expression.ArrayMax.enabled` | Enable Comet acceleration for `ArrayMax` | true |
| `spark.comet.expression.ArrayMin.enabled` | Enable Comet acceleration for `ArrayMin` | true |
| `spark.comet.expression.ArrayRemove.enabled` | Enable Comet acceleration for `ArrayRemove` | true |
| `spark.comet.expression.ArrayRepeat.enabled` | Enable Comet acceleration for `ArrayRepeat` | true |
| `spark.comet.expression.ArrayUnion.enabled` | Enable Comet acceleration for `ArrayUnion` | true |
| `spark.comet.expression.ArraysOverlap.enabled` | Enable Comet acceleration for `ArraysOverlap` | true |
| `spark.comet.expression.Ascii.enabled` | Enable Comet acceleration for `Ascii` | true |
| `spark.comet.expression.Asin.enabled` | Enable Comet acceleration for `Asin` | true |
| `spark.comet.expression.Atan.enabled` | Enable Comet acceleration for `Atan` | true |
| `spark.comet.expression.Atan2.enabled` | Enable Comet acceleration for `Atan2` | true |
| `spark.comet.expression.AttributeReference.enabled` | Enable Comet acceleration for `AttributeReference` | true |
| `spark.comet.expression.BitLength.enabled` | Enable Comet acceleration for `BitLength` | true |
| `spark.comet.expression.BitwiseAnd.enabled` | Enable Comet acceleration for `BitwiseAnd` | true |
| `spark.comet.expression.BitwiseCount.enabled` | Enable Comet acceleration for `BitwiseCount` | true |
| `spark.comet.expression.BitwiseGet.enabled` | Enable Comet acceleration for `BitwiseGet` | true |
| `spark.comet.expression.BitwiseNot.enabled` | Enable Comet acceleration for `BitwiseNot` | true |
| `spark.comet.expression.BitwiseOr.enabled` | Enable Comet acceleration for `BitwiseOr` | true |
| `spark.comet.expression.BitwiseXor.enabled` | Enable Comet acceleration for `BitwiseXor` | true |
| `spark.comet.expression.CaseWhen.enabled` | Enable Comet acceleration for `CaseWhen` | true |
| `spark.comet.expression.Cast.enabled` | Enable Comet acceleration for `Cast` | true |
| `spark.comet.expression.Ceil.enabled` | Enable Comet acceleration for `Ceil` | true |
| `spark.comet.expression.CheckOverflow.enabled` | Enable Comet acceleration for `CheckOverflow` | true |
| `spark.comet.expression.Chr.enabled` | Enable Comet acceleration for `Chr` | true |
| `spark.comet.expression.Coalesce.enabled` | Enable Comet acceleration for `Coalesce` | true |
| `spark.comet.expression.Concat.enabled` | Enable Comet acceleration for `Concat` | true |
| `spark.comet.expression.ConcatWs.enabled` | Enable Comet acceleration for `ConcatWs` | true |
| `spark.comet.expression.Contains.enabled` | Enable Comet acceleration for `Contains` | true |
| `spark.comet.expression.Cos.enabled` | Enable Comet acceleration for `Cos` | true |
| `spark.comet.expression.CreateArray.enabled` | Enable Comet acceleration for `CreateArray` | true |
| `spark.comet.expression.CreateNamedStruct.enabled` | Enable Comet acceleration for `CreateNamedStruct` | true |
| `spark.comet.expression.DateAdd.enabled` | Enable Comet acceleration for `DateAdd` | true |
| `spark.comet.expression.DateSub.enabled` | Enable Comet acceleration for `DateSub` | true |
| `spark.comet.expression.DayOfMonth.enabled` | Enable Comet acceleration for `DayOfMonth` | true |
| `spark.comet.expression.DayOfWeek.enabled` | Enable Comet acceleration for `DayOfWeek` | true |
| `spark.comet.expression.DayOfYear.enabled` | Enable Comet acceleration for `DayOfYear` | true |
| `spark.comet.expression.Divide.enabled` | Enable Comet acceleration for `Divide` | true |
| `spark.comet.expression.ElementAt.enabled` | Enable Comet acceleration for `ElementAt` | true |
| `spark.comet.expression.EndsWith.enabled` | Enable Comet acceleration for `EndsWith` | true |
| `spark.comet.expression.EqualNullSafe.enabled` | Enable Comet acceleration for `EqualNullSafe` | true |
| `spark.comet.expression.EqualTo.enabled` | Enable Comet acceleration for `EqualTo` | true |
| `spark.comet.expression.Exp.enabled` | Enable Comet acceleration for `Exp` | true |
| `spark.comet.expression.Expm1.enabled` | Enable Comet acceleration for `Expm1` | true |
| `spark.comet.expression.Flatten.enabled` | Enable Comet acceleration for `Flatten` | true |
| `spark.comet.expression.Floor.enabled` | Enable Comet acceleration for `Floor` | true |
| `spark.comet.expression.FromUnixTime.enabled` | Enable Comet acceleration for `FromUnixTime` | true |
| `spark.comet.expression.GetArrayItem.enabled` | Enable Comet acceleration for `GetArrayItem` | true |
| `spark.comet.expression.GetArrayStructFields.enabled` | Enable Comet acceleration for `GetArrayStructFields` | true |
| `spark.comet.expression.GetMapValue.enabled` | Enable Comet acceleration for `GetMapValue` | true |
| `spark.comet.expression.GetStructField.enabled` | Enable Comet acceleration for `GetStructField` | true |
| `spark.comet.expression.GreaterThan.enabled` | Enable Comet acceleration for `GreaterThan` | true |
| `spark.comet.expression.GreaterThanOrEqual.enabled` | Enable Comet acceleration for `GreaterThanOrEqual` | true |
| `spark.comet.expression.Hex.enabled` | Enable Comet acceleration for `Hex` | true |
| `spark.comet.expression.Hour.enabled` | Enable Comet acceleration for `Hour` | true |
| `spark.comet.expression.If.enabled` | Enable Comet acceleration for `If` | true |
| `spark.comet.expression.In.enabled` | Enable Comet acceleration for `In` | true |
| `spark.comet.expression.InSet.enabled` | Enable Comet acceleration for `InSet` | true |
| `spark.comet.expression.InitCap.enabled` | Enable Comet acceleration for `InitCap` | true |
| `spark.comet.expression.IntegralDivide.enabled` | Enable Comet acceleration for `IntegralDivide` | true |
| `spark.comet.expression.IsNaN.enabled` | Enable Comet acceleration for `IsNaN` | true |
| `spark.comet.expression.IsNotNull.enabled` | Enable Comet acceleration for `IsNotNull` | true |
| `spark.comet.expression.IsNull.enabled` | Enable Comet acceleration for `IsNull` | true |
| `spark.comet.expression.Length.enabled` | Enable Comet acceleration for `Length` | true |
| `spark.comet.expression.LessThan.enabled` | Enable Comet acceleration for `LessThan` | true |
| `spark.comet.expression.LessThanOrEqual.enabled` | Enable Comet acceleration for `LessThanOrEqual` | true |
| `spark.comet.expression.Like.enabled` | Enable Comet acceleration for `Like` | true |
| `spark.comet.expression.Literal.enabled` | Enable Comet acceleration for `Literal` | true |
| `spark.comet.expression.Log.enabled` | Enable Comet acceleration for `Log` | true |
| `spark.comet.expression.Log10.enabled` | Enable Comet acceleration for `Log10` | true |
| `spark.comet.expression.Log2.enabled` | Enable Comet acceleration for `Log2` | true |
| `spark.comet.expression.Lower.enabled` | Enable Comet acceleration for `Lower` | true |
| `spark.comet.expression.MapEntries.enabled` | Enable Comet acceleration for `MapEntries` | true |
| `spark.comet.expression.MapFromArrays.enabled` | Enable Comet acceleration for `MapFromArrays` | true |
| `spark.comet.expression.MapKeys.enabled` | Enable Comet acceleration for `MapKeys` | true |
| `spark.comet.expression.MapValues.enabled` | Enable Comet acceleration for `MapValues` | true |
| `spark.comet.expression.Md5.enabled` | Enable Comet acceleration for `Md5` | true |
| `spark.comet.expression.Minute.enabled` | Enable Comet acceleration for `Minute` | true |
| `spark.comet.expression.MonotonicallyIncreasingID.enabled` | Enable Comet acceleration for `MonotonicallyIncreasingID` | true |
| `spark.comet.expression.Month.enabled` | Enable Comet acceleration for `Month` | true |
| `spark.comet.expression.Multiply.enabled` | Enable Comet acceleration for `Multiply` | true |
| `spark.comet.expression.Murmur3Hash.enabled` | Enable Comet acceleration for `Murmur3Hash` | true |
| `spark.comet.expression.Not.enabled` | Enable Comet acceleration for `Not` | true |
| `spark.comet.expression.OctetLength.enabled` | Enable Comet acceleration for `OctetLength` | true |
| `spark.comet.expression.Or.enabled` | Enable Comet acceleration for `Or` | true |
| `spark.comet.expression.Pow.enabled` | Enable Comet acceleration for `Pow` | true |
| `spark.comet.expression.Quarter.enabled` | Enable Comet acceleration for `Quarter` | true |
| `spark.comet.expression.RLike.enabled` | Enable Comet acceleration for `RLike` | true |
| `spark.comet.expression.Rand.enabled` | Enable Comet acceleration for `Rand` | true |
| `spark.comet.expression.Randn.enabled` | Enable Comet acceleration for `Randn` | true |
| `spark.comet.expression.RegExpReplace.enabled` | Enable Comet acceleration for `RegExpReplace` | true |
| `spark.comet.expression.Remainder.enabled` | Enable Comet acceleration for `Remainder` | true |
| `spark.comet.expression.Reverse.enabled` | Enable Comet acceleration for `Reverse` | true |
| `spark.comet.expression.Round.enabled` | Enable Comet acceleration for `Round` | true |
| `spark.comet.expression.Second.enabled` | Enable Comet acceleration for `Second` | true |
| `spark.comet.expression.Sha1.enabled` | Enable Comet acceleration for `Sha1` | true |
| `spark.comet.expression.Sha2.enabled` | Enable Comet acceleration for `Sha2` | true |
| `spark.comet.expression.ShiftLeft.enabled` | Enable Comet acceleration for `ShiftLeft` | true |
| `spark.comet.expression.ShiftRight.enabled` | Enable Comet acceleration for `ShiftRight` | true |
| `spark.comet.expression.Signum.enabled` | Enable Comet acceleration for `Signum` | true |
| `spark.comet.expression.Sin.enabled` | Enable Comet acceleration for `Sin` | true |
| `spark.comet.expression.SortOrder.enabled` | Enable Comet acceleration for `SortOrder` | true |
| `spark.comet.expression.SparkPartitionID.enabled` | Enable Comet acceleration for `SparkPartitionID` | true |
| `spark.comet.expression.Sqrt.enabled` | Enable Comet acceleration for `Sqrt` | true |
| `spark.comet.expression.StartsWith.enabled` | Enable Comet acceleration for `StartsWith` | true |
| `spark.comet.expression.StaticInvoke.enabled` | Enable Comet acceleration for `StaticInvoke` | true |
| `spark.comet.expression.StringInstr.enabled` | Enable Comet acceleration for `StringInstr` | true |
| `spark.comet.expression.StringLPad.enabled` | Enable Comet acceleration for `StringLPad` | true |
| `spark.comet.expression.StringRPad.enabled` | Enable Comet acceleration for `StringRPad` | true |
| `spark.comet.expression.StringRepeat.enabled` | Enable Comet acceleration for `StringRepeat` | true |
| `spark.comet.expression.StringReplace.enabled` | Enable Comet acceleration for `StringReplace` | true |
| `spark.comet.expression.StringSpace.enabled` | Enable Comet acceleration for `StringSpace` | true |
| `spark.comet.expression.StringTranslate.enabled` | Enable Comet acceleration for `StringTranslate` | true |
| `spark.comet.expression.StringTrim.enabled` | Enable Comet acceleration for `StringTrim` | true |
| `spark.comet.expression.StringTrimBoth.enabled` | Enable Comet acceleration for `StringTrimBoth` | true |
| `spark.comet.expression.StringTrimLeft.enabled` | Enable Comet acceleration for `StringTrimLeft` | true |
| `spark.comet.expression.StringTrimRight.enabled` | Enable Comet acceleration for `StringTrimRight` | true |
| `spark.comet.expression.StructsToJson.enabled` | Enable Comet acceleration for `StructsToJson` | true |
| `spark.comet.expression.Substring.enabled` | Enable Comet acceleration for `Substring` | true |
| `spark.comet.expression.Subtract.enabled` | Enable Comet acceleration for `Subtract` | true |
| `spark.comet.expression.Tan.enabled` | Enable Comet acceleration for `Tan` | true |
| `spark.comet.expression.TruncDate.enabled` | Enable Comet acceleration for `TruncDate` | true |
| `spark.comet.expression.TruncTimestamp.enabled` | Enable Comet acceleration for `TruncTimestamp` | true |
| `spark.comet.expression.UnaryMinus.enabled` | Enable Comet acceleration for `UnaryMinus` | true |
| `spark.comet.expression.Unhex.enabled` | Enable Comet acceleration for `Unhex` | true |
| `spark.comet.expression.Upper.enabled` | Enable Comet acceleration for `Upper` | true |
| `spark.comet.expression.WeekDay.enabled` | Enable Comet acceleration for `WeekDay` | true |
| `spark.comet.expression.WeekOfYear.enabled` | Enable Comet acceleration for `WeekOfYear` | true |
| `spark.comet.expression.XxHash64.enabled` | Enable Comet acceleration for `XxHash64` | true |
| `spark.comet.expression.Year.enabled` | Enable Comet acceleration for `Year` | true |
<!--END:CONFIG_TABLE-->

## Enabling or Disabling Individual Aggregate Expressions

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[enable_agg_expr]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| `spark.comet.expression.Average.enabled` | Enable Comet acceleration for `Average` | true |
| `spark.comet.expression.BitAndAgg.enabled` | Enable Comet acceleration for `BitAndAgg` | true |
| `spark.comet.expression.BitOrAgg.enabled` | Enable Comet acceleration for `BitOrAgg` | true |
| `spark.comet.expression.BitXorAgg.enabled` | Enable Comet acceleration for `BitXorAgg` | true |
| `spark.comet.expression.BloomFilterAggregate.enabled` | Enable Comet acceleration for `BloomFilterAggregate` | true |
| `spark.comet.expression.Corr.enabled` | Enable Comet acceleration for `Corr` | true |
| `spark.comet.expression.Count.enabled` | Enable Comet acceleration for `Count` | true |
| `spark.comet.expression.CovPopulation.enabled` | Enable Comet acceleration for `CovPopulation` | true |
| `spark.comet.expression.CovSample.enabled` | Enable Comet acceleration for `CovSample` | true |
| `spark.comet.expression.First.enabled` | Enable Comet acceleration for `First` | true |
| `spark.comet.expression.Last.enabled` | Enable Comet acceleration for `Last` | true |
| `spark.comet.expression.Max.enabled` | Enable Comet acceleration for `Max` | true |
| `spark.comet.expression.Min.enabled` | Enable Comet acceleration for `Min` | true |
| `spark.comet.expression.StddevPop.enabled` | Enable Comet acceleration for `StddevPop` | true |
| `spark.comet.expression.StddevSamp.enabled` | Enable Comet acceleration for `StddevSamp` | true |
| `spark.comet.expression.Sum.enabled` | Enable Comet acceleration for `Sum` | true |
| `spark.comet.expression.VariancePop.enabled` | Enable Comet acceleration for `VariancePop` | true |
| `spark.comet.expression.VarianceSamp.enabled` | Enable Comet acceleration for `VarianceSamp` | true |
<!--END:CONFIG_TABLE-->
