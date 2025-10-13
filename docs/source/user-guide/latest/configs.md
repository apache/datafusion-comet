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


## Parquet Reader Configuration

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[parquet]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| spark.comet.convert.parquet.enabled | When enabled, data from Spark (non-native) Parquet v1 and v2 scans will be converted to Arrow format. Note that to enable native vectorized execution, both this config and 'spark.comet.exec.enabled' need to be enabled. | false |
| spark.comet.parquet.enable.directBuffer | Whether to use Java direct byte buffer when reading Parquet. | false |
| spark.comet.parquet.read.io.adjust.readRange.skew | In the parallel reader, if the read ranges submitted are skewed in sizes, this option will cause the reader to break up larger read ranges into smaller ranges to reduce the skew. This will result in a slightly larger number of connections opened to the file system but may give improved performance. | false |
| spark.comet.parquet.read.io.mergeRanges | When enabled the parallel reader will try to merge ranges of data that are separated by less than 'comet.parquet.read.io.mergeRanges.delta' bytes. Longer continuous reads are faster on cloud storage. | true |
| spark.comet.parquet.read.io.mergeRanges.delta | The delta in bytes between consecutive read ranges below which the parallel reader will try to merge the ranges. The default is 8MB. | 8388608 |
| spark.comet.parquet.read.parallel.io.enabled | Whether to enable Comet's parallel reader for Parquet files. The parallel reader reads ranges of consecutive data in a  file in parallel. It is faster for large files and row groups but uses more resources. | true |
| spark.comet.parquet.read.parallel.io.thread-pool.size | The maximum number of parallel threads the parallel reader will use in a single executor. For executors configured with a smaller number of cores, use a smaller number. | 16 |
| spark.comet.parquet.respectFilterPushdown | Whether to respect Spark's PARQUET_FILTER_PUSHDOWN_ENABLED config. This needs to be respected when running the Spark SQL test suite but the default setting results in poor performance in Comet when using the new native scans, disabled by default | false |
<!--END:CONFIG_TABLE-->

## Shuffle Configuration

<!-- WARNING! DO NOT MANUALLY MODIFY CONTENT BETWEEN THE BEGIN AND END TAGS -->
<!--BEGIN:CONFIG_TABLE[shuffle]-->
| Config | Description | Default Value |
|--------|-------------|---------------|
| spark.comet.columnar.shuffle.async.enabled | Whether to enable asynchronous shuffle for Arrow-based shuffle. | false |
| spark.comet.columnar.shuffle.async.max.thread.num | Maximum number of threads on an executor used for Comet async columnar shuffle. This is the upper bound of total number of shuffle threads per executor. In other words, if the number of cores * the number of shuffle threads per task `spark.comet.columnar.shuffle.async.thread.num` is larger than this config. Comet will use this config as the number of shuffle threads per executor instead. | 100 |
| spark.comet.columnar.shuffle.async.thread.num | Number of threads used for Comet async columnar shuffle per shuffle task. Note that more threads means more memory requirement to buffer shuffle data before flushing to disk. Also, more threads may not always improve performance, and should be set based on the number of cores available. | 3 |
| spark.comet.exec.shuffle.compression.codec | The codec of Comet native shuffle used to compress shuffle data. lz4, zstd, and snappy are supported. Compression can be disabled by setting spark.shuffle.compress=false. | lz4 |
| spark.comet.exec.shuffle.compression.zstd.level | The compression level to use when compressing shuffle files with zstd. | 1 |
| spark.comet.exec.shuffle.enabled | Whether to enable Comet native shuffle. Note that this requires setting 'spark.shuffle.manager' to 'org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager'. 'spark.shuffle.manager' must be set before starting the Spark application and cannot be changed during the application. | true |
| spark.comet.native.shuffle.partitioning.hash.enabled | Whether to enable hash partitioning for Comet native shuffle. | true |
| spark.comet.native.shuffle.partitioning.range.enabled | Whether to enable range partitioning for Comet native shuffle. | true |
| spark.comet.shuffle.preferDictionary.ratio | The ratio of total values to distinct values in a string column to decide whether to prefer dictionary encoding when shuffling the column. If the ratio is higher than this config, dictionary encoding will be used on shuffling string column. This config is effective if it is higher than 1.0. Note that this config is only used when `spark.comet.exec.shuffle.mode` is `jvm`. | 10.0 |
| spark.comet.shuffle.sizeInBytesMultiplier | Comet reports smaller sizes for shuffle due to using Arrow's columnar memory format and this can result in Spark choosing a different join strategy due to the estimated size of the exchange being smaller. Comet will multiple sizeInBytes by this amount to avoid regressions in join strategy. | 1.0 |
<!--END:CONFIG_TABLE-->
