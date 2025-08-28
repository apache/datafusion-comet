/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet

import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ListBuffer

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.shims.ShimCometConf

/**
 * Configurations for a Comet application. Mostly inspired by [[SQLConf]] in Spark.
 *
 * To get the value of a Comet config key from a [[SQLConf]], you can do the following:
 *
 * {{{
 *   CometConf.COMET_ENABLED.get
 * }}}
 *
 * which retrieves the config value from the thread-local [[SQLConf]] object. Alternatively, you
 * can also explicitly pass a [[SQLConf]] object to the `get` method.
 */
object CometConf extends ShimCometConf {

  val COMPAT_GUIDE: String = "For more information, refer to the Comet Compatibility " +
    "Guide (https://datafusion.apache.org/comet/user-guide/compatibility.html)"

  private val TUNING_GUIDE = "For more information, refer to the Comet Tuning " +
    "Guide (https://datafusion.apache.org/comet/user-guide/tuning.html)"

  private val TRACING_GUIDE = "For more information, refer to the Comet Tracing " +
    "Guide (https://datafusion.apache.org/comet/user-guide/tracing.html)"

  /** List of all configs that is used for generating documentation */
  val allConfs = new ListBuffer[ConfigEntry[_]]

  def register(conf: ConfigEntry[_]): Unit = {
    allConfs.append(conf)
  }

  def conf(key: String): ConfigBuilder = ConfigBuilder(key)

  val COMET_EXEC_CONFIG_PREFIX = "spark.comet.exec";

  val COMET_ENABLED: ConfigEntry[Boolean] = conf("spark.comet.enabled")
    .doc(
      "Whether to enable Comet extension for Spark. When this is turned on, Spark will use " +
        "Comet to read Parquet data source. Note that to enable native vectorized execution, " +
        "both this config and 'spark.comet.exec.enabled' need to be enabled. By default, this " +
        "config is the value of the env var `ENABLE_COMET` if set, or true otherwise.")
    .booleanConf
    .createWithDefault(sys.env.getOrElse("ENABLE_COMET", "true").toBoolean)

  val COMET_NATIVE_SCAN_ENABLED: ConfigEntry[Boolean] = conf("spark.comet.scan.enabled")
    .doc(
      "Whether to enable native scans. When this is turned on, Spark will use Comet to " +
        "read supported data sources (currently only Parquet is supported natively). Note " +
        "that to enable native vectorized execution, both this config and " +
        "'spark.comet.exec.enabled' need to be enabled.")
    .booleanConf
    .createWithDefault(true)

  val SCAN_NATIVE_COMET = "native_comet"
  val SCAN_NATIVE_DATAFUSION = "native_datafusion"
  val SCAN_NATIVE_ICEBERG_COMPAT = "native_iceberg_compat"
  val SCAN_AUTO = "auto"

  val COMET_NATIVE_SCAN_IMPL: ConfigEntry[String] = conf("spark.comet.scan.impl")
    .doc(
      s"The implementation of Comet Native Scan to use. Available modes are '$SCAN_NATIVE_COMET'," +
        s"'$SCAN_NATIVE_DATAFUSION', and '$SCAN_NATIVE_ICEBERG_COMPAT'. " +
        s"'$SCAN_NATIVE_COMET' is for the original Comet native scan which uses a jvm based " +
        "parquet file reader and native column decoding. Supports simple types only " +
        s"'$SCAN_NATIVE_DATAFUSION' is a fully native implementation of scan based on DataFusion" +
        s"'$SCAN_NATIVE_ICEBERG_COMPAT' is a native implementation that exposes apis to read " +
        s"parquet columns natively. $SCAN_AUTO chooses the best scan.")
    .internal()
    .stringConf
    .transform(_.toLowerCase(Locale.ROOT))
    .checkValues(
      Set(SCAN_NATIVE_COMET, SCAN_NATIVE_DATAFUSION, SCAN_NATIVE_ICEBERG_COMPAT, SCAN_AUTO))
    .createWithDefault(sys.env
      .getOrElse("COMET_PARQUET_SCAN_IMPL", SCAN_AUTO)
      .toLowerCase(Locale.ROOT))

  val COMET_RESPECT_PARQUET_FILTER_PUSHDOWN: ConfigEntry[Boolean] =
    conf("spark.comet.parquet.respectFilterPushdown")
      .doc(
        "Whether to respect Spark's PARQUET_FILTER_PUSHDOWN_ENABLED config. This needs to be " +
          "respected when running the Spark SQL test suite but the default setting " +
          "results in poor performance in Comet when using the new native scans, " +
          "disabled by default")
      .booleanConf
      .createWithDefault(false)

  val COMET_PARQUET_PARALLEL_IO_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.parquet.read.parallel.io.enabled")
      .doc(
        "Whether to enable Comet's parallel reader for Parquet files. The parallel reader reads " +
          "ranges of consecutive data in a  file in parallel. It is faster for large files and " +
          "row groups but uses more resources.")
      .booleanConf
      .createWithDefault(true)

  val COMET_PARQUET_PARALLEL_IO_THREADS: ConfigEntry[Int] =
    conf("spark.comet.parquet.read.parallel.io.thread-pool.size")
      .doc("The maximum number of parallel threads the parallel reader will use in a single " +
        "executor. For executors configured with a smaller number of cores, use a smaller number.")
      .intConf
      .createWithDefault(16)

  val COMET_IO_MERGE_RANGES: ConfigEntry[Boolean] =
    conf("spark.comet.parquet.read.io.mergeRanges")
      .doc(
        "When enabled the parallel reader will try to merge ranges of data that are separated " +
          "by less than 'comet.parquet.read.io.mergeRanges.delta' bytes. Longer continuous reads " +
          "are faster on cloud storage.")
      .booleanConf
      .createWithDefault(true)

  val COMET_IO_MERGE_RANGES_DELTA: ConfigEntry[Int] =
    conf("spark.comet.parquet.read.io.mergeRanges.delta")
      .doc(
        "The delta in bytes between consecutive read ranges below which the parallel reader " +
          "will try to merge the ranges. The default is 8MB.")
      .intConf
      .createWithDefault(1 << 23) // 8 MB

  val COMET_IO_ADJUST_READRANGE_SKEW: ConfigEntry[Boolean] =
    conf("spark.comet.parquet.read.io.adjust.readRange.skew")
      .doc("In the parallel reader, if the read ranges submitted are skewed in sizes, this " +
        "option will cause the reader to break up larger read ranges into smaller ranges to " +
        "reduce the skew. This will result in a slightly larger number of connections opened to " +
        "the file system but may give improved performance.")
      .booleanConf
      .createWithDefault(false)

  val COMET_CONVERT_FROM_PARQUET_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.convert.parquet.enabled")
      .doc(
        "When enabled, data from Spark (non-native) Parquet v1 and v2 scans will be converted to " +
          "Arrow format. Note that to enable native vectorized execution, both this config and " +
          "'spark.comet.exec.enabled' need to be enabled.")
      .booleanConf
      .createWithDefault(false)

  val COMET_CONVERT_FROM_JSON_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.convert.json.enabled")
      .doc(
        "When enabled, data from Spark (non-native) JSON v1 and v2 scans will be converted to " +
          "Arrow format. Note that to enable native vectorized execution, both this config and " +
          "'spark.comet.exec.enabled' need to be enabled.")
      .booleanConf
      .createWithDefault(false)

  val COMET_CONVERT_FROM_CSV_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.convert.csv.enabled")
      .doc(
        "When enabled, data from Spark (non-native) CSV v1 and v2 scans will be converted to " +
          "Arrow format. Note that to enable native vectorized execution, both this config and " +
          "'spark.comet.exec.enabled' need to be enabled.")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXEC_ENABLED: ConfigEntry[Boolean] = conf(s"$COMET_EXEC_CONFIG_PREFIX.enabled")
    .doc(
      "Whether to enable Comet native vectorized execution for Spark. This controls whether " +
        "Spark should convert operators into their Comet counterparts and execute them in " +
        "native space. Note: each operator is associated with a separate config in the " +
        "format of 'spark.comet.exec.<operator_name>.enabled' at the moment, and both the " +
        "config and this need to be turned on, in order for the operator to be executed in " +
        "native.")
    .booleanConf
    .createWithDefault(true)

  val COMET_EXEC_PROJECT_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("project", defaultValue = true)
  val COMET_EXEC_FILTER_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("filter", defaultValue = true)
  val COMET_EXEC_SORT_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("sort", defaultValue = true)
  val COMET_EXEC_LOCAL_LIMIT_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("localLimit", defaultValue = true)
  val COMET_EXEC_GLOBAL_LIMIT_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("globalLimit", defaultValue = true)
  val COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("broadcastHashJoin", defaultValue = true)
  val COMET_EXEC_BROADCAST_EXCHANGE_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("broadcastExchange", defaultValue = true)
  val COMET_EXEC_HASH_JOIN_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("hashJoin", defaultValue = true)
  val COMET_EXEC_SORT_MERGE_JOIN_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("sortMergeJoin", defaultValue = true)
  val COMET_EXEC_AGGREGATE_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("aggregate", defaultValue = true)
  val COMET_EXEC_COLLECT_LIMIT_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("collectLimit", defaultValue = true)
  val COMET_EXEC_COALESCE_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("coalesce", defaultValue = true)
  val COMET_EXEC_UNION_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("union", defaultValue = true)
  val COMET_EXEC_EXPAND_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("expand", defaultValue = true)
  val COMET_EXEC_WINDOW_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("window", defaultValue = true)
  val COMET_EXEC_TAKE_ORDERED_AND_PROJECT_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("takeOrderedAndProject", defaultValue = true)
  val COMET_EXEC_INITCAP_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig("initCap", defaultValue = false)

  val COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.exec.sortMergeJoinWithJoinFilter.enabled")
      .doc("Experimental support for Sort Merge Join with filter")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXPR_STDDEV_ENABLED: ConfigEntry[Boolean] =
    createExecEnabledConfig(
      "stddev",
      defaultValue = true,
      notes = Some("stddev is slower than Spark's implementation"))

  val COMET_TRACING_ENABLED: ConfigEntry[Boolean] = conf("spark.comet.tracing.enabled")
    .doc(s"Enable fine-grained tracing of events and memory usage. $TRACING_GUIDE.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val COMET_MEMORY_OVERHEAD: OptionalConfigEntry[Long] = conf("spark.comet.memoryOverhead")
    .doc(
      "The amount of additional memory to be allocated per executor process for Comet, in MiB, " +
        "when running Spark in on-heap mode. " +
        "This config is optional. If this is not specified, it will be set to " +
        s"`spark.comet.memory.overhead.factor` * `spark.executor.memory`. $TUNING_GUIDE.")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  val COMET_MEMORY_OVERHEAD_FACTOR: ConfigEntry[Double] =
    conf("spark.comet.memory.overhead.factor")
      .doc(
        "Fraction of executor memory to be allocated as additional memory for Comet " +
          "when running Spark in on-heap mode. " +
          s"$TUNING_GUIDE.")
      .doubleConf
      .checkValue(
        factor => factor > 0,
        "Ensure that Comet memory overhead factor is a double greater than 0")
      .createWithDefault(0.2)

  val COMET_MEMORY_OVERHEAD_MIN_MIB: ConfigEntry[Long] = conf("spark.comet.memory.overhead.min")
    .doc("Minimum amount of additional memory to be allocated per executor process for Comet, " +
      s"in MiB, when running Spark in on-heap mode. $TUNING_GUIDE.")
    .bytesConf(ByteUnit.MiB)
    .checkValue(
      _ >= 0,
      "Ensure that Comet memory overhead min is a long greater than or equal to 0")
    .createWithDefault(384)

  val COMET_EXEC_SHUFFLE_ENABLED: ConfigEntry[Boolean] =
    conf(s"$COMET_EXEC_CONFIG_PREFIX.shuffle.enabled")
      .doc(
        "Whether to enable Comet native shuffle. " +
          "Note that this requires setting 'spark.shuffle.manager' to " +
          "'org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager'. " +
          "'spark.shuffle.manager' must be set before starting the Spark application and " +
          "cannot be changed during the application.")
      .booleanConf
      .createWithDefault(true)

  val COMET_SHUFFLE_MODE: ConfigEntry[String] = conf(s"$COMET_EXEC_CONFIG_PREFIX.shuffle.mode")
    .doc(
      "This is test config to allow tests to force a particular shuffle implementation to be " +
        "used. Valid values are `jvm` for Columnar Shuffle, `native` for Native Shuffle, " +
        s"and `auto` to pick the best supported option (`native` has priority). $TUNING_GUIDE.")
    .internal()
    .stringConf
    .transform(_.toLowerCase(Locale.ROOT))
    .checkValues(Set("native", "jvm", "auto"))
    .createWithDefault("auto")

  val COMET_EXEC_BROADCAST_FORCE_ENABLED: ConfigEntry[Boolean] =
    conf(s"$COMET_EXEC_CONFIG_PREFIX.broadcast.enabled")
      .doc(
        "Whether to force enabling broadcasting for Comet native operators. " +
          "Comet broadcast feature will be enabled automatically by " +
          "Comet extension. But for unit tests, we need this feature to force enabling it " +
          "for invalid cases. So this config is only used for unit test.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val COMET_REPLACE_SMJ: ConfigEntry[Boolean] =
    conf(s"$COMET_EXEC_CONFIG_PREFIX.replaceSortMergeJoin")
      .doc("Experimental feature to force Spark to replace SortMergeJoin with ShuffledHashJoin " +
        s"for improved performance. This feature is not stable yet. $TUNING_GUIDE.")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXEC_SHUFFLE_WITH_HASH_PARTITIONING_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.native.shuffle.partitioning.hash.enabled")
      .doc("Whether to enable hash partitioning for Comet native shuffle.")
      .booleanConf
      .createWithDefault(true)

  // RangePartitioning contains bugs https://github.com/apache/datafusion-comet/issues/1906
  val COMET_EXEC_SHUFFLE_WITH_RANGE_PARTITIONING_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.native.shuffle.partitioning.range.enabled")
      .doc("Experimental feature to enable range partitioning for Comet native shuffle. " +
        "This feature is experimental while we investigate scenarios that don't partition data " +
        "correctly.")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXEC_SHUFFLE_COMPRESSION_CODEC: ConfigEntry[String] =
    conf(s"$COMET_EXEC_CONFIG_PREFIX.shuffle.compression.codec")
      .doc(
        "The codec of Comet native shuffle used to compress shuffle data. lz4, zstd, and " +
          "snappy are supported. Compression can be disabled by setting " +
          "spark.shuffle.compress=false.")
      .stringConf
      .checkValues(Set("zstd", "lz4", "snappy"))
      .createWithDefault("lz4")

  val COMET_EXEC_SHUFFLE_COMPRESSION_ZSTD_LEVEL: ConfigEntry[Int] =
    conf(s"$COMET_EXEC_CONFIG_PREFIX.shuffle.compression.zstd.level")
      .doc("The compression level to use when compressing shuffle files with zstd.")
      .intConf
      .createWithDefault(1)

  val COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.columnar.shuffle.async.enabled")
      .doc("Whether to enable asynchronous shuffle for Arrow-based shuffle.")
      .booleanConf
      .createWithDefault(false)

  val COMET_COLUMNAR_SHUFFLE_ASYNC_THREAD_NUM: ConfigEntry[Int] =
    conf("spark.comet.columnar.shuffle.async.thread.num")
      .doc(
        "Number of threads used for Comet async columnar shuffle per shuffle task. " +
          "Note that more threads means more memory requirement to " +
          "buffer shuffle data before flushing to disk. Also, more threads may not always " +
          "improve performance, and should be set based on the number of cores available.")
      .intConf
      .createWithDefault(3)

  val COMET_COLUMNAR_SHUFFLE_ASYNC_MAX_THREAD_NUM: ConfigEntry[Int] = {
    conf("spark.comet.columnar.shuffle.async.max.thread.num")
      .doc("Maximum number of threads on an executor used for Comet async columnar shuffle. " +
        "This is the upper bound of total number of shuffle " +
        "threads per executor. In other words, if the number of cores * the number of shuffle " +
        "threads per task `spark.comet.columnar.shuffle.async.thread.num` is larger than " +
        "this config. Comet will use this config as the number of shuffle threads per " +
        "executor instead.")
      .intConf
      .createWithDefault(100)
  }

  val COMET_COLUMNAR_SHUFFLE_SPILL_THRESHOLD: ConfigEntry[Int] =
    conf("spark.comet.columnar.shuffle.spill.threshold")
      .doc(
        "Number of rows to be spilled used for Comet columnar shuffle. " +
          "For every configured number of rows, a new spill file will be created. " +
          "Higher value means more memory requirement to buffer shuffle data before " +
          "flushing to disk. As Comet uses columnar shuffle which is columnar format, " +
          "higher value usually helps to improve shuffle data compression ratio. This is " +
          "internal config for testing purpose or advanced tuning.")
      .internal()
      .intConf
      .createWithDefault(Int.MaxValue)

  val COMET_COLUMNAR_SHUFFLE_MEMORY_SIZE: OptionalConfigEntry[Long] =
    conf("spark.comet.columnar.shuffle.memorySize")
      .internal()
      .doc("Amount of memory to reserve for columnar shuffle when running in on-heap mode. " +
        s"$TUNING_GUIDE.")
      .bytesConf(ByteUnit.MiB)
      .createOptional

  val COMET_COLUMNAR_SHUFFLE_MEMORY_FACTOR: ConfigEntry[Double] =
    conf("spark.comet.columnar.shuffle.memory.factor")
      .internal()
      .doc("Fraction of Comet memory to be allocated per executor process for columnar shuffle " +
        s"when running in on-heap mode. $TUNING_GUIDE.")
      .doubleConf
      .checkValue(
        factor => factor > 0,
        "Ensure that Comet shuffle memory overhead factor is a double greater than 0")
      .createWithDefault(1.0)

  val COMET_COLUMNAR_SHUFFLE_BATCH_SIZE: ConfigEntry[Int] =
    conf("spark.comet.columnar.shuffle.batch.size")
      .internal()
      .doc("Batch size when writing out sorted spill files on the native side. Note that " +
        "this should not be larger than batch size (i.e., `spark.comet.batchSize`). Otherwise " +
        "it will produce larger batches than expected in the native operator after shuffle.")
      .intConf
      .createWithDefault(8192)

  val COMET_SHUFFLE_PREFER_DICTIONARY_RATIO: ConfigEntry[Double] = conf(
    "spark.comet.shuffle.preferDictionary.ratio")
    .doc(
      "The ratio of total values to distinct values in a string column to decide whether to " +
        "prefer dictionary encoding when shuffling the column. If the ratio is higher than " +
        "this config, dictionary encoding will be used on shuffling string column. This config " +
        "is effective if it is higher than 1.0. Note that this " +
        "config is only used when `spark.comet.exec.shuffle.mode` is `jvm`.")
    .doubleConf
    .createWithDefault(10.0)

  val COMET_EXCHANGE_SIZE_MULTIPLIER: ConfigEntry[Double] = conf(
    "spark.comet.shuffle.sizeInBytesMultiplier")
    .doc(
      "Comet reports smaller sizes for shuffle due to using Arrow's columnar memory format " +
        "and this can result in Spark choosing a different join strategy due to the estimated " +
        "size of the exchange being smaller. Comet will multiple sizeInBytes by this amount to " +
        "avoid regressions in join strategy.")
    .doubleConf
    .createWithDefault(1.0)

  val COMET_DPP_FALLBACK_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.dppFallback.enabled")
      .doc("Whether to fall back to Spark for queries that use DPP.")
      .booleanConf
      .createWithDefault(true)

  val COMET_DEBUG_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.debug.enabled")
      .doc(
        "Whether to enable debug mode for Comet. " +
          "When enabled, Comet will do additional checks for debugging purpose. For example, " +
          "validating array when importing arrays from JVM at native side. Note that these " +
          "checks may be expensive in performance and should only be enabled for debugging " +
          "purpose.")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXPLAIN_VERBOSE_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.explain.verbose.enabled")
      .doc(
        "When this setting is enabled, Comet will provide a verbose tree representation of " +
          "the extended information.")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXPLAIN_NATIVE_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.explain.native.enabled")
      .doc(
        "When this setting is enabled, Comet will provide a tree representation of " +
          "the native query plan before execution and again after execution, with " +
          "metrics.")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXPLAIN_TRANSFORMATIONS: ConfigEntry[Boolean] =
    conf("spark.comet.explain.rules")
      .doc("When this setting is enabled, Comet will log all plan transformations performed " +
        "in physical optimizer rules. Default: false")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val COMET_LOG_FALLBACK_REASONS: ConfigEntry[Boolean] =
    conf("spark.comet.logFallbackReasons.enabled")
      .doc("When this setting is enabled, Comet will log warnings for all fallback reasons.")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXPLAIN_FALLBACK_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.explainFallback.enabled")
      .doc(
        "When this setting is enabled, Comet will provide logging explaining the reason(s) " +
          "why a query stage cannot be executed natively. Set this to false to " +
          "reduce the amount of logging.")
      .booleanConf
      .createWithDefault(false)

  val COMET_BATCH_SIZE: ConfigEntry[Int] = conf("spark.comet.batchSize")
    .doc("The columnar batch size, i.e., the maximum number of rows that a batch can contain.")
    .intConf
    .createWithDefault(8192)

  val COMET_PARQUET_ENABLE_DIRECT_BUFFER: ConfigEntry[Boolean] =
    conf("spark.comet.parquet.enable.directBuffer")
      .doc("Whether to use Java direct byte buffer when reading Parquet.")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXEC_MEMORY_POOL_TYPE: ConfigEntry[String] = conf("spark.comet.exec.memoryPool")
    .doc("The type of memory pool to be used for Comet native execution. " +
      "When running Spark in on-heap mode, available pool types are 'greedy', 'fair_spill', " +
      "'greedy_task_shared', 'fair_spill_task_shared', 'greedy_global', 'fair_spill_global', " +
      "and `unbounded`. When running Spark in off-heap mode, available pool types are " +
      "'unified' and `fair_unified`. The default pool type is `greedy_task_shared` for on-heap " +
      s"mode and `unified` for off-heap mode. $TUNING_GUIDE.")
    .stringConf
    .createWithDefault("default")

  val COMET_SCAN_PREFETCH_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.scan.preFetch.enabled")
      .doc("Whether to enable pre-fetching feature of CometScan.")
      .booleanConf
      .createWithDefault(false)

  val COMET_SCAN_PREFETCH_THREAD_NUM: ConfigEntry[Int] =
    conf("spark.comet.scan.preFetch.threadNum")
      .doc(
        "The number of threads running pre-fetching for CometScan. Effective if " +
          s"${COMET_SCAN_PREFETCH_ENABLED.key} is enabled. Note that more " +
          "pre-fetching threads means more memory requirement to store pre-fetched row groups.")
      .intConf
      .createWithDefault(2)

  val COMET_NATIVE_LOAD_REQUIRED: ConfigEntry[Boolean] = conf("spark.comet.nativeLoadRequired")
    .doc(
      "Whether to require Comet native library to load successfully when Comet is enabled. " +
        "If not, Comet will silently fallback to Spark when it fails to load the native lib. " +
        "Otherwise, an error will be thrown and the Spark job will be aborted.")
    .booleanConf
    .createWithDefault(false)

  val COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP: ConfigEntry[Boolean] =
    conf("spark.comet.exceptionOnDatetimeRebase")
      .doc("Whether to throw exception when seeing dates/timestamps from the legacy hybrid " +
        "(Julian + Gregorian) calendar. Since Spark 3, dates/timestamps were written according " +
        "to the Proleptic Gregorian calendar. When this is true, Comet will " +
        "throw exceptions when seeing these dates/timestamps that were written by Spark version " +
        "before 3.0. If this is false, these dates/timestamps will be read as if they were " +
        "written to the Proleptic Gregorian calendar and will not be rebased.")
      .booleanConf
      .createWithDefault(false)

  val COMET_USE_DECIMAL_128: ConfigEntry[Boolean] = conf("spark.comet.use.decimal128")
    .internal()
    .doc("If true, Comet will always use 128 bits to represent a decimal value, regardless of " +
      "its precision. If false, Comet will use 32, 64 and 128 bits respectively depending on " +
      "the precision. N.B. this is NOT a user-facing config but should be inferred and set by " +
      "Comet itself.")
    .booleanConf
    .createWithDefault(false)

  val COMET_USE_LAZY_MATERIALIZATION: ConfigEntry[Boolean] = conf(
    "spark.comet.use.lazyMaterialization")
    .internal()
    .doc(
      "Whether to enable lazy materialization for Comet. When this is turned on, Comet will " +
        "read Parquet data source lazily for string and binary columns. For filter operations, " +
        "lazy materialization will improve read performance by skipping unused pages.")
    .booleanConf
    .createWithDefault(true)

  val COMET_SCHEMA_EVOLUTION_ENABLED: ConfigEntry[Boolean] = conf(
    "spark.comet.schemaEvolution.enabled")
    .internal()
    .doc(
      "Whether to enable schema evolution in Comet. For instance, promoting a integer " +
        "column to a long column, a float column to a double column, etc. This is automatically" +
        "enabled when reading from Iceberg tables.")
    .booleanConf
    .createWithDefault(COMET_SCHEMA_EVOLUTION_ENABLED_DEFAULT)

  val COMET_SPARK_TO_ARROW_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.sparkToColumnar.enabled")
      .internal()
      .doc("Whether to enable Spark to Arrow columnar conversion. When this is turned on, " +
        "Comet will convert operators in " +
        "`spark.comet.sparkToColumnar.supportedOperatorList` into Arrow columnar format before " +
        "processing.")
      .booleanConf
      .createWithDefault(false)

  val COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST: ConfigEntry[Seq[String]] =
    conf("spark.comet.sparkToColumnar.supportedOperatorList")
      .doc(
        "A comma-separated list of operators that will be converted to Arrow columnar " +
          "format when 'spark.comet.sparkToColumnar.enabled' is true")
      .stringConf
      .toSequence
      .createWithDefault(Seq("Range,InMemoryTableScan"))

  val COMET_CASE_CONVERSION_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.caseConversion.enabled")
      .doc(
        "Java uses locale-specific rules when converting strings to upper or lower case and " +
          "Rust does not, so we disable upper and lower by default.")
      .booleanConf
      .createWithDefault(false)

  val COMET_SCAN_ALLOW_INCOMPATIBLE: ConfigEntry[Boolean] =
    conf("spark.comet.scan.allowIncompatible")
      .doc(
        "Some Comet scan implementations are not currently fully compatible with Spark for " +
          s"all datatypes. Set this config to true to allow them anyway. $COMPAT_GUIDE.")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXPR_ALLOW_INCOMPATIBLE: ConfigEntry[Boolean] =
    conf("spark.comet.expression.allowIncompatible")
      .doc(
        "Comet is not currently fully compatible with Spark for all expressions. " +
          s"Set this config to true to allow them anyway. $COMPAT_GUIDE.")
      .booleanConf
      .createWithDefault(false)

  val COMET_CAST_ALLOW_INCOMPATIBLE: ConfigEntry[Boolean] =
    conf("spark.comet.cast.allowIncompatible")
      .doc(
        "Comet is not currently fully compatible with Spark for all cast operations. " +
          s"Set this config to true to allow them anyway. $COMPAT_GUIDE.")
      .booleanConf
      .createWithDefault(false)

  val COMET_REGEXP_ALLOW_INCOMPATIBLE: ConfigEntry[Boolean] =
    conf("spark.comet.regexp.allowIncompatible")
      .doc(
        "Comet is not currently fully compatible with Spark for all regular expressions. " +
          s"Set this config to true to allow them anyway. $COMPAT_GUIDE.")
      .booleanConf
      .createWithDefault(false)

  val COMET_METRICS_UPDATE_INTERVAL: ConfigEntry[Long] =
    conf("spark.comet.metrics.updateInterval")
      .doc(
        "The interval in milliseconds to update metrics. If interval is negative," +
          " metrics will be updated upon task completion.")
      .longConf
      .createWithDefault(3000L)

  val COMET_ENABLE_GROUPING_ON_MAP_TYPE: ConfigEntry[Boolean] =
    conf("spark.comet.enableGroupingOnMapType")
      .doc(
        "An experimental feature with limited capabilities to enable grouping on Spark Map type." +
          "Requires Spark 4.0 and beyond along with support for scan on Map type." +
          s"Set this config to true to enable grouping on map type. $COMPAT_GUIDE.")
      .booleanConf
      .createWithDefault(false)

  /** Create a config to enable a specific operator */
  private def createExecEnabledConfig(
      exec: String,
      defaultValue: Boolean,
      notes: Option[String] = None): ConfigEntry[Boolean] = {
    conf(s"$COMET_EXEC_CONFIG_PREFIX.$exec.enabled")
      .doc(
        s"Whether to enable $exec by default." + notes
          .map(s => s" $s.")
          .getOrElse(""))
      .booleanConf
      .createWithDefault(defaultValue)
  }
}

object ConfigHelpers {
  def toNumber[T](s: String, converter: String => T, key: String, configType: String): T = {
    try {
      converter(s.trim)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$key should be $configType, but was $s")
    }
  }

  def toBoolean(s: String, key: String): Boolean = {
    try {
      s.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s")
    }
  }

  def stringToSeq[T](str: String, converter: String => T): Seq[T] = {
    Utils.stringToSeq(str).map(converter)
  }

  def seqToString[T](v: Seq[T], stringConverter: T => String): String = {
    v.map(stringConverter).mkString(",")
  }

  def timeFromString(str: String, unit: TimeUnit): Long = JavaUtils.timeStringAs(str, unit)

  def timeToString(v: Long, unit: TimeUnit): String =
    TimeUnit.MILLISECONDS.convert(v, unit) + "ms"

  def byteFromString(str: String, unit: ByteUnit): Long = {
    val (input, multiplier) =
      if (str.nonEmpty && str.charAt(0) == '-') {
        (str.substring(1), -1)
      } else {
        (str, 1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
  }

  def byteToString(v: Long, unit: ByteUnit): String = unit.convertTo(v, ByteUnit.BYTE) + "b"
}

private class TypedConfigBuilder[T](
    val parent: ConfigBuilder,
    val converter: String => T,
    val stringConverter: T => String) {

  import ConfigHelpers._

  def this(parent: ConfigBuilder, converter: String => T) = {
    this(parent, converter, Option(_).map(_.toString).orNull)
  }

  /** Apply a transformation to the user-provided values of the config entry. */
  def transform(fn: T => T): TypedConfigBuilder[T] = {
    new TypedConfigBuilder(parent, s => fn(converter(s)), stringConverter)
  }

  /** Checks if the user-provided value for the config matches the validator. */
  def checkValue(validator: T => Boolean, errorMsg: String): TypedConfigBuilder[T] = {
    transform { v =>
      if (!validator(v)) {
        throw new IllegalArgumentException(s"'$v' in ${parent.key} is invalid. $errorMsg")
      }
      v
    }
  }

  /** Check that user-provided values for the config match a pre-defined set. */
  def checkValues(validValues: Set[T]): TypedConfigBuilder[T] = {
    transform { v =>
      if (!validValues.contains(v)) {
        throw new IllegalArgumentException(
          s"The value of ${parent.key} should be one of ${validValues.mkString(", ")}, but was $v")
      }
      v
    }
  }

  /** Turns the config entry into a sequence of values of the underlying type. */
  def toSequence: TypedConfigBuilder[Seq[T]] = {
    new TypedConfigBuilder(parent, stringToSeq(_, converter), seqToString(_, stringConverter))
  }

  /** Creates a [[ConfigEntry]] that does not have a default value. */
  def createOptional: OptionalConfigEntry[T] = {
    val conf = new OptionalConfigEntry[T](
      parent.key,
      converter,
      stringConverter,
      parent._doc,
      parent._public,
      parent._version)
    CometConf.register(conf)
    conf
  }

  /** Creates a [[ConfigEntry]] that has a default value. */
  def createWithDefault(default: T): ConfigEntry[T] = {
    val transformedDefault = converter(stringConverter(default))
    val conf = new ConfigEntryWithDefault[T](
      parent.key,
      transformedDefault,
      converter,
      stringConverter,
      parent._doc,
      parent._public,
      parent._version)
    CometConf.register(conf)
    conf
  }
}

private[comet] abstract class ConfigEntry[T](
    val key: String,
    val valueConverter: String => T,
    val stringConverter: T => String,
    val doc: String,
    val isPublic: Boolean,
    val version: String) {

  /**
   * Retrieves the config value from the given [[SQLConf]].
   */
  def get(conf: SQLConf): T

  /**
   * Retrieves the config value from the current thread-local [[SQLConf]]
   *
   * @return
   */
  def get(): T = get(SQLConf.get)

  def defaultValue: Option[T] = None

  def defaultValueString: String

  override def toString: String = {
    s"ConfigEntry(key=$key, defaultValue=$defaultValueString, doc=$doc, " +
      s"public=$isPublic, version=$version)"
  }
}

private[comet] class ConfigEntryWithDefault[T](
    key: String,
    _defaultValue: T,
    valueConverter: String => T,
    stringConverter: T => String,
    doc: String,
    isPublic: Boolean,
    version: String)
    extends ConfigEntry(key, valueConverter, stringConverter, doc, isPublic, version) {
  override def defaultValue: Option[T] = Some(_defaultValue)

  override def defaultValueString: String = stringConverter(_defaultValue)

  def get(conf: SQLConf): T = {
    val tmp = conf.getConfString(key, null)
    if (tmp == null) {
      _defaultValue
    } else {
      valueConverter(tmp)
    }
  }
}

private[comet] class OptionalConfigEntry[T](
    key: String,
    val rawValueConverter: String => T,
    val rawStringConverter: T => String,
    doc: String,
    isPublic: Boolean,
    version: String)
    extends ConfigEntry[Option[T]](
      key,
      s => Some(rawValueConverter(s)),
      v => v.map(rawStringConverter).orNull,
      doc,
      isPublic,
      version) {

  override def defaultValueString: String = ConfigEntry.UNDEFINED

  override def get(conf: SQLConf): Option[T] = {
    Option(conf.getConfString(key, null)).map(rawValueConverter)
  }
}

private[comet] case class ConfigBuilder(key: String) {

  import ConfigHelpers._

  var _public = true
  var _doc = ""
  var _version = ""

  def internal(): ConfigBuilder = {
    _public = false
    this
  }

  def doc(s: String): ConfigBuilder = {
    _doc = s
    this
  }

  def version(v: String): ConfigBuilder = {
    _version = v
    this
  }

  def intConf: TypedConfigBuilder[Int] = {
    new TypedConfigBuilder(this, toNumber(_, _.toInt, key, "int"))
  }

  def longConf: TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, toNumber(_, _.toLong, key, "long"))
  }

  def doubleConf: TypedConfigBuilder[Double] = {
    new TypedConfigBuilder(this, toNumber(_, _.toDouble, key, "double"))
  }

  def booleanConf: TypedConfigBuilder[Boolean] = {
    new TypedConfigBuilder(this, toBoolean(_, key))
  }

  def stringConf: TypedConfigBuilder[String] = {
    new TypedConfigBuilder(this, v => v)
  }

  def timeConf(unit: TimeUnit): TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, timeFromString(_, unit), timeToString(_, unit))
  }

  def bytesConf(unit: ByteUnit): TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, byteFromString(_, unit), byteToString(_, unit))
  }
}

private object ConfigEntry {
  val UNDEFINED = "<undefined>"
}
