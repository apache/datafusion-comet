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

import java.util.concurrent.TimeUnit

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.internal.SQLConf

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
object CometConf {
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

  val COMET_SCAN_ENABLED: ConfigEntry[Boolean] = conf("spark.comet.scan.enabled")
    .doc(
      "Whether to enable Comet scan. When this is turned on, Spark will use Comet to read " +
        "Parquet data source. Note that to enable native vectorized execution, both this " +
        "config and 'spark.comet.exec.enabled' need to be enabled. By default, this config " +
        "is true.")
    .booleanConf
    .createWithDefault(true)

  val COMET_EXEC_ENABLED: ConfigEntry[Boolean] = conf(s"$COMET_EXEC_CONFIG_PREFIX.enabled")
    .doc(
      "Whether to enable Comet native vectorized execution for Spark. This controls whether " +
        "Spark should convert operators into their Comet counterparts and execute them in " +
        "native space. Note: each operator is associated with a separate config in the " +
        "format of 'spark.comet.exec.<operator_name>.enabled' at the moment, and both the " +
        "config and this need to be turned on, in order for the operator to be executed in " +
        "native. By default, this config is false.")
    .booleanConf
    .createWithDefault(false)

  val COMET_MEMORY_OVERHEAD: OptionalConfigEntry[Long] = conf("spark.comet.memoryOverhead")
    .doc(
      "The amount of additional memory to be allocated per executor process for Comet, in MiB. " +
        "This config is optional. If this is not specified, it will be set to " +
        "`spark.comet.memory.overhead.factor` * `spark.executor.memory`. " +
        "This is memory that accounts for things like Comet native execution, Comet shuffle, etc.")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  val COMET_MEMORY_OVERHEAD_FACTOR: ConfigEntry[Double] = conf(
    "spark.comet.memory.overhead.factor")
    .doc(
      "Fraction of executor memory to be allocated as additional non-heap memory per executor " +
        "process for Comet. Default value is 0.2.")
    .doubleConf
    .checkValue(
      factor => factor > 0,
      "Ensure that Comet memory overhead factor is a double greater than 0")
    .createWithDefault(0.2)

  val COMET_MEMORY_OVERHEAD_MIN_MIB: ConfigEntry[Long] = conf("spark.comet.memory.overhead.min")
    .doc("Minimum amount of additional memory to be allocated per executor process for Comet, " +
      "in MiB.")
    .bytesConf(ByteUnit.MiB)
    .checkValue(
      _ >= 0,
      "Ensure that Comet memory overhead min is a long greater than or equal to 0")
    .createWithDefault(384)

  val COMET_EXEC_ALL_OPERATOR_ENABLED: ConfigEntry[Boolean] = conf(
    s"$COMET_EXEC_CONFIG_PREFIX.all.enabled")
    .doc(
      "Whether to enable all Comet operators. By default, this config is false. Note that " +
        "this config precedes all separate config 'spark.comet.exec.<operator_name>.enabled'. " +
        "That being said, if this config is enabled, separate configs are ignored.")
    .booleanConf
    .createWithDefault(false)

  val COMET_EXEC_ALL_EXPR_ENABLED: ConfigEntry[Boolean] =
    conf(s"$COMET_EXEC_CONFIG_PREFIX.all.expr.enabled")
      .doc(
        "Whether to enable all Comet exprs. By default, this config is false. Note that " +
          "this config precedes all separate config 'spark.comet.exec.<expr_name>.enabled'. " +
          "That being said, if this config is enabled, separate configs are ignored.")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXEC_SHUFFLE_ENABLED: ConfigEntry[Boolean] =
    conf(s"$COMET_EXEC_CONFIG_PREFIX.shuffle.enabled")
      .doc(
        "Whether to enable Comet native shuffle. By default, this config is false. " +
          "Note that this requires setting 'spark.shuffle.manager' to " +
          "'org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager'. " +
          "'spark.shuffle.manager' must be set before starting the Spark application and " +
          "cannot be changed during the application.")
      .booleanConf
      .createWithDefault(false)

  val COMET_COLUMNAR_SHUFFLE_ENABLED: ConfigEntry[Boolean] = conf(
    "spark.comet.columnar.shuffle.enabled")
    .doc(
      "Force Comet to only use columnar shuffle for CometScan and Spark regular operators. " +
        "If this is enabled, Comet native shuffle will not be enabled but only Arrow shuffle. " +
        "By default, this config is false.")
    .booleanConf
    .createWithDefault(false)

  val COMET_EXEC_BROADCAST_FORCE_ENABLED: ConfigEntry[Boolean] =
    conf(s"$COMET_EXEC_CONFIG_PREFIX.broadcast.enabled")
      .doc(
        "Whether to force enabling broadcasting for Comet native operators. By default, " +
          "this config is false. Comet broadcast feature will be enabled automatically by " +
          "Comet extension. But for unit tests, we need this feature to force enabling it " +
          "for invalid cases. So this config is only used for unit test.")
      .booleanConf
      .createWithDefault(false)

  val COMET_EXEC_SHUFFLE_CODEC: ConfigEntry[String] = conf(
    s"$COMET_EXEC_CONFIG_PREFIX.shuffle.codec")
    .doc(
      "The codec of Comet native shuffle used to compress shuffle data. Only zstd is supported.")
    .stringConf
    .createWithDefault("zstd")

  val COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED: ConfigEntry[Boolean] = conf(
    "spark.comet.columnar.shuffle.async.enabled")
    .doc(
      "Whether to enable asynchronous shuffle for Arrow-based shuffle. By default, this config " +
        "is false.")
    .booleanConf
    .createWithDefault(false)

  val COMET_EXEC_SHUFFLE_ASYNC_THREAD_NUM: ConfigEntry[Int] =
    conf("spark.comet.columnar.shuffle.async.thread.num")
      .doc("Number of threads used for Comet async columnar shuffle per shuffle task. " +
        "By default, this config is 3. Note that more threads means more memory requirement to " +
        "buffer shuffle data before flushing to disk. Also, more threads may not always " +
        "improve performance, and should be set based on the number of cores available.")
      .intConf
      .createWithDefault(3)

  val COMET_EXEC_SHUFFLE_ASYNC_MAX_THREAD_NUM: ConfigEntry[Int] = {
    conf("spark.comet.columnar.shuffle.async.max.thread.num")
      .doc("Maximum number of threads on an executor used for Comet async columnar shuffle. " +
        "By default, this config is 100. This is the upper bound of total number of shuffle " +
        "threads per executor. In other words, if the number of cores * the number of shuffle " +
        "threads per task `spark.comet.columnar.shuffle.async.thread.num` is larger than " +
        "this config. Comet will use this config as the number of shuffle threads per " +
        "executor instead.")
      .intConf
      .createWithDefault(100)
  }

  val COMET_EXEC_SHUFFLE_SPILL_THRESHOLD: ConfigEntry[Int] =
    conf("spark.comet.columnar.shuffle.spill.threshold")
      .doc(
        "Number of rows to be spilled used for Comet columnar shuffle. " +
          "For every configured number of rows, a new spill file will be created. " +
          "Higher value means more memory requirement to buffer shuffle data before " +
          "flushing to disk. As Comet uses columnar shuffle which is columnar format, " +
          "higher value usually helps to improve shuffle data compression ratio. This is " +
          "internal config for testing purpose or advanced tuning. By default, " +
          "this config is Int.Max.")
      .internal()
      .intConf
      .createWithDefault(Int.MaxValue)

  val COMET_COLUMNAR_SHUFFLE_MEMORY_SIZE: OptionalConfigEntry[Long] =
    conf("spark.comet.columnar.shuffle.memorySize")
      .doc(
        "The optional maximum size of the memory used for Comet columnar shuffle, in MiB. " +
          "Note that this config is only used when `spark.comet.columnar.shuffle.enabled` is " +
          "true. Once allocated memory size reaches this config, the current batch will be " +
          "flushed to disk immediately. If this is not configured, Comet will use " +
          "`spark.comet.shuffle.memory.factor` * `spark.comet.memoryOverhead` as " +
          "shuffle memory size. If final calculated value is larger than Comet memory " +
          "overhead, Comet will use Comet memory overhead as shuffle memory size.")
      .bytesConf(ByteUnit.MiB)
      .createOptional

  val COMET_COLUMNAR_SHUFFLE_MEMORY_FACTOR: ConfigEntry[Double] =
    conf("spark.comet.columnar.shuffle.memory.factor")
      .doc(
        "Fraction of Comet memory to be allocated per executor process for Comet shuffle. " +
          "Comet memory size is specified by `spark.comet.memoryOverhead` or " +
          "calculated by `spark.comet.memory.overhead.factor` * `spark.executor.memory`. " +
          "By default, this config is 1.0.")
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
    .doc("The ratio of total values to distinct values in a string column to decide whether to " +
      "prefer dictionary encoding when shuffling the column. If the ratio is higher than " +
      "this config, dictionary encoding will be used on shuffling string column. This config " +
      "is effective if it is higher than 1.0. By default, this config is 10.0. Note that this " +
      "config is only used when 'spark.comet.columnar.shuffle.enabled' is true.")
    .doubleConf
    .createWithDefault(10.0)

  val COMET_DEBUG_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.debug.enabled")
      .doc(
        "Whether to enable debug mode for Comet. By default, this config is false. " +
          "When enabled, Comet will do additional checks for debugging purpose. For example, " +
          "validating array when importing arrays from JVM at native side. Note that these " +
          "checks may be expensive in performance and should only be enabled for debugging " +
          "purpose.")
      .booleanConf
      .createWithDefault(false)

  val COMET_BATCH_SIZE: ConfigEntry[Int] = conf("spark.comet.batchSize")
    .doc("The columnar batch size, i.e., the maximum number of rows that a batch can contain.")
    .intConf
    .createWithDefault(8192)

  val COMET_EXEC_MEMORY_FRACTION: ConfigEntry[Double] = conf("spark.comet.exec.memoryFraction")
    .doc(
      "The fraction of memory from Comet memory overhead that the native memory " +
        "manager can use for execution. The purpose of this config is to set aside memory for " +
        "untracked data structures, as well as imprecise size estimation during memory " +
        "acquisition. Default value is 0.7.")
    .doubleConf
    .createWithDefault(0.7)

  val COMET_PARQUET_ENABLE_DIRECT_BUFFER: ConfigEntry[Boolean] = conf(
    "spark.comet.parquet.enable.directBuffer")
    .doc("Whether to use Java direct byte buffer when reading Parquet. By default, this is false")
    .booleanConf
    .createWithDefault(false)

  val COMET_SCAN_PREFETCH_ENABLED: ConfigEntry[Boolean] =
    conf("spark.comet.scan.preFetch.enabled")
      .doc("Whether to enable pre-fetching feature of CometScan. By default is disabled.")
      .booleanConf
      .createWithDefault(false)

  val COMET_SCAN_PREFETCH_THREAD_NUM: ConfigEntry[Int] =
    conf("spark.comet.scan.preFetch.threadNum")
      .doc(
        "The number of threads running pre-fetching for CometScan. Effective if " +
          s"${COMET_SCAN_PREFETCH_ENABLED.key} is enabled. By default it is 2. Note that more " +
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
    .createWithDefault(false)
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
    new OptionalConfigEntry[T](
      parent.key,
      converter,
      stringConverter,
      parent._doc,
      parent._public,
      parent._version)
  }

  /** Creates a [[ConfigEntry]] that has a default value. */
  def createWithDefault(default: T): ConfigEntry[T] = {
    val transformedDefault = converter(stringConverter(default))
    new ConfigEntryWithDefault[T](
      parent.key,
      transformedDefault,
      converter,
      stringConverter,
      parent._doc,
      parent._public,
      parent._version)
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
