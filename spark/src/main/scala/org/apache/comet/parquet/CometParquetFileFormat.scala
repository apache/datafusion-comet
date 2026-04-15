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

package org.apache.comet.parquet

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.FileSourceGeneratedMetadataStructField
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.comet.CometMetricNode
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.RecordReaderIterator
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DateType, StructField, StructType, TimestampType}
import org.apache.spark.util.SerializableConfiguration

import org.apache.comet.CometConf
import org.apache.comet.MetricsSupport
import org.apache.comet.shims.ShimSQLConf
import org.apache.comet.vector.CometVector

/**
 * A Comet specific Parquet format. This mostly reuse the functionalities from Spark's
 * [[ParquetFileFormat]], but overrides:
 *
 *   - `vectorTypes`, so Spark allocates [[CometVector]] instead of it's own on-heap or off-heap
 *     column vector in the whole-stage codegen path.
 *   - `supportBatch`, which simply returns true since data types should have already been checked
 *     in [[org.apache.comet.CometSparkSessionExtensions]]
 *   - `buildReaderWithPartitionValues`, so Spark calls Comet's Parquet reader to read values.
 */
class CometParquetFileFormat(session: SparkSession)
    extends ParquetFileFormat
    with MetricsSupport
    with ShimSQLConf {
  metrics =
    CometMetricNode.nativeScanMetrics(session.sparkContext) ++ CometMetricNode.parquetScanMetrics(
      session.sparkContext)

  override def shortName(): String = "parquet"
  override def toString: String = "CometParquet"
  override def hashCode(): Int = getClass.hashCode()
  override def equals(other: Any): Boolean = other.isInstanceOf[CometParquetFileFormat]

  override def vectorTypes(
      requiredSchema: StructType,
      partitionSchema: StructType,
      sqlConf: SQLConf): Option[Seq[String]] = {
    val length = requiredSchema.fields.length + partitionSchema.fields.length
    Option(Seq.fill(length)(classOf[CometVector].getName))
  }

  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = true

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val sqlConf = sparkSession.sessionState.conf
    CometParquetFileFormat.populateConf(sqlConf, hadoopConf)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val useFieldId = CometParquetUtils.readFieldId(sqlConf)
    val ignoreMissingIds = CometParquetUtils.ignoreMissingIds(sqlConf)
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringPredicate = sqlConf.parquetFilterPushDownStringPredicate
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val optionsMap = CaseInsensitiveMap[String](options)
    val parquetOptions = new ParquetOptions(optionsMap, sqlConf)
    val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
    val parquetFilterPushDown = sqlConf.parquetFilterPushDown &&
      CometConf.COMET_RESPECT_PARQUET_FILTER_PUSHDOWN.get(sqlConf)

    // Comet specific configurations
    val capacity = CometConf.COMET_BATCH_SIZE.get(sqlConf)

    // Delta's row-tracking / generated-metadata columns appear in the scan's
    // required/data schemas under their LOGICAL names (e.g. `row_id`) with a
    // `FileSourceGeneratedMetadataStructField` metadata marker carrying the
    // PHYSICAL column name stored in the parquet file (when materialised) --
    // see Spark's `FileSourceGeneratedMetadataStructField` +
    // Delta's `RowId.RowIdMetadataStructField`. The stock Parquet reader
    // translates these before the actual read; Comet's `NativeBatchReader`
    // doesn't know about Spark metadata columns, so we substitute the
    // physical name here. Only touches fields where `getInternalNameIfValid`
    // returns `Some(name)`; everything else is passed through unchanged.
    val substitutedRequiredSchema =
      CometParquetFileFormat.substituteGeneratedMetadataFields(requiredSchema)
    val substitutedDataSchema =
      CometParquetFileFormat.substituteGeneratedMetadataFields(dataSchema)

    (file: PartitionedFile) => {
      val sharedConf = broadcastedHadoopConf.value.value
      val footer = FooterReader.readFooter(sharedConf, file)
      val footerFileMetaData = footer.getFileMetaData
      val datetimeRebaseSpec = CometParquetFileFormat.getDatetimeRebaseSpec(
        file,
        substitutedRequiredSchema,
        sharedConf,
        footerFileMetaData,
        datetimeRebaseModeInRead)

      val parquetSchema = footerFileMetaData.getSchema
      val parquetFilters = new ParquetFilters(
        parquetSchema,
        substitutedDataSchema,
        pushDownDate,
        pushDownTimestamp,
        pushDownDecimal,
        pushDownStringPredicate,
        pushDownInFilterThreshold,
        isCaseSensitive,
        datetimeRebaseSpec)

      val pushed = if (parquetFilterPushDown) {
        filters
          .flatMap(parquetFilters.createFilter)
          .reduceOption(FilterApi.and)
      } else {
        None
      }
      pushed.foreach(p => ParquetInputFormat.setFilterPredicate(sharedConf, p))
      val pushedNative = if (parquetFilterPushDown) {
        parquetFilters.createNativeFilters(filters)
      } else {
        None
      }
      val recordBatchReader = new NativeBatchReader(
        sharedConf,
        file,
        footer,
        pushedNative.orNull,
        capacity,
        substitutedRequiredSchema,
        substitutedDataSchema,
        isCaseSensitive,
        useFieldId,
        ignoreMissingIds,
        datetimeRebaseSpec.mode == CORRECTED,
        partitionSchema,
        file.partitionValues,
        metrics.asJava,
        CometMetricNode(metrics))
      try {
        recordBatchReader.init()
      } catch {
        case e: Throwable =>
          recordBatchReader.close()
          throw e
      }
      val iter = new RecordReaderIterator(recordBatchReader)
      try {
        iter.asInstanceOf[Iterator[InternalRow]]
      } catch {
        case e: Throwable =>
          iter.close()
          throw e
      }
    }
  }
}

object CometParquetFileFormat extends Logging with ShimSQLConf {

  /**
   * Rewrite any `FileSourceGeneratedMetadataStructField`-marked fields in `schema` so they use
   * the PHYSICAL column name stored in the parquet file (carried in the field's metadata) instead
   * of the LOGICAL attribute name. Fields that don't have the marker are returned unchanged.
   *
   * Delta's row-tracking feature emits fields like `row_id` / `row_commit_version` this way: the
   * attribute surfaced to callers uses the logical name while the parquet file stores the column
   * under a materialised name like `_metadata.row_id` or a generated UUID. The stock
   * `ParquetFileFormat` handles this substitution inside `ParquetReadSupport`, but Comet's
   * `NativeBatchReader` reads the schema as-is -- without the substitution, the read fails with
   * "Required column 'row_id' is missing in data file".
   */
  def substituteGeneratedMetadataFields(schema: StructType): StructType = {
    val fields = schema.fields.map { f =>
      FileSourceGeneratedMetadataStructField
        .getInternalNameIfValid(f.dataType, f.metadata)
        .map(physical => StructField(physical, f.dataType, f.nullable, f.metadata))
        .getOrElse(f)
    }
    if (fields.sameElements(schema.fields)) schema else StructType(fields)
  }

  /**
   * Populates Parquet related configurations from the input `sqlConf` to the `hadoopConf`
   */
  def populateConf(sqlConf: SQLConf, hadoopConf: Configuration): Unit = {
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, sqlConf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sqlConf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE.key, sqlConf.caseSensitiveAnalysis)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, sqlConf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sqlConf.isParquetINT96AsTimestamp)

    // Comet specific configs
    hadoopConf.setBoolean(
      CometConf.COMET_USE_DECIMAL_128.key,
      CometConf.COMET_USE_DECIMAL_128.get())
    hadoopConf.setBoolean(
      CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key,
      CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.get())
    hadoopConf.setInt(CometConf.COMET_BATCH_SIZE.key, CometConf.COMET_BATCH_SIZE.get())
  }

  def getDatetimeRebaseSpec(
      file: PartitionedFile,
      sparkSchema: StructType,
      sharedConf: Configuration,
      footerFileMetaData: FileMetaData,
      datetimeRebaseModeInRead: String): RebaseSpec = {
    val exceptionOnRebase = sharedConf.getBoolean(
      CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.key,
      CometConf.COMET_EXCEPTION_ON_LEGACY_DATE_TIMESTAMP.defaultValue.get)
    var datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(
      footerFileMetaData.getKeyValueMetaData.get,
      datetimeRebaseModeInRead)
    val hasDateOrTimestamp = sparkSchema.exists(f =>
      f.dataType match {
        case DateType | TimestampType => true
        case _ => false
      })

    if (hasDateOrTimestamp && datetimeRebaseSpec.mode == LEGACY) {
      if (exceptionOnRebase) {
        logWarning(
          s"""Found Parquet file $file that could potentially contain dates/timestamps that were
              written in legacy hybrid Julian/Gregorian calendar. Unlike Spark 3+, which will rebase
              and return these according to the new Proleptic Gregorian calendar, Comet will throw
              exception when reading them. If you want to read them as it is according to the hybrid
              Julian/Gregorian calendar, please set `spark.comet.exceptionOnDatetimeRebase` to
              false. Otherwise, if you want to read them according to the new Proleptic Gregorian
              calendar, please disable Comet for this query.""")
      } else {
        // do not throw exception on rebase - read as it is
        datetimeRebaseSpec = datetimeRebaseSpec.copy(CORRECTED)
      }
    }

    datetimeRebaseSpec
  }
}
