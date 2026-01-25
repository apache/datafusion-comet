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

import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.comet.CometMetricNode
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import org.apache.comet.CometConf
import org.apache.comet.shims.ShimSQLConf

/**
 * A partition reader factory for V2 Parquet scans that uses NativeBatchReader (DataFusion-based
 * Parquet reader) instead of the legacy BatchReader.
 *
 * This is used for native_iceberg_compat scan implementation with V2 data sources.
 */
case class CometNativeParquetPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    dataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    options: ParquetOptions,
    metrics: Map[String, SQLMetric])
    extends FilePartitionReaderFactory
    with ShimSQLConf
    with Logging {

  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val useFieldId = CometParquetUtils.readFieldId(sqlConf)
  private val ignoreMissingIds = CometParquetUtils.ignoreMissingIds(sqlConf)
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  private val pushDownStringPredicate = sqlConf.parquetFilterPushDownStringPredicate
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val datetimeRebaseModeInRead = options.datetimeRebaseModeInRead
  private val parquetFilterPushDown = sqlConf.parquetFilterPushDown

  // Comet specific configurations
  private val batchSize = CometConf.COMET_BATCH_SIZE.get(sqlConf)

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException(
      "CometNativeParquetPartitionReaderFactory doesn't support row-based reads")

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val sharedConf = broadcastedConf.value.value
    val footer = FooterReader.readFooter(sharedConf, file)
    val footerFileMetaData = footer.getFileMetaData
    val datetimeRebaseSpec = CometParquetFileFormat.getDatetimeRebaseSpec(
      file,
      readDataSchema,
      sharedConf,
      footerFileMetaData,
      datetimeRebaseModeInRead)

    val parquetSchema = footerFileMetaData.getSchema
    val parquetFilters = new ParquetFilters(
      parquetSchema,
      dataSchema,
      pushDownDate,
      pushDownTimestamp,
      pushDownDecimal,
      pushDownStringPredicate,
      pushDownInFilterThreshold,
      isCaseSensitive,
      datetimeRebaseSpec)

    // Set the predicate in the conf for row index generation
    val pushed: Option[FilterPredicate] = if (parquetFilterPushDown) {
      filters
        .flatMap(parquetFilters.createFilter)
        .reduceOption(FilterApi.and)
    } else {
      None
    }
    pushed.foreach(p => ParquetInputFormat.setFilterPredicate(sharedConf, p))

    // Create native filter for DataFusion
    val pushedNative: Option[Array[Byte]] = if (parquetFilterPushDown) {
      parquetFilters.createNativeFilters(filters)
    } else {
      None
    }

    val batchReader = new NativeBatchReader(
      sharedConf,
      file,
      footer,
      pushedNative.orNull,
      batchSize,
      readDataSchema,
      dataSchema,
      isCaseSensitive,
      useFieldId,
      ignoreMissingIds,
      datetimeRebaseSpec.mode == CORRECTED,
      partitionSchema,
      file.partitionValues,
      metrics.asJava,
      CometMetricNode(metrics))

    try {
      batchReader.init()
    } catch {
      case e: Throwable =>
        batchReader.close()
        throw e
    }

    CometNativePartitionReader(batchReader)
  }

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException("Only 'createColumnarReader' is supported.")

  /**
   * A simple adapter on Comet's [[NativeBatchReader]].
   */
  private case class CometNativePartitionReader(reader: NativeBatchReader)
      extends PartitionReader[ColumnarBatch] {

    override def next(): Boolean = {
      reader.nextBatch()
    }

    override def get(): ColumnarBatch = {
      reader.currentBatch()
    }

    override def close(): Unit = {
      reader.close()
    }
  }
}
