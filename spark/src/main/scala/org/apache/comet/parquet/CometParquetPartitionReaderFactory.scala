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

import scala.collection.JavaConverters
import scala.collection.mutable

import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import org.apache.comet.{CometConf, CometRuntimeException}
import org.apache.comet.shims.ShimSQLConf

case class CometParquetPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
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
  private val pushDownStringPredicate = getPushDownStringPredicate(sqlConf)
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val datetimeRebaseModeInRead = options.datetimeRebaseModeInRead
  private val parquetFilterPushDown = sqlConf.parquetFilterPushDown

  // Comet specific configurations
  private val batchSize = CometConf.COMET_BATCH_SIZE.get(sqlConf)

  // This is only called at executor on a Broadcast variable, so we don't want it to be
  // materialized at driver.
  @transient private lazy val preFetchEnabled = {
    val conf = broadcastedConf.value.value

    conf.getBoolean(
      CometConf.COMET_SCAN_PREFETCH_ENABLED.key,
      CometConf.COMET_SCAN_PREFETCH_ENABLED.defaultValue.get)
  }

  private var cometReaders: Iterator[BatchReader] = _
  private val cometReaderExceptionMap = new mutable.HashMap[PartitionedFile, Throwable]()

  // TODO: we may want to revisit this as we're going to only support flat types at the beginning
  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    if (preFetchEnabled) {
      val filePartition = partition.asInstanceOf[FilePartition]
      val conf = broadcastedConf.value.value

      val threadNum = conf.getInt(
        CometConf.COMET_SCAN_PREFETCH_THREAD_NUM.key,
        CometConf.COMET_SCAN_PREFETCH_THREAD_NUM.defaultValue.get)
      val prefetchThreadPool = CometPrefetchThreadPool.getOrCreateThreadPool(threadNum)

      this.cometReaders = filePartition.files
        .map { file =>
          // `init()` call is deferred to when the prefetch task begins.
          // Otherwise we will hold too many resources for readers which are not ready
          // to prefetch.
          val cometReader = buildCometReader(file)
          if (cometReader != null) {
            cometReader.submitPrefetchTask(prefetchThreadPool)
          }

          cometReader
        }
        .toSeq
        .toIterator
    }

    super.createColumnarReader(partition)
  }

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException("Comet doesn't support 'buildReader'")

  private def buildCometReader(file: PartitionedFile): BatchReader = {
    val conf = broadcastedConf.value.value

    try {
      val (datetimeRebaseSpec, footer, filters) = getFilter(file)
      filters.foreach(pushed => ParquetInputFormat.setFilterPredicate(conf, pushed))
      val cometReader = new BatchReader(
        conf,
        file,
        footer,
        batchSize,
        readDataSchema,
        isCaseSensitive,
        useFieldId,
        ignoreMissingIds,
        datetimeRebaseSpec.mode == LegacyBehaviorPolicy.CORRECTED,
        partitionSchema,
        file.partitionValues,
        JavaConverters.mapAsJavaMap(metrics))
      val taskContext = Option(TaskContext.get)
      taskContext.foreach(_.addTaskCompletionListener[Unit](_ => cometReader.close()))
      return cometReader
    } catch {
      case e: Throwable if preFetchEnabled =>
        // Keep original exception
        cometReaderExceptionMap.put(file, e)
    }
    null
  }

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val cometReader = if (!preFetchEnabled) {
      // Prefetch is not enabled, create comet reader and initiate it.
      val cometReader = buildCometReader(file)
      cometReader.init()

      cometReader
    } else {
      // If prefetch is enabled, we already tried to access the file when in `buildCometReader`.
      // It is possibly we got an exception like `FileNotFoundException` and we need to throw it
      // now to let Spark handle it.
      val reader = cometReaders.next()
      val exception = cometReaderExceptionMap.get(file)
      exception.foreach(e => throw e)

      if (reader == null) {
        throw new CometRuntimeException(s"Cannot find comet file reader for $file")
      }
      reader
    }
    CometPartitionReader(cometReader)
  }

  def getFilter(file: PartitionedFile): (RebaseSpec, ParquetMetadata, Option[FilterPredicate]) = {
    val sharedConf = broadcastedConf.value.value
    val footer = FooterReader.readFooter(sharedConf, file)
    val footerFileMetaData = footer.getFileMetaData
    val datetimeRebaseSpec = CometParquetFileFormat.getDatetimeRebaseSpec(
      file,
      readDataSchema,
      sharedConf,
      footerFileMetaData,
      datetimeRebaseModeInRead)

    val pushed = if (parquetFilterPushDown) {
      val parquetSchema = footerFileMetaData.getSchema
      val parquetFilters = new ParquetFilters(
        parquetSchema,
        pushDownDate,
        pushDownTimestamp,
        pushDownDecimal,
        pushDownStringPredicate,
        pushDownInFilterThreshold,
        isCaseSensitive,
        datetimeRebaseSpec)
      filters
        // Collects all converted Parquet filter predicates. Notice that not all predicates can be
        // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
        // is used here.
        .flatMap(parquetFilters.createFilter)
        .reduceOption(FilterApi.and)
    } else {
      None
    }
    (datetimeRebaseSpec, footer, pushed)
  }

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException("Only 'createColumnarReader' is supported.")

  /**
   * A simple adapter on Comet's [[BatchReader]].
   */
  protected case class CometPartitionReader(reader: BatchReader)
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
