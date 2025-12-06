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

package org.apache.comet.csv

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.comet.execution.arrow.CometArrowConverters
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, ByteType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import org.apache.comet.CometConf

case class CometCsvPartitionReaderFactory(
    sqlConf: SQLConf,
    options: CSVOptions,
    broadcastedConf: Broadcast[SerializableConfiguration])
    extends FilePartitionReaderFactory {

  private var partitionReaders: Iterator[CometCsvPartitionReader] = _

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val filePartition = partition.asInstanceOf[FilePartition]
    this.partitionReaders = filePartition.files
      .map(file => buildCometCsvPartitionReader(file))
      .toSeq
      .toIterator
    super.createColumnarReader(partition)
  }

  override def buildColumnarReader(
      partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    if (partitionReaders.hasNext) {
      return partitionReaders.next()
    }
    buildCometCsvPartitionReader(partitionedFile)
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException("Comet doesn't support 'buildReader'")

  private def buildCometCsvPartitionReader(file: PartitionedFile): CometCsvPartitionReader = {
    val conf = broadcastedConf.value.value
    val lines = {
      val linesReader = new HadoopFileLinesReader(file, options.lineSeparatorInRead, conf)
      Option(TaskContext.get())
        .foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
      linesReader.map(line => InternalRow.apply(line.getBytes))
    }
    val maxRecordsPerBatch = CometConf.COMET_BATCH_SIZE.get(sqlConf)
    val timeZoneId = sqlConf.sessionLocalTimeZone
    val batches = CometArrowConverters.rowToArrowBatchIter(
      lines,
      CometCsvPartitionReaderFactory.SCHEMA,
      maxRecordsPerBatch,
      timeZoneId,
      TaskContext.get())
    CometCsvPartitionReader(batches)
  }
}

object CometCsvPartitionReaderFactory {
  private val SCHEMA =
    new StructType().add("value", ArrayType(elementType = ByteType), nullable = false)
}

private case class CometCsvPartitionReader(it: Iterator[ColumnarBatch])
    extends PartitionReader[ColumnarBatch] {
  override def next(): Boolean = it.hasNext

  override def get(): ColumnarBatch = it.next()

  override def close(): Unit = ()
}
