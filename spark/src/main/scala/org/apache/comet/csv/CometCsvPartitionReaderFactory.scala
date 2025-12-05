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

  private val schema =
    new StructType().add("value", ArrayType(elementType = ByteType), nullable = false)

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val filePartition = partition.asInstanceOf[FilePartition]
    filePartition.files.map { file =>
      file.filePath.toPath
    }
    null
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
      schema,
      maxRecordsPerBatch,
      timeZoneId,
      TaskContext.get())
    CometCsvPartitionReader(batches)
  }
}

private case class CometCsvPartitionReader(it: Iterator[ColumnarBatch])
    extends PartitionReader[ColumnarBatch] {
  override def next(): Boolean = it.hasNext

  override def get(): ColumnarBatch = it.next()

  override def close(): Unit = ()
}
