package org.apache.comet.csv

import scala.jdk.CollectionConverters.mapAsScalaMapConverter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

case class CometCsvScan(
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression])
    extends TextBasedFileScan(sparkSession, options) {

  private lazy val parsedOptions: CSVOptions = new CSVOptions(
    options.asScala.toMap,
    columnPruning = false,
    sparkSession.sessionState.conf.sessionLocalTimeZone,
    sparkSession.sessionState.conf.columnNameOfCorruptRecord)

  override def createReaderFactory(): PartitionReaderFactory = {
    val hadoopConf =
      sparkSession.sessionState.newHadoopConfWithOptions(options.asCaseSensitiveMap.asScala.toMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    CometCsvPartitionReaderFactory(parsedOptions, broadcastedConf)
  }
}
