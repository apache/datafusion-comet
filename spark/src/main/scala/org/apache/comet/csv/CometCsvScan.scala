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

import scala.jdk.CollectionConverters.mapAsScalaMapConverter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
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
    CometCsvPartitionReaderFactory(sparkSession.sessionState.conf, parsedOptions, broadcastedConf)
  }
}

object CometCsvScan {
  def apply(session: SparkSession, scan: CSVScan): CometCsvScan = {
    CometCsvScan(
      session,
      scan.options,
      scan.fileIndex,
      scan.dataSchema,
      scan.readDataSchema,
      scan.readPartitionSchema,
      scan.partitionFilters,
      scan.dataFilters)
  }
}
