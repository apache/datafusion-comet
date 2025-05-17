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

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import org.apache.comet.MetricsSupport

trait CometParquetScan extends FileScan with MetricsSupport {
  def sparkSession: SparkSession
  def hadoopConf: Configuration
  def readDataSchema: StructType
  def readPartitionSchema: StructType
  def pushedFilters: Array[Filter]
  def options: CaseInsensitiveStringMap

  override def equals(obj: Any): Boolean = obj match {
    case other: CometParquetScan =>
      super.equals(other) && readDataSchema == other.readDataSchema &&
      readPartitionSchema == other.readPartitionSchema &&
      equivalentFilters(pushedFilters, other.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def createReaderFactory(): PartitionReaderFactory = {
    val sqlConf = sparkSession.sessionState.conf
    CometParquetFileFormat.populateConf(sqlConf, hadoopConf)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    CometParquetPartitionReaderFactory(
      usingDataFusionReader = false, // this value is not used since this is v2 scan
      sqlConf,
      broadcastedConf,
      readDataSchema,
      readPartitionSchema,
      pushedFilters,
      new ParquetOptions(options.asScala.toMap, sqlConf),
      metrics)
  }
}

object CometParquetScan {
  def apply(scan: ParquetScan): CometParquetScan =
    new ParquetScan(
      scan.sparkSession,
      scan.hadoopConf,
      scan.fileIndex,
      scan.dataSchema,
      scan.readDataSchema,
      scan.readPartitionSchema,
      scan.pushedFilters,
      scan.options,
      partitionFilters = scan.partitionFilters,
      dataFilters = scan.dataFilters) with CometParquetScan
}
