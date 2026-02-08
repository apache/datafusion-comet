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

package org.apache.comet.serde.operator

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PlanExpression}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometConf.COMET_EXEC_ENABLED
import org.apache.comet.CometSparkSessionExtensions.{hasExplainInfo, withInfo}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

/**
 * Validation and serde logic for `native_datafusion` scans.
 */
object CometNativeScan extends Logging {

  /** Determine whether the scan is supported and tag the Spark plan with any fallback reasons */
  def isSupported(scanExec: FileSourceScanExec): Boolean = {

    if (hasExplainInfo(scanExec)) {
      // this node has already been tagged with fallback reasons
      return false
    }

    if (!COMET_EXEC_ENABLED.get()) {
      withInfo(scanExec, s"Full native scan disabled because ${COMET_EXEC_ENABLED.key} disabled")
    }

    if (SQLConf.get.ignoreCorruptFiles ||
      scanExec.relation.options
        .get("ignorecorruptfiles") // Spark sets this to lowercase.
        .contains("true")) {
      withInfo(scanExec, "Full native scan disabled because ignoreCorruptFiles enabled")
    }

    if (SQLConf.get.ignoreMissingFiles ||
      scanExec.relation.options
        .get("ignoremissingfiles") // Spark sets this to lowercase.
        .contains("true")) {

      withInfo(scanExec, "Full native scan disabled because ignoreMissingFiles enabled")
    }

    // the scan is supported if no fallback reasons were added to the node
    !hasExplainInfo(scanExec)
  }

  private[comet] def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

  /**
   * Convert FileSourceScanExec to a placeholder NativeScan operator. The actual partition data is
   * populated at execution time by serializePartitions().
   */
  def convert(
      scanExec: FileSourceScanExec,
      builder: Operator.Builder): Option[OperatorOuterClass.Operator] = {

    val scanTypes = scanExec.output.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length != scanExec.output.length) {
      withInfo(
        scanExec,
        s"unsupported Comet operator: ${scanExec.nodeName}, due to unsupported data types above")
      return None
    }

    // Build placeholder NativeScan with just scan_id for matching at execution time.
    // All other fields are populated by serializePartitions() at execution time.
    val commonBuilder = OperatorOuterClass.NativeScanCommon.newBuilder()
    commonBuilder.setScanId(getScanId(scanExec))

    val nativeScanBuilder = OperatorOuterClass.NativeScan.newBuilder()
    nativeScanBuilder.setCommon(commonBuilder.build())
    // file_partition intentionally empty - will be populated at execution time

    builder.clearChildren()
    Some(builder.setNativeScan(nativeScanBuilder).build())
  }

  /** Unique identifier for this scan, used to match planning data at execution time. */
  def getScanId(scanExec: FileSourceScanExec): String = {
    scanExec.relation.location.rootPaths.headOption
      .map(_.toString)
      .getOrElse(scanExec.simpleStringWithNodeId())
  }

  /**
   * Serializes partitions at execution time, after DPP filters have been resolved.
   *
   * @param exec
   *   The CometNativeScanExec with resolved DPP filters
   * @param filePartitions
   *   The DPP-filtered file partitions (computed by exec using shim-compatible methods)
   * @return
   *   Tuple of (commonBytes, perPartitionBytes) for native execution
   */
  def serializePartitions(
      exec: CometNativeScanExec,
      filePartitions: Seq[FilePartition]): (Array[Byte], Array[Array[Byte]]) = {
    val scanExec = exec.originalPlan
    val relation = exec.relation

    val commonBuilder = OperatorOuterClass.NativeScanCommon.newBuilder()
    commonBuilder.setSource(scanExec.simpleStringWithNodeId())
    commonBuilder.setScanId(exec.scanId)

    val scanTypes = exec.output.flatten { attr =>
      serializeDataType(attr.dataType)
    }
    commonBuilder.addAllFields(scanTypes.asJava)

    // Filter out DPP filters from data filters - these are partition filters, not data filters
    val supportedDataFilters = exec.dataFilters.filterNot(isDynamicPruningFilter)

    val conf = relation.sparkSession.sessionState.conf
    if (conf.getConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED) &&
      CometConf.COMET_RESPECT_PARQUET_FILTER_PUSHDOWN.get(conf)) {

      val dataFilters = new ListBuffer[Expr]()
      for (filter <- supportedDataFilters) {
        exprToProto(filter, exec.output) match {
          case Some(proto) => dataFilters += proto
          case _ =>
            logWarning(s"Unsupported data filter $filter")
        }
      }
      commonBuilder.addAllDataFilters(dataFilters.asJava)
    }

    val possibleDefaultValues = getExistenceDefaultValues(exec.requiredSchema)
    if (possibleDefaultValues.exists(_ != null)) {
      val (defaultValues, indexes) = possibleDefaultValues.zipWithIndex
        .filter { case (expr, _) => expr != null }
        .map { case (expr, index) =>
          (Literal(expr), index.toLong.asInstanceOf[java.lang.Long])
        }
        .unzip
      commonBuilder.addAllDefaultValues(
        defaultValues.flatMap(exprToProto(_, exec.output)).toIterable.asJava)
      commonBuilder.addAllDefaultValuesIndexes(indexes.toIterable.asJava)
    }

    val partitionSchema = schema2Proto(relation.partitionSchema.fields)
    val requiredSchema = schema2Proto(exec.requiredSchema.fields)
    val dataSchema = schema2Proto(relation.dataSchema.fields)

    val dataSchemaIndexes = exec.requiredSchema.fields.map(field => {
      relation.dataSchema.fieldIndex(field.name)
    })
    val partitionSchemaIndexes = Array
      .range(
        relation.dataSchema.fields.length,
        relation.dataSchema.length + relation.partitionSchema.fields.length)

    val projectionVector = (dataSchemaIndexes ++ partitionSchemaIndexes).map(idx =>
      idx.toLong.asInstanceOf[java.lang.Long])

    commonBuilder.addAllProjectionVector(projectionVector.toIterable.asJava)
    commonBuilder.addAllDataSchema(dataSchema.toIterable.asJava)
    commonBuilder.addAllRequiredSchema(requiredSchema.toIterable.asJava)
    commonBuilder.addAllPartitionSchema(partitionSchema.toIterable.asJava)
    commonBuilder.setSessionTimezone(conf.getConfString("spark.sql.session.timeZone"))
    commonBuilder.setCaseSensitive(conf.getConf[Boolean](SQLConf.CASE_SENSITIVE))

    // Collect S3/cloud storage configurations
    val hadoopConf = relation.sparkSession.sessionState
      .newHadoopConfWithOptions(relation.options)

    commonBuilder.setEncryptionEnabled(CometParquetUtils.encryptionEnabled(hadoopConf))

    filePartitions.headOption.flatMap(_.files.headOption).foreach { partitionFile =>
      val objectStoreOptions =
        NativeConfig.extractObjectStoreOptions(hadoopConf, partitionFile.pathUri)
      objectStoreOptions.foreach { case (key, value) =>
        commonBuilder.putObjectStoreOptions(key, value)
      }
    }

    val commonBytes = commonBuilder.build().toByteArray

    // Build per-partition data - each partition gets its own SparkFilePartition
    val perPartitionBytes = filePartitions.map { partition =>
      val partitionProto = partition2Proto(partition, relation.partitionSchema)
      val scanBuilder = OperatorOuterClass.NativeScan.newBuilder()
      scanBuilder.setFilePartition(partitionProto)
      // common is set at injection time, not here
      scanBuilder.build().toByteArray
    }.toArray

    (commonBytes, perPartitionBytes)
  }
}
