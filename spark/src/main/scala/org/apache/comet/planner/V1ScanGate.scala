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

package org.apache.comet.planner

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet.CometScanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, SubqueryAdaptiveBroadcastExec}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf._
import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.parquet.CometParquetUtils.{encryptionEnabled, isEncryptionConfigSupported}
import org.apache.comet.rules.CometScanTypeChecker
import org.apache.comet.serde.operator.CometNativeScan
import org.apache.comet.shims.ShimFileFormat

sealed trait V1ScanClassification

object V1ScanClassification {
  case object Convertible extends V1ScanClassification
  final case class NotConvertible(reasons: Set[String]) extends V1ScanClassification
}

/**
 * Classifies a V1 FileSourceScanExec for CometPlanner. Ports the gates from
 * `CometScanRule.transformV1Scan` and `nativeDataFusionScan` into a pure ADT. Callers attach
 * `withInfo` entries from the returned reasons themselves.
 *
 * Plan-wide checks (input_file_name / input_file_block_start / input_file_block_length) require
 * the caller to pre-compute `hasInputFileExpressions` once per `CometPlanner.apply` invocation.
 * V1ScanGate only receives the boolean so it does not re-walk the full plan per scan.
 *
 * Duplicates validation that also lives in `CometScanRule.transformV1Scan`. Both copies are live
 * (one per registered rule path) until the legacy rule is deleted.
 */
object V1ScanGate extends Logging {

  def classify(
      scanExec: FileSourceScanExec,
      session: SparkSession,
      conf: SQLConf,
      hasInputFileExpressions: Boolean): V1ScanClassification = {
    val reasons = new ListBuffer[String]()

    def reject(reason: String): V1ScanClassification = {
      reasons += reason
      logDebug(s"V1ScanGate reject scan=${scanExec.id} reason=$reason")
      V1ScanClassification.NotConvertible(reasons.toSet)
    }

    if (!isSpark35Plus && scanExec.partitionFilters.exists(isAqeDynamicPruningFilter)) {
      return reject("AQE Dynamic Partition Pruning requires Spark 3.5+")
    }

    val r = scanExec.relation match {
      case rel: HadoopFsRelation => rel
      case other =>
        return reject(s"Unsupported relation $other")
    }

    if (!CometScanExec.isFileFormatSupported(r.fileFormat)) {
      return reject(s"Unsupported file format ${r.fileFormat}")
    }

    val hadoopConf = r.sparkSession.sessionState.newHadoopConfWithOptions(r.options)

    val possibleDefaultValues = getExistenceDefaultValues(scanExec.requiredSchema)
    if (possibleDefaultValues.exists(d =>
        d != null && (d.isInstanceOf[ArrayBasedMapData]
          || d.isInstanceOf[GenericInternalRow]
          || d.isInstanceOf[GenericArrayData]))) {
      return reject(
        "Full native scan disabled because default values for nested types are not supported")
    }

    // CometNativeScan.isSupported covers COMET_EXEC_ENABLED, the AQE-DPP-on-3.4 safety net,
    // ignoreCorruptFiles and ignoreMissingFiles. Today it records fallbacks via `withInfo`
    // side effects on the scan. That is acceptable: those messages should reach explain
    // output regardless of which rule decides to fall back.
    if (!CometNativeScan.isSupported(scanExec)) {
      return reject(s"$SCAN_NATIVE_DATAFUSION scan unsupported. See scan info for details.")
    }

    if (encryptionEnabled(hadoopConf) && !isEncryptionConfigSupported(hadoopConf)) {
      return reject(s"$SCAN_NATIVE_DATAFUSION does not support encryption")
    }

    if (scanExec.fileConstantMetadataColumns.nonEmpty) {
      return reject("Native DataFusion scan does not support metadata columns")
    }

    // input_file_name, input_file_block_start and input_file_block_length read from
    // InputFileBlockHolder, a thread-local that Spark's FileScanRDD populates. The native
    // DataFusion scan bypasses FileScanRDD, so these expressions would see empty values.
    if (hasInputFileExpressions) {
      return reject(
        "Native DataFusion scan is not compatible with input_file_name, " +
          "input_file_block_start, or input_file_block_length")
    }

    if (ShimFileFormat.findRowIndexColumnIndexInSchema(scanExec.requiredSchema) >= 0) {
      return reject("Native DataFusion scan does not support row index generation")
    }

    if (session.sessionState.conf.getConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED) &&
      ParquetUtils.hasFieldIds(scanExec.requiredSchema)) {
      return reject("Native DataFusion scan does not support Parquet field ID matching")
    }

    val typeChecker = CometScanTypeChecker(SCAN_NATIVE_DATAFUSION)
    val schemaFallback = new ListBuffer[String]()
    val schemaSupported =
      typeChecker.isSchemaSupported(scanExec.requiredSchema, schemaFallback)
    if (!schemaSupported) {
      return reject(
        s"Unsupported schema ${scanExec.requiredSchema} " +
          s"for $SCAN_NATIVE_DATAFUSION. ${schemaFallback.mkString(", ")}")
    }
    val partitionSchemaSupported =
      typeChecker.isSchemaSupported(r.partitionSchema, schemaFallback)
    if (!partitionSchemaSupported) {
      return reject(
        s"Unsupported partitioning schema ${r.partitionSchema} " +
          s"for $SCAN_NATIVE_DATAFUSION. ${schemaFallback.mkString(", ")}")
    }

    V1ScanClassification.Convertible
  }

  private def isAqeDynamicPruningFilter(e: Expression): Boolean =
    e.exists {
      case sub: InSubqueryExec => sub.plan.isInstanceOf[SubqueryAdaptiveBroadcastExec]
      case _ => false
    }
}
