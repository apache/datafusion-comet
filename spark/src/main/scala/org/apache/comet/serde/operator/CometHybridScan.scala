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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.comet.{CometNativeExec, CometScanExec, CometScanWrapper}
import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{serializeDataType, supportedDataType}

/**
 * Serde implementation for hybrid JVM/native scans (CometScanExec with non-native scanImpl).
 * These scans run on the JVM but produce data that can be consumed by native operators.
 */
object CometHybridScanForScanExec extends CometOperatorSerde[CometScanExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = None

  override def getSupportLevel(scan: CometScanExec): SupportLevel = {
    // Only handle non-native DataFusion scans (native_comet, native_iceberg_compat, etc.)
    if (scan.scanImpl == CometConf.SCAN_NATIVE_DATAFUSION) {
      // Native DataFusion scans are handled by CometNativeScan
      return super.getSupportLevel(scan)
    }
    // This handles SCAN_NATIVE_COMET, SCAN_NATIVE_ICEBERG_COMPAT, etc.
    super.getSupportLevel(scan)
  }

  override def convert(
      scan: CometScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    val supportedTypes =
      scan.output.forall(a => supportedDataType(a.dataType, allowComplex = true))

    if (!supportedTypes) {
      withInfo(scan, "Unsupported data type")
      return None
    }

    // These operators are source of Comet native execution chain
    val scanBuilder = OperatorOuterClass.Scan.newBuilder()
    val source = scan.simpleStringWithNodeId()
    if (source.isEmpty) {
      scanBuilder.setSource(scan.getClass.getSimpleName)
    } else {
      scanBuilder.setSource(source)
    }

    val ffiSafe = scan.scanImpl match {
      case CometConf.SCAN_NATIVE_COMET =>
        // native_comet scan reuses mutable buffers
        false
      case CometConf.SCAN_NATIVE_ICEBERG_COMPAT =>
        // native_iceberg_compat scan reuses mutable buffers for constant columns
        // https://github.com/apache/datafusion-comet/issues/2152
        false
      case _ =>
        false
    }
    scanBuilder.setArrowFfiSafe(ffiSafe)

    val scanTypes = scan.output.flatMap { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == scan.output.length) {
      scanBuilder.addAllFields(scanTypes.asJava)

      // Sink operators don't have children
      builder.clearChildren()

      Some(builder.setScan(scanBuilder).build())
    } else {
      // There are unsupported scan type
      withInfo(
        scan,
        s"unsupported Comet operator: ${scan.nodeName}, due to unsupported data types above")
      None
    }
  }

  override def createExec(nativeOp: Operator, op: CometScanExec): CometNativeExec = {
    CometScanWrapper(nativeOp, op)
  }
}

/**
 * Serde implementation for hybrid JVM/native scans (CometBatchScanExec without native metadata).
 * These scans run on the JVM but produce data that can be consumed by native operators.
 */
object CometHybridScanForBatchScanExec extends CometOperatorSerde[org.apache.spark.sql.comet.CometBatchScanExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = None

  override def getSupportLevel(scan: org.apache.spark.sql.comet.CometBatchScanExec): SupportLevel = {
    // Only handle scans without native Iceberg metadata
    if (scan.nativeIcebergScanMetadata.isDefined) {
      // Native Iceberg scans are handled by CometIcebergNativeScan
      return super.getSupportLevel(scan)
    }
    super.getSupportLevel(scan)
  }

  override def convert(
      scan: org.apache.spark.sql.comet.CometBatchScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    val supportedTypes =
      scan.output.forall(a => supportedDataType(a.dataType, allowComplex = true))

    if (!supportedTypes) {
      withInfo(scan, "Unsupported data type")
      return None
    }

    // These operators are source of Comet native execution chain
    val scanBuilder = OperatorOuterClass.Scan.newBuilder()
    val source = scan.simpleStringWithNodeId()
    if (source.isEmpty) {
      scanBuilder.setSource(scan.getClass.getSimpleName)
    } else {
      scanBuilder.setSource(source)
    }

    // CometBatchScanExec is not FFI safe by default
    scanBuilder.setArrowFfiSafe(false)

    val scanTypes = scan.output.flatMap { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == scan.output.length) {
      scanBuilder.addAllFields(scanTypes.asJava)

      // Sink operators don't have children
      builder.clearChildren()

      Some(builder.setScan(scanBuilder).build())
    } else {
      // There are unsupported scan type
      withInfo(
        scan,
        s"unsupported Comet operator: ${scan.nodeName}, due to unsupported data types above")
      None
    }
  }

  override def createExec(nativeOp: Operator, op: org.apache.spark.sql.comet.CometBatchScanExec): CometNativeExec = {
    CometScanWrapper(nativeOp, op)
  }
}
