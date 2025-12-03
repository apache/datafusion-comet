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

import org.apache.spark.sql.comet.{CometNativeExec, CometScanExec, CometScanWrapper}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{Compatible, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Serde implementation for hybrid JVM/native scans (CometScanExec with non-native scanImpl).
 * These scans run on the JVM but produce data that can be consumed by native operators. Extends
 * CometSink since they follow the same pattern of creating a generic Scan protobuf.
 */
object CometHybridScanForScanExec extends CometSink[CometScanExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = None

  override def getSupportLevel(scan: CometScanExec): SupportLevel = {
    scan.scanImpl match {
      case CometConf.SCAN_NATIVE_COMET => Compatible()
      case CometConf.SCAN_NATIVE_ICEBERG_COMPAT => Compatible()
      case _ => Unsupported()
    }
  }

  override def isFfiSafe: Boolean = false

  override def createExec(nativeOp: Operator, op: CometScanExec): CometNativeExec = {
    CometScanWrapper(nativeOp, op)
  }
}

/**
 * Serde implementation for hybrid JVM/native scans (CometBatchScanExec without native metadata).
 * These scans run on the JVM but produce data that can be consumed by native operators. Extends
 * CometSink since they follow the same pattern of creating a generic Scan protobuf.
 */
object CometHybridScanForBatchScanExec
    extends CometSink[org.apache.spark.sql.comet.CometBatchScanExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = None

  override def getSupportLevel(
      scan: org.apache.spark.sql.comet.CometBatchScanExec): SupportLevel = {
    // Only handle scans without native Iceberg metadata
    if (scan.nativeIcebergScanMetadata.isDefined) {
      return Unsupported()
    }
    Compatible()
  }

  override def isFfiSafe: Boolean = false

  override def createExec(
      nativeOp: Operator,
      op: org.apache.spark.sql.comet.CometBatchScanExec): CometNativeExec = {
    CometScanWrapper(nativeOp, op)
  }
}
