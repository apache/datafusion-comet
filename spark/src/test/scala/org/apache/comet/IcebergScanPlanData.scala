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

package org.apache.comet

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.comet.CometIcebergNativeScanExec

import org.apache.comet.serde.OperatorOuterClass.{IcebergPredicate, IcebergScan, IcebergScanCommon}

/**
 * Reads the plan data a native Iceberg scan ships, for tests. The common block holds no per-file
 * pool entries; each partition ships the ones its own tasks use.
 */
object IcebergScanPlanData {

  /** Each partition's message, as its task's injector reads it. */
  def partitionScans(scan: CometIcebergNativeScanExec): Seq[IcebergScan] =
    scan.perPartitionData.toSeq.map(data => IcebergScan.parseFrom(data.bytes))

  /** The pool entries a scan ships: the common block's, then each partition's own. */
  def storedPools(scan: CometIcebergNativeScanExec): Seq[IcebergScanCommon] =
    IcebergScanCommon.parseFrom(scan.commonData) +: partitionScans(scan).map(_.getCommon)

  /** The pools each partition's tasks index into once the injector merges them. */
  def partitionPools(scan: CometIcebergNativeScanExec): Seq[IcebergScanCommon] = {
    val common = IcebergScanCommon.parseFrom(scan.commonData)
    partitionScans(scan).map(partition => common.toBuilder.mergeFrom(partition.getCommon).build())
  }

  /** The residuals a scan ships, each counted once however many partitions use it. */
  def distinctResiduals(scan: CometIcebergNativeScanExec): Set[IcebergPredicate] =
    storedPools(scan).flatMap(_.getResidualPoolList.asScala).toSet

  /** The file scan tasks planned for the scan, over all partitions. */
  def fileScanTaskCount(scan: CometIcebergNativeScanExec): Int =
    partitionScans(scan).map(_.getFileScanTasksCount).sum
}
