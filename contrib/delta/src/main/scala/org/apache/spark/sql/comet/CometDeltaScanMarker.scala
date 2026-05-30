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

package org.apache.spark.sql.comet

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.contrib.delta.DeltaScanMetadata

/**
 * Planning-time marker the Delta contrib's `DeltaScanRule` produces for a Delta scan it can
 * accelerate. Mirrors how Iceberg uses `CometBatchScanExec` with `nativeIcebergScanMetadata`:
 *
 *   - it wraps the ORIGINAL, untouched `FileSourceScanExec` (so its planning `logicalLink` is
 *     intact -- no rebuild, no AQE `setLogicalLinkForNewQueryStage` workaround), and
 *   - it carries the Delta-specific planning info as a [[DeltaScanMetadata]] FIELD (which survives
 *     node copies/AQE re-planning, unlike a `TreeNodeTag`), instead of mutating the scan's schema
 *     or smuggling it through `relation.options`.
 *
 * `CometExecRule` detects it by type (via `DeltaIntegration.isDeltaScanMarker`) and converts it to a
 * `CometDeltaNativeScanExec` through the contrib serde. If conversion declines, the marker executes
 * by delegating to the wrapped scan (i.e. vanilla Spark Delta read), so leaving it in the plan is
 * safe.
 *
 * The accessors mirror the `FileSourceScanExec`/`CometScanExec` surface the serde reads
 * (`relation`, `requiredSchema`, `partitionFilters`, `output`, `wrapped`) so the serde body is
 * unchanged apart from reading metadata from [[deltaMetadata]].
 */
case class CometDeltaScanMarker(originalScan: FileSourceScanExec, deltaMetadata: DeltaScanMetadata)
    extends LeafExecNode {

  override def output: Seq[Attribute] = originalScan.output

  def relation: HadoopFsRelation = originalScan.relation

  def requiredSchema: StructType = originalScan.requiredSchema

  def partitionFilters: Seq[Expression] = originalScan.partitionFilters

  def dataFilters: Seq[Expression] = originalScan.dataFilters

  /**
   * Data filters Comet can push down -- delegated to `CometScanExec`'s logic (drops dynamic-pruning
   * and array null-check filters). A transient `CometScanExec` is built just to reuse that lazy val.
   */
  def supportedDataFilters: Seq[Expression] =
    CometScanExec(originalScan, originalScan.relation.sparkSession).supportedDataFilters

  /** The original scan; used as the produced exec's `originalPlan` (retains the logicalLink). */
  def wrapped: FileSourceScanExec = originalScan

  override def supportsColumnar: Boolean = originalScan.supportsColumnar

  // The marker is normally converted by CometExecRule before execution. If conversion declines and
  // the marker is left in the plan, fall back to the wrapped (vanilla Spark) Delta scan.
  override protected def doExecute(): RDD[InternalRow] = originalScan.execute()

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = originalScan.executeColumnar()
}
