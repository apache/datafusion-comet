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

import scala.jdk.CollectionConverters._

import org.apache.comet.contrib.delta.DeltaSparkScanEnvelope
import org.apache.comet.serde.{OperatorOuterClass, QueryContextInterner}
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * PlanDataInjector for the Delta contrib scan, discovered by core's ServiceLoader (see the
 * `META-INF/services` resource). Lives in this package because [[PlanDataInjector]] is
 * `private[comet]`.
 */
class DeltaPlanDataInjector extends PlanDataInjector {

  override val opStructCase: Operator.OpStructCase = Operator.OpStructCase.CONTRIB_SCAN

  override def canInject(op: Operator): Boolean =
    DeltaSparkScanEnvelope.matches(op) && {
      val scan = DeltaSparkScanEnvelope.unpack(op)
      scan.hasCommon && !scan.hasFilePartition
    }

  override def getKey(op: Operator): Option[String] =
    Some(DeltaSparkScanEnvelope.unpack(op).getDeltaCommon.getSourceKey)

  override def inject(
      op: Operator,
      commonBytes: Array[Byte],
      partitionBytes: Array[Byte]): Operator = {
    // commonBytes is a DeltaSparkScan proto carrying common + delta_common (no file partition);
    // partitionBytes is a DeltaSparkScan proto carrying only this partition's file list.
    val common = OperatorOuterClass.DeltaSparkScan.parseFrom(commonBytes)
    val partitionOnly = OperatorOuterClass.DeltaSparkScan.parseFrom(partitionBytes)

    val scanBuilder = OperatorOuterClass.DeltaSparkScan
      .newBuilder()
      .setCommon(common.getCommon)
      .setDeltaCommon(common.getDeltaCommon)
      .setFilePartition(partitionOnly.getFilePartition)

    op.toBuilder.setContribScan(DeltaSparkScanEnvelope.pack(scanBuilder.build())).build()
  }
}

object DeltaPlanDataInjector {

  /**
   * The key under which a Delta scan's planning data is stored and looked up. Written into
   * `DeltaSparkScanCommon.source_key` on the driver and read back by
   * [[DeltaPlanDataInjector.getKey]] on the executor, so both sides agree by construction.
   * Mirrors `NativeScanPlanDataInjector.sourceKey` (source string carries the plan node id, so
   * two scans of the same table in one plan, self-join, MERGE, get distinct keys), plus the table
   * root for extra safety across tables with identical projections.
   */
  def sourceKey(tableRoot: String, common: OperatorOuterClass.NativeScanCommon): String = {
    val dataFilters = common.getDataFiltersList.asScala
      .map(QueryContextInterner.stripQueryContexts(_).toString)
    val keyComponents = Seq(
      tableRoot,
      common.getRequiredSchemaList.toString,
      dataFilters.mkString("[", ", ", "]"),
      common.getProjectionVectorList.toString,
      common.getFieldsList.toString)
    s"delta_${common.getSource}_${keyComponents.mkString("|").hashCode}"
  }
}
