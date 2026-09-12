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

package org.apache.comet.lance

import scala.collection.mutable.ListBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.comet.CometBatchScanExec
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometSparkSessionExtensions.withFallbackReasons
import org.apache.comet.rules.CometContribScanMarker
import org.apache.comet.serde.CometOperatorSerde
import org.apache.comet.serde.operator.CometLanceNativeScan

object CometLanceSupport {

  def tryTransform(scanExec: BatchScanExec, nativeScanPlan: Object): Option[SparkPlan] = {
    val fallbackReasons = new ListBuffer[String]()
    val schemaSupported =
      CometBatchScanExec.isSchemaSupported(scanExec.scan.readSchema(), fallbackReasons)

    if (!schemaSupported) {
      fallbackReasons += s"Schema ${scanExec.scan.readSchema()} is not supported"
      withFallbackReasons(scanExec, fallbackReasons.toSet)
      None
    } else {
      val marker = LanceScanExec(scanExec, nativeScanPlan)
      scanExec.logicalLink.foreach(marker.setLogicalLink)
      Some(marker)
    }
  }
}

case class LanceScanExec(originalPlan: BatchScanExec, nativeScanPlan: Object)
    extends LeafExecNode
    with CometContribScanMarker {

  override def scanHandler: CometOperatorSerde[_ <: SparkPlan] = CometLanceNativeScan

  override def output: Seq[Attribute] = originalPlan.output

  override def outputPartitioning: Partitioning = originalPlan.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = originalPlan.outputOrdering

  override def supportsColumnar: Boolean = originalPlan.supportsColumnar

  override protected def doExecute(): RDD[InternalRow] = originalPlan.execute()

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = originalPlan.executeColumnar()
}
