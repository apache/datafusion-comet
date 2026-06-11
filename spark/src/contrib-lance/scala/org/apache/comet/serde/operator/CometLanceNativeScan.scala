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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.comet.{CometLanceNativeScanExec, CometNativeExec, SerializedPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.StructType

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.OperatorOuterClass.Operator

object CometLanceNativeScan extends CometOperatorSerde[BatchScanExec] with Logging {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_LANCE_NATIVE_ENABLED)

  override def getSupportLevel(operator: BatchScanExec): SupportLevel = Compatible()

  override def convert(
      scanExec: BatchScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] =
    convert(scanExec, builder, None)

  def convert(
      scanExec: BatchScanExec,
      builder: Operator.Builder,
      nativeScanPlan: Option[Any]): Option[Operator] = {
    val sourceKey = scanKey(scanExec)
    val requiredSchema = scanExec.scan.readSchema()
    val nativePlanClass = nativeScanPlan
      .map(_.getClass.getName)
      .getOrElse("")

    val commonBuilder = OperatorOuterClass.LanceScanCommon
      .newBuilder()
      .setScanId(sourceKey)
      .setNativeScanPlanClass(nativePlanClass)
      .addAllRequiredSchema(schema2Proto(requiredSchema.fields).toSeq.asJava)

    val lanceScanBuilder = OperatorOuterClass.LanceScan
      .newBuilder()
      .setCommon(commonBuilder.build())

    builder.clearChildren()
    Some(builder.setLanceScan(lanceScanBuilder).build())
  }

  override def createExec(nativeOp: Operator, op: BatchScanExec): CometNativeExec =
    createExec(nativeOp, op, None)

  def createExec(
      nativeOp: Operator,
      op: BatchScanExec,
      nativeScanPlan: Option[Any]): CometNativeExec = {
    val nativePlanClass = nativeScanPlan
      .map(_.getClass.getName)
      .getOrElse("")
    val exec = CometLanceNativeScanExec(
      nativeOp,
      op.output,
      op.runtimeFilters,
      op.scan.readSchema(),
      op,
      SerializedPlan(None),
      scanKey(op),
      nativePlanClass)
    op.logicalLink.foreach(exec.setLogicalLink)
    exec
  }

  def serializePartitions(
      sourceKey: String,
      requiredSchema: StructType,
      nativeScanPlanClassName: String): (Array[Byte], Array[Array[Byte]]) = {
    val common = OperatorOuterClass.LanceScanCommon
      .newBuilder()
      .setScanId(sourceKey)
      .setNativeScanPlanClass(nativeScanPlanClassName)
      .addAllRequiredSchema(schema2Proto(requiredSchema.fields).toSeq.asJava)
      .build()

    val partition = OperatorOuterClass.LanceScanPartition
      .newBuilder()
      .setPartitionIndex(0)
      .build()

    val partitionScan = OperatorOuterClass.LanceScan
      .newBuilder()
      .setPartition(partition)
      .build()

    (common.toByteArray, Array(partitionScan.toByteArray))
  }

  private def scanKey(scanExec: BatchScanExec): String =
    s"lance_${scanExec.id}_${scanExec.scan.hashCode()}"
}
