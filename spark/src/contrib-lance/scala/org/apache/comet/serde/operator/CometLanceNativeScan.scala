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

import org.apache.spark.sql.comet.{CometLanceNativeScanExec, CometNativeExec, SerializedPlan}
import org.apache.spark.sql.types.{DataType, StructType}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.lance.LanceScanExec
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.OperatorOuterClass.Operator

object CometLanceNativeScan extends CometOperatorSerde[LanceScanExec] {

  val LanceScanTypeUrl = "type.googleapis.com/comet.contrib.lance.LanceScan"

  case class LanceNativeScanSplitDescriptor(partitionIndex: Int, fragmentIds: Seq[Int])

  case class LanceNativeScanDescriptor(
      descriptorVersion: Int,
      scanId: String,
      datasetUri: String,
      resolvedVersion: Long,
      storageOptions: Map[String, String],
      requiredSchema: StructType,
      projectedSchema: StructType,
      filterSql: Option[String],
      limit: Option[Long],
      offset: Option[Long],
      batchSize: Int,
      nativeScanPlanClass: String,
      splits: Seq[LanceNativeScanSplitDescriptor])

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_LANCE_NATIVE_ENABLED)

  override def getSupportLevel(operator: LanceScanExec): SupportLevel = Compatible()

  override def convert(
      scanExec: LanceScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    val descriptor = descriptorFromNativePlan(scanExec.nativeScanPlan)
    val lanceScan = OperatorOuterClass.LanceScan
      .newBuilder()
      .setCommon(commonFromDescriptor(descriptor))
      .build()
    val contribScan = OperatorOuterClass.ContribScan
      .newBuilder()
      .setTypeUrl(LanceScanTypeUrl)
      .setValue(lanceScan.toByteString)

    builder.clearChildren()
    Some(builder.setContribScan(contribScan).build())
  }

  override def createExec(nativeOp: Operator, op: LanceScanExec): CometNativeExec = {
    val descriptor = descriptorFromNativePlan(op.nativeScanPlan)
    val scan = op.originalPlan
    val exec = CometLanceNativeScanExec(
      nativeOp,
      scan.output,
      scan.runtimeFilters,
      scan,
      SerializedPlan(None),
      descriptor.scanId,
      descriptor)
    scan.logicalLink.foreach(exec.setLogicalLink)
    exec
  }

  def serializePartitions(descriptor: LanceNativeScanDescriptor): (Array[Byte], Array[Array[Byte]]) =
    (
      commonFromDescriptor(descriptor).toByteArray,
      descriptor.splits.map { split =>
        val partition = OperatorOuterClass.LanceScanPartition
          .newBuilder()
          .setPartitionIndex(split.partitionIndex)
          .addAllFragmentIds(split.fragmentIds.map(Int.box).asJava)
          .build()

        OperatorOuterClass.LanceScan
          .newBuilder()
          .setPartition(partition)
          .build()
          .toByteArray
      }.toArray)

  private[comet] def serializeNativePlan(
      nativeScanPlan: Object): (Array[Byte], Array[Array[Byte]]) =
    serializePartitions(descriptorFromNativePlan(nativeScanPlan))

  private def descriptorFromNativePlan(
      nativeScanPlan: Object): LanceNativeScanDescriptor = {
    def invoke(methodName: String): AnyRef =
      nativeScanPlan.getClass.getMethod(methodName).invoke(nativeScanPlan)

    def int(methodName: String): Int =
      invoke(methodName).asInstanceOf[java.lang.Number].intValue()

    def long(methodName: String): Long =
      invoke(methodName).asInstanceOf[java.lang.Number].longValue()

    def optionalString(hasMethod: String, valueMethod: String): Option[String] =
      if (invoke(hasMethod).asInstanceOf[java.lang.Boolean].booleanValue()) {
        Some(invoke(valueMethod).asInstanceOf[String])
      } else None

    def optionalLong(hasMethod: String, valueMethod: String): Option[Long] =
      if (invoke(hasMethod).asInstanceOf[java.lang.Boolean].booleanValue()) {
        Some(long(valueMethod))
      } else None

    val requiredSchema =
      DataType
        .fromJson(invoke("getSparkReadSchemaJson").asInstanceOf[String])
        .asInstanceOf[StructType]
    val projectedSchema =
      DataType
        .fromJson(invoke("getProjectedReadSchemaJson").asInstanceOf[String])
        .asInstanceOf[StructType]
    val storageOptions = invoke("getStorageOptions")
      .asInstanceOf[java.util.Map[String, String]]
      .asScala
      .toMap
    val splits = invoke("getSplits")
      .asInstanceOf[java.lang.Iterable[Object]]
      .asScala
      .map { split =>
        val splitClass = split.getClass
        val fragmentIds = splitClass
          .getMethod("getFragmentIds")
          .invoke(split)
          .asInstanceOf[java.lang.Iterable[java.lang.Number]]
          .asScala
          .map(_.intValue())
          .toSeq
        LanceNativeScanSplitDescriptor(
          splitClass
            .getMethod("getSplitIndex")
            .invoke(split)
            .asInstanceOf[java.lang.Number]
            .intValue(),
          fragmentIds)
      }
      .toSeq

    LanceNativeScanDescriptor(
      descriptorVersion = int("getDescriptorVersion"),
      scanId = invoke("getScanId").asInstanceOf[String],
      datasetUri = invoke("getDatasetUri").asInstanceOf[String],
      resolvedVersion = long("getResolvedVersion"),
      storageOptions = storageOptions,
      requiredSchema = requiredSchema,
      projectedSchema = projectedSchema,
      filterSql = optionalString("hasPushedFilterSql", "getPushedFilterSql"),
      limit = optionalLong("hasLimit", "getLimit"),
      offset = optionalLong("hasOffset", "getOffset"),
      batchSize = int("getBatchSize"),
      nativeScanPlanClass = nativeScanPlan.getClass.getName,
      splits = splits)
  }

  private def commonFromDescriptor(
      descriptor: LanceNativeScanDescriptor): OperatorOuterClass.LanceScanCommon = {
    val commonBuilder = OperatorOuterClass.LanceScanCommon
      .newBuilder()
      .setScanId(descriptor.scanId)
      .setNativeScanPlanClass(descriptor.nativeScanPlanClass)
      .setDatasetUri(descriptor.datasetUri)
      .setResolvedVersion(descriptor.resolvedVersion)
      .putAllStorageOptions(descriptor.storageOptions.asJava)
      .addAllRequiredSchema(schema2Proto(descriptor.requiredSchema.fields.toIndexedSeq).asJava)
      .addAllProjectedSchema(schema2Proto(descriptor.projectedSchema.fields.toIndexedSeq).asJava)
      .setBatchSize(descriptor.batchSize)
      .setDescriptorVersion(descriptor.descriptorVersion)

    descriptor.filterSql.foreach(commonBuilder.setFilterSql)
    descriptor.limit.foreach(commonBuilder.setLimit)
    descriptor.offset.foreach(commonBuilder.setOffset)
    commonBuilder.build()
  }
}
