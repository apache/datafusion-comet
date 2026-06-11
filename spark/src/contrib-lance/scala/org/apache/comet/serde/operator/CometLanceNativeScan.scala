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

import java.lang.reflect.InvocationTargetException

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.comet.{CometLanceNativeScanExec, CometNativeExec, SerializedPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.{DataType, StructType}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.OperatorOuterClass.Operator

object CometLanceNativeScan extends CometOperatorSerde[BatchScanExec] with Logging {

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
    val descriptor = descriptorFor(scanExec, nativeScanPlan)

    val lanceScanBuilder = OperatorOuterClass.LanceScan
      .newBuilder()
      .setCommon(commonFromDescriptor(descriptor))

    builder.clearChildren()
    Some(builder.setLanceScan(lanceScanBuilder).build())
  }

  override def createExec(nativeOp: Operator, op: BatchScanExec): CometNativeExec =
    createExec(nativeOp, op, None)

  def createExec(
      nativeOp: Operator,
      op: BatchScanExec,
      nativeScanPlan: Option[Any]): CometNativeExec = {
    val descriptor = descriptorFor(op, nativeScanPlan)
    val exec = CometLanceNativeScanExec(
      nativeOp,
      op.output,
      op.runtimeFilters,
      op,
      SerializedPlan(None),
      descriptor.scanId,
      descriptor)
    op.logicalLink.foreach(exec.setLogicalLink)
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
      nativeScanPlan: Any,
      fallbackScanId: String,
      fallbackRequiredSchema: StructType): (Array[Byte], Array[Array[Byte]]) = {
    serializePartitions(descriptorFromNativePlan(
      nativeScanPlan,
      fallbackScanId,
      fallbackRequiredSchema))
  }

  private def descriptorFor(
      scanExec: BatchScanExec,
      nativeScanPlan: Option[Any]): LanceNativeScanDescriptor = {
    val fallbackScanId = scanKey(scanExec)
    val fallbackRequiredSchema = scanExec.scan.readSchema()
    nativeScanPlan
      .map(descriptorFromNativePlan(_, fallbackScanId, fallbackRequiredSchema))
      .getOrElse(fallbackDescriptor(fallbackScanId, fallbackRequiredSchema))
  }

  private def descriptorFromNativePlan(
      nativeScanPlan: Any,
      fallbackScanId: String,
      fallbackRequiredSchema: StructType): LanceNativeScanDescriptor = {
    val requiredSchema =
      structTypeFromJson(
        requireString(invokeRequired(nativeScanPlan, "getSparkReadSchemaJson")),
        "getSparkReadSchemaJson")
    val projectedSchema =
      structTypeFromJson(
        requireString(invokeRequired(nativeScanPlan, "getProjectedReadSchemaJson")),
        "getProjectedReadSchemaJson")

    LanceNativeScanDescriptor(
      descriptorVersion = toUInt32(
        invokeRequired(nativeScanPlan, "getDescriptorVersion"),
        "getDescriptorVersion"),
      scanId = nonEmptyString(
        invokeRequired(nativeScanPlan, "getScanId"),
        fallbackScanId),
      datasetUri = requireString(invokeRequired(nativeScanPlan, "getDatasetUri")),
      resolvedVersion = toLong(invokeRequired(nativeScanPlan, "getResolvedVersion")),
      storageOptions = toStringMap(invokeRequired(nativeScanPlan, "getStorageOptions")),
      requiredSchema = requiredSchema,
      projectedSchema = projectedSchema,
      filterSql = optionalString(nativeScanPlan, "hasPushedFilterSql", "getPushedFilterSql"),
      limit = optionalLong(nativeScanPlan, "hasLimit", "getLimit"),
      offset = optionalLong(nativeScanPlan, "hasOffset", "getOffset"),
      batchSize = toUInt32(invokeRequired(nativeScanPlan, "getBatchSize"), "getBatchSize"),
      nativeScanPlanClass = nativeScanPlan.getClass.getName,
      splits = toSeq(invokeRequired(nativeScanPlan, "getSplits")).map(splitFromNativeSplit))
  }

  private def fallbackDescriptor(
      scanId: String,
      requiredSchema: StructType): LanceNativeScanDescriptor =
    LanceNativeScanDescriptor(
      descriptorVersion = 0,
      scanId = scanId,
      datasetUri = "",
      resolvedVersion = 0L,
      storageOptions = Map.empty,
      requiredSchema = requiredSchema,
      projectedSchema = requiredSchema,
      filterSql = None,
      limit = None,
      offset = None,
      batchSize = 0,
      nativeScanPlanClass = "",
      splits = Seq(LanceNativeScanSplitDescriptor(0, Nil)))

  private def commonFromDescriptor(
      descriptor: LanceNativeScanDescriptor): OperatorOuterClass.LanceScanCommon = {
    val commonBuilder = OperatorOuterClass.LanceScanCommon
      .newBuilder()
      .setScanId(descriptor.scanId)
      .setNativeScanPlanClass(descriptor.nativeScanPlanClass)
      .setDatasetUri(descriptor.datasetUri)
      .setResolvedVersion(descriptor.resolvedVersion)
      .putAllStorageOptions(descriptor.storageOptions.asJava)
      .addAllRequiredSchema(schema2Proto(descriptor.requiredSchema.fields).toSeq.asJava)
      .addAllProjectedSchema(schema2Proto(descriptor.projectedSchema.fields).toSeq.asJava)
      .setBatchSize(descriptor.batchSize)
      .setDescriptorVersion(descriptor.descriptorVersion)

    descriptor.filterSql.foreach(commonBuilder.setFilterSql)
    descriptor.limit.foreach(commonBuilder.setLimit)
    descriptor.offset.foreach(commonBuilder.setOffset)
    commonBuilder.build()
  }

  private def splitFromNativeSplit(nativeSplit: Any): LanceNativeScanSplitDescriptor =
    LanceNativeScanSplitDescriptor(
      partitionIndex = toUInt32(invokeRequired(nativeSplit, "getSplitIndex"), "getSplitIndex"),
      fragmentIds = toSeq(invokeRequired(nativeSplit, "getFragmentIds"))
        .map(toUInt32(_, "getFragmentIds")))

  private def structTypeFromJson(json: String, methodName: String): StructType =
    try {
      DataType.fromJson(json) match {
        case schema: StructType => schema
        case other =>
          throw new IllegalArgumentException(
            s"expected StructType JSON but got ${other.typeName}")
      }
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Native Lance scan descriptor method $methodName returned invalid Spark schema JSON",
          e)
    }

  private def optionalString(target: Any, hasMethod: String, valueMethod: String): Option[String] =
    if (toBoolean(invokeRequired(target, hasMethod))) {
      Some(requireString(invokeRequired(target, valueMethod)))
    } else {
      None
    }

  private def optionalLong(target: Any, hasMethod: String, valueMethod: String): Option[Long] =
    if (toBoolean(invokeRequired(target, hasMethod))) {
      Some(toLong(invokeRequired(target, valueMethod)))
    } else {
      None
    }

  private def invokeRequired(target: Any, methodName: String): Any = {
    require(target != null, s"Native Lance scan descriptor target is null for $methodName")
    try {
      findNoArgMethod(target.getClass, methodName)
        .getOrElse {
          throw new NoSuchMethodException(s"${target.getClass.getName}.$methodName()")
        }
        .invoke(target.asInstanceOf[AnyRef])
    } catch {
      case e: InvocationTargetException if e.getCause != null =>
        throw e.getCause
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Unable to read native Lance scan descriptor method $methodName",
          e)
    }
  }

  private def findNoArgMethod(
      clazz: Class[_],
      methodName: String): Option[java.lang.reflect.Method] = {
    var current = clazz
    while (current != null) {
      try {
        val method = current.getDeclaredMethod(methodName)
        method.setAccessible(true)
        return Some(method)
      } catch {
        case _: NoSuchMethodException =>
          current = current.getSuperclass
      }
    }
    None
  }

  private def toSeq(value: Any): Seq[Any] = value match {
    case null => Seq.empty
    case values: java.lang.Iterable[_] => values.asScala.toSeq
    case values: Iterable[_] => values.toSeq
    case values: Array[_] => values.toSeq
    case other =>
      throw new IllegalArgumentException(
        s"Expected a collection in native Lance scan descriptor, got ${other.getClass.getName}")
  }

  private def toStringMap(value: Any): Map[String, String] = value match {
    case null => Map.empty
    case values: java.util.Map[_, _] =>
      values.asScala.map { case (key, value) => key.toString -> value.toString }.toMap
    case values: collection.Map[_, _] =>
      values.map { case (key, value) => key.toString -> value.toString }.toMap
    case other =>
      throw new IllegalArgumentException(
        s"Expected a map in native Lance scan descriptor, got ${other.getClass.getName}")
  }

  private def toBoolean(value: Any): Boolean = value match {
    case value: java.lang.Boolean => value.booleanValue()
    case value: Boolean => value
    case other =>
      throw new IllegalArgumentException(
        s"Expected boolean in native Lance scan descriptor, got ${typeName(other)}")
  }

  private def toLong(value: Any): Long = value match {
    case value: java.lang.Number => value.longValue()
    case value: String => value.toLong
    case other =>
      throw new IllegalArgumentException(
        s"Expected integer in native Lance scan descriptor, got ${typeName(other)}")
  }

  private def toUInt32(value: Any, methodName: String): Int = {
    val longValue = toLong(value)
    if (longValue < 0 || longValue > 0xffffffffL) {
      throw new IllegalArgumentException(
        s"Native Lance scan descriptor method $methodName returned out-of-range uint32 " +
          s"value $longValue")
    }
    longValue.toInt
  }

  private def requireString(value: Any): String = value match {
    case null => ""
    case value: String => value
    case other => other.toString
  }

  private def nonEmptyString(value: Any, fallback: String): String = {
    val stringValue = requireString(value)
    if (stringValue.nonEmpty) stringValue else fallback
  }

  private def typeName(value: Any): String =
    Option(value).map(_.getClass.getName).getOrElse("null")

  private def scanKey(scanExec: BatchScanExec): String =
    s"lance_${scanExec.id}_${scanExec.scan.hashCode()}"
}
