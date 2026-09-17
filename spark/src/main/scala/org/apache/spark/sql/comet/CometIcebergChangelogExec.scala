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

import java.lang.invoke.SerializedLambda

import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{DeserializeToObjectExec, MapPartitionsExec, SerializeFromObjectExec}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.CometOperatorSerde
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

/** Preserves the original Iceberg iterator as the fallback for its native replacement. */
case class IcebergChangelogExec(
    original: SerializeFromObjectExec,
    mode: Int,
    identifierIndices: Seq[Int],
    outputIndices: Seq[Int],
    child: SparkPlan)
    extends UnaryExecNode {
  override def output: Seq[Attribute] = original.output
  override def producedAttributes: AttributeSet = outputSet
  override def outputPartitioning: Partitioning =
    UnknownPartitioning(child.outputPartitioning.numPartitions)
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
  override protected def doExecute(): RDD[InternalRow] = {
    val mapper = original.child.asInstanceOf[MapPartitionsExec]
    val decoder = mapper.child.asInstanceOf[DeserializeToObjectExec]
    original.copy(child = mapper.copy(child = decoder.copy(child = child))).execute()
  }
}

object CometIcebergChangelogExec extends CometOperatorSerde[IcebergChangelogExec] {
  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_ICEBERG_CHANGELOG_ENABLED)
  override def requiresNativeChildren: Boolean = true

  // Match the procedure's serialized Java lambda, never an arbitrary mapPartitions function.
  // Unknown Spark/Iceberg closure layouts retain the original Spark iterator.
  private def lambda(function: AnyRef): Option[SerializedLambda] = {
    val method = function.getClass.getDeclaredMethod("writeReplace")
    method.setAccessible(true)
    method.invoke(function) match {
      case value: SerializedLambda => Some(value)
      case _ => None
    }
  }

  private def comparable(dt: DataType, nested: Boolean = false): Boolean = dt match {
    // Iceberg's external-row iterator uses reference equality for binary values in some
    // containers. Preserve that JVM behavior until a common value-equality contract exists.
    case BinaryType => false
    case FloatType | DoubleType if nested => false
    case StructType(fields) => fields.forall(f => comparable(f.dataType, nested = true))
    case ArrayType(element, _) => comparable(element, nested = true)
    case MapType(key, value, _) =>
      comparable(key, nested = true) && comparable(value, nested = true)
    case _ => true
  }

  def rewrite(plan: SparkPlan): SparkPlan = {
    if (!CometConf.COMET_ICEBERG_CHANGELOG_ENABLED.get() ||
      !CometConf.COMET_ICEBERG_NATIVE_ENABLED.get()) {
      return plan
    }
    plan.transformDown { case original: SerializeFromObjectExec =>
      val replacement =
        try {
          original.child match {
            case mapper: MapPartitionsExec =>
              mapper.child match {
                case decoder: DeserializeToObjectExec =>
                  for {
                    wrapper <- lambda(mapper.func.asInstanceOf[AnyRef])
                    if wrapper.getImplClass == "org/apache/spark/sql/internal/ToScalaUDF$"
                    if wrapper.getCapturedArgCount == 1
                    function <- lambda(wrapper.getCapturedArg(0))
                    if function.getImplClass ==
                      "org/apache/iceberg/spark/procedures/CreateChangelogViewProcedure"
                    arguments = (0 until function.getCapturedArgCount).map(
                      function.getCapturedArg)
                    schema <- arguments.collectFirst { case s: StructType => s }
                    if schema.fields.forall(f => comparable(f.dataType)) || {
                      withFallbackReason(
                        original,
                        "Iceberg changelog binary or nested floating comparison " +
                          "uses the JVM iterator")
                      false
                    }
                    if schema.fields.map(f => f.name -> f.dataType).toSeq ==
                      decoder.child.output.map(a => a.name -> a.dataType)
                    method = function.getImplMethodName
                    mode <-
                      if (method.startsWith("lambda$applyChangelogIterator$")) {
                        Some(1)
                      } else if (method.startsWith("lambda$applyCarryoverRemoveIterator$")) {
                        arguments.collectFirst { case b: java.lang.Boolean => if (b) 2 else 0 }
                      } else {
                        None
                      }
                    identifiers =
                      if (mode == 1) {
                        arguments.collectFirst { case a: Array[String] => a.toSeq }.get
                      } else {
                        Seq.empty[String]
                      }
                    indices = original.output.map(a => schema.fieldIndex(a.name))
                    if original.output.zip(indices).forall { case (a, i) =>
                      a.dataType == schema(i).dataType
                    }
                  } yield IcebergChangelogExec(
                    original,
                    mode,
                    identifiers.map(schema.fieldIndex),
                    indices,
                    decoder.child)
                case _ => None
              }
            case _ => None
          }
        } catch { case NonFatal(_) => None }
      replacement.getOrElse(original)
    }
  }

  override def convert(
      op: IcebergChangelogExec,
      builder: Operator.Builder,
      children: Operator*): Option[Operator] = {
    if (children.isEmpty) {
      withFallbackReason(op, "Native Iceberg changelog requires a native input")
      return None
    }
    val changelog = OperatorOuterClass.IcebergChangelog.newBuilder().setMode(op.mode)
    val names = op.child.output.map(_.name)
    changelog.setChangeTypeIndex(names.indexOf("_change_type"))
    changelog.setChangeOrdinalIndex(names.indexOf("_change_ordinal"))
    changelog.setCommitSnapshotIdIndex(names.indexOf("_commit_snapshot_id"))
    op.identifierIndices.foreach(changelog.addIdentifierIndices)
    op.outputIndices.foreach(changelog.addOutputIndices)
    Some(builder.setIcebergChangelog(changelog).build())
  }

  override def createExec(nativeOp: Operator, op: IcebergChangelogExec): CometNativeExec =
    CometIcebergChangelogExec(nativeOp, op, op.output, op.child, SerializedPlan(None))
}

case class CometIcebergChangelogExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {
  override def producedAttributes: AttributeSet = outputSet
  override def outputPartitioning: Partitioning =
    UnknownPartitioning(child.outputPartitioning.numPartitions)
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
