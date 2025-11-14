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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression, SortOrder, WindowExpression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.window.WindowExec

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.{CometOperatorSerde, Incompatible, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, windowExprToProto}

object CometWindowExec extends CometOperatorSerde[WindowExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_WINDOW_ENABLED)

  override def getSupportLevel(op: WindowExec): SupportLevel = {
    Incompatible(Some("Native WindowExec has known correctness issues"))
  }

  override def convert(
      op: WindowExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    val output = op.child.output

    val winExprs: Array[WindowExpression] = op.windowExpression.flatMap { expr =>
      expr match {
        case alias: Alias =>
          alias.child match {
            case winExpr: WindowExpression =>
              Some(winExpr)
            case _ =>
              None
          }
        case _ =>
          None
      }
    }.toArray

    if (winExprs.length != op.windowExpression.length) {
      withInfo(op, "Unsupported window expression(s)")
      return None
    }

    if (op.partitionSpec.nonEmpty && op.orderSpec.nonEmpty &&
      !validatePartitionAndSortSpecsForWindowFunc(op.partitionSpec, op.orderSpec, op)) {
      return None
    }

    val windowExprProto = winExprs.map(windowExprToProto(_, output, op.conf))
    val partitionExprs = op.partitionSpec.map(exprToProto(_, op.child.output))

    val sortOrders = op.orderSpec.map(exprToProto(_, op.child.output))

    if (windowExprProto.forall(_.isDefined) && partitionExprs.forall(_.isDefined)
      && sortOrders.forall(_.isDefined)) {
      val windowBuilder = OperatorOuterClass.Window.newBuilder()
      windowBuilder.addAllWindowExpr(windowExprProto.map(_.get).toIterable.asJava)
      windowBuilder.addAllPartitionByList(partitionExprs.map(_.get).asJava)
      windowBuilder.addAllOrderByList(sortOrders.map(_.get).asJava)
      Some(builder.setWindow(windowBuilder).build())
    } else {
      None
    }

  }

  override def createExec(nativeOp: Operator, op: WindowExec): CometNativeExec = {
    CometWindowExec(
      nativeOp,
      op,
      op.output,
      op.windowExpression,
      op.partitionSpec,
      op.orderSpec,
      op.child,
      SerializedPlan(None))
  }

  private def validatePartitionAndSortSpecsForWindowFunc(
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      op: SparkPlan): Boolean = {
    if (partitionSpec.length != orderSpec.length) {
      return false
    }

    val partitionColumnNames = partitionSpec.collect {
      case a: AttributeReference => a.name
      case other =>
        withInfo(op, s"Unsupported partition expression: ${other.getClass.getSimpleName}")
        return false
    }

    val orderColumnNames = orderSpec.collect { case s: SortOrder =>
      s.child match {
        case a: AttributeReference => a.name
        case other =>
          withInfo(op, s"Unsupported sort expression: ${other.getClass.getSimpleName}")
          return false
      }
    }

    if (partitionColumnNames.zip(orderColumnNames).exists { case (partCol, orderCol) =>
        partCol != orderCol
      }) {
      withInfo(op, "Partitioning and sorting specifications must be the same.")
      return false
    }

    true
  }

}

/**
 * Comet physical plan node for Spark `WindowsExec`.
 *
 * It is used to execute a `WindowsExec` physical operator by using Comet native engine. It is not
 * like other physical plan nodes which are wrapped by `CometExec`, because it contains two native
 * executions separated by a Comet shuffle exchange.
 */
case class CometWindowExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def nodeName: String = "CometWindowExec"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions"))

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] =
    Iterator(output, windowExpression, partitionSpec, orderSpec, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometWindowExec =>
        this.output == other.output &&
        this.windowExpression == other.windowExpression && this.child == other.child &&
        this.partitionSpec == other.partitionSpec && this.orderSpec == other.orderSpec &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(output, windowExpression, partitionSpec, orderSpec, child)
}
