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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CurrentRow, Expression, NamedExpression, RangeFrame, RowFrame, SortOrder, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.{AggSerde, CometOperatorSerde, Incompatible, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{aggExprToProto, exprToProto}

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

  private def windowExprToProto(
      windowExpr: WindowExpression,
      output: Seq[Attribute],
      conf: SQLConf): Option[OperatorOuterClass.WindowExpr] = {

    val aggregateExpressions: Array[AggregateExpression] = windowExpr.flatMap { expr =>
      expr match {
        case agg: AggregateExpression =>
          agg.aggregateFunction match {
            case _: Count =>
              Some(agg)
            case min: Min =>
              if (AggSerde.minMaxDataTypeSupported(min.dataType)) {
                Some(agg)
              } else {
                withInfo(windowExpr, s"datatype ${min.dataType} is not supported", expr)
                None
              }
            case max: Max =>
              if (AggSerde.minMaxDataTypeSupported(max.dataType)) {
                Some(agg)
              } else {
                withInfo(windowExpr, s"datatype ${max.dataType} is not supported", expr)
                None
              }
            case s: Sum =>
              if (AggSerde.sumDataTypeSupported(s.dataType) && !s.dataType
                  .isInstanceOf[DecimalType]) {
                Some(agg)
              } else {
                withInfo(windowExpr, s"datatype ${s.dataType} is not supported", expr)
                None
              }
            case _ =>
              withInfo(
                windowExpr,
                s"aggregate ${agg.aggregateFunction}" +
                  " is not supported for window function",
                expr)
              None
          }
        case _ =>
          None
      }
    }.toArray

    val (aggExpr, builtinFunc) = if (aggregateExpressions.nonEmpty) {
      val modes = aggregateExpressions.map(_.mode).distinct
      assert(modes.size == 1 && modes.head == Complete)
      (aggExprToProto(aggregateExpressions.head, output, true, conf), None)
    } else {
      (None, exprToProto(windowExpr.windowFunction, output))
    }

    if (aggExpr.isEmpty && builtinFunc.isEmpty) {
      return None
    }

    val f = windowExpr.windowSpec.frameSpecification

    val (frameType, lowerBound, upperBound) = f match {
      case SpecifiedWindowFrame(frameType, lBound, uBound) =>
        val frameProto = frameType match {
          case RowFrame => OperatorOuterClass.WindowFrameType.Rows
          case RangeFrame => OperatorOuterClass.WindowFrameType.Range
        }

        val lBoundProto = lBound match {
          case UnboundedPreceding =>
            OperatorOuterClass.LowerWindowFrameBound
              .newBuilder()
              .setUnboundedPreceding(OperatorOuterClass.UnboundedPreceding.newBuilder().build())
              .build()
          case CurrentRow =>
            OperatorOuterClass.LowerWindowFrameBound
              .newBuilder()
              .setCurrentRow(OperatorOuterClass.CurrentRow.newBuilder().build())
              .build()
          case e if frameType == RowFrame =>
            val offset = e.eval() match {
              case i: Integer => i.toLong
              case l: Long => l
              case _ => return None
            }
            OperatorOuterClass.LowerWindowFrameBound
              .newBuilder()
              .setPreceding(
                OperatorOuterClass.Preceding
                  .newBuilder()
                  .setOffset(offset)
                  .build())
              .build()
          case _ =>
            // TODO add support for numeric and temporal RANGE BETWEEN expressions
            // see https://github.com/apache/datafusion-comet/issues/1246
            return None
        }

        val uBoundProto = uBound match {
          case UnboundedFollowing =>
            OperatorOuterClass.UpperWindowFrameBound
              .newBuilder()
              .setUnboundedFollowing(OperatorOuterClass.UnboundedFollowing.newBuilder().build())
              .build()
          case CurrentRow =>
            OperatorOuterClass.UpperWindowFrameBound
              .newBuilder()
              .setCurrentRow(OperatorOuterClass.CurrentRow.newBuilder().build())
              .build()
          case e if frameType == RowFrame =>
            val offset = e.eval() match {
              case i: Integer => i.toLong
              case l: Long => l
              case _ => return None
            }
            OperatorOuterClass.UpperWindowFrameBound
              .newBuilder()
              .setFollowing(
                OperatorOuterClass.Following
                  .newBuilder()
                  .setOffset(offset)
                  .build())
              .build()
          case _ =>
            // TODO add support for numeric and temporal RANGE BETWEEN expressions
            // see https://github.com/apache/datafusion-comet/issues/1246
            return None
        }

        (frameProto, lBoundProto, uBoundProto)
      case _ =>
        (
          OperatorOuterClass.WindowFrameType.Rows,
          OperatorOuterClass.LowerWindowFrameBound
            .newBuilder()
            .setUnboundedPreceding(OperatorOuterClass.UnboundedPreceding.newBuilder().build())
            .build(),
          OperatorOuterClass.UpperWindowFrameBound
            .newBuilder()
            .setUnboundedFollowing(OperatorOuterClass.UnboundedFollowing.newBuilder().build())
            .build())
    }

    val frame = OperatorOuterClass.WindowFrame
      .newBuilder()
      .setFrameType(frameType)
      .setLowerBound(lowerBound)
      .setUpperBound(upperBound)
      .build()

    val spec =
      OperatorOuterClass.WindowSpecDefinition.newBuilder().setFrameSpecification(frame).build()

    if (builtinFunc.isDefined) {
      Some(
        OperatorOuterClass.WindowExpr
          .newBuilder()
          .setBuiltInWindowFunction(builtinFunc.get)
          .setSpec(spec)
          .build())
    } else if (aggExpr.isDefined) {
      Some(
        OperatorOuterClass.WindowExpr
          .newBuilder()
          .setAggFunc(aggExpr.get)
          .setSpec(spec)
          .build())
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
