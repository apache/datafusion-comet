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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, CumeDist, CurrentRow, DenseRank, Expression, Lag, Lead, Literal, MakeDecimal, NamedExpression, NthValue, NTile, PercentRank, RangeFrame, Rank, RowFrame, RowNumber, SortOrder, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Complete, Count, First, Last, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, NumericType}
import org.apache.spark.sql.types.Decimal

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.{AggSerde, CometOperatorSerde, Incompatible, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{aggExprToProto, exprToProto, scalarFunctionExprToProto, serializeDataType}

object CometWindowExec extends CometOperatorSerde[WindowExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_WINDOW_ENABLED)

  override def convert(
      op: WindowExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    val output = op.child.output

    val winExprs: Array[WindowExpression] = op.windowExpression.map {
      case Alias(w: WindowExpression, _) => w
      case Alias(MakeDecimal(w: WindowExpression, _, _, _), _) => w
      case other =>
        withInfo(op, s"Unsupported window expression: $other", other)
        return None
    }.toArray

    if (winExprs.length != op.windowExpression.length) {
      withFallbackReason(op, "Unsupported window expression(s)")
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
      // Roll up reasons already attached to per-expression nodes so the Window
      // operator itself carries a fallback attribution. Without this, the plan
      // prints a bare `Window` and the real reason lives on a sub-expression
      // that isn't obvious in the standard explain output.
      val failing = winExprs.toSeq.zip(windowExprProto).collect { case (we, None) => we } ++
        op.partitionSpec.zip(partitionExprs).collect { case (e, None) => e } ++
        op.orderSpec.zip(sortOrders).collect { case (e, None) => e }
      withInfo(op, failing: _*)
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
                withFallbackReason(windowExpr, s"datatype ${min.dataType} is not supported", expr)
                None
              }
            case max: Max =>
              if (AggSerde.minMaxDataTypeSupported(max.dataType)) {
                Some(agg)
              } else {
                withFallbackReason(windowExpr, s"datatype ${max.dataType} is not supported", expr)
                None
              }
            case s: Sum =>
              if (AggSerde.sumDataTypeSupported(s.dataType)) {
                Some(agg)
              } else {
                withFallbackReason(windowExpr, s"datatype ${s.dataType} is not supported", expr)
                None
              }
            case a: Average =>
              if (AggSerde.avgDataTypeSupported(a.dataType)) {
                Some(agg)
              } else {
                withInfo(windowExpr, s"datatype ${a.dataType} is not supported", expr)
                None
              }
            case _: First =>
              Some(agg)
            case _: Last =>
              Some(agg)
            case _ =>
              withFallbackReason(
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

    // If the window function is itself an (unsupported) AggregateExpression the
    // filter above already recorded a specific reason on `windowExpr`. Short-circuit
    // here to avoid the fallthrough `exprToProto` path tagging an additional generic
    // "aggregateexpression is not supported" message.
    if (aggregateExpressions.isEmpty &&
      windowExpr.windowFunction.isInstanceOf[AggregateExpression]) {
      return None
    }

    val (aggExpr, builtinFunc, ignoreNulls) = if (aggregateExpressions.nonEmpty) {
      val modes = aggregateExpressions.map(_.mode).distinct
      assert(modes.size == 1 && modes.head == Complete)
      val agg = aggregateExpressions.head
      val ignoreNulls = agg.aggregateFunction match {
        case f: First => f.ignoreNulls
        case l: Last => l.ignoreNulls
        case _ => false
      }
      (aggExprToProto(agg, output, true, conf), None, ignoreNulls)
    } else {
      windowExpr.windowFunction match {
        case lag: Lag =>
          val inputExpr = exprToProto(lag.input, output)
          val offsetExpr = exprToProto(lag.inputOffset, output)
          val defaultExpr = exprToProto(lag.default, output)
          val func = scalarFunctionExprToProto("lag", inputExpr, offsetExpr, defaultExpr)
          (None, func, lag.ignoreNulls)
        case lead: Lead =>
          val inputExpr = exprToProto(lead.input, output)
          val offsetExpr = exprToProto(lead.offset, output)
          val defaultExpr = exprToProto(lead.default, output)
          val func = scalarFunctionExprToProto("lead", inputExpr, offsetExpr, defaultExpr)
          (None, func, lead.ignoreNulls)
        case _: RowNumber =>
          (None, scalarFunctionExprToProto("row_number"), false)
        case _: Rank =>
          (None, scalarFunctionExprToProto("rank"), false)
        case _: DenseRank =>
          (None, scalarFunctionExprToProto("dense_rank"), false)
        case _: PercentRank =>
          (None, scalarFunctionExprToProto("percent_rank"), false)
        case _: CumeDist =>
          (None, scalarFunctionExprToProto("cume_dist"), false)
        case nt: NTile =>
          // Known correctness bug: Comet's NTILE produces different bucket
          // assignments than Spark; tracked in #4255. Fall back to Spark.
          withInfo(windowExpr, "NTILE has a correctness bug in Comet tracked in #4255", nt)
          (None, None, false)
        case nv: NthValue =>
          val inputExpr = exprToProto(nv.input, output)

          val offsetExpr = nv.offset.eval() match {
            case n: Number =>
              exprToProto(Literal(n.longValue(), LongType), output)
            case _ =>
              withInfo(
                windowExpr,
                s"Unsupported NTH_VALUE offset: ${nv.offset} (${nv.offset.dataType})")
              None
          }
          val func = scalarFunctionExprToProto("nth_value", inputExpr, offsetExpr)
          (None, func, nv.ignoreNulls)

        case other =>
          withInfo(
            windowExpr,
            s"window function ${other.getClass.getSimpleName} is not supported",
            other)
          (None, None, false)
      }
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
              case _ =>
                withInfo(windowExpr, s"Unsupported ROWS frame lower offset: $e (${e.dataType})")
                return None
            }
            OperatorOuterClass.LowerWindowFrameBound
              .newBuilder()
              .setPreceding(
                OperatorOuterClass.Preceding
                  .newBuilder()
                  .setOffset(offset)
                  .build())
              .build()
          case e if frameType == RangeFrame && e.dataType.isInstanceOf[NumericType] =>
            rangeBoundLiteral(e, isLower = true, output) match {
              case Some(lit) =>
                OperatorOuterClass.LowerWindowFrameBound
                  .newBuilder()
                  .setPreceding(
                    OperatorOuterClass.Preceding
                      .newBuilder()
                      .setRangeOffset(lit)
                      .build())
                  .build()
              case None =>
                withInfo(windowExpr, s"Unsupported RANGE frame lower offset: $e")
                return None
            }
          case e =>
            withInfo(
              windowExpr,
              s"RANGE frame with non-numeric offset is not supported: ${e.dataType}")
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
              case _ =>
                withInfo(windowExpr, s"Unsupported ROWS frame upper offset: $e (${e.dataType})")
                return None
            }
            OperatorOuterClass.UpperWindowFrameBound
              .newBuilder()
              .setFollowing(
                OperatorOuterClass.Following
                  .newBuilder()
                  .setOffset(offset)
                  .build())
              .build()
          case e if frameType == RangeFrame && e.dataType.isInstanceOf[NumericType] =>
            rangeBoundLiteral(e, isLower = false, output) match {
              case Some(lit) =>
                OperatorOuterClass.UpperWindowFrameBound
                  .newBuilder()
                  .setFollowing(
                    OperatorOuterClass.Following
                      .newBuilder()
                      .setRangeOffset(lit)
                      .build())
                  .build()
              case None =>
                withInfo(windowExpr, s"Unsupported RANGE frame upper offset: $e")
                return None
            }
          case e =>
            withInfo(
              windowExpr,
              s"RANGE frame with non-numeric offset is not supported: ${e.dataType}")
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

    val resultTypeProto = serializeDataType(windowExpr.dataType)

    if (builtinFunc.isDefined) {
      val b = OperatorOuterClass.WindowExpr
        .newBuilder()
        .setBuiltInWindowFunction(builtinFunc.get)
        .setSpec(spec)
        .setIgnoreNulls(ignoreNulls)
      resultTypeProto.foreach(b.setResultType)
      Some(b.build())
    } else if (aggExpr.isDefined) {
      val b = OperatorOuterClass.WindowExpr
        .newBuilder()
        .setAggFunc(aggExpr.get)
        .setSpec(spec)
        .setIgnoreNulls(ignoreNulls)
      resultTypeProto.foreach(b.setResultType)
      Some(b.build())
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

  // Folds a RANGE frame bound expression to a constant and serializes its
  // magnitude as a typed Literal proto. Spark encodes PRECEDING/FOLLOWING via
  // the sign of the literal (negative => PRECEDING, positive => FOLLOWING),
  // but the proto only carries magnitude with direction implied by Lower vs
  // Upper position. So we reject lower=positive (FOLLOWING) and upper=negative
  // (PRECEDING) by returning None.
  private def rangeBoundLiteral(
      bound: Expression,
      isLower: Boolean,
      output: Seq[Attribute]): Option[LiteralOuterClass.Literal] = {
    val rawValue =
      try {
        bound.eval()
      } catch {
        case _: Exception => return None
      }
    if (rawValue == null) {
      return None
    }
    // Taking the absolute value of the narrow signed MIN_VALUE constants
    // overflows silently (Math.abs(Byte.MinValue).toByte == Byte.MinValue),
    // which would flip the sign and produce an incorrect frame bound. Reject
    // those pathological inputs up front.
    val (signum, absValue): (Int, Any) = rawValue match {
      case b: java.lang.Byte =>
        if (b.byteValue() == Byte.MinValue) return None
        (Integer.signum(b.intValue()), java.lang.Byte.valueOf(Math.abs(b.intValue()).toByte))
      case s: java.lang.Short =>
        if (s.shortValue() == Short.MinValue) return None
        (Integer.signum(s.intValue()), java.lang.Short.valueOf(Math.abs(s.intValue()).toShort))
      case i: java.lang.Integer =>
        if (i.intValue() == Int.MinValue) return None
        (Integer.signum(i.intValue()), java.lang.Integer.valueOf(Math.abs(i.intValue())))
      case l: java.lang.Long =>
        if (l.longValue() == Long.MinValue) return None
        (java.lang.Long.signum(l.longValue()), java.lang.Long.valueOf(Math.abs(l.longValue())))
      case f: java.lang.Float =>
        (Math.signum(f.doubleValue()).toInt, java.lang.Float.valueOf(Math.abs(f.floatValue())))
      case d: java.lang.Double =>
        (Math.signum(d.doubleValue()).toInt, java.lang.Double.valueOf(Math.abs(d.doubleValue())))
      case d: Decimal =>
        (d.toBigDecimal.signum, d.abs)
      case _ => return None
    }
    if (isLower && signum > 0) return None
    if (!isLower && signum < 0) return None

    exprToProto(Literal(absValue, bound.dataType), output).flatMap { exprProto =>
      if (exprProto.hasLiteral) Some(exprProto.getLiteral) else None
    }
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
  override def producedAttributes: AttributeSet = outputSet ++ AttributeSet(windowExpression)

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
